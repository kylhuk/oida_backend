package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"global-osint-backend/internal/migrate"
	"global-osint-backend/internal/place"
)

const defaultControlPlaneClickHouseURL = "http://clickhouse:8123"

func init() {
	jobRegistry["place-build"] = jobRunner{
		description: "Materialize place graph tables and reverse geocoder.",
		run:         runPlaceBuild,
	}
}

func runPlaceBuild(ctx context.Context) error {
	startedAt := time.Now().UTC().Truncate(time.Millisecond)
	runner := migrate.NewHTTPRunner(controlPlaneClickHouseURL())
	jobID := fmt.Sprintf("job:place-build:%d", startedAt.UnixMilli())

	recordFailure := func(err error, message string, stats map[string]any) error {
		if recordErr := recordJobRun(ctx, runner, jobID, "place-build", "failed", startedAt, time.Now().UTC().Truncate(time.Millisecond), message, stats); recordErr != nil {
			return fmt.Errorf("%w (job log failed: %v)", err, recordErr)
		}
		return err
	}

	bundle, err := place.BuildBundle(startedAt)
	if err != nil {
		return recordFailure(err, "build place bundle", map[string]any{"stage": "bundle"})
	}

	h3Coverage, err := fetchH3Coverage(ctx, runner, bundle.Polygons)
	if err != nil {
		return recordFailure(err, "compute h3 coverage", map[string]any{"stage": "h3"})
	}
	bundle.ApplyH3Coverage(h3Coverage)

	if err := ensurePlaceBuildArtifacts(ctx, runner); err != nil {
		return recordFailure(err, "ensure place build artifacts", map[string]any{"stage": "ddl"})
	}
	if err := truncatePlaceBuildTargets(ctx, runner); err != nil {
		return recordFailure(err, "truncate place build targets", map[string]any{"stage": "truncate"})
	}
	if err := insertPlaces(ctx, runner, bundle.Places); err != nil {
		return recordFailure(err, "insert dim_place", map[string]any{"stage": "dim_place"})
	}
	if err := insertHierarchies(ctx, runner, bundle.Hierarchies); err != nil {
		return recordFailure(err, "insert place_hierarchy", map[string]any{"stage": "place_hierarchy"})
	}
	if err := insertPolygons(ctx, runner, bundle.Polygons); err != nil {
		return recordFailure(err, "insert place_polygon", map[string]any{"stage": "place_polygon"})
	}
	if err := insertPolygonDictionarySource(ctx, runner, bundle.Polygons); err != nil {
		return recordFailure(err, "insert polygon dictionary source", map[string]any{"stage": "dictionary_source"})
	}
	if err := recreateReverseGeocodeDictionary(ctx, runner); err != nil {
		return recordFailure(err, "create reverse geocode dictionary", map[string]any{"stage": "dictionary"})
	}
	if err := verifyReverseFixtures(ctx, runner, bundle.ReverseFixtures); err != nil {
		return recordFailure(err, "verify reverse geocode fixtures", map[string]any{"stage": "fixtures"})
	}

	stats := map[string]any{
		"places":                  len(bundle.Places),
		"hierarchy_edges":         len(bundle.Hierarchies),
		"polygons":                len(bundle.Polygons),
		"dictionary":              place.ReverseGeocodeDictionaryName,
		"dictionary_source_table": place.PolygonDictionarySourceTable,
	}
	if err := recordJobRun(ctx, runner, jobID, "place-build", "success", startedAt, time.Now().UTC().Truncate(time.Millisecond), "materialized place graph", stats); err != nil {
		return err
	}
	return nil
}

func controlPlaneClickHouseURL() string {
	if value := strings.TrimSpace(os.Getenv("CLICKHOUSE_HTTP_URL")); value != "" {
		return value
	}
	return defaultControlPlaneClickHouseURL
}

func ensurePlaceBuildArtifacts(ctx context.Context, runner *migrate.HTTPRunner) error {
	return runner.ApplySQL(ctx, `
CREATE TABLE IF NOT EXISTS silver.place_polygon_dictionary_source
(
    polygon_key Array(Array(Array(Tuple(Float64, Float64)))),
    place_id String,
    canonical_name String,
    admin_level UInt8,
    place_type LowCardinality(String),
    country_code LowCardinality(String),
    polygon_id String,
    record_version UInt64,
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (place_id, polygon_id)
`)
}

func truncatePlaceBuildTargets(ctx context.Context, runner *migrate.HTTPRunner) error {
	return runner.ApplySQL(ctx, strings.Join([]string{
		"DROP DICTIONARY IF EXISTS " + place.ReverseGeocodeDictionaryName,
		"TRUNCATE TABLE silver.dim_place",
		"TRUNCATE TABLE silver.place_hierarchy",
		"TRUNCATE TABLE silver.place_polygon",
		"TRUNCATE TABLE " + place.PolygonDictionarySourceTable,
	}, ";\n"))
}

func fetchH3Coverage(ctx context.Context, runner *migrate.HTTPRunner, polygons []place.PolygonRow) (map[string][]string, error) {
	coverage := make(map[string][]string, len(polygons))
	for _, polygon := range polygons {
		query := fmt.Sprintf("SELECT arrayMap(x -> h3ToString(x), arraySort(h3PolygonToCells(%s, 7))) AS cells FORMAT JSONEachRow", polygon.Geometry.SQLLiteral())
		out, err := runner.Query(ctx, query)
		if err != nil {
			if !strings.Contains(err.Error(), "h3PolygonToCells") {
				return nil, err
			}
			centroidLon := (polygon.BBoxMinLon + polygon.BBoxMaxLon) / 2
			centroidLat := (polygon.BBoxMinLat + polygon.BBoxMaxLat) / 2
			fallbackQuery := fmt.Sprintf("SELECT h3ToString(geoToH3(toFloat64(%s), toFloat64(%s), 7)) AS cell FORMAT JSONEachRow", formatFloat(centroidLon), formatFloat(centroidLat))
			fallbackOut, fallbackErr := runner.Query(ctx, fallbackQuery)
			if fallbackErr != nil {
				return nil, err
			}
			var fallback struct {
				Cell string `json:"cell"`
			}
			if err := json.Unmarshal([]byte(strings.TrimSpace(fallbackOut)), &fallback); err != nil {
				return nil, err
			}
			if fallback.Cell == "" {
				return nil, err
			}
			coverage[polygon.PlaceID] = []string{fallback.Cell}
			continue
		}
		var payload struct {
			Cells []string `json:"cells"`
		}
		if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &payload); err != nil {
			return nil, err
		}
		coverage[polygon.PlaceID] = payload.Cells
	}
	return coverage, nil
}

func insertPlaces(ctx context.Context, runner *migrate.HTTPRunner, rows []place.PlaceRow) error {
	if len(rows) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO silver.dim_place (place_id, parent_place_id, canonical_name, place_type, admin_level, country_code, continent_code, source_place_key, source_system, status, centroid_lat, centroid_lon, bbox_min_lat, bbox_min_lon, bbox_max_lat, bbox_max_lon, valid_from, valid_to, schema_version, record_version, api_contract_version, updated_at, attrs, evidence) VALUES ")
	for idx, row := range rows {
		if idx > 0 {
			b.WriteString(",")
		}
		attrs, err := marshalJSONString(row.Attrs)
		if err != nil {
			return err
		}
		evidence, err := marshalJSONString(row.Evidence)
		if err != nil {
			return err
		}
		fmt.Fprintf(&b, "(%s,%s,%s,%s,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,%d,%d,%d,%s,%s,%s)",
			sqlString(row.PlaceID),
			nullableSQLString(row.ParentPlaceID),
			sqlString(row.CanonicalName),
			sqlString(row.PlaceType),
			row.AdminLevel,
			sqlString(row.CountryCode),
			sqlString(row.ContinentCode),
			sqlString(row.SourcePlaceKey),
			sqlString(row.SourceSystem),
			sqlString(row.Status),
			formatFloat(row.CentroidLat),
			formatFloat(row.CentroidLon),
			formatFloat(row.BBoxMinLat),
			formatFloat(row.BBoxMinLon),
			formatFloat(row.BBoxMaxLat),
			formatFloat(row.BBoxMaxLon),
			sqlTime(row.ValidFrom),
			row.SchemaVersion,
			row.RecordVersion,
			row.APIContractVersion,
			sqlTime(row.UpdatedAt),
			sqlString(attrs),
			sqlString(evidence),
		)
	}
	return runner.ApplySQL(ctx, b.String())
}

func insertHierarchies(ctx context.Context, runner *migrate.HTTPRunner, rows []place.HierarchyRow) error {
	if len(rows) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO silver.place_hierarchy (edge_id, ancestor_place_id, descendant_place_id, relationship_type, depth, is_direct, path_source, valid_from, valid_to, schema_version, record_version, updated_at, attrs, evidence) VALUES ")
	for idx, row := range rows {
		if idx > 0 {
			b.WriteString(",")
		}
		attrs, err := marshalJSONString(row.Attrs)
		if err != nil {
			return err
		}
		evidence, err := marshalJSONString(row.Evidence)
		if err != nil {
			return err
		}
		fmt.Fprintf(&b, "(%s,%s,%s,%s,%d,%d,%s,%s,NULL,%d,%d,%s,%s,%s)",
			sqlString(row.EdgeID),
			sqlString(row.AncestorPlaceID),
			sqlString(row.DescendantPlaceID),
			sqlString(row.RelationshipType),
			row.Depth,
			row.IsDirect,
			sqlString(row.PathSource),
			sqlTime(row.ValidFrom),
			row.SchemaVersion,
			row.RecordVersion,
			sqlTime(row.UpdatedAt),
			sqlString(attrs),
			sqlString(evidence),
		)
	}
	return runner.ApplySQL(ctx, b.String())
}

func insertPolygons(ctx context.Context, runner *migrate.HTTPRunner, rows []place.PolygonRow) error {
	if len(rows) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO silver.place_polygon (polygon_id, place_id, polygon_role, geometry_format, source_system, ring_count, point_count, bbox_min_lat, bbox_min_lon, bbox_max_lat, bbox_max_lon, valid_from, valid_to, schema_version, record_version, updated_at, geometry, attrs, evidence) VALUES ")
	for idx, row := range rows {
		if idx > 0 {
			b.WriteString(",")
		}
		geometry, err := row.Geometry.JSONString()
		if err != nil {
			return err
		}
		attrs, err := marshalJSONString(row.Attrs)
		if err != nil {
			return err
		}
		evidence, err := marshalJSONString(row.Evidence)
		if err != nil {
			return err
		}
		fmt.Fprintf(&b, "(%s,%s,%s,%s,%s,%d,%d,%s,%s,%s,%s,%s,NULL,%d,%d,%s,%s,%s,%s)",
			sqlString(row.PolygonID),
			sqlString(row.PlaceID),
			sqlString(row.PolygonRole),
			sqlString(row.GeometryFormat),
			sqlString(row.SourceSystem),
			row.RingCount,
			row.PointCount,
			formatFloat(row.BBoxMinLat),
			formatFloat(row.BBoxMinLon),
			formatFloat(row.BBoxMaxLat),
			formatFloat(row.BBoxMaxLon),
			sqlTime(row.ValidFrom),
			row.SchemaVersion,
			row.RecordVersion,
			sqlTime(row.UpdatedAt),
			sqlString(geometry),
			sqlString(attrs),
			sqlString(evidence),
		)
	}
	return runner.ApplySQL(ctx, b.String())
}

func insertPolygonDictionarySource(ctx context.Context, runner *migrate.HTTPRunner, rows []place.PolygonRow) error {
	if len(rows) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(place.PolygonDictionarySourceTable)
	b.WriteString(" (polygon_key, place_id, canonical_name, admin_level, place_type, country_code, polygon_id, record_version, updated_at) VALUES ")
	for idx, row := range rows {
		if idx > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "(%s,%s,%s,%d,%s,%s,%s,%d,%s)",
			row.Geometry.SQLLiteral(),
			sqlString(row.PlaceID),
			sqlString(row.CanonicalName),
			row.AdminLevel,
			sqlString(row.PlaceType),
			sqlString(row.CountryCode),
			sqlString(row.PolygonID),
			row.RecordVersion,
			sqlTime(row.UpdatedAt),
		)
	}
	return runner.ApplySQL(ctx, b.String())
}

func recreateReverseGeocodeDictionary(ctx context.Context, runner *migrate.HTTPRunner) error {
	create := fmt.Sprintf(`
CREATE DICTIONARY %s
(
    polygon_key Array(Array(Array(Tuple(Float64, Float64)))),
    place_id String,
    canonical_name String,
    admin_level UInt8,
    place_type String,
    country_code String
)
PRIMARY KEY polygon_key
SOURCE(CLICKHOUSE(DB 'silver' TABLE 'place_polygon_dictionary_source'))
LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
LIFETIME(0)
`, place.ReverseGeocodeDictionaryName)
	return runner.ApplySQL(ctx, create)
}

func verifyReverseFixtures(ctx context.Context, runner *migrate.HTTPRunner, fixtures []place.ReverseFixture) error {
	for _, fixture := range fixtures {
		query := fmt.Sprintf("SELECT dictGetString('%s', 'place_id', tuple(%s, %s)) AS place_id, dictGetUInt8('%s', 'admin_level', tuple(%s, %s)) AS admin_level FORMAT JSONEachRow",
			place.ReverseGeocodeDictionaryName,
			formatFloat(fixture.Lon),
			formatFloat(fixture.Lat),
			place.ReverseGeocodeDictionaryName,
			formatFloat(fixture.Lon),
			formatFloat(fixture.Lat),
		)
		out, err := runner.Query(ctx, query)
		if err != nil {
			return err
		}
		var payload struct {
			PlaceID    string `json:"place_id"`
			AdminLevel uint8  `json:"admin_level"`
		}
		if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &payload); err != nil {
			return err
		}
		if payload.PlaceID != fixture.ExpectedPlaceID {
			return fmt.Errorf("fixture %s resolved to %s, want %s", fixture.Name, payload.PlaceID, fixture.ExpectedPlaceID)
		}
		if payload.AdminLevel != fixture.ExpectedDepth {
			return fmt.Errorf("fixture %s resolved depth %d, want %d", fixture.Name, payload.AdminLevel, fixture.ExpectedDepth)
		}
	}
	return nil
}

func recordJobRun(ctx context.Context, runner *migrate.HTTPRunner, jobID, jobType, status string, startedAt, finishedAt time.Time, message string, stats map[string]any) error {
	statsJSON, err := marshalJSONString(stats)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("INSERT INTO ops.job_run (job_id, job_type, status, started_at, finished_at, message, stats) VALUES (%s,%s,%s,%s,%s,%s,%s)",
		sqlString(jobID),
		sqlString(jobType),
		sqlString(status),
		sqlTime(startedAt),
		sqlTime(finishedAt),
		sqlString(message),
		sqlString(statsJSON),
	)
	return runner.ApplySQL(ctx, query)
}

func marshalJSONString(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func sqlString(v string) string {
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}

func nullableSQLString(v string) string {
	if v == "" {
		return "NULL"
	}
	return sqlString(v)
}

func sqlTime(v time.Time) string {
	return sqlString(v.UTC().Format("2006-01-02 15:04:05.000"))
}

func formatFloat(v float64) string {
	formatted := fmt.Sprintf("%.8f", v)
	formatted = strings.TrimRight(formatted, "0")
	formatted = strings.TrimRight(formatted, ".")
	if formatted == "" || formatted == "-" {
		return "0"
	}
	return formatted
}
