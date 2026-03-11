package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"global-osint-backend/internal/canonical"
	"global-osint-backend/internal/location"
	coremetrics "global-osint-backend/internal/metrics"
	"global-osint-backend/internal/migrate"
	"global-osint-backend/internal/packs/space"
)

const spaceJobName = "ingest-space"

func init() {
	jobRegistry[spaceJobName] = jobRunner{
		description: "Replay space fixtures into satellite entities, overpass events, and metrics.",
		run:         runIngestSpace,
	}
}

type spaceEntityRow struct {
	EntityID        string
	EntityType      string
	CanonicalName   string
	Status          string
	RiskBand        string
	PrimaryPlaceID  string
	SourceEntityKey string
	SourceSystem    string
	ValidFrom       time.Time
	RecordVersion   uint64
	UpdatedAt       time.Time
	Attrs           map[string]any
	Evidence        []canonical.Evidence
}

type spaceFixtureBundle struct {
	entities []spaceEntityRow
	result   space.Result
	registry []coremetrics.RegistryRecord
	bundleID string
}

func runIngestSpace(ctx context.Context) error {
	startedAt := time.Now().UTC().Truncate(time.Millisecond)
	runner := migrate.NewHTTPRunner(controlPlaneClickHouseURL())
	jobID := fmt.Sprintf("job:%s:%d", spaceJobName, startedAt.UnixMilli())
	bundleID := strings.TrimSpace(currentJobOptions(ctx).SourceID)
	if bundleID == "" {
		bundleID = "fixture:space"
	}

	recordFailure := func(err error, message string, stats map[string]any) error {
		if recordErr := recordJobRun(ctx, runner, jobID, spaceJobName, "failed", startedAt, time.Now().UTC().Truncate(time.Millisecond), message, stats); recordErr != nil {
			return fmt.Errorf("%w (job log failed: %v)", err, recordErr)
		}
		return err
	}

	bundle, err := buildSpaceFixtureBundle(startedAt, bundleID)
	if err != nil {
		return recordFailure(err, "build space fixture bundle", map[string]any{"stage": "bundle", "bundle_id": bundleID})
	}
	statements, err := spaceSQLStatements(bundle)
	if err != nil {
		return recordFailure(err, "build space sql", map[string]any{"stage": "sql", "bundle_id": bundle.bundleID})
	}
	for _, statement := range statements {
		if err := runner.ApplySQL(ctx, statement); err != nil {
			return recordFailure(err, "apply space sql", map[string]any{"stage": "apply", "bundle_id": bundle.bundleID})
		}
	}

	stats := map[string]any{
		"bundle_id":         bundle.bundleID,
		"entity_rows":       len(bundle.entities),
		"satellite_reports": len(bundle.result.Satellites),
		"track_points":      spaceTrackPointCount(bundle.result),
		"overpass_windows":  spaceWindowCount(bundle.result),
		"metric_rows":       len(bundle.result.Metrics),
		"registry_rows":     len(bundle.registry),
		"sql_statements":    len(statements),
		"metric_ids": []string{
			"conjunction_risk_score",
			"coverage_gap_hours",
			"maneuver_frequency_score",
			"orbital_decay_indicator",
			"overpass_density_score",
			"revisit_capability_index",
			"satellite_health_index",
		},
	}
	if err := recordJobRun(ctx, runner, jobID, spaceJobName, "success", startedAt, time.Now().UTC().Truncate(time.Millisecond), "replayed space pack fixtures", stats); err != nil {
		return err
	}
	return nil
}

func buildSpaceFixtureBundle(now time.Time, bundleID string) (spaceFixtureBundle, error) {
	result, err := analyzeSpaceFixtures()
	if err != nil {
		return spaceFixtureBundle{}, err
	}
	entities := make([]spaceEntityRow, 0, len(result.Satellites))
	for idx, satellite := range result.Satellites {
		primaryPlaceID := ""
		if len(satellite.Windows) > 0 {
			primaryPlaceID = satellite.Windows[0].PlaceID
		}
		canonicalName := strings.TrimSpace(satellite.Element.ObjectName)
		if canonicalName == "" {
			canonicalName = satellite.SatelliteID
		}
		entities = append(entities, spaceEntityRow{
			EntityID:        satellite.SatelliteID,
			EntityType:      "satellite",
			CanonicalName:   canonicalName,
			Status:          "observed",
			RiskBand:        spaceRiskBand(satellite),
			PrimaryPlaceID:  primaryPlaceID,
			SourceEntityKey: strings.TrimSpace(satellite.Element.NORADID),
			SourceSystem:    satellite.Element.SourceFormat,
			ValidFrom:       satellite.Element.Epoch.UTC(),
			RecordVersion:   uint64(idx + 1),
			UpdatedAt:       now.UTC(),
			Attrs: map[string]any{
				"norad_id":                 satellite.Element.NORADID,
				"international_designator": satellite.Element.InternationalDesignator,
				"classification":           satellite.Element.Classification,
				"pack":                     "space",
			},
			Evidence: append([]canonical.Evidence(nil), satellite.Element.Evidence...),
		})
	}
	return spaceFixtureBundle{entities: entities, result: result, registry: buildSpaceMetricRegistryRecords(now), bundleID: bundleID}, nil
}

func analyzeSpaceFixtures() (space.Result, error) {
	start := time.Date(2026, time.March, 10, 0, 0, 0, 0, time.UTC)
	tles, err := space.ParseTLEFeed([]byte(strings.Join([]string{
		"EQUATOR-OBS-1",
		"1 99991U 26069A   26069.00000000  .00000000  00000-0  00000-0 0  9991",
		"2 99991   0.0000  15.0000 0001000   0.0000   0.0000 15.00000000   101",
	}, "\n")))
	if err != nil {
		return space.Result{}, err
	}
	omm, err := space.ParseOMMFeed([]byte(`{
	  "metadata": {
	    "source_id": "fixture:space:catalog",
	    "supported_metrics": [
	      "overpass_density_score",
	      "conjunction_risk_score",
	      "revisit_capability_index",
	      "orbital_decay_indicator",
	      "maneuver_frequency_score",
	      "coverage_gap_hours",
	      "satellite_health_index"
	    ]
	  },
	  "objects": [
	    {
	      "OBJECT_NAME": "ISS-LIKE-1",
	      "OBJECT_ID": "1998-067A",
	      "NORAD_CAT_ID": "25544",
	      "CLASSIFICATION_TYPE": "U",
	      "EPOCH": "2026-03-10T00:00:00Z",
	      "MEAN_MOTION": "15.50000000",
	      "ECCENTRICITY": "0.0007000",
	      "INCLINATION": "51.6400",
	      "RA_OF_ASC_NODE": "120.0000",
	      "ARG_OF_PERICENTER": "80.0000",
	      "MEAN_ANOMALY": "30.0000",
	      "BSTAR": "0.00002100",
	      "MANEUVER_COUNT_30D": 3,
	      "THRUSTER_FIRINGS_7D": 2,
	      "HEALTH_SCORE": 91,
	      "BATTERY_MARGIN_PCT": 88,
	      "UPTIME_PCT": 97,
	      "ANOMALY_COUNT_30D": 1,
	      "TRANSMITTERS": [
	        {"callsign": "ISS-DL1", "mode": "s-band", "downlink_mhz": 2210.0, "status": "active"},
	        {"callsign": "ISS-DL2", "mode": "uhf", "downlink_mhz": 437.8, "status": "nominal"}
	      ]
	    }
	  ]
	}`))
	if err != nil {
		return space.Result{}, err
	}
	input := space.Input{
		Catalog: append(tles, omm...),
		Places: []space.Place{
			{
				PlaceID:  "plc:eq-belt",
				Name:     "Equatorial Belt",
				Center:   location.Coordinate{Lat: 0, Lon: 0},
				BBox:     &location.BBox{MinLat: -3, MinLon: -40, MaxLat: 3, MaxLon: 40},
				RadiusKM: 4500,
				Tags:     []string{"critical_infrastructure"},
			},
			{
				PlaceID:  "plc:polar-watch",
				Name:     "Polar Watch",
				Center:   location.Coordinate{Lat: 50, Lon: 0},
				BBox:     &location.BBox{MinLat: 45, MinLon: -180, MaxLat: 55, MaxLon: 180},
				RadiusKM: 2500,
				Tags:     []string{"maritime"},
			},
		},
		Conjunctions: []space.ConjunctionAdvisory{
			{AdvisoryID: "cdm:99991:1", SatelliteID: "sat:99991", SecondaryNORADID: "44001", ClosestApproachAt: start.Add(90 * time.Minute), MissDistanceKM: 5, Probability: 0.0008, SourceID: "fixture:cdm", Evidence: []canonical.Evidence{{Kind: "conjunction", Ref: "cdm:99991:1", Value: "high"}}},
			{AdvisoryID: "cdm:25544:1", SatelliteID: "sat:25544", SecondaryNORADID: "44002", ClosestApproachAt: start.Add(2 * time.Hour), MissDistanceKM: 30, Probability: 0.0002, SourceID: "fixture:cdm", Evidence: []canonical.Evidence{{Kind: "conjunction", Ref: "cdm:25544:1", Value: "low"}}},
		},
		Start: start,
		End:   start.Add(4 * time.Hour),
		Step:  5 * time.Minute,
	}
	return space.Analyze(input)
}

func buildSpaceMetricRegistryRecords(now time.Time) []coremetrics.RegistryRecord {
	now = now.UTC().Truncate(time.Millisecond)
	defs := []coremetrics.MetricDefinition{
		{MetricID: "conjunction_risk_score", MetricFamily: "space", SubjectGrain: "satellite", Unit: "score", ValueType: "ratio", RollupEngine: "AggregatingMergeTree", RollupRule: "weighted_avg", RefreshCadence: "1 HOUR", Description: "Collision-risk severity derived from conjunction advisories.", Formula: "avg(0.6*probability + 0.4*miss_distance_weight)", Windows: []string{"day"}},
		{MetricID: "coverage_gap_hours", MetricFamily: "space", SubjectGrain: "place", Unit: "hours", ValueType: "gauge", RollupEngine: "AggregatingMergeTree", RollupRule: "latest_daily_pack_score", RefreshCadence: "1 HOUR", Description: "Largest uncovered time gap between overpass windows in the evaluation horizon.", Formula: "max(gap_hours between adjacent overpass windows including horizon edges)", Windows: []string{"day"}},
		{MetricID: "maneuver_frequency_score", MetricFamily: "space", SubjectGrain: "satellite", Unit: "score", ValueType: "gauge", RollupEngine: "AggregatingMergeTree", RollupRule: "latest_daily_pack_score", RefreshCadence: "1 HOUR", Description: "Operational maneuver tempo derived from source-envelope thruster and maneuver counts.", Formula: "maneuver_count_30d * 18 + thruster_firings_7d * 4 capped at 100", Windows: []string{"day"}},
		{MetricID: "orbital_decay_indicator", MetricFamily: "space", SubjectGrain: "satellite", Unit: "score", ValueType: "gauge", RollupEngine: "AggregatingMergeTree", RollupRule: "latest_daily_pack_score", RefreshCadence: "1 HOUR", Description: "Decay pressure derived from BSTAR drag, altitude, and mean-motion regime.", Formula: "drag_component + altitude_component + mean_motion_component capped at 100", Windows: []string{"day"}},
		{MetricID: "overpass_density_score", MetricFamily: "space", SubjectGrain: "place", Unit: "passes_per_day", ValueType: "gauge", RollupEngine: "AggregatingMergeTree", RollupRule: "sum", RefreshCadence: "1 HOUR", Description: "Normalized satellite overpass frequency per place.", Formula: "pass_count * 24 / horizon_hours", Windows: []string{"day"}},
		{MetricID: "revisit_capability_index", MetricFamily: "space", SubjectGrain: "place", Unit: "score", ValueType: "gauge", RollupEngine: "AggregatingMergeTree", RollupRule: "latest_daily_pack_score", RefreshCadence: "1 HOUR", Description: "Revisit capability index derived from largest coverage gap and unique satellites serving a place.", Formula: "(1 - coverage_gap_hours / horizon_hours) * 70 + min(satellite_count * 15, 30)", Windows: []string{"day"}},
		{MetricID: "satellite_health_index", MetricFamily: "space", SubjectGrain: "satellite", Unit: "score", ValueType: "gauge", RollupEngine: "AggregatingMergeTree", RollupRule: "latest_daily_pack_score", RefreshCadence: "1 HOUR", Description: "Satellite health index derived from declared health, power margin, uptime, anomalies, and transmitter status.", Formula: "declared_or_derived_health - anomaly_penalty - decay_penalty + transmitter_bonus capped at 100", Windows: []string{"day"}},
	}
	legacyAliases := map[string]string{
		"conjunction_risk": "conjunction_risk_score",
		"overpass_density": "overpass_density_score",
	}
	records := make([]coremetrics.RegistryRecord, 0, len(defs)+len(legacyAliases))
	index := make(map[string]coremetrics.RegistryRecord, len(defs))
	for idx, def := range defs {
		attrs := map[string]any{
			"description":     def.Description,
			"formula":         def.Formula,
			"refresh_cadence": def.RefreshCadence,
			"window_grains":   append([]string(nil), def.Windows...),
			"domain_family":   "space",
		}
		for legacyID, canonicalID := range legacyAliases {
			if canonicalID == def.MetricID {
				attrs["legacy_metric_ids"] = []string{legacyID}
			}
		}
		record := coremetrics.RegistryRecord{
			MetricID:           def.MetricID,
			MetricFamily:       def.MetricFamily,
			SubjectGrain:       def.SubjectGrain,
			Unit:               def.Unit,
			ValueType:          def.ValueType,
			RollupEngine:       def.RollupEngine,
			RollupRule:         def.RollupRule,
			Attrs:              attrs,
			Evidence:           []canonical.Evidence{{Kind: "metric_spec", Ref: def.MetricID, Value: def.Formula}},
			SchemaVersion:      coremetrics.SchemaVersion,
			RecordVersion:      uint64(idx + 1),
			APIContractVersion: coremetrics.APIContractVersion,
			Enabled:            true,
			UpdatedAt:          now,
		}
		records = append(records, record)
		index[def.MetricID] = record
	}
	aliasIDs := make([]string, 0, len(legacyAliases))
	for legacyID := range legacyAliases {
		aliasIDs = append(aliasIDs, legacyID)
	}
	sort.Strings(aliasIDs)
	for idx, legacyID := range aliasIDs {
		canonicalID := legacyAliases[legacyID]
		base := index[canonicalID]
		attrs := copyMetricAttrs(base.Attrs)
		attrs["canonical_metric_id"] = canonicalID
		attrs["compatibility_alias"] = true
		attrs["deprecated"] = true
		attrs["replacement_metric_id"] = canonicalID
		records = append(records, coremetrics.RegistryRecord{
			MetricID:           legacyID,
			MetricFamily:       base.MetricFamily,
			SubjectGrain:       base.SubjectGrain,
			Unit:               base.Unit,
			ValueType:          base.ValueType,
			RollupEngine:       base.RollupEngine,
			RollupRule:         base.RollupRule,
			Attrs:              attrs,
			Evidence:           []canonical.Evidence{{Kind: "metric_alias", Ref: legacyID, Value: canonicalID, Attrs: map[string]any{"pack": "space"}}},
			SchemaVersion:      coremetrics.SchemaVersion,
			RecordVersion:      uint64(len(defs) + idx + 1),
			APIContractVersion: coremetrics.APIContractVersion,
			Enabled:            true,
			UpdatedAt:          now,
		})
	}
	sort.Slice(records, func(i, j int) bool { return records[i].MetricID < records[j].MetricID })
	return records
}

func copyMetricAttrs(input map[string]any) map[string]any {
	output := make(map[string]any, len(input)+4)
	for key, value := range input {
		output[key] = value
	}
	return output
}

func spaceSQLStatements(bundle spaceFixtureBundle) ([]string, error) {
	statements := make([]string, 0, 5)
	if sql, err := insertSpaceMetricRegistrySQL(bundle.registry); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	if sql, err := insertSpaceEntitiesSQL(bundle.entities); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	if sql, err := insertSpaceTrackPointsSQL(bundle.result.Satellites); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	if sql, err := insertSpaceEventsSQL(bundle.result.Satellites); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	metricSQL, err := coremetrics.UpsertMaterializationSQL(spaceMetricContributions(bundle.result.Metrics), bundle.result.End)
	if err != nil {
		return nil, err
	}
	statements = append(statements, metricSQL...)
	return statements, nil
}

func spaceMetricContributions(rows []space.Metric) []coremetrics.Contribution {
	contributions := make([]coremetrics.Contribution, 0, len(rows))
	for _, row := range rows {
		placeID := ""
		if row.SubjectType == "place" {
			placeID = row.SubjectID
		}
		contributions = append(contributions, coremetrics.Contribution{
			ContributionID:     fmt.Sprintf("mc:%s:%s:%s:%d", row.MetricID, row.SubjectType, row.SubjectID, row.WindowStart.UTC().Unix()),
			MetricID:           row.MetricID,
			SubjectGrain:       row.SubjectType,
			SubjectID:          row.SubjectID,
			SourceRecordType:   "space_metric",
			SourceRecordID:     row.SubjectID,
			PlaceID:            placeID,
			WindowGrain:        "day",
			WindowStart:        row.WindowStart,
			WindowEnd:          row.WindowEnd,
			ContributionType:   "derived_metric",
			ContributionValue:  row.Value,
			ContributionWeight: 1,
			SchemaVersion:      row.SchemaVersion,
			Attrs:              row.Attrs,
			Evidence:           row.Evidence,
		})
	}
	return contributions
}

func insertSpaceMetricRegistrySQL(rows []coremetrics.RegistryRecord) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO meta.metric_registry (metric_id, metric_family, subject_grain, unit, value_type, rollup_engine, rollup_rule, attrs, evidence, schema_version, record_version, api_contract_version, enabled, updated_at) VALUES ")
	for idx, row := range rows {
		if idx > 0 {
			b.WriteString(",")
		}
		attrs, err := marshalJSONString(row.Attrs)
		if err != nil {
			return "", err
		}
		evidence, err := marshalJSONString(row.Evidence)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,%d,%d,%s)", sqlString(row.MetricID), sqlString(row.MetricFamily), sqlString(row.SubjectGrain), sqlString(row.Unit), sqlString(row.ValueType), sqlString(row.RollupEngine), sqlString(row.RollupRule), sqlString(attrs), sqlString(evidence), row.SchemaVersion, row.RecordVersion, row.APIContractVersion, spaceBoolAsInt(row.Enabled), sqlTime(row.UpdatedAt))
	}
	return b.String(), nil
}

func insertSpaceEntitiesSQL(rows []spaceEntityRow) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO silver.dim_entity (entity_id, entity_type, canonical_name, status, risk_band, primary_place_id, source_entity_key, source_system, valid_from, valid_to, schema_version, record_version, api_contract_version, updated_at, attrs, evidence) VALUES ")
	for idx, row := range rows {
		if idx > 0 {
			b.WriteString(",")
		}
		attrs, err := marshalJSONString(row.Attrs)
		if err != nil {
			return "", err
		}
		evidence, err := marshalJSONString(row.Evidence)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "(%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,%d,%d,%d,%s,%s,%s)", sqlString(row.EntityID), sqlString(row.EntityType), sqlString(row.CanonicalName), sqlString(row.Status), sqlString(row.RiskBand), nullableSQLString(row.PrimaryPlaceID), sqlString(row.SourceEntityKey), sqlString(row.SourceSystem), sqlTime(row.ValidFrom), canonical.SchemaVersion, row.RecordVersion, canonical.SchemaVersion, sqlTime(row.UpdatedAt), sqlString(attrs), sqlString(evidence))
	}
	return b.String(), nil
}

func insertSpaceTrackPointsSQL(reports []space.SatelliteReport) (string, error) {
	pointCount := 0
	for _, report := range reports {
		pointCount += len(report.Track)
	}
	if pointCount == 0 {
		return "", nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO silver.fact_track_point (track_point_id, track_id, source_id, track_type, entity_id, place_id, observed_at, latitude, longitude, altitude_m, speed_kph, course_deg, schema_version, attrs, evidence) VALUES ")
	idx := 0
	for _, report := range reports {
		for _, point := range report.Track {
			if idx > 0 {
				b.WriteString(",")
			}
			attrs, err := marshalJSONString(point.Attrs)
			if err != nil {
				return "", err
			}
			evidence, err := marshalJSONString(point.Evidence)
			if err != nil {
				return "", err
			}
			fmt.Fprintf(&b, "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,NULL,%d,%s,%s)", sqlString(point.PointID), sqlString("trk:"+report.SatelliteID), sqlString("fixture:space:catalog"), sqlString("space_ground_track"), sqlString(report.SatelliteID), sqlString(""), sqlTime(point.ObservedAt), formatFloat(point.Latitude), formatFloat(point.Longitude), formatFloat(point.AltitudeKM*1000), point.SchemaVersion, sqlString(attrs), sqlString(evidence))
			idx++
		}
	}
	return b.String(), nil
}

func insertSpaceEventsSQL(reports []space.SatelliteReport) (string, error) {
	rows := 0
	for _, report := range reports {
		rows += len(report.Windows) + len(report.Conjunctions)
	}
	if rows == 0 {
		return "", nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO silver.fact_event (event_id, source_id, event_type, event_subtype, place_id, parent_place_chain, starts_at, ends_at, status, confidence_band, impact_score, schema_version, attrs, evidence) VALUES ")
	idx := 0
	for _, report := range reports {
		for _, window := range report.Windows {
			if idx > 0 {
				b.WriteString(",")
			}
			attrs, err := marshalJSONString(window.Attrs)
			if err != nil {
				return "", err
			}
			evidence, err := marshalJSONString(window.Evidence)
			if err != nil {
				return "", err
			}
			fmt.Fprintf(&b, "(%s,%s,%s,%s,%s,[],%s,%s,%s,%s,%s,%d,%s,%s)", sqlString(window.WindowID), sqlString("fixture:space:analysis"), sqlString("space_overpass"), sqlString("ground_track_window"), sqlString(window.PlaceID), sqlTime(window.StartedAt), sqlTime(window.EndedAt), sqlString("observed"), sqlString("high"), formatFloat(window.Duration.Hours()), window.SchemaVersion, sqlString(attrs), sqlString(evidence))
			idx++
		}
		for _, advisory := range report.Conjunctions {
			if idx > 0 {
				b.WriteString(",")
			}
			attrs, err := marshalJSONString(map[string]any{"secondary_norad_id": advisory.SecondaryNORADID})
			if err != nil {
				return "", err
			}
			evidence, err := marshalJSONString(advisory.Evidence)
			if err != nil {
				return "", err
			}
			fmt.Fprintf(&b, "(%s,%s,%s,%s,%s,[],%s,%s,%s,%s,%s,%d,%s,%s)", sqlString(advisory.AdvisoryID), sqlString(advisory.SourceID), sqlString("space_conjunction_alert"), sqlString("conjunction"), sqlString(""), sqlTime(advisory.ClosestApproachAt), sqlTime(advisory.ClosestApproachAt), sqlString("observed"), sqlString("high"), formatFloat(advisory.Probability), space.SchemaVersion, sqlString(attrs), sqlString(evidence))
			idx++
		}
	}
	return b.String(), nil
}

func spaceTrackPointCount(result space.Result) int {
	total := 0
	for _, report := range result.Satellites {
		total += len(report.Track)
	}
	return total
}

func spaceWindowCount(result space.Result) int {
	total := 0
	for _, report := range result.Satellites {
		total += len(report.Windows)
	}
	return total
}

func spaceRiskBand(report space.SatelliteReport) string {
	for _, metric := range report.Conjunctions {
		if metric.Probability >= 0.0005 || metric.MissDistanceKM <= 10 {
			return "high"
		}
	}
	if len(report.Windows) > 0 {
		return "watch"
	}
	return "low"
}

func spaceBoolAsInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
