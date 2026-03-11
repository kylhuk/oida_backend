package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"global-osint-backend/internal/canonical"
	coremetrics "global-osint-backend/internal/metrics"
	"global-osint-backend/internal/migrate"
	"global-osint-backend/internal/packs/aviation"
)

const aviationJobName = "ingest-aviation"

func init() {
	jobRegistry[aviationJobName] = jobRunner{
		description: "Replay aviation fixtures into aircraft entities, tracks, events, and metrics.",
		run:         runIngestAviation,
	}
}

func runIngestAviation(ctx context.Context) error {
	startedAt := time.Now().UTC().Truncate(time.Millisecond)
	runner := migrate.NewHTTPRunner(controlPlaneClickHouseURL())
	jobID := fmt.Sprintf("job:%s:%d", aviationJobName, startedAt.UnixMilli())
	sourceID := strings.TrimSpace(currentJobOptions(ctx).SourceID)
	if sourceID == "" {
		sourceID = aviation.DefaultFixtureSourceID
	}

	recordFailure := func(err error, message string, stats map[string]any) error {
		if recordErr := recordJobRun(ctx, runner, jobID, aviationJobName, "failed", startedAt, time.Now().UTC().Truncate(time.Millisecond), message, stats); recordErr != nil {
			return fmt.Errorf("%w (job log failed: %v)", err, recordErr)
		}
		return err
	}

	input, err := aviation.LoadFixtureBundle(sourceID)
	if err != nil {
		return recordFailure(err, "load aviation fixture bundle", map[string]any{"stage": "load_bundle", "source_id": sourceID})
	}
	bundle, err := aviation.Analyze(input, aviation.Options{Now: func() time.Time { return startedAt }})
	if err != nil {
		return recordFailure(err, "analyze aviation fixture bundle", map[string]any{"stage": "analyze", "source_id": sourceID})
	}
	statements, err := aviationSQLStatements(startedAt, bundle)
	if err != nil {
		return recordFailure(err, "build aviation sql", map[string]any{"stage": "sql", "source_id": sourceID})
	}
	for _, statement := range statements {
		if err := runner.ApplySQL(ctx, statement); err != nil {
			return recordFailure(err, "apply aviation sql", map[string]any{"stage": "apply", "source_id": sourceID})
		}
	}

	stats := map[string]any{
		"source_id":            sourceID,
		"aircraft_entities":    bundle.Stats.AircraftEntities,
		"track_points":         bundle.Stats.TrackPoints,
		"flight_segments":      bundle.Stats.FlightSegments,
		"transponder_gaps":     bundle.Stats.TransponderGaps,
		"airport_interactions": bundle.Stats.AirportInteractions,
		"metric_snapshots":     bundle.Stats.Metrics,
		"metric_ids": []string{
			aviation.MetricAltitudeVarianceScore,
			aviation.MetricDiversionRate,
			aviation.MetricHoldPatternFrequency,
			aviation.MetricMilitaryAircraftProximity,
			aviation.MetricMilitaryLikelihood,
			aviation.MetricRouteIrregularity,
			aviation.MetricSquawkChangeRate,
			aviation.MetricTransponderGapHours,
		},
		"sql_statements": len(statements),
	}
	if err := recordJobRun(ctx, runner, jobID, aviationJobName, "success", startedAt, time.Now().UTC().Truncate(time.Millisecond), "replayed aviation pack fixtures", stats); err != nil {
		return err
	}
	return nil
}

func aviationSQLStatements(now time.Time, bundle aviation.Bundle) ([]string, error) {
	statements := make([]string, 0, 6)
	if sql, err := insertAviationMetricRegistrySQL(now); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	if sql, err := insertAviationEntitiesSQL(now, bundle.Aircraft); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	if sql, err := insertAviationTrackPointsSQL(bundle.TrackPoints); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	if sql, err := insertAviationSegmentsSQL(bundle.Segments); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	if sql, err := insertAviationEventsSQL(bundle.GapEvents, bundle.AirportInteractions); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	metricSQL, err := coremetrics.UpsertMaterializationSQL(aviationMetricContributions(bundle.Metrics), now)
	if err != nil {
		return nil, err
	}
	statements = append(statements, metricSQL...)
	return statements, nil
}

func aviationMetricContributions(rows []aviation.MetricSnapshot) []coremetrics.Contribution {
	contributions := make([]coremetrics.Contribution, 0, len(rows))
	for _, row := range rows {
		contributions = append(contributions, coremetrics.Contribution{
			ContributionID:     fmt.Sprintf("mc:%s:%s:%s:%d", row.MetricID, row.SubjectGrain, row.SubjectID, row.WindowStart.UTC().Unix()),
			MetricID:           row.MetricID,
			SubjectGrain:       row.SubjectGrain,
			SubjectID:          row.SubjectID,
			SourceRecordType:   "aviation_metric",
			SourceRecordID:     row.SnapshotID,
			PlaceID:            row.PlaceID,
			WindowGrain:        row.WindowGrain,
			WindowStart:        row.WindowStart,
			WindowEnd:          row.WindowEnd,
			ContributionType:   "derived_metric",
			ContributionValue:  row.MetricValue,
			ContributionWeight: 1,
			SchemaVersion:      aviation.SchemaVersion,
			Attrs:              row.Attrs,
			Evidence:           row.Evidence,
		})
	}
	return contributions
}

func insertAviationMetricRegistrySQL(now time.Time) (string, error) {
	definitions := aviation.MetricDefinitions()
	if len(definitions) == 0 {
		return "", nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO meta.metric_registry (metric_id, metric_family, subject_grain, unit, value_type, rollup_engine, rollup_rule, attrs, evidence, schema_version, record_version, api_contract_version, enabled, updated_at) VALUES ")
	for idx, definition := range definitions {
		if idx > 0 {
			b.WriteString(",")
		}
		attrs, err := marshalJSONString(map[string]any{
			"description": definition.Description,
			"formula":     definition.Formula,
			"pack":        "aviation",
		})
		if err != nil {
			return "", err
		}
		evidence, err := marshalJSONString([]canonical.Evidence{{Kind: "metric_spec", Ref: definition.MetricID, Value: definition.Formula}})
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,%d,%d,%s)",
			sqlString(definition.MetricID),
			sqlString(definition.MetricFamily),
			sqlString(definition.SubjectGrain),
			sqlString(definition.Unit),
			sqlString(definition.ValueType),
			sqlString(definition.RollupEngine),
			sqlString(definition.RollupRule),
			sqlString(attrs),
			sqlString(evidence),
			aviation.SchemaVersion,
			now.UnixMilli()+int64(idx+1),
			aviation.SchemaVersion,
			1,
			sqlTime(now),
		)
	}
	return b.String(), nil
}

func insertAviationEntitiesSQL(now time.Time, rows []aviation.AircraftEntity) (string, error) {
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
		canonicalName := nonEmptyString(row.Registration, row.Callsign, strings.ToUpper(row.ICAO24))
		sourceEntityKey := nonEmptyString(row.Registration, row.ICAO24)
		fmt.Fprintf(&b, "(%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,%d,%d,%d,%s,%s,%s)",
			sqlString(row.EntityID),
			sqlString("aircraft"),
			sqlString(canonicalName),
			sqlString(row.MilitaryStatus),
			sqlString(row.RiskBand),
			nullableSQLString(row.PrimaryPlaceID),
			sqlString(sourceEntityKey),
			sqlString(row.SourceSystem),
			sqlTime(row.ObservedFrom),
			aviation.SchemaVersion,
			now.UnixMilli()+int64(idx+1),
			aviation.SchemaVersion,
			sqlTime(now),
			sqlString(attrs),
			sqlString(evidence),
		)
	}
	return b.String(), nil
}

func insertAviationTrackPointsSQL(rows []aviation.TrackPoint) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO silver.fact_track_point (track_point_id, track_id, source_id, track_type, entity_id, place_id, observed_at, latitude, longitude, altitude_m, speed_kph, course_deg, schema_version, attrs, evidence) VALUES ")
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
		fmt.Fprintf(&b, "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%s)",
			sqlString(row.TrackPointID),
			sqlString(row.TrackID),
			sqlString(row.SourceID),
			sqlString(aviation.TrackType),
			sqlString(row.EntityID),
			sqlString(row.PlaceID),
			sqlTime(row.ObservedAt),
			formatFloat(row.Latitude),
			formatFloat(row.Longitude),
			nullableSQLFloat(row.AltitudeM),
			nullableSQLFloat(row.SpeedKPH),
			nullableSQLFloat(row.CourseDeg),
			aviation.SchemaVersion,
			sqlString(attrs),
			sqlString(evidence),
		)
	}
	return b.String(), nil
}

func insertAviationSegmentsSQL(rows []aviation.FlightSegment) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO silver.fact_track_segment (track_segment_id, track_id, source_id, track_type, entity_id, from_place_id, to_place_id, started_at, ended_at, point_count, distance_km, avg_speed_kph, schema_version, attrs, evidence) VALUES ")
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
		fmt.Fprintf(&b, "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%s,%d,%s,%s)",
			sqlString(row.SegmentID),
			sqlString(row.TrackID),
			sqlString(row.SourceID),
			sqlString(aviation.TrackType),
			sqlString(row.EntityID),
			sqlString(row.FromPlaceID),
			sqlString(row.ToPlaceID),
			sqlTime(row.StartedAt),
			sqlTime(row.EndedAt),
			row.PointCount,
			formatFloat(row.DistanceKM),
			nullableSQLFloat(floatPointer(row.AvgSpeedKPH)),
			aviation.SchemaVersion,
			sqlString(attrs),
			sqlString(evidence),
		)
	}
	return b.String(), nil
}

func insertAviationEventsSQL(gaps []aviation.GapEvent, interactions []aviation.AirportInteractionEvent) (string, error) {
	if len(gaps) == 0 && len(interactions) == 0 {
		return "", nil
	}
	type eventRow struct {
		EventID        string
		SourceID       string
		EventType      string
		EventSubtype   string
		PlaceID        string
		StartsAt       time.Time
		EndsAt         *time.Time
		Status         string
		ConfidenceBand string
		ImpactScore    float64
		Attrs          map[string]any
		Evidence       []canonical.Evidence
	}
	rows := make([]eventRow, 0, len(gaps)+len(interactions))
	for _, gap := range gaps {
		endAt := gap.EndedAt
		rows = append(rows, eventRow{
			EventID:        gap.EventID,
			SourceID:       gap.SourceID,
			EventType:      "aviation_transponder_gap",
			EventSubtype:   "transponder_gap",
			PlaceID:        gap.PlaceID,
			StartsAt:       gap.StartedAt,
			EndsAt:         &endAt,
			Status:         "observed",
			ConfidenceBand: "high",
			ImpactScore:    gap.GapHours,
			Attrs:          gap.Attrs,
			Evidence:       gap.Evidence,
		})
	}
	for _, interaction := range interactions {
		endsAt := interaction.ObservedAt
		rows = append(rows, eventRow{
			EventID:        interaction.EventID,
			SourceID:       interaction.SourceID,
			EventType:      "aviation_airport_interaction",
			EventSubtype:   interaction.InteractionType,
			PlaceID:        interaction.PlaceID,
			StartsAt:       interaction.ObservedAt,
			EndsAt:         &endsAt,
			Status:         "observed",
			ConfidenceBand: "high",
			ImpactScore:    0,
			Attrs:          interaction.Attrs,
			Evidence:       interaction.Evidence,
		})
	}
	var b strings.Builder
	b.WriteString("INSERT INTO silver.fact_event (event_id, source_id, event_type, event_subtype, place_id, parent_place_chain, starts_at, ends_at, status, confidence_band, impact_score, schema_version, attrs, evidence) VALUES ")
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
		fmt.Fprintf(&b, "(%s,%s,%s,%s,%s,[],%s,%s,%s,%s,%s,%d,%s,%s)",
			sqlString(row.EventID),
			sqlString(row.SourceID),
			sqlString(row.EventType),
			sqlString(row.EventSubtype),
			sqlString(row.PlaceID),
			sqlTime(row.StartsAt),
			nullableSQLTime(row.EndsAt),
			sqlString(row.Status),
			sqlString(row.ConfidenceBand),
			formatFloat(row.ImpactScore),
			aviation.SchemaVersion,
			sqlString(attrs),
			sqlString(evidence),
		)
	}
	return b.String(), nil
}

func floatPointer(v float64) *float64 {
	return &v
}

func nullableSQLFloat(v *float64) string {
	if v == nil {
		return "NULL"
	}
	return formatFloat(*v)
}

func nullableSQLTime(v *time.Time) string {
	if v == nil || v.IsZero() {
		return "NULL"
	}
	return sqlTime(v.UTC())
}

func nonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
