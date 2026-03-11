package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"global-osint-backend/internal/canonical"
	coremetrics "global-osint-backend/internal/metrics"
	"global-osint-backend/internal/migrate"
	"global-osint-backend/internal/packs/maritime"
)

const maritimeJobName = "ingest-maritime"

func init() {
	jobRegistry[maritimeJobName] = jobRunner{
		description: "Replay maritime fixtures into vessel entities, tracks, events, and metrics.",
		run:         runIngestMaritime,
	}
}

type maritimeFixtureBundle struct {
	entities      []canonical.EntityEnvelope
	trackPoints   []canonical.TrackEnvelope
	trackSegments []canonical.TrackEnvelope
	events        []canonical.EventEnvelope
	metrics       []maritime.MetricReading
	registry      []coremetrics.RegistryRecord
	bundleID      string
}

func runIngestMaritime(ctx context.Context) error {
	startedAt := time.Now().UTC().Truncate(time.Millisecond)
	runner := migrate.NewHTTPRunner(controlPlaneClickHouseURL())
	jobID := fmt.Sprintf("job:%s:%d", maritimeJobName, startedAt.UnixMilli())
	bundleID := strings.TrimSpace(currentJobOptions(ctx).SourceID)
	if bundleID == "" {
		bundleID = "fixture:maritime"
	}

	recordFailure := func(err error, message string, stats map[string]any) error {
		if recordErr := recordJobRun(ctx, runner, jobID, maritimeJobName, "failed", startedAt, time.Now().UTC().Truncate(time.Millisecond), message, stats); recordErr != nil {
			return fmt.Errorf("%w (job log failed: %v)", err, recordErr)
		}
		return err
	}

	bundle := buildMaritimeFixtureBundle(startedAt, bundleID)
	statements, err := maritimeSQLStatements(startedAt, bundle)
	if err != nil {
		return recordFailure(err, "build maritime sql", map[string]any{"stage": "sql", "bundle_id": bundle.bundleID})
	}
	for _, statement := range statements {
		if err := runner.ApplySQL(ctx, statement); err != nil {
			return recordFailure(err, "apply maritime sql", map[string]any{"stage": "apply", "bundle_id": bundle.bundleID})
		}
	}

	stats := map[string]any{
		"bundle_id":         bundle.bundleID,
		"entity_rows":       len(bundle.entities),
		"track_points":      len(bundle.trackPoints),
		"track_segments":    len(bundle.trackSegments),
		"event_rows":        len(bundle.events),
		"metric_rows":       len(bundle.metrics),
		"registry_rows":     len(bundle.registry),
		"sql_statements":    len(statements),
		"metric_ids":        []string{"ais_dark_hours_sum", "anchorage_dwell_hours", "flag_registry_mismatch_score", "port_gap_hours", "route_deviation_score", "shadow_fleet_score"},
		"canonical_job":     maritimeJobName,
		"orchestration_cli": "run-once",
	}
	if err := recordJobRun(ctx, runner, jobID, maritimeJobName, "success", startedAt, time.Now().UTC().Truncate(time.Millisecond), "replayed maritime pack fixtures", stats); err != nil {
		return err
	}
	return nil

}

func buildMaritimeFixtureBundle(now time.Time, bundleID string) maritimeFixtureBundle {
	now = now.UTC().Truncate(time.Millisecond)
	rawEvidence := canonical.NewRawDocumentEvidence("maritime:registry:vessel", "raw:vessel-1", "https://example.test/vessels/9303801")
	registryEvidence := canonical.Evidence{Kind: "registry_page", Ref: "imo:9303801", URL: "https://example.test/vessels/9303801", Value: "Northern Light"}

	vessel := maritime.Vessel{
		SourceID:       "maritime:registry:vessel",
		NativeID:       "imo:9303801",
		Name:           "MV Northern Light",
		Aliases:        []string{"Northern Light"},
		IMO:            "9303801",
		MMSI:           "538009877",
		CallSign:       "V7NL8",
		VesselType:     "oil_tanker",
		FlagState:      "PA",
		Status:         "active",
		RiskBand:       "high",
		PrimaryPlaceID: "plc:port:ae-fjr",
		OwnerName:      "Northern Light Maritime Ltd",
		OperatorName:   "Gulf Route Shipping",
		BuildYear:      2007,
		DeadweightTons: 105432,
		GrossTonnage:   58521,
		ValidFrom:      time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
		Evidence:       []canonical.Evidence{rawEvidence, registryEvidence},
	}
	entity := vessel.EntityEnvelope()

	speed := float32(22.4)
	course := float32(134.2)
	trackPoint := maritime.VesselTrackPoint{
		SourceID:       "maritime:ais:community",
		TrackID:        "trk:northern-light",
		EntityID:       entity.ID,
		PlaceID:        "plc:sea:arabian-gulf",
		ObservedAt:     now.Add(-2 * time.Hour),
		Latitude:       25.2472,
		Longitude:      56.3575,
		SpeedKPH:       &speed,
		CourseDeg:      &course,
		Status:         "under_way_using_engine",
		ParentPlaceIDs: []string{"plc:ae", "plc:continent:as"},
		Evidence:       []canonical.Evidence{canonical.NewRawDocumentEvidence("maritime:ais:community", "raw:ais-point-1", "https://example.test/ais/point-1")},
	}

	avgSpeed := float32(21.1)
	trackSegment := maritime.VesselTrackSegment{
		SourceID:       "maritime:ais:community",
		TrackID:        "trk:northern-light",
		EntityID:       entity.ID,
		PlaceID:        "plc:sea:gulf-of-oman",
		FromPlaceID:    "plc:port:iruqn",
		ToPlaceID:      "plc:port:ae-fjr",
		StartedAt:      now.Add(-36 * time.Hour),
		EndedAt:        now.Add(-6 * time.Hour),
		DistanceKM:     612.4,
		PointCount:     48,
		AvgSpeedKPH:    &avgSpeed,
		ParentPlaceIDs: []string{"plc:ae", "plc:continent:as"},
		Evidence:       []canonical.Evidence{canonical.NewRawDocumentEvidence("maritime:ais:community", "raw:ais-segment-1", "https://example.test/ais/segment-1")},
	}

	portCall := maritime.PortCall{
		SourceID:         "maritime:port:unlocode",
		EntityID:         entity.ID,
		PlaceID:          "plc:port:ae-fjr",
		PortName:         "Fujairah Anchorage",
		Terminal:         "Outer Anchorage",
		Berth:            "A-12",
		CallType:         "turnaround",
		StartedAt:        now.Add(-10 * time.Hour),
		EndedAt:          now.Add(-2 * time.Hour),
		NextPlaceID:      "plc:port:pkkhi",
		ParentPlaceChain: []string{"plc:ae-fuj", "plc:ae", "plc:continent:as"},
		Evidence:         []canonical.Evidence{canonical.NewRawDocumentEvidence("maritime:port:unlocode", "raw:port-call-1", "https://example.test/port-calls/1")},
	}

	gapOne := maritime.AISGap{
		SourceID:         "maritime:ais:community",
		TrackID:          trackSegment.TrackID,
		EntityID:         entity.ID,
		PlaceID:          "plc:sea:arabian-gulf",
		StartsAt:         now.Add(-72 * time.Hour),
		EndsAt:           now.Add(-54 * time.Hour),
		Reason:           "dark_activity",
		LastKnownPortID:  "plc:port:iruqn",
		NextKnownPortID:  "plc:sea:arabian-gulf",
		ParentPlaceChain: []string{"plc:ae", "plc:continent:as"},
		Evidence:         []canonical.Evidence{canonical.NewRawDocumentEvidence("maritime:ais:community", "raw:gap-1", "https://example.test/ais/gap-1")},
	}
	gapTwo := maritime.AISGap{
		SourceID:         "maritime:ais:community",
		TrackID:          trackSegment.TrackID,
		EntityID:         entity.ID,
		PlaceID:          "plc:sea:gulf-of-oman",
		StartsAt:         now.Add(-30 * time.Hour),
		EndsAt:           now.Add(-23*time.Hour - 30*time.Minute),
		Reason:           "dark_activity",
		LastKnownPortID:  "plc:sea:arabian-gulf",
		NextKnownPortID:  "plc:port:ae-fjr",
		ParentPlaceChain: []string{"plc:ae", "plc:continent:as"},
		Evidence:         []canonical.Evidence{canonical.NewRawDocumentEvidence("maritime:ais:community", "raw:gap-2", "https://example.test/ais/gap-2")},
	}

	darkHours := maritime.AISDarkHours(entity.ID, []maritime.AISGap{gapOne, gapTwo}, now)
	routeDeviation := maritime.RouteDeviationScore(entity.ID, 0.8, append(append([]canonical.Evidence(nil), trackSegment.Envelope().Evidence...), gapTwo.EventEnvelope().Evidence...), now)
	portGap := maritime.PortGapHours(entity.ID, []maritime.AISGap{gapOne, gapTwo}, []maritime.PortCall{portCall}, now)
	anchorageDwell := maritime.AnchorageDwellHours(entity.ID, []maritime.PortCall{portCall}, now)
	flagMismatch := maritime.FlagRegistryMismatchScore(entity.ID, maritime.FlagRegistrySignals{
		RegistryFlagState: vessel.FlagState,
		ObservedFlagState: "TZ",
		FlagChanges90d:    2,
		Evidence: []canonical.Evidence{{
			Kind:  "registry_comparison",
			Ref:   vessel.IMO,
			Value: "PA->TZ",
		}},
	}, now)
	shadowScore := maritime.ShadowFleetScore(entity.ID, maritime.ShadowFleetSignals{
		AISDarkHours:         darkHours.MetricValue,
		AISGapFrequency:      2,
		FlagChanges90d:       2,
		OwnershipChanges180d: 1,
		SanctionsExposure:    1,
		HighRiskPortCalls:    2,
		STSSuspicionScore:    0.7,
		RouteDeviationScore:  0.8,
		VesselAgeYears:       19,
		Evidence:             append(append([]canonical.Evidence(nil), darkHours.Evidence...), canonical.Evidence{Kind: "watchlist_match", Ref: "sanctions:northern-light", Value: "matched_sanctioned_operator"}),
	}, now)

	return maritimeFixtureBundle{
		entities:      []canonical.EntityEnvelope{entity},
		trackPoints:   []canonical.TrackEnvelope{trackPoint.Envelope()},
		trackSegments: []canonical.TrackEnvelope{trackSegment.Envelope()},
		events:        []canonical.EventEnvelope{portCall.EventEnvelope(), gapOne.EventEnvelope(), gapTwo.EventEnvelope()},
		metrics:       []maritime.MetricReading{darkHours, anchorageDwell, flagMismatch, portGap, routeDeviation, shadowScore},
		registry:      maritime.BuildMetricRegistryRecords(now),
		bundleID:      bundleID,
	}
}

func maritimeSQLStatements(now time.Time, bundle maritimeFixtureBundle) ([]string, error) {
	statements := make([]string, 0, 6)
	if sql, err := insertMaritimeMetricRegistrySQL(bundle.registry); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	if sql, err := insertMaritimeEntitiesSQL(now, bundle.entities); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	if sql, err := insertMaritimeTrackPointsSQL(bundle.trackPoints); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	if sql, err := insertMaritimeTrackSegmentsSQL(bundle.trackSegments); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	if sql, err := insertMaritimeEventsSQL(bundle.events); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	metricSQL, err := coremetrics.UpsertMaterializationSQL(maritimeMetricContributions(bundle.entities, bundle.metrics), now)
	if err != nil {
		return nil, err
	}
	statements = append(statements, metricSQL...)
	return statements, nil
}

func maritimeMetricContributions(entities []canonical.EntityEnvelope, rows []maritime.MetricReading) []coremetrics.Contribution {
	entityPlaces := map[string]string{}
	for _, entity := range entities {
		entityPlaces[entity.ID] = entity.PrimaryPlaceID
	}
	contributions := make([]coremetrics.Contribution, 0, len(rows))
	for _, row := range rows {
		windowStart := row.CalculatedAt.UTC().Add(-24 * time.Hour)
		contributions = append(contributions, coremetrics.Contribution{
			ContributionID:     fmt.Sprintf("mc:%s:%s:%s:%d", row.MetricID, row.SubjectGrain, row.SubjectID, windowStart.Unix()),
			MetricID:           row.MetricID,
			SubjectGrain:       row.SubjectGrain,
			SubjectID:          row.SubjectID,
			SourceRecordType:   "maritime_metric",
			SourceRecordID:     row.SubjectID,
			PlaceID:            entityPlaces[row.SubjectID],
			WindowGrain:        row.WindowGrain,
			WindowStart:        windowStart,
			WindowEnd:          row.CalculatedAt.UTC(),
			ContributionType:   "derived_metric",
			ContributionValue:  row.MetricValue,
			ContributionWeight: 1,
			SchemaVersion:      row.SchemaVersion,
			Attrs:              row.Attrs,
			Evidence:           row.Evidence,
		})
	}
	return contributions
}

func insertMaritimeMetricRegistrySQL(rows []coremetrics.RegistryRecord) (string, error) {
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
		fmt.Fprintf(&b, "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,%d,%d,%s)",
			sqlString(row.MetricID),
			sqlString(row.MetricFamily),
			sqlString(row.SubjectGrain),
			sqlString(row.Unit),
			sqlString(row.ValueType),
			sqlString(row.RollupEngine),
			sqlString(row.RollupRule),
			sqlString(attrs),
			sqlString(evidence),
			row.SchemaVersion,
			row.RecordVersion,
			row.APIContractVersion,
			maritimeBoolAsInt(row.Enabled),
			sqlTime(row.UpdatedAt),
		)
	}
	return b.String(), nil
}

func insertMaritimeEntitiesSQL(now time.Time, rows []canonical.EntityEnvelope) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO silver.dim_entity (entity_id, entity_type, canonical_name, status, risk_band, primary_place_id, source_entity_key, source_system, valid_from, valid_to, schema_version, record_version, api_contract_version, updated_at, attrs, evidence) VALUES ")
	for idx, row := range rows {
		if idx > 0 {
			b.WriteString(",")
		}
		attrs, err := marshalJSONString(map[string]any{"record_attrs": row.Attrs, "aliases": row.Aliases, "payload": row.Payload})
		if err != nil {
			return "", err
		}
		evidence, err := marshalJSONString(row.Evidence)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,%d,%s,%s,%s)",
			sqlString(row.ID),
			sqlString(row.EntityType),
			sqlString(row.CanonicalName),
			sqlString(row.Status),
			sqlString(row.RiskBand),
			nullableSQLString(row.PrimaryPlaceID),
			sqlString(nonEmptyString(row.NativeID, row.CanonicalName)),
			sqlString(row.SourceID),
			nullableSQLTimeValue(row.ValidFrom),
			nullableSQLTimeValue(row.ValidTo),
			row.SchemaVersion,
			row.RecordVersion,
			1,
			sqlTime(now),
			sqlString(attrs),
			sqlString(evidence),
		)
	}
	return b.String(), nil
}

func insertMaritimeTrackPointsSQL(rows []canonical.TrackEnvelope) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO silver.fact_track_point (track_point_id, track_id, source_id, track_type, entity_id, place_id, observed_at, latitude, longitude, altitude_m, speed_kph, course_deg, schema_version, attrs, evidence) VALUES ")
	for idx, row := range rows {
		if idx > 0 {
			b.WriteString(",")
		}
		attrs, err := marshalJSONString(map[string]any{"record_attrs": row.Attrs, "payload": row.Payload})
		if err != nil {
			return "", err
		}
		evidence, err := marshalJSONString(row.Evidence)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "(%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,%s,%s,%d,%s,%s)",
			sqlString(row.ID),
			sqlString(row.TrackID),
			sqlString(row.SourceID),
			sqlString(row.TrackType),
			sqlString(row.EntityID),
			sqlString(row.PlaceID),
			nullableSQLTimeValue(row.ObservedAt),
			nullableSQLFloat64Value(row.Latitude),
			nullableSQLFloat64Value(row.Longitude),
			nullableSQLFloat32Value(row.SpeedKPH),
			nullableSQLFloat32Value(row.CourseDeg),
			row.SchemaVersion,
			sqlString(attrs),
			sqlString(evidence),
		)
	}
	return b.String(), nil
}

func insertMaritimeTrackSegmentsSQL(rows []canonical.TrackEnvelope) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO silver.fact_track_segment (track_segment_id, track_id, source_id, track_type, entity_id, from_place_id, to_place_id, started_at, ended_at, point_count, distance_km, avg_speed_kph, schema_version, attrs, evidence) VALUES ")
	for idx, row := range rows {
		if idx > 0 {
			b.WriteString(",")
		}
		attrs, err := marshalJSONString(map[string]any{"record_attrs": row.Attrs, "payload": row.Payload})
		if err != nil {
			return "", err
		}
		evidence, err := marshalJSONString(row.Evidence)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%s)",
			sqlString(row.ID),
			sqlString(row.TrackID),
			sqlString(row.SourceID),
			sqlString(row.TrackType),
			sqlString(row.EntityID),
			sqlString(row.FromPlaceID),
			sqlString(row.ToPlaceID),
			nullableSQLTimeValue(row.StartedAt),
			nullableSQLTimeValue(row.EndedAt),
			nullableSQLUInt32Value(row.PointCount),
			nullableSQLFloat64Value(row.DistanceKM),
			nullableSQLFloat32Value(row.SpeedKPH),
			row.SchemaVersion,
			sqlString(attrs),
			sqlString(evidence),
		)
	}
	return b.String(), nil
}

func insertMaritimeEventsSQL(rows []canonical.EventEnvelope) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO silver.fact_event (event_id, source_id, event_type, event_subtype, place_id, parent_place_chain, starts_at, ends_at, status, confidence_band, impact_score, schema_version, attrs, evidence) VALUES ")
	for idx, row := range rows {
		if idx > 0 {
			b.WriteString(",")
		}
		attrs, err := marshalJSONString(map[string]any{"record_attrs": row.Attrs, "payload": row.Payload})
		if err != nil {
			return "", err
		}
		evidence, err := marshalJSONString(row.Evidence)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%s)",
			sqlString(row.ID),
			sqlString(row.SourceID),
			sqlString(row.EventType),
			sqlString(row.EventSubtype),
			sqlString(row.PlaceID),
			maritimeStringSliceLiteral(row.ParentPlaceChain),
			sqlTime(row.StartsAt),
			nullableSQLTimeValue(row.EndsAt),
			sqlString(row.Status),
			sqlString(row.ConfidenceBand),
			nullableSQLFloat32Value(row.ImpactScore),
			row.SchemaVersion,
			sqlString(attrs),
			sqlString(evidence),
		)
	}
	return b.String(), nil
}

func maritimeBoolAsInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func nullableSQLTimeValue(v *time.Time) string {
	if v == nil || v.IsZero() {
		return "NULL"
	}
	return sqlTime(v.UTC())
}

func nullableSQLFloat64Value(v *float64) string {
	if v == nil {
		return "NULL"
	}
	return formatFloat(*v)
}

func nullableSQLFloat32Value(v *float32) string {
	if v == nil {
		return "NULL"
	}
	return formatFloat(float64(*v))
}

func nullableSQLUInt32Value(v *uint32) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprintf("%d", *v)
}

func maritimeStringSliceLiteral(values []string) string {
	if len(values) == 0 {
		return "[]"
	}
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, sqlString(value))
	}
	return "[" + strings.Join(quoted, ",") + "]"
}
