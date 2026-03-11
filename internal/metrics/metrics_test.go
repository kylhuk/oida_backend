package metrics

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"global-osint-backend/internal/canonical"
)

var coreMetricIDs = []string{
	"acceleration_7d_vs_30d",
	"anomaly_zscore_30d",
	"burst_score",
	"confidence_weighted_activity",
	"cross_source_confirmation_rate",
	"dedup_rate",
	"entity_count_approx",
	"event_count",
	"evidence_density",
	"freshness_lag_minutes",
	"geolocation_success_rate",
	"obs_count",
	"risk_composite_global",
	"schema_drift_rate",
	"source_count_approx",
	"source_diversity_score",
	"trend_24h",
	"trend_7d",
}

func TestMetricContributions(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	registry := BuildRegistryRecords(now)
	if len(registry) != len(coreMetricIDs) {
		t.Fatalf("expected %d core registry records, got %d", len(coreMetricIDs), len(registry))
	}
	for _, metricID := range coreMetricIDs {
		if !hasRegistryMetric(registry, metricID) {
			t.Fatalf("missing metric registry entry %q", metricID)
		}
	}

	contributions := EmitCoreMetricContributions(metricTestRecords(now))
	if len(contributions) == 0 {
		t.Fatal("expected contributions")
	}
	for _, metricID := range coreMetricIDs {
		if !hasMetricContribution(contributions, metricID) {
			t.Fatalf("expected contributions for %q", metricID)
		}
	}
	if !hasContribution(contributions, "obs_count", "world", worldPlaceID) {
		t.Fatal("expected world obs_count contribution")
	}
	if !hasContribution(contributions, "event_count", "continent", "plc:continent:na") {
		t.Fatal("expected continent event_count contribution")
	}
	if !hasContribution(contributions, "trend_24h", "admin0", "plc:fr") {
		t.Fatal("expected admin0 trend_24h contribution")
	}
	if !hasContribution(contributions, "entity_count_approx", "world", worldPlaceID) {
		t.Fatal("expected world entity_count_approx contribution")
	}
	for _, contribution := range contributions {
		if len(contribution.Evidence) == 0 {
			t.Fatalf("expected evidence for %s", contribution.ContributionID)
		}
		explainability, ok := contribution.Attrs["explainability"].(map[string]any)
		if !ok || len(explainability) == 0 {
			t.Fatalf("expected explainability payload for %s", contribution.ContributionID)
		}
		if _, ok := explainability["evidence_refs"]; !ok {
			t.Fatalf("expected evidence refs for %s", contribution.ContributionID)
		}
	}
	if got := contributionValueForRecord(contributions, "freshness_lag_minutes", "obs:paris-fr:current", "place", "plc:fr-idf-paris"); got != 90 {
		t.Fatalf("expected freshness lag 90, got %v", got)
	}
	if got := contributionValueForRecord(contributions, "geolocation_success_rate", "evt:paris-tx:current", "place", "plc:us-tx-paris"); got != 0 {
		t.Fatalf("expected failed geolocation contribution to be 0, got %v", got)
	}
	if got := contributionValueForRecord(contributions, "confidence_weighted_activity", "obs:paris-fr:current", "world", worldPlaceID); got != 0.9 {
		t.Fatalf("expected confidence weighted activity 0.9, got %v", got)
	}
	if got := contributionValueForRecord(contributions, "acceleration_7d_vs_30d", "evt:paris-tx:current", "world", worldPlaceID); got != -0.2 {
		t.Fatalf("expected acceleration contribution -0.2, got %v", got)
	}
}

func TestMetricSnapshots(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	previous := now.AddDate(0, 0, -7)
	states := BuildMetricState(EmitCoreMetricContributions(metricTestRecords(now)), now)
	if len(states) == 0 {
		t.Fatal("expected state rows")
	}
	if !hasState(states, "trend_7d", "world", worldPlaceID, previous) {
		t.Fatal("expected previous world trend_7d state")
	}
	if !hasState(states, "trend_7d", "world", worldPlaceID, now) {
		t.Fatal("expected current world trend_7d state")
	}
	if !hasState(states, "trend_24h", "world", worldPlaceID, now) {
		t.Fatal("expected current world trend_24h state")
	}
	if got := finalizedValue(states, "geolocation_success_rate", "world", worldPlaceID, now); got != 0.6667 {
		t.Fatalf("expected world geolocation_success_rate 0.6667, got %v", got)
	}
	if got := finalizedValue(states, "source_diversity_score", "world", worldPlaceID, now); got != 0.6667 {
		t.Fatalf("expected world source_diversity_score 0.6667, got %v", got)
	}
	if got := finalizedValue(states, "source_count_approx", "world", worldPlaceID, now); got != 2 {
		t.Fatalf("expected world source_count_approx 2, got %v", got)
	}
	if got := finalizedValue(states, "entity_count_approx", "world", worldPlaceID, now); got != 2 {
		t.Fatalf("expected world entity_count_approx 2, got %v", got)
	}
	if got := finalizedValue(states, "confidence_weighted_activity", "world", worldPlaceID, now); got != 2.1 {
		t.Fatalf("expected world confidence_weighted_activity 2.1, got %v", got)
	}
	if got := finalizedValue(states, "evidence_density", "world", worldPlaceID, now); got != 2 {
		t.Fatalf("expected world evidence_density 2, got %v", got)
	}
	if got := finalizedValue(states, "dedup_rate", "world", worldPlaceID, now); got != 0.6667 {
		t.Fatalf("expected world dedup_rate 0.6667, got %v", got)
	}
	if got := finalizedValue(states, "schema_drift_rate", "world", worldPlaceID, now); got != 0.3333 {
		t.Fatalf("expected world schema_drift_rate 0.3333, got %v", got)
	}
	if got := finalizedValue(states, "cross_source_confirmation_rate", "world", worldPlaceID, now); got != 0.6667 {
		t.Fatalf("expected world cross_source_confirmation_rate 0.6667, got %v", got)
	}
	if got := finalizedValue(states, "anomaly_zscore_30d", "world", worldPlaceID, now); got != 1.3333 {
		t.Fatalf("expected world anomaly_zscore_30d 1.3333, got %v", got)
	}
	if got := finalizedValue(states, "trend_24h", "world", worldPlaceID, now); got != 3 {
		t.Fatalf("expected world trend_24h 3, got %v", got)
	}
	if got := finalizedValue(states, "acceleration_7d_vs_30d", "world", worldPlaceID, now); got != 0.3 {
		t.Fatalf("expected world acceleration_7d_vs_30d 0.3, got %v", got)
	}
	if got := finalizeMetricValue(StateRow{MetricID: "schema_drift_rate"}); got != 0 {
		t.Fatalf("expected zero-denominator ratio metric to resolve to 0, got %v", got)
	}

	snapshots := BuildMetricSnapshots(states, now.Add(5*time.Minute))
	if len(snapshots) == 0 {
		t.Fatal("expected snapshots")
	}
	for _, metricID := range coreMetricIDs {
		if !hasMetricSnapshot(snapshots, metricID) {
			t.Fatalf("expected snapshots for %q", metricID)
		}
	}
	if got := snapshotDelta(snapshots, "trend_7d", "world", worldPlaceID, now); got != 2 {
		t.Fatalf("expected world trend delta 2, got %v", got)
	}
	if !hasSnapshot(snapshots, "obs_count", "continent", "plc:continent:eu") {
		t.Fatal("expected continent obs_count snapshot")
	}
	for _, snapshot := range snapshots {
		if len(snapshot.Evidence) == 0 {
			t.Fatalf("expected snapshot evidence for %s", snapshot.SnapshotID)
		}
		explainability, ok := snapshot.Attrs["explainability"].(map[string]any)
		if !ok || len(explainability) == 0 {
			t.Fatalf("expected explainability attrs for %s", snapshot.SnapshotID)
		}
	}
	if !stringsContain(MetricStateTableSQL(), "ENGINE = AggregatingMergeTree") {
		t.Fatal("expected metric state DDL to use AggregatingMergeTree")
	}
	if !stringsContain(MetricStateMaterializedViewSQL("metric_state_mv"), "sumState(contribution_value)") {
		t.Fatal("expected metric state MV to aggregate contribution values")
	}
	if !stringsContain(MetricStateTableSQL(), "materialization_key String") {
		t.Fatal("expected metric state DDL to include materialization key")
	}
	if !stringsContain(MetricStateTableSQL(), "AggregateFunction(uniqExact, String)") {
		t.Fatal("expected metric state DDL to track distinct source state")
	}
	if !stringsContain(MetricStateMaterializedViewSQL("metric_state_mv"), "argMaxState(contribution_value, window_end)") {
		t.Fatal("expected metric state MV to track latest contribution value")
	}
	views := RefreshableMetricSnapshotViews()
	if len(views) != 4 {
		t.Fatalf("expected 4 refreshable snapshot views, got %d", len(views))
	}
	if !stringsContain(views["metric_snapshot_day_mv"], "REFRESH EVERY 15 MINUTE") {
		t.Fatal("expected day snapshot view refresh cadence")
	}
	if !stringsContain(views["metric_snapshot_24h_mv"], "window_grain = '24h'") {
		t.Fatal("expected 24h snapshot filter")
	}
	if !stringsContain(views["metric_snapshot_7d_mv"], "window_grain = '7d'") {
		t.Fatal("expected 7d snapshot filter")
	}
	if !stringsContain(views["metric_snapshot_30d_mv"], "window_grain = '30d'") {
		t.Fatal("expected 30d snapshot filter")
	}
	if !stringsContain(views["metric_snapshot_day_mv"], "concat('snapshot:', materialization_key)") {
		t.Fatal("expected snapshot view to use deterministic materialization key")
	}
}

func TestSchemaDriftFixture(t *testing.T) {
	path := filepath.Join("..", "..", "testdata", "fixtures", "quality", "schema_drift.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var fixture struct {
		MetricID         string    `json:"metric_id"`
		CheckedAt        time.Time `json:"checked_at"`
		PlaceID          string    `json:"place_id"`
		Admin0PlaceID    string    `json:"admin0_place_id"`
		ContinentPlaceID string    `json:"continent_place_id"`
		SourceID         string    `json:"source_id"`
		Tables           []struct {
			Table         string `json:"table"`
			EntityID      string `json:"entity_id"`
			DriftScore    int    `json:"drift_score"`
			EvidenceCount int    `json:"evidence_count"`
			Notes         string `json:"notes"`
		} `json:"tables"`
		Expected struct {
			SchemaDriftRate   float64 `json:"schema_drift_rate"`
			EntityCountApprox float64 `json:"entity_count_approx"`
			SourceCountApprox float64 `json:"source_count_approx"`
		} `json:"expected"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	if fixture.MetricID != "schema_drift_rate" {
		t.Fatalf("expected schema_drift_rate fixture, got %q", fixture.MetricID)
	}

	records := make([]InputRecord, 0, len(fixture.Tables))
	for _, table := range fixture.Tables {
		records = append(records, InputRecord{
			RecordID:             "schema:" + table.Table,
			RecordType:           "observation",
			EntityID:             table.EntityID,
			PlaceID:              fixture.PlaceID,
			Admin0PlaceID:        fixture.Admin0PlaceID,
			ContinentPlaceID:     fixture.ContinentPlaceID,
			SourceID:             fixture.SourceID,
			OccurredAt:           fixture.CheckedAt,
			PublishedAt:          &fixture.CheckedAt,
			GeolocationSucceeded: true,
			SchemaDriftDetected:  table.DriftScore > 0,
			Confidence:           1,
			EvidenceCount:        table.EvidenceCount,
			Evidence: []canonical.Evidence{{
				Kind:  "schema_check",
				Ref:   table.Table,
				Value: table.Notes,
			}},
		})
	}

	contributions := EmitCoreMetricContributions(records)
	if !hasMetricContribution(contributions, "schema_drift_rate") {
		t.Fatal("expected schema_drift_rate contributions from fixture")
	}
	states := BuildMetricState(contributions, fixture.CheckedAt)
	if got := finalizedValue(states, "schema_drift_rate", "world", worldPlaceID, fixture.CheckedAt); got != fixture.Expected.SchemaDriftRate {
		t.Fatalf("expected fixture schema_drift_rate %v, got %v", fixture.Expected.SchemaDriftRate, got)
	}
	if got := finalizedValue(states, "entity_count_approx", "world", worldPlaceID, fixture.CheckedAt); got != fixture.Expected.EntityCountApprox {
		t.Fatalf("expected fixture entity_count_approx %v, got %v", fixture.Expected.EntityCountApprox, got)
	}
	if got := finalizedValue(states, "source_count_approx", "world", worldPlaceID, fixture.CheckedAt); got != fixture.Expected.SourceCountApprox {
		t.Fatalf("expected fixture source_count_approx %v, got %v", fixture.Expected.SourceCountApprox, got)
	}
	snapshots := BuildMetricSnapshots(states, fixture.CheckedAt.Add(5*time.Minute))
	if !hasSnapshot(snapshots, "schema_drift_rate", "world", worldPlaceID) {
		t.Fatal("expected schema_drift_rate snapshot from fixture")
	}
}

func TestUpsertMaterializationSQL(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 5, 0, 0, time.UTC)
	statements, err := UpsertMaterializationSQL([]Contribution{{
		ContributionID:     "mc:event_count:place:plc:fr-idf-paris:evt:1:1741564800",
		MetricID:           "event_count",
		SubjectGrain:       "place",
		SubjectID:          "plc:fr-idf-paris",
		SourceRecordType:   "event",
		SourceRecordID:     "evt:1",
		SourceID:           "src:alpha",
		PlaceID:            "plc:fr-idf-paris",
		WindowGrain:        "day",
		WindowStart:        time.Date(2026, time.March, 10, 0, 0, 0, 0, time.UTC),
		WindowEnd:          time.Date(2026, time.March, 11, 0, 0, 0, 0, time.UTC),
		MaterializationKey: materializationKey("event_count", "place", "plc:fr-idf-paris", "plc:fr-idf-paris", "day", time.Date(2026, time.March, 10, 0, 0, 0, 0, time.UTC)),
		ContributionType:   "count",
		ContributionValue:  1,
		ContributionWeight: 1,
		SchemaVersion:      SchemaVersion,
		Attrs:              map[string]any{"source_id": "src:alpha"},
		Evidence:           []canonical.Evidence{canonical.NewRawDocumentEvidence("src:alpha", "raw:1", "https://example.test/1")},
	}}, now)
	if err != nil {
		t.Fatalf("UpsertMaterializationSQL: %v", err)
	}
	if len(statements) != 10 {
		t.Fatalf("expected 10 materialization statements, got %d", len(statements))
	}
	joined := strings.Join(statements, "\n")
	for _, fragment := range []string{
		"ALTER TABLE silver.metric_contribution DELETE WHERE contribution_id IN",
		"INSERT INTO silver.metric_contribution",
		"materialization_key",
		"ALTER TABLE gold.metric_state DELETE WHERE materialization_key IN",
		"INSERT INTO gold.metric_state",
		"ALTER TABLE gold.metric_snapshot DELETE WHERE materialization_key IN",
		"INSERT INTO gold.metric_snapshot",
		"TRUNCATE TABLE gold.hotspot_snapshot",
		"INSERT INTO gold.hotspot_snapshot",
		"TRUNCATE TABLE gold.cross_domain_snapshot",
		"INSERT INTO gold.cross_domain_snapshot",
		"registry->silver.metric_contribution->gold.metric_state->gold.metric_snapshot",
	} {
		if !strings.Contains(joined, fragment) {
			t.Fatalf("expected fragment %q in materialization SQL", fragment)
		}
	}
}

func TestRuntimeOutputsBuildRealHotspotsAndCrossDomain(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 10, 0, 0, time.UTC)
	start := time.Date(2026, time.March, 10, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	snapshots := []SnapshotRow{
		{
			SnapshotID:         "snapshot:geo:paris",
			MetricID:           "media_attention_score",
			SubjectGrain:       "place",
			SubjectID:          "plc:fr-idf-paris",
			PlaceID:            "plc:fr-idf-paris",
			WindowGrain:        "day",
			WindowStart:        start,
			WindowEnd:          end,
			MaterializationKey: "geo|place|plc:fr-idf-paris|day",
			SnapshotAt:         now,
			MetricValue:        84,
			MetricDelta:        16,
			Evidence:           []canonical.Evidence{canonical.NewRawDocumentEvidence("fixture:geo", "raw:geo:paris", "https://example.test/geo/paris")},
		},
		{
			SnapshotID:         "snapshot:geo:yaren",
			MetricID:           "media_attention_score",
			SubjectGrain:       "place",
			SubjectID:          "plc:nr-yaren",
			PlaceID:            "plc:nr-yaren",
			WindowGrain:        "day",
			WindowStart:        start,
			WindowEnd:          end,
			MaterializationKey: "geo|place|plc:nr-yaren|day",
			SnapshotAt:         now,
			MetricValue:        35,
			MetricDelta:        4,
			Evidence:           []canonical.Evidence{canonical.NewRawDocumentEvidence("fixture:geo", "raw:geo:yaren", "https://example.test/geo/yaren")},
		},
		{
			SnapshotID:         "snapshot:safety:paris",
			MetricID:           "fire_hotspot_score",
			SubjectGrain:       "place",
			SubjectID:          "plc:fr-idf-paris",
			PlaceID:            "plc:fr-idf-paris",
			WindowGrain:        "day",
			WindowStart:        start,
			WindowEnd:          end,
			MaterializationKey: "safety|place|plc:fr-idf-paris|day",
			SnapshotAt:         now.Add(5 * time.Minute),
			MetricValue:        58,
			MetricDelta:        9,
			Evidence:           []canonical.Evidence{canonical.NewRawDocumentEvidence("fixture:safety", "raw:safety:paris", "https://example.test/safety/paris")},
		},
	}
	metricFamilies := map[string]string{
		"fire_hotspot_score":    "safety_security",
		"media_attention_score": "geopolitical",
	}

	hotspots := BuildHotspotRows(snapshots, metricFamilies)
	if len(hotspots) != 3 {
		t.Fatalf("expected 3 hotspot rows, got %d", len(hotspots))
	}
	parisHotspot := findHotspot(t, hotspots, "media_attention_score", "plc:fr-idf-paris")
	yarenHotspot := findHotspot(t, hotspots, "media_attention_score", "plc:nr-yaren")
	if parisHotspot.Rank != 1 {
		t.Fatalf("expected Paris hotspot rank 1, got %d", parisHotspot.Rank)
	}
	if parisHotspot.HotspotScore <= yarenHotspot.HotspotScore {
		t.Fatalf("expected Paris hotspot score %v to exceed Yaren %v", parisHotspot.HotspotScore, yarenHotspot.HotspotScore)
	}
	if parisHotspot.Attrs["metric_family"] != "geopolitical" {
		t.Fatalf("expected hotspot attrs to preserve metric family, got %#v", parisHotspot.Attrs)
	}

	crossDomain := BuildCrossDomainRows(snapshots, metricFamilies)
	if len(crossDomain) != 2 {
		t.Fatalf("expected 2 cross-domain rows, got %d", len(crossDomain))
	}
	parisCross := findCrossDomain(t, crossDomain, "plc:fr-idf-paris")
	joinedDomains := strings.Join(parisCross.Domains, ",")
	if !stringsContain(joinedDomains, "geopolitical") || !stringsContain(joinedDomains, "safety_security") {
		t.Fatalf("expected Paris cross-domain domains to include geopolitical and safety_security, got %#v", parisCross.Domains)
	}
	if parisCross.CompositeScore <= 0 {
		t.Fatalf("expected Paris cross-domain score > 0, got %v", parisCross.CompositeScore)
	}
	if got := parisCross.Attrs["domain_count"]; got != 2 {
		t.Fatalf("expected domain_count 2, got %#v", got)
	}
}

func metricTestRecords(now time.Time) []InputRecord {
	previous := now.AddDate(0, 0, -7)
	prevPublished := previous.Add(-30 * time.Minute)
	currentPublished := now.Add(-90 * time.Minute)
	secondPublished := now.Add(-60 * time.Minute)
	return []InputRecord{
		{
			RecordID:              "obs:fr:previous",
			RecordType:            "observation",
			EntityID:              "ent:alpha",
			PlaceID:               "plc:fr-idf-paris",
			Admin0PlaceID:         "plc:fr",
			ContinentPlaceID:      "plc:continent:eu",
			SourceID:              "src:alpha",
			OccurredAt:            previous,
			PublishedAt:           &prevPublished,
			GeolocationSucceeded:  true,
			Deduplicated:          false,
			SchemaDriftDetected:   false,
			ConfirmingSourceCount: 1,
			Confidence:            0.5,
			EvidenceCount:         1,
			BurstScore:            0.2,
			RiskScore:             0.3,
			Acceleration7dVs30d:   0.05,
			AnomalyZScore30d:      0.7,
			Evidence:              []canonical.Evidence{canonical.NewRawDocumentEvidence("src:alpha", "raw:prev", "https://example.test/prev")},
		},
		{
			RecordID:              "obs:paris-fr:current",
			RecordType:            "observation",
			EntityID:              "ent:alpha",
			PlaceID:               "plc:fr-idf-paris",
			Admin0PlaceID:         "plc:fr",
			ContinentPlaceID:      "plc:continent:eu",
			SourceID:              "src:bravo",
			OccurredAt:            now,
			PublishedAt:           &currentPublished,
			GeolocationSucceeded:  true,
			Deduplicated:          true,
			SchemaDriftDetected:   false,
			ConfirmingSourceCount: 2,
			Confidence:            0.9,
			EvidenceCount:         3,
			BurstScore:            0.8,
			RiskScore:             0.7,
			Acceleration7dVs30d:   0.4,
			AnomalyZScore30d:      1.5,
			Evidence:              []canonical.Evidence{canonical.NewRawDocumentEvidence("src:bravo", "raw:curr-1", "https://example.test/curr-1")},
		},
		{
			RecordID:              "evt:paris-tx:current",
			RecordType:            "event",
			EntityID:              "ent:bravo",
			PlaceID:               "plc:us-tx-paris",
			Admin0PlaceID:         "plc:us",
			ContinentPlaceID:      "plc:continent:na",
			SourceID:              "src:charlie",
			OccurredAt:            now,
			GeolocationSucceeded:  false,
			Deduplicated:          false,
			SchemaDriftDetected:   true,
			ConfirmingSourceCount: 1,
			Confidence:            0.4,
			EvidenceCount:         1,
			BurstScore:            0.6,
			RiskScore:             0.2,
			Acceleration7dVs30d:   -0.2,
			AnomalyZScore30d:      2.0,
			Evidence:              []canonical.Evidence{canonical.NewRawDocumentEvidence("src:charlie", "raw:evt", "https://example.test/evt")},
		},
		{
			RecordID:              "obs:paris-fr:duplicate",
			RecordType:            "observation",
			EntityID:              "ent:alpha",
			PlaceID:               "plc:fr-idf-paris",
			Admin0PlaceID:         "plc:fr",
			ContinentPlaceID:      "plc:continent:eu",
			SourceID:              "src:bravo",
			OccurredAt:            now,
			PublishedAt:           &secondPublished,
			GeolocationSucceeded:  true,
			Deduplicated:          true,
			SchemaDriftDetected:   false,
			ConfirmingSourceCount: 3,
			Confidence:            0.8,
			EvidenceCount:         2,
			BurstScore:            0.7,
			RiskScore:             0.6,
			Acceleration7dVs30d:   0.1,
			AnomalyZScore30d:      0.5,
			Evidence:              []canonical.Evidence{canonical.NewRawDocumentEvidence("src:bravo", "raw:curr-2", "https://example.test/curr-2")},
		},
	}
}

func hasRegistryMetric(records []RegistryRecord, metricID string) bool {
	for _, record := range records {
		if record.MetricID == metricID {
			return true
		}
	}
	return false
}

func hasMetricContribution(contributions []Contribution, metricID string) bool {
	for _, contribution := range contributions {
		if contribution.MetricID == metricID {
			return true
		}
	}
	return false
}

func hasContribution(contributions []Contribution, metricID, grain, subjectID string) bool {
	for _, contribution := range contributions {
		if contribution.MetricID == metricID && contribution.SubjectGrain == grain && contribution.SubjectID == subjectID {
			return true
		}
	}
	return false
}

func contributionValueForRecord(contributions []Contribution, metricID, recordID, grain, subjectID string) float64 {
	for _, contribution := range contributions {
		if contribution.MetricID == metricID && contribution.SourceRecordID == recordID && contribution.SubjectGrain == grain && contribution.SubjectID == subjectID {
			return contribution.ContributionValue
		}
	}
	return -1
}

func hasState(states []StateRow, metricID, grain, subjectID string, occurredAt time.Time) bool {
	for _, state := range states {
		if state.MetricID == metricID && state.SubjectGrain == grain && state.SubjectID == subjectID && sameDay(state.WindowEnd.Add(-time.Nanosecond), occurredAt) {
			return true
		}
	}
	return false
}

func finalizedValue(states []StateRow, metricID, grain, subjectID string, occurredAt time.Time) float64 {
	for _, state := range states {
		if state.MetricID == metricID && state.SubjectGrain == grain && state.SubjectID == subjectID && sameDay(state.WindowEnd.Add(-time.Nanosecond), occurredAt) {
			return roundMetric(finalizeMetricValue(state))
		}
	}
	return -1
}

func snapshotDelta(rows []SnapshotRow, metricID, grain, subjectID string, occurredAt time.Time) float64 {
	for _, row := range rows {
		if row.MetricID == metricID && row.SubjectGrain == grain && row.SubjectID == subjectID && sameDay(row.WindowEnd.Add(-time.Nanosecond), occurredAt) {
			return row.MetricDelta
		}
	}
	return -1
}

func hasMetricSnapshot(rows []SnapshotRow, metricID string) bool {
	for _, row := range rows {
		if row.MetricID == metricID {
			return true
		}
	}
	return false
}

func hasSnapshot(rows []SnapshotRow, metricID, grain, subjectID string) bool {
	for _, row := range rows {
		if row.MetricID == metricID && row.SubjectGrain == grain && row.SubjectID == subjectID {
			return true
		}
	}
	return false
}

func findHotspot(t *testing.T, rows []HotspotRow, metricID, scopeID string) HotspotRow {
	t.Helper()
	for _, row := range rows {
		if row.MetricID == metricID && row.ScopeID == scopeID {
			return row
		}
	}
	t.Fatalf("missing hotspot row for metric=%s scope=%s", metricID, scopeID)
	return HotspotRow{}
}

func findCrossDomain(t *testing.T, rows []CrossDomainRow, subjectID string) CrossDomainRow {
	t.Helper()
	for _, row := range rows {
		if row.SubjectID == subjectID {
			return row
		}
	}
	t.Fatalf("missing cross-domain row for subject=%s", subjectID)
	return CrossDomainRow{}
}

func sameDay(a, b time.Time) bool {
	a = a.UTC()
	b = b.UTC()
	return a.Year() == b.Year() && a.Month() == b.Month() && a.Day() == b.Day()
}

func stringsContain(s, fragment string) bool {
	return strings.Contains(s, fragment)
}
