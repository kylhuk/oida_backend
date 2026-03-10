package metrics

import (
	"strings"
	"testing"
	"time"

	"global-osint-backend/internal/canonical"
)

func TestMetricContributions(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	registry := BuildRegistryRecords(now)
	if len(registry) != 8 {
		t.Fatalf("expected 8 core registry records, got %d", len(registry))
	}
	for _, metricID := range []string{"obs_count", "event_count", "source_diversity_score", "freshness_lag_minutes", "geolocation_success_rate", "trend_7d", "burst_score", "risk_composite_global"} {
		if !hasRegistryMetric(registry, metricID) {
			t.Fatalf("missing metric registry entry %q", metricID)
		}
	}

	published := now.Add(-90 * time.Minute)
	records := []InputRecord{
		{
			RecordID:             "obs:paris-fr:1",
			RecordType:           "observation",
			PlaceID:              "plc:fr-idf-paris",
			Admin0PlaceID:        "plc:fr",
			ContinentPlaceID:     "plc:continent:eu",
			SourceID:             "src:alpha",
			OccurredAt:           now,
			PublishedAt:          &published,
			GeolocationSucceeded: true,
			Confidence:           0.9,
			EvidenceCount:        2,
			BurstScore:           0.8,
			RiskScore:            0.7,
			Evidence: []canonical.Evidence{
				canonical.NewRawDocumentEvidence("src:alpha", "raw:1", "https://example.test/1"),
			},
		},
		{
			RecordID:             "evt:paris-tx:1",
			RecordType:           "event",
			PlaceID:              "plc:us-tx-paris",
			Admin0PlaceID:        "plc:us",
			ContinentPlaceID:     "plc:continent:na",
			SourceID:             "src:bravo",
			OccurredAt:           now,
			GeolocationSucceeded: false,
			Confidence:           0.6,
			EvidenceCount:        1,
			BurstScore:           0.3,
			RiskScore:            0.5,
			Evidence: []canonical.Evidence{
				canonical.NewRawDocumentEvidence("src:bravo", "raw:2", "https://example.test/2"),
			},
		},
	}

	contributions := EmitCoreMetricContributions(records)
	if len(contributions) == 0 {
		t.Fatal("expected contributions")
	}
	if !hasContribution(contributions, "obs_count", "world", worldPlaceID) {
		t.Fatal("expected world obs_count contribution")
	}
	if !hasContribution(contributions, "event_count", "continent", "plc:continent:na") {
		t.Fatal("expected continent event_count contribution")
	}
	if !hasContribution(contributions, "trend_7d", "admin0", "plc:fr") {
		t.Fatal("expected admin0 trend_7d contribution")
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
	if got := contributionValueFor(contributions, "freshness_lag_minutes", "place", "plc:fr-idf-paris"); got != 90 {
		t.Fatalf("expected freshness lag 90, got %v", got)
	}
	if got := contributionValueFor(contributions, "geolocation_success_rate", "place", "plc:us-tx-paris"); got != 0 {
		t.Fatalf("expected failed geolocation contribution to be 0, got %v", got)
	}
}

func TestMetricSnapshots(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	previous := now.AddDate(0, 0, -7)
	prevPublished := previous.Add(-30 * time.Minute)
	currentPublished := now.Add(-90 * time.Minute)
	records := []InputRecord{
		{
			RecordID:             "obs:fr:previous",
			RecordType:           "observation",
			PlaceID:              "plc:fr-idf-paris",
			Admin0PlaceID:        "plc:fr",
			ContinentPlaceID:     "plc:continent:eu",
			SourceID:             "src:alpha",
			OccurredAt:           previous,
			PublishedAt:          &prevPublished,
			GeolocationSucceeded: true,
			Confidence:           0.5,
			BurstScore:           0.2,
			RiskScore:            0.3,
			Evidence:             []canonical.Evidence{canonical.NewRawDocumentEvidence("src:alpha", "raw:prev", "https://example.test/prev")},
		},
		{
			RecordID:             "obs:fr:current",
			RecordType:           "observation",
			PlaceID:              "plc:fr-idf-paris",
			Admin0PlaceID:        "plc:fr",
			ContinentPlaceID:     "plc:continent:eu",
			SourceID:             "src:bravo",
			OccurredAt:           now,
			PublishedAt:          &currentPublished,
			GeolocationSucceeded: true,
			Confidence:           0.9,
			BurstScore:           0.8,
			RiskScore:            0.7,
			Evidence:             []canonical.Evidence{canonical.NewRawDocumentEvidence("src:bravo", "raw:curr", "https://example.test/curr")},
		},
		{
			RecordID:             "evt:us:current",
			RecordType:           "event",
			PlaceID:              "plc:us-tx-paris",
			Admin0PlaceID:        "plc:us",
			ContinentPlaceID:     "plc:continent:na",
			SourceID:             "src:charlie",
			OccurredAt:           now,
			GeolocationSucceeded: false,
			Confidence:           0.4,
			BurstScore:           0.6,
			RiskScore:            0.2,
			Evidence:             []canonical.Evidence{canonical.NewRawDocumentEvidence("src:charlie", "raw:evt", "https://example.test/evt")},
		},
	}

	states := BuildMetricState(EmitCoreMetricContributions(records), now)
	if len(states) == 0 {
		t.Fatal("expected state rows")
	}
	if !hasState(states, "trend_7d", "world", worldPlaceID, previous) {
		t.Fatal("expected previous world trend_7d state")
	}
	if !hasState(states, "trend_7d", "world", worldPlaceID, now) {
		t.Fatal("expected current world trend_7d state")
	}
	if got := finalizedValue(states, "geolocation_success_rate", "world", worldPlaceID, now); got != 0.5 {
		t.Fatalf("expected world geolocation_success_rate 0.5, got %v", got)
	}
	if got := finalizedValue(states, "source_diversity_score", "world", worldPlaceID, now); got != 1 {
		t.Fatalf("expected world source_diversity_score 1, got %v", got)
	}
	if got := finalizeMetricValue(StateRow{MetricID: "geolocation_success_rate"}); got != 0 {
		t.Fatalf("expected zero-denominator ratio metric to resolve to 0, got %v", got)
	}

	snapshots := BuildMetricSnapshots(states, now.Add(5*time.Minute))
	if len(snapshots) == 0 {
		t.Fatal("expected snapshots")
	}
	if got := snapshotDelta(snapshots, "trend_7d", "world", worldPlaceID, now); got != 1 {
		t.Fatalf("expected world trend delta 1, got %v", got)
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
	views := RefreshableMetricSnapshotViews()
	if len(views) != 2 {
		t.Fatalf("expected 2 refreshable snapshot views, got %d", len(views))
	}
	if !stringsContain(views["metric_snapshot_day_mv"], "REFRESH EVERY 15 MINUTE") {
		t.Fatal("expected day snapshot view refresh cadence")
	}
	if !stringsContain(views["metric_snapshot_7d_mv"], "window_grain = '7d'") {
		t.Fatal("expected 7d snapshot filter")
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

func hasContribution(contributions []Contribution, metricID, grain, subjectID string) bool {
	for _, contribution := range contributions {
		if contribution.MetricID == metricID && contribution.SubjectGrain == grain && contribution.SubjectID == subjectID {
			return true
		}
	}
	return false
}

func contributionValueFor(contributions []Contribution, metricID, grain, subjectID string) float64 {
	for _, contribution := range contributions {
		if contribution.MetricID == metricID && contribution.SubjectGrain == grain && contribution.SubjectID == subjectID {
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
		if state.MetricID == metricID && state.SubjectGrain == grain && state.SubjectID == subjectID && sameDay(state.WindowStart, occurredAt) {
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

func hasSnapshot(rows []SnapshotRow, metricID, grain, subjectID string) bool {
	for _, row := range rows {
		if row.MetricID == metricID && row.SubjectGrain == grain && row.SubjectID == subjectID {
			return true
		}
	}
	return false
}

func sameDay(a, b time.Time) bool {
	a = a.UTC()
	b = b.UTC()
	return a.Year() == b.Year() && a.Month() == b.Month() && a.Day() == b.Day()
}

func stringsContain(s, fragment string) bool {
	return strings.Contains(s, fragment)
}
