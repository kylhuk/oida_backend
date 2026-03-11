package geopolitical

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type metricFixture struct {
	MetricIDs []string `json:"metric_ids"`
}

func TestBuildIngestPlanDefaultSources(t *testing.T) {
	now := time.Date(2026, 3, 10, 13, 0, 0, 0, time.UTC)
	plan, err := BuildIngestPlan(context.Background(), Options{Now: now})
	if err != nil {
		t.Fatalf("BuildIngestPlan: %v", err)
	}
	if len(plan.ExecutedSources) != 2 {
		t.Fatalf("expected 2 executed public sources, got %d", len(plan.ExecutedSources))
	}
	if !contains(plan.ExecutedSources, SourceGDELT) || !contains(plan.ExecutedSources, SourceReliefWeb) {
		t.Fatalf("expected %s and %s in executed sources: %#v", SourceGDELT, SourceReliefWeb, plan.ExecutedSources)
	}
	if !hasDisabledReason(plan.DisabledSources, SourceACLED, "missing credentials") {
		t.Fatalf("expected %s disabled without credentials", SourceACLED)
	}
	if len(plan.Events) == 0 || len(plan.Contributions) == 0 || len(plan.Snapshots) == 0 {
		t.Fatal("expected events, contributions, and snapshots")
	}
	fixture := loadMetricFixture(t, "testdata/fixture_geopolitical_metrics.json")
	for _, metricID := range fixture.MetricIDs {
		if !hasMetricRegistry(plan, metricID) {
			t.Fatalf("missing registry metric %q", metricID)
		}
		if !hasMetricContribution(plan, metricID) {
			t.Fatalf("missing contribution metric %q", metricID)
		}
		if !hasMetricSnapshot(plan, metricID) {
			t.Fatalf("missing snapshot metric %q", metricID)
		}
	}
	if !hasEventWithCrossLink(plan, "evt:geo:seed-gdelt:gdelt-1001") {
		t.Fatal("expected normalized cross-source links on GDELT event")
	}
	if !hasSourceEvent(plan, "reliefweb:2003") {
		t.Fatal("expected sanction fixture event in normalized plan")
	}
	if !hasEventPlaceRelation(plan, "related") {
		t.Fatal("expected related place links for spillover events")
	}

	statements, err := plan.SQLStatements()
	if err != nil {
		t.Fatalf("SQLStatements: %v", err)
	}
	joined := strings.Join(statements, "\n---\n")
	for _, fragment := range []string{
		"INSERT INTO meta.metric_registry",
		"INSERT INTO silver.fact_event",
		"INSERT INTO silver.metric_contribution",
		"INSERT INTO gold.metric_snapshot",
		"INSERT INTO gold.hotspot_snapshot",
		"INSERT INTO gold.cross_domain_snapshot",
	} {
		if !strings.Contains(joined, fragment) {
			t.Fatalf("expected %q in generated SQL", fragment)
		}
	}
	writeGeopoliticalEvidenceFile(t, ".sisyphus/evidence/task-20-geopolitical.txt", []byte(joined))
}

func TestBuildIngestPlanACLEDCredentialGating(t *testing.T) {
	now := time.Date(2026, 3, 10, 13, 0, 0, 0, time.UTC)
	withoutKey, err := BuildIngestPlan(context.Background(), Options{Now: now, SourceID: SourceACLED})
	if err != nil {
		t.Fatalf("BuildIngestPlan without key: %v", err)
	}
	if len(withoutKey.ExecutedSources) != 0 {
		t.Fatalf("expected no ACLED execution without key, got %#v", withoutKey.ExecutedSources)
	}
	if !hasDisabledReason(withoutKey.DisabledSources, SourceACLED, ACLEDKeyEnv) {
		t.Fatalf("expected disabled reason mentioning %s", ACLEDKeyEnv)
	}

	withKey, err := BuildIngestPlan(context.Background(), Options{Now: now, SourceID: SourceACLED, ACLEDKey: "fixture-key"})
	if err != nil {
		t.Fatalf("BuildIngestPlan with key: %v", err)
	}
	if len(withKey.ExecutedSources) != 1 || withKey.ExecutedSources[0] != SourceACLED {
		t.Fatalf("expected ACLED to execute with fixture key, got %#v", withKey.ExecutedSources)
	}
	if len(withKey.Events) == 0 {
		t.Fatal("expected ACLED fixture events with credentials")
	}
	writeGeopoliticalEvidenceFile(t, ".sisyphus/evidence/task-20-geopolitical-edge.txt", []byte("without_key=disabled\nwith_key=loaded\n"))
}

func contains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

func hasDisabledReason(items []DisabledSource, sourceID, want string) bool {
	for _, item := range items {
		if item.SourceID == sourceID && strings.Contains(item.Reason, want) {
			return true
		}
	}
	return false
}

func loadMetricFixture(tb testing.TB, relativePath string) metricFixture {
	tb.Helper()
	payload, err := os.ReadFile(filepath.Join(mustRepoRoot(tb), "internal", "packs", "geopolitical", relativePath))
	if err != nil {
		tb.Fatalf("read metric fixture: %v", err)
	}
	var fixture metricFixture
	if err := json.Unmarshal(payload, &fixture); err != nil {
		tb.Fatalf("unmarshal metric fixture: %v", err)
	}
	return fixture
}

func hasMetricRegistry(plan Plan, metricID string) bool {
	for _, item := range plan.MetricRegistry {
		if item.MetricID == metricID {
			return true
		}
	}
	return false
}

func hasMetricContribution(plan Plan, metricID string) bool {
	for _, item := range plan.Contributions {
		if item.MetricID == metricID {
			return true
		}
	}
	return false
}

func hasMetricSnapshot(plan Plan, metricID string) bool {
	for _, item := range plan.Snapshots {
		if item.MetricID == metricID {
			return true
		}
	}
	return false
}

func hasSourceEvent(plan Plan, sourceEventID string) bool {
	for _, item := range plan.Events {
		if item.Attrs["source_event_id"] == sourceEventID {
			return true
		}
	}
	return false
}

func hasEventWithCrossLink(plan Plan, eventID string) bool {
	for _, item := range plan.Events {
		if item.EventID != eventID {
			continue
		}
		links, ok := item.Attrs["cross_source_links"].([]map[string]any)
		return ok && len(links) > 0
	}
	return false
}

func hasEventPlaceRelation(plan Plan, relation string) bool {
	for _, item := range plan.EventPlaces {
		if item.RelationType == relation {
			return true
		}
	}
	return false
}

func writeGeopoliticalEvidenceFile(tb testing.TB, relativePath string, content []byte) {
	tb.Helper()
	artifactPath := filepath.Join(mustRepoRoot(tb), relativePath)
	if err := os.MkdirAll(filepath.Dir(artifactPath), 0o755); err != nil {
		tb.Fatalf("mkdir evidence dir: %v", err)
	}
	if err := os.WriteFile(artifactPath, content, 0o644); err != nil {
		tb.Fatalf("write evidence file: %v", err)
	}
}

func mustRepoRoot(tb testing.TB) string {
	tb.Helper()
	wd, err := os.Getwd()
	if err != nil {
		tb.Fatalf("getwd: %v", err)
	}
	for dir := wd; dir != filepath.Dir(dir); dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
	}
	tb.Fatal("unable to locate repo root")
	return ""
}
