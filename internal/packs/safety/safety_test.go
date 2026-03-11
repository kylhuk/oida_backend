package safety

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBuildIngestPlanDefaultSources(t *testing.T) {
	now := time.Date(2026, 3, 10, 13, 0, 0, 0, time.UTC)
	plan, err := BuildIngestPlan(context.Background(), Options{Now: now})
	if err != nil {
		t.Fatalf("BuildIngestPlan: %v", err)
	}
	if len(plan.ExecutedSources) != 4 {
		t.Fatalf("expected 4 executed sources, got %d", len(plan.ExecutedSources))
	}
	for _, want := range []string{SourceOpenSanctions, SourceNASAFIRMS, SourceNOAAHazards, SourceKEV} {
		if !contains(plan.ExecutedSources, want) {
			t.Fatalf("expected source %q in executed sources: %#v", want, plan.ExecutedSources)
		}
	}
	if len(plan.Entities) == 0 || len(plan.Observations) == 0 || len(plan.Contributions) == 0 || len(plan.Snapshots) == 0 {
		t.Fatal("expected entities, observations, contributions, and snapshots")
	}
	for _, metricID := range []string{
		"coastal_flood_risk_index",
		"cyber_exposure_index",
		"fire_hotspot_score",
		"infrastructure_vulnerability_score",
		"sanctions_exposure_score",
		"weather_event_impact_score",
	} {
		if !hasMetric(plan, metricID) {
			t.Fatalf("missing metric %q", metricID)
		}
		if !hasRegistryMetric(plan, metricID) {
			t.Fatalf("missing metric registry row %q", metricID)
		}
	}
	for _, legacyID := range []string{"fire_hotspot", "sanctions_exposure"} {
		if !hasRegistryMetric(plan, legacyID) {
			t.Fatalf("missing legacy metric alias %q", legacyID)
		}
	}
	for _, observationType := range []string{"sanctions_match", "fire_hotspot", "known_exploited_vulnerability", "coastal_hazard_bulletin"} {
		if !hasObservationType(plan, observationType) {
			t.Fatalf("missing observation type %q", observationType)
		}
	}
	if !hasEntityGraphRelation(plan, "beneficial_owner") {
		t.Fatal("expected OpenSanctions graph relation evidence")
	}

	statements, err := plan.SQLStatements()
	if err != nil {
		t.Fatalf("SQLStatements: %v", err)
	}
	joined := strings.Join(statements, "\n---\n")
	for _, fragment := range []string{
		"INSERT INTO meta.metric_registry",
		"INSERT INTO silver.dim_entity",
		"INSERT INTO silver.fact_observation",
		"INSERT INTO silver.bridge_entity_place",
		"INSERT INTO silver.metric_contribution",
		"INSERT INTO gold.metric_snapshot",
		"INSERT INTO gold.hotspot_snapshot",
		"INSERT INTO gold.cross_domain_snapshot",
	} {
		if !strings.Contains(joined, fragment) {
			t.Fatalf("expected %q in generated SQL", fragment)
		}
	}
	writeSafetyEvidenceFile(t, ".sisyphus/evidence/task-25-safety.txt", []byte(joined))
}

func TestBuildIngestPlanWeakMappingsStayExplicit(t *testing.T) {
	now := time.Date(2026, 3, 10, 13, 0, 0, 0, time.UTC)
	plan, err := BuildIngestPlan(context.Background(), Options{Now: now, SourceID: SourceKEV})
	if err != nil {
		t.Fatalf("BuildIngestPlan: %v", err)
	}
	observation := findObservation(plan, "kev:CVE-2025-1777")
	if observation == nil {
		t.Fatal("expected weak-mapping KEV observation")
	}
	sector, _ := observation.Attrs["sector_mapping"].(map[string]any)
	location, _ := observation.Attrs["location_resolution"].(map[string]any)
	if sector["status"] != "review" {
		t.Fatalf("expected sector mapping review status, got %#v", sector)
	}
	if location["status"] != "coarse" {
		t.Fatalf("expected coarse location resolution, got %#v", location)
	}
	if observation.PlaceID != "plc:us" {
		t.Fatalf("expected coarse place fallback plc:us, got %s", observation.PlaceID)
	}
	encoded, err := json.MarshalIndent(map[string]any{
		"observation_id":      observation.ObservationID,
		"place_id":            observation.PlaceID,
		"sector_mapping":      sector,
		"location_resolution": location,
	}, "", "  ")
	if err != nil {
		t.Fatalf("marshal edge evidence: %v", err)
	}
	writeSafetyEvidenceFile(t, ".sisyphus/evidence/task-25-safety-edge.txt", encoded)
}

func contains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

func hasMetric(plan Plan, metricID string) bool {
	for _, item := range plan.Snapshots {
		if item.MetricID == metricID {
			return true
		}
	}
	return false
}

func hasRegistryMetric(plan Plan, metricID string) bool {
	for _, item := range plan.MetricRegistry {
		if item.MetricID == metricID {
			return true
		}
	}
	return false
}

func hasObservationType(plan Plan, observationType string) bool {
	for _, item := range plan.Observations {
		if item.ObservationType == observationType {
			return true
		}
	}
	return false
}

func hasEntityGraphRelation(plan Plan, relation string) bool {
	for _, item := range plan.Entities {
		relations, ok := item.Attrs["graph_relations"].([]map[string]any)
		if !ok {
			continue
		}
		for _, candidate := range relations {
			if candidate["relation"] == relation {
				return true
			}
		}
	}
	return false
}

func findObservation(plan Plan, rawID string) *ObservationRecord {
	for idx := range plan.Observations {
		for _, evidence := range plan.Observations[idx].Evidence {
			if evidence.RawID == rawID {
				return &plan.Observations[idx]
			}
		}
	}
	return nil
}

func writeSafetyEvidenceFile(tb testing.TB, relativePath string, content []byte) {
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
