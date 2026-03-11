package discovery

import (
	"testing"
	"time"
)

func TestFingerprintProbeGeneration(t *testing.T) {
	now := time.Date(2026, time.March, 11, 0, 0, 0, 0, time.UTC)
	probe := FingerprintProbe{
		CatalogID:            "catalog:fingerprint:discovery:ckan-action-api",
		ProbeName:            "CKAN Action API",
		IntegrationArchetype: "catalog_ckan",
		ProbePatterns:        []string{"/api/3/action/package_search", "ckan"},
	}
	got := GenerateFingerprintCandidates(probe, []string{
		"https://data.example.org/api/3/action/package_search?rows=1",
		"https://data.example.org/api/3/action/package_search?rows=1&utm_source=test",
		"https://other.example.org/about",
	}, now)
	if len(got) != 1 {
		t.Fatalf("expected 1 deduplicated fingerprint candidate, got %d", len(got))
	}
	if got[0].CandidateURL != "https://data.example.org/api/3/action/package_search?rows=1" {
		t.Fatalf("unexpected canonical candidate url %q", got[0].CandidateURL)
	}
	if got[0].DetectedPlatform != "CKAN Action API" {
		t.Fatalf("expected detected platform CKAN Action API, got %q", got[0].DetectedPlatform)
	}
	if got[0].IntegrationArchetype != "catalog_ckan" {
		t.Fatalf("expected integration archetype catalog_ckan, got %q", got[0].IntegrationArchetype)
	}
	if got[0].ReviewStatus != "review_required" {
		t.Fatalf("expected review_required candidates, got %q", got[0].ReviewStatus)
	}
}
