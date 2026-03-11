package discovery

import (
	"testing"
	"time"
)

func TestFamilyTemplateGeneration(t *testing.T) {
	now := time.Date(2026, time.March, 11, 0, 0, 0, 0, time.UTC)
	template := FamilyTemplate{
		CatalogID:            "catalog:family:recurring-national-and-subnational-source-families:open-data-portals",
		Name:                 "Open-data portals",
		Outputs:              "datasets, APIs, metadata, files",
		IntegrationArchetype: "deferred_transport",
		Tags:                 []string{"catalog", "national", "subnational"},
	}
	got := GenerateFamilyCandidates(template, []FamilyScope{
		{Geography: "france", AdminLevel: "admin0", BaseURL: "https://data.example.fr/catalog?utm_source=test"},
		{Geography: "france", AdminLevel: "admin0", BaseURL: "https://data.example.fr/catalog"},
		{Geography: "berlin", AdminLevel: "admin1", BaseURL: "https://daten.example.de/portal"},
	}, now)
	if len(got) != 2 {
		t.Fatalf("expected 2 deduplicated family candidates, got %d", len(got))
	}
	if got[0].ReviewStatus != "review_required" || got[1].ReviewStatus != "review_required" {
		t.Fatalf("expected family candidates to remain review_required, got %#v", got)
	}
	if got[0].MaterializedSourceID != "" || got[1].MaterializedSourceID != "" {
		t.Fatalf("expected family candidates to avoid materialized source ids, got %#v", got)
	}
}

func TestFamilyTemplateGenerationFromMembers(t *testing.T) {
	now := time.Date(2026, time.March, 11, 0, 0, 0, 0, time.UTC)
	template := FamilyTemplate{
		CatalogID:            "catalog:family:recurring-national-and-subnational-source-families:open-data-portals",
		Name:                 "Open-data portals",
		Scope:                "national/subnational",
		Outputs:              "datasets, APIs, metadata, files",
		IntegrationArchetype: "deferred_transport",
		Tags:                 []string{"catalog", "national", "subnational"},
	}
	members := []FamilyMember{
		{CatalogID: "catalog:concrete:example:data-gov", Name: "Data.gov Catalog", Scope: "United States national", Tags: []string{"catalog", "national", "official"}, CandidateURL: "https://data.gov/catalog"},
		{CatalogID: "catalog:concrete:example:city-portal", Name: "City Portal", Scope: "Berlin subnational", Tags: []string{"catalog", "subnational", "open-data"}, CandidateURL: "https://daten.example.de/portal"},
		{CatalogID: "catalog:concrete:example:off-topic", Name: "Weather Feed", Scope: "global", Tags: []string{"weather", "alerts"}, CandidateURL: "https://weather.example.test/feed"},
	}
	got := GenerateFamilyCandidatesFromMembers(template, members, now)
	if len(got) != 2 {
		t.Fatalf("expected 2 family candidates generated from real members, got %d", len(got))
	}
	if got[0].ReviewStatus != "review_required" || got[1].ReviewStatus != "review_required" {
		t.Fatalf("expected generated family candidates to remain review_required, got %#v", got)
	}
}
