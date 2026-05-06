package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"global-osint-backend/internal/discovery"
	"global-osint-backend/internal/migrate"
)

func TestDiscoveryCandidatesStayReviewRequired(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := requestSQL(r)
		queries = append(queries, query)
		if strings.Contains(query, "FROM meta.discovery_candidate FINAL") {
			_, _ = w.Write([]byte(""))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	t.Setenv(controlPlaneCompiledCatalogPathEnv, filepath.Join(mustRepoRoot(t), "seed", "source_catalog_compiled.json"))
	if err := runFingerprintProbeGenerationTick(context.Background()); err != nil {
		t.Fatalf("runFingerprintProbeGenerationTick: %v", err)
	}
	joined := strings.Join(queries, "\n")
	if !strings.Contains(joined, "INSERT INTO meta.discovery_candidate") {
		t.Fatalf("expected discovery candidate insert, got %s", joined)
	}
	if !strings.Contains(joined, "'review_required'") {
		t.Fatalf("expected inserted candidate to remain review_required, got %s", joined)
	}
	if strings.Contains(joined, "T00:") || strings.Contains(joined, "Z',") {
		t.Fatalf("expected discovery candidate timestamp to use ClickHouse-friendly millisecond SQL format, got %s", joined)
	}
	if !strings.Contains(joined, "NULL AS materialized_source_id") {
		t.Fatalf("expected inserted candidate to keep materialized_source_id null, got %s", joined)
	}
	if strings.Contains(joined, "approved_enabled") {
		t.Fatalf("expected discovery candidate persistence to avoid runnable source states, got %s", joined)
	}
	if !strings.Contains(joined, `"kind":"fingerprint_probe"`) || !strings.Contains(joined, `"platform":"`) {
		t.Fatalf("expected fingerprint classifier metadata in attrs, got %s", joined)
	}
}

func TestFamilyCandidateReviewGate(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := requestSQL(r)
		queries = append(queries, query)
		if strings.Contains(query, "FROM meta.discovery_candidate FINAL") {
			_, _ = w.Write([]byte(""))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	candidates := []discovery.FamilyCandidate{{
		CandidateID:          "candidate:family:1",
		CatalogID:            "catalog:family:recurring-national-and-subnational-source-families:open-data-portals",
		CandidateName:        "Open-data portals - france (admin0)",
		CandidateURL:         "https://data.example.fr/catalog",
		IntegrationArchetype: "catalog_ckan",
		DetectedPlatform:     "Open-data portals",
		Geography:            "france",
		AdminLevel:           "admin0",
		ChildSource: discovery.GeneratedChildSource{
			SourceID:             "generated:family:france-admin0",
			Domain:               "data.example.fr",
			Entrypoints:          []string{"https://data.example.fr/catalog"},
			TransportType:        "http",
			IntegrationArchetype: "catalog_ckan",
			FormatHint:           "json",
			ParserID:             "parser:json",
			SourceClass:          "family_generated",
			RefreshStrategy:      "scheduled",
			CrawlStrategy:        "delta",
			ExpectedPlaceTypes:   []string{"admin0"},
			Geography:            "france",
			AdminLevel:           "admin0",
		},
		ReviewStatus:         "review_required",
		MaterializedSourceID: "generated:family:france-admin0",
		DiscoveredAt:         time.Date(2026, time.March, 11, 0, 0, 0, 0, time.UTC),
	}}
	if err := persistFamilyCandidates(context.Background(), migrate.NewHTTPRunner(server.URL), candidates, time.Date(2026, time.March, 11, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("persistFamilyCandidates: %v", err)
	}
	joined := strings.Join(queries, "\n")
	if !strings.Contains(joined, "INSERT INTO meta.discovery_candidate") {
		t.Fatalf("expected family-generated candidate insert, got %s", joined)
	}
	if !strings.Contains(joined, "'review_required'") {
		t.Fatalf("expected family-generated candidate to stay review-gated, got %s", joined)
	}
	if strings.Contains(joined, "T00:") || strings.Contains(joined, "Z',") {
		t.Fatalf("expected family-generated candidate timestamp to use ClickHouse-friendly millisecond SQL format, got %s", joined)
	}
	if strings.Contains(joined, "approved_enabled") || strings.Contains(joined, "approved_disabled") {
		t.Fatalf("expected family generation to avoid source lifecycle materialization, got %s", joined)
	}
	if strings.Contains(joined, "NULL AS materialized_source_id") {
		t.Fatalf("expected family-generated candidate to carry a stable materialized source id, got %s", joined)
	}
	if !strings.Contains(joined, `"geography":"france"`) || !strings.Contains(joined, `"admin_level":"admin0"`) || !strings.Contains(joined, `"child_source":`) {
		t.Fatalf("expected family-generated candidate attrs to include family child-source metadata, got %s", joined)
	}
}

func TestFamilyTemplateGeneration(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := requestSQL(r)
		queries = append(queries, query)
		if strings.Contains(query, "FROM meta.discovery_candidate FINAL") {
			_, _ = w.Write([]byte(""))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	t.Setenv(controlPlaneCompiledCatalogPathEnv, filepath.Join(mustRepoRoot(t), "seed", "source_catalog_compiled.json"))
	if err := runFamilyTemplateGenerationTick(context.Background()); err != nil {
		t.Fatalf("runFamilyTemplateGenerationTick: %v", err)
	}
	joined := strings.Join(queries, "\n")
	if !strings.Contains(joined, "INSERT INTO meta.discovery_candidate") {
		t.Fatalf("expected family template generation to insert discovery candidates, got %s", joined)
	}
	for _, fragment := range []string{`"kind":"family_template"`, `"transport_type":"http"`, `"source_class":"family_generated"`} {
		if !strings.Contains(joined, fragment) {
			t.Fatalf("expected family template generation attrs to include %q, got %s", fragment, joined)
		}
	}
}

func requestSQL(r *http.Request) string {
	query := r.URL.Query().Get("query")
	if query != "" {
		return query
	}
	body, _ := io.ReadAll(r.Body)
	return string(body)
}
