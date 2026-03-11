package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"global-osint-backend/internal/discovery"
	"global-osint-backend/internal/migrate"
)

func TestDiscoveryCandidatesStayReviewRequired(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		if strings.Contains(query, "FROM meta.discovery_candidate FINAL") {
			_, _ = w.Write([]byte(""))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	candidates := []discovery.FingerprintCandidate{{
		CandidateID:          "candidate:test:1",
		CatalogID:            "catalog:fingerprint:discovery:ckan-action-api",
		CandidateName:        "data.example.org",
		CandidateURL:         "https://data.example.org/api/3/action/package_search",
		IntegrationArchetype: "catalog_ckan",
		DetectedPlatform:     "CKAN Action API",
		ReviewStatus:         "review_required",
		MaterializedSourceID: "",
		DiscoveredAt:         time.Date(2026, time.March, 11, 0, 0, 0, 0, time.UTC),
	}}
	if err := persistFingerprintCandidates(context.Background(), migrate.NewHTTPRunner(server.URL), candidates, time.Date(2026, time.March, 11, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("persistFingerprintCandidates: %v", err)
	}
	joined := strings.Join(queries, "\n")
	if !strings.Contains(joined, "INSERT INTO meta.discovery_candidate") {
		t.Fatalf("expected discovery candidate insert, got %s", joined)
	}
	if !strings.Contains(joined, "'review_required'") {
		t.Fatalf("expected inserted candidate to remain review_required, got %s", joined)
	}
	if !strings.Contains(joined, ",NULL,") {
		t.Fatalf("expected inserted candidate to keep materialized_source_id null, got %s", joined)
	}
	if strings.Contains(joined, "approved_enabled") {
		t.Fatalf("expected discovery candidate persistence to avoid runnable source states, got %s", joined)
	}
}

func TestGeneratedChildSourcesRequireApproval(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
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
		IntegrationArchetype: "deferred_transport",
		DetectedPlatform:     "Open-data portals",
		ReviewStatus:         "review_required",
		MaterializedSourceID: "",
		DiscoveredAt:         time.Date(2026, time.March, 11, 0, 0, 0, 0, time.UTC),
	}}
	if err := persistFamilyCandidates(context.Background(), migrate.NewHTTPRunner(server.URL), candidates, time.Date(2026, time.March, 11, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("persistFamilyCandidates: %v", err)
	}
	joined := strings.Join(queries, "\n")
	if !strings.Contains(joined, "INSERT INTO meta.discovery_candidate") {
		t.Fatalf("expected family-generated candidate insert, got %s", joined)
	}
	if !strings.Contains(joined, "'review_required'") || !strings.Contains(joined, ",NULL,") {
		t.Fatalf("expected family-generated candidate to stay review-gated with null materialized source, got %s", joined)
	}
	if strings.Contains(joined, "approved_enabled") || strings.Contains(joined, "approved_disabled") {
		t.Fatalf("expected family generation to avoid source lifecycle materialization, got %s", joined)
	}
}
