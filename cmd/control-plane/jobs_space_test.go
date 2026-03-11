package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunIngestSpaceExecutesPackStatements(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query().Get("query"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	ctx := context.WithValue(context.Background(), jobOptionsContextKey{}, jobOptions{})
	if err := runIngestSpace(ctx); err != nil {
		t.Fatalf("runIngestSpace: %v", err)
	}
	joined := strings.Join(queries, "\n")
	for _, want := range []string{
		"INSERT INTO meta.metric_registry",
		"INSERT INTO silver.dim_entity",
		"INSERT INTO silver.fact_track_point",
		"INSERT INTO silver.fact_event",
		"INSERT INTO gold.metric_snapshot",
		"INSERT INTO gold.hotspot_snapshot",
		"INSERT INTO gold.cross_domain_snapshot",
		"INSERT INTO ops.job_run",
		"conjunction_risk_score",
		"coverage_gap_hours",
		"maneuver_frequency_score",
		"orbital_decay_indicator",
		"overpass_density_score",
		"revisit_capability_index",
		"satellite_health_index",
		"overpass_density",
		"conjunction_risk",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected query fragment %q, got %s", want, joined)
		}
	}
}
