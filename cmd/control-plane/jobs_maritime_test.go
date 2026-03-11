package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunIngestMaritimeExecutesPackStatements(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query().Get("query"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	ctx := context.WithValue(context.Background(), jobOptionsContextKey{}, jobOptions{})
	if err := runIngestMaritime(ctx); err != nil {
		t.Fatalf("runIngestMaritime: %v", err)
	}
	joined := strings.Join(queries, "\n")
	for _, want := range []string{
		"INSERT INTO meta.metric_registry",
		"INSERT INTO silver.dim_entity",
		"INSERT INTO silver.fact_track_point",
		"INSERT INTO silver.fact_track_segment",
		"INSERT INTO silver.fact_event",
		"INSERT INTO gold.metric_snapshot",
		"INSERT INTO gold.hotspot_snapshot",
		"INSERT INTO gold.cross_domain_snapshot",
		"INSERT INTO ops.job_run",
		"ais_dark_hours_sum",
		"anchorage_dwell_hours",
		"flag_registry_mismatch_score",
		"port_gap_hours",
		"route_deviation_score",
		"shadow_fleet_score",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected query fragment %q, got %s", want, joined)
		}
	}
}
