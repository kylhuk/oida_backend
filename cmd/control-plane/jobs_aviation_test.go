package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunIngestAviationExecutesPackStatements(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query().Get("query"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	ctx := context.WithValue(context.Background(), jobOptionsContextKey{}, jobOptions{SourceID: "fixture:aviation"})
	if err := runIngestAviation(ctx); err != nil {
		t.Fatalf("runIngestAviation: %v", err)
	}
	if len(queries) != 7 {
		t.Fatalf("expected 7 queries including job log insert, got %d", len(queries))
	}
	joined := strings.Join(queries, "\n")
	for _, want := range []string{
		"INSERT INTO meta.metric_registry",
		"INSERT INTO silver.dim_entity",
		"INSERT INTO silver.fact_track_point",
		"INSERT INTO silver.fact_track_segment",
		"INSERT INTO silver.fact_event",
		"INSERT INTO gold.metric_snapshot",
		"military_likelihood_score",
		"route_irregularity_score",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected query fragment %q, got %s", want, joined)
		}
	}
	if !strings.Contains(queries[len(queries)-1], "INSERT INTO ops.job_run") {
		t.Fatalf("expected final query to insert job log, got %s", queries[len(queries)-1])
	}
	if !strings.Contains(joined, "ops.job_run") {
		t.Fatalf("expected job log insert, got %s", joined)
	}
}
