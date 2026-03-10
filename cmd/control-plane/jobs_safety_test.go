package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunIngestSafetySecurityExecutesFixtureStatements(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query().Get("query"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	ctx := context.WithValue(context.Background(), jobOptionsContextKey{}, jobOptions{})
	if err := runIngestSafetySecurity(ctx); err != nil {
		t.Fatalf("runIngestSafetySecurity: %v", err)
	}
	joined := strings.Join(queries, "\n")
	for _, want := range []string{
		"INSERT INTO meta.metric_registry",
		"INSERT INTO silver.dim_entity",
		"INSERT INTO silver.fact_observation",
		"INSERT INTO silver.bridge_entity_place",
		"INSERT INTO silver.metric_contribution",
		"INSERT INTO gold.metric_snapshot",
		"INSERT INTO ops.job_run",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected query %q, got %s", want, joined)
		}
	}
	for _, marker := range []string{"opensanctions:atlas-maritime-holdings", "firms:new-orleans:20260310T0330Z", "CVE-2025-1001"} {
		if !strings.Contains(joined, marker) {
			t.Fatalf("expected source marker %q in generated queries", marker)
		}
	}
}

func TestRunCommandSupportsFixtureAggregateForSafetyJob(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query().Get("query"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	stdout := &strings.Builder{}
	stderr := &strings.Builder{}
	code := run([]string{"run-once", "--job", ingestSafetySecurityJobName, "--source-id", "fixture:safety"}, stdout, stderr)
	if code != 0 {
		t.Fatalf("expected zero exit code, got %d stderr=%s", code, stderr.String())
	}
	joined := strings.Join(queries, "\n")
	for _, marker := range []string{"opensanctions:blue-gulf-bunkering", "firms:tokyo-bay:20260310T0300Z", "kev:CVE-2025-1777"} {
		if !strings.Contains(joined, marker) {
			t.Fatalf("expected aggregate source marker %q, got %s", marker, joined)
		}
	}
	if !strings.Contains(stdout.String(), ingestSafetySecurityJobName) {
		t.Fatalf("expected completion output, got %s", stdout.String())
	}
}
