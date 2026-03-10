package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunIngestGeopoliticalExecutesFixtureStatements(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query().Get("query"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	t.Setenv("ACLED_API_KEY", "")
	ctx := context.WithValue(context.Background(), jobOptionsContextKey{}, jobOptions{})
	if err := runIngestGeopolitical(ctx); err != nil {
		t.Fatalf("runIngestGeopolitical: %v", err)
	}
	joined := strings.Join(queries, "\n")
	for _, want := range []string{
		"INSERT INTO meta.metric_registry",
		"INSERT INTO silver.fact_event",
		"INSERT INTO silver.bridge_event_entity",
		"INSERT INTO silver.bridge_event_place",
		"INSERT INTO silver.bridge_entity_place",
		"INSERT INTO silver.metric_contribution",
		"INSERT INTO gold.metric_snapshot",
		"INSERT INTO ops.job_run",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected query %q, got %s", want, joined)
		}
	}
	if strings.Contains(joined, "acled:3001") {
		t.Fatal("did not expect ACLED fixture inserts without credentials")
	}
}

func TestRunCommandSupportsSourceIDForGeopoliticalJob(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query().Get("query"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	t.Setenv("ACLED_API_KEY", "fixture-key")
	stdout := &strings.Builder{}
	stderr := &strings.Builder{}
	code := run([]string{"run-once", "--job", ingestGeopoliticalJobName, "--source-id", "fixture:acled"}, stdout, stderr)
	if code != 0 {
		t.Fatalf("expected zero exit code, got %d stderr=%s", code, stderr.String())
	}
	joined := strings.Join(queries, "\n")
	if !strings.Contains(joined, "acled:3001") {
		t.Fatalf("expected ACLED fixture query with source-id scoping, got %s", joined)
	}
	if strings.Contains(joined, "gdelt:1001") || strings.Contains(joined, "reliefweb:2001") {
		t.Fatalf("expected source-id to scope execution to ACLED, got %s", joined)
	}
	if !strings.Contains(stdout.String(), ingestGeopoliticalJobName) {
		t.Fatalf("expected completion output, got %s", stdout.String())
	}
}

func TestRunCommandExecutesGeopoliticalJobWithoutSourceID(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query().Get("query"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	t.Setenv("ACLED_API_KEY", "")
	stdout := &strings.Builder{}
	stderr := &strings.Builder{}
	code := run([]string{"run-once", "--job", ingestGeopoliticalJobName}, stdout, stderr)
	if code != 0 {
		t.Fatalf("expected zero exit code, got %d stderr=%s", code, stderr.String())
	}
	joined := strings.Join(queries, "\n")
	if !strings.Contains(joined, "gdelt:1001") || !strings.Contains(joined, "reliefweb:2001") {
		t.Fatalf("expected default public sources to execute, got %s", joined)
	}
	if strings.Contains(joined, "acled:3001") {
		t.Fatalf("did not expect ACLED fixture without credentials, got %s", joined)
	}
	if !strings.Contains(stdout.String(), ingestGeopoliticalJobName) {
		t.Fatalf("expected completion output, got %s", stdout.String())
	}
}
