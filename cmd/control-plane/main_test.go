package main

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunOnceUsageListsSupportedJobs(t *testing.T) {
	usage := runOnceUsage()
	for _, jobName := range []string{
		"ingest-aviation",
		"ingest-geopolitical",
		"ingest-maritime",
		"ingest-safety-security",
		"ingest-space",
		"noop",
		"place-build",
		"promote",
	} {
		if !strings.Contains(usage, jobName) {
			t.Fatalf("expected run-once help to list %s, got %s", jobName, usage)
		}
	}
}

func TestRunOnceHelp(t *testing.T) {
	usage := runOnceUsage()
	for _, jobName := range []string{"ingest-geopolitical", "ingest-safety-security"} {
		if !strings.Contains(usage, jobName) {
			t.Fatalf("expected run-once help to list %s, got %s", jobName, usage)
		}
	}
}

func TestRunAutomaticSyncTick(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		if strings.Contains(query, "FROM meta.source_registry FINAL") {
			_, _ = w.Write([]byte(mockSourceRegistryJSONLines(extractQuotedValues(query))))
			return
		}
		if strings.Contains(query, "SELECT count() FROM ops.crawl_frontier") {
			_, _ = w.Write([]byte("0\n"))
			return
		}
		if strings.Contains(query, "SELECT max(next_fetch_at) FROM ops.crawl_frontier") {
			_, _ = w.Write([]byte("\\N\n"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	t.Setenv("ACLED_API_KEY", "demo-key")
	t.Setenv(controlPlaneCompiledCatalogPathEnv, filepath.Join(mustRepoRoot(t), "seed", "source_catalog_compiled.json"))
	if err := runAutomaticSyncTick(nil); err != nil {
		t.Fatalf("runAutomaticSyncTick: %v", err)
	}
	joined := strings.Join(queries, "\n")
	for _, want := range []string{"INSERT INTO meta.discovery_candidate", "orchestrated geopolitical http sources", "orchestrated safety/security http sources"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected automatic sync tick to run %q, got %s", want, joined)
		}
	}
}
