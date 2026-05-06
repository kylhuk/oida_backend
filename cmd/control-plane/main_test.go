package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunOnceUsageListsSupportedJobs(t *testing.T) {
	usage := runOnceUsage()
	for _, jobName := range []string{
		"backup-clickhouse",
		"geoboundaries-sync",
		"geonames-sync",
		"ingest-aviation",
		"ingest-geopolitical",
		"ingest-maritime",
		"ingest-safety-security",
		"ingest-space",
		"noop",
		"pipeline-execute",
		"place-build",
		"promote",
		"restore-clickhouse",
		"retention-materialize",
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
	for _, flagName := range []string{"--window-start", "--window-end", "--delta-only"} {
		if !strings.Contains(usage, flagName) {
			t.Fatalf("expected run-once help to list %s, got %s", flagName, usage)
		}
	}
}

func TestRunAutomaticSyncTick(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := requestSQL(r)
		queries = append(queries, query)
		if strings.Contains(query, "FROM ops.job_run FINAL") && strings.Contains(query, "job_type = 'promote'") {
			_, _ = w.Write([]byte("{\"last_successful_promote_at\":null}\n"))
			return
		}
		if strings.Contains(query, "FROM ops.parse_checkpoint FINAL") && strings.Contains(query, "UNION ALL") {
			_, _ = w.Write([]byte(`{"source_id":"seed:gdelt","bronze_table":"bronze.src_seed_gdelt_v1","changed_since":"2026-03-10T10:00:00Z"}` + "\n"))
			return
		}
		if strings.Contains(query, "FROM meta.source_registry FINAL") && strings.Contains(query, "FORMAT TabSeparated") {
			_, _ = w.Write([]byte(strings.Join(mockAutomaticHTTPSyncSourceIDs(), "\n") + "\n"))
			return
		}
		if strings.Contains(query, "FROM meta.source_registry FINAL") {
			_, _ = w.Write([]byte(mockSourceRegistryJSONLines(extractQuotedValues(query))))
			return
		}
		if strings.Contains(query, "FROM bronze.src_seed_gdelt_v1") {
			_, _ = w.Write([]byte(sampleBronzePromoteRowJSONLine()))
			return
		}
		if strings.Contains(query, "FROM meta.source_catalog") {
			_, _ = w.Write([]byte(`{"catalog_total":309,"catalog_concrete":267,"catalog_fingerprint":16,"catalog_family":26,"catalog_runnable":7,"catalog_approved_runtime_linked":7,"catalog_deferred":260,"catalog_credential_gated":23,"catalog_public_concrete":244,"catalog_public_runtime_linked":6,"catalog_public_deferred":238,"catalog_runtime_credential_gated":1,"catalog_deferred_credential_gated":22}` + "\n"))
			return
		}
		if strings.Contains(query, "SELECT count() FROM ops.crawl_frontier") {
			_, _ = w.Write([]byte("0\n"))
			return
		}
		if strings.Contains(query, "SELECT max(next_fetch_at), max(last_attempt_at) FROM ops.crawl_frontier") {
			_, _ = w.Write([]byte("\\N\t\\N\n"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	t.Setenv("ACLED_API_KEY", "demo-key")
	t.Setenv(controlPlaneCompiledCatalogPathEnv, filepath.Join(mustRepoRoot(t), "seed", "source_catalog_compiled.json"))
	if err := runAutomaticSyncTick(context.Background()); err != nil {
		t.Fatalf("runAutomaticSyncTick: %v", err)
	}
	joined := strings.Join(queries, "\n")
	for _, want := range []string{"INSERT INTO meta.discovery_candidate", "INSERT INTO silver.fact_event", "orchestrated automatic http source sync", "promoted canonical records into silver", "catalog_public_runtime_linked", "catalog_runtime_credential_gated", "orchestrated fetch stage for fixture:who-outbreaks", "orchestrated promote stage for seed:gdelt"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected automatic sync tick to run %q, got %s", want, joined)
		}
	}
	if silverIdx, promoteStageIdx := strings.Index(joined, "INSERT INTO silver.fact_event"), strings.Index(joined, "orchestrated promote stage for seed:gdelt"); silverIdx == -1 || promoteStageIdx == -1 || silverIdx > promoteStageIdx {
		t.Fatalf("expected real silver promote SQL before automatic promote stage record, got %s", joined)
	}
	if promoteJobIdx, automaticSuccessIdx := strings.Index(joined, "promoted canonical records into silver"), strings.Index(joined, "orchestrated automatic http source sync"); promoteJobIdx == -1 || automaticSuccessIdx == -1 || promoteJobIdx > automaticSuccessIdx {
		t.Fatalf("expected automatic sync success to be recorded after promote job success, got %s", joined)
	}
	for _, unwanted := range []string{"orchestrated fetch stage for fixture:safety", "orchestrated fetch stage for fixture:non-http", "orchestrated fetch stage for fixture:no-bronze"} {
		if strings.Contains(joined, unwanted) {
			t.Fatalf("expected automatic sync tick to exclude %q, got %s", unwanted, joined)
		}
	}
}
