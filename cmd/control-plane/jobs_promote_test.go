package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"global-osint-backend/internal/promote"
)

const controlPlanePromoteEvidencePath = ".sisyphus/evidence/task-15-promote-job.txt"

func TestRunPromoteExecutesSilverAndQueueStatements(t *testing.T) {
	inputPath := filepath.Join(t.TempDir(), "promote-input.json")
	payload, err := json.Marshal(promote.SampleInputs())
	if err != nil {
		t.Fatalf("marshal inputs: %v", err)
	}
	if err := os.WriteFile(inputPath, payload, 0o644); err != nil {
		t.Fatalf("write inputs: %v", err)
	}

	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query().Get("query"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	t.Setenv(promoteInputPathEnv, inputPath)
	t.Setenv(promoteInputJSONEnv, "")

	if err := runPromote(context.Background()); err != nil {
		t.Fatalf("runPromote: %v", err)
	}
	if len(queries) != 5 {
		t.Fatalf("expected 5 queries, got %d", len(queries))
	}
	joined := strings.Join(queries, "\n")
	for _, want := range []string{
		"INSERT INTO silver.dim_entity",
		"INSERT INTO silver.fact_observation",
		"INSERT INTO silver.fact_event",
		"INSERT INTO ops.unresolved_location_queue",
		"INSERT INTO ops.job_run",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected query %q, got %s", want, joined)
		}
	}

	var evidence strings.Builder
	evidence.WriteString("queries\n")
	for _, query := range queries {
		evidence.WriteString(query)
		evidence.WriteString("\n---\n")
	}
	writeControlPlaneEvidenceFile(t, controlPlanePromoteEvidencePath, []byte(evidence.String()))
}

func writeControlPlaneEvidenceFile(tb testing.TB, relativePath string, content []byte) {
	tb.Helper()
	artifactPath := filepath.Join(mustRepoRoot(tb), relativePath)
	if err := os.MkdirAll(filepath.Dir(artifactPath), 0o755); err != nil {
		tb.Fatalf("mkdir evidence dir: %v", err)
	}
	if err := os.WriteFile(artifactPath, content, 0o644); err != nil {
		tb.Fatalf("write evidence file: %v", err)
	}
}

func mustRepoRoot(tb testing.TB) string {
	tb.Helper()
	wd, err := os.Getwd()
	if err != nil {
		tb.Fatalf("getwd: %v", err)
	}
	for dir := wd; dir != filepath.Dir(dir); dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
	}
	tb.Fatal("unable to locate repo root")
	return ""
}
