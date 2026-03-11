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

	"global-osint-backend/internal/migrate"
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

func TestPromoteParity(t *testing.T) {
	pipeline := promote.NewPipeline(promote.Options{})
	inputsA := append([]promote.Input{}, promote.SampleInputs()...)
	inputsB := append([]promote.Input{}, promote.SampleInputs()...)
	for i := range inputsB {
		inputsB[i].Fetch.RawID = inputsB[i].Fetch.RawID + ":rerun"
		inputsB[i].Parse.Candidate.RawID = inputsB[i].Fetch.RawID
	}
	planA, err := pipeline.Prepare(inputsA)
	if err != nil {
		t.Fatalf("prepare plan A: %v", err)
	}
	planB, err := pipeline.Prepare(inputsB)
	if err != nil {
		t.Fatalf("prepare plan B: %v", err)
	}
	if len(planA.Observations) != len(planB.Observations) || len(planA.Events) != len(planB.Events) || len(planA.Entities) != len(planB.Entities) {
		t.Fatalf("expected parity plan shapes, got obs %d/%d events %d/%d entities %d/%d", len(planA.Observations), len(planB.Observations), len(planA.Events), len(planB.Events), len(planA.Entities), len(planB.Entities))
	}
	if planA.Observations[0].ObservationID != planB.Observations[0].ObservationID {
		t.Fatalf("expected observation parity across reruns, got %q vs %q", planA.Observations[0].ObservationID, planB.Observations[0].ObservationID)
	}
	if planA.Events[0].EventID != planB.Events[0].EventID {
		t.Fatalf("expected event parity across reruns, got %q vs %q", planA.Events[0].EventID, planB.Events[0].EventID)
	}
}

func TestIncrementalPromoteSelection(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		switch {
		case strings.Contains(query, "FROM ops.parse_checkpoint FINAL"):
			_, _ = w.Write([]byte(`{"source_id":"seed:gdelt","bronze_table":"bronze.src_seed_gdelt_v1","changed_since":"2026-03-10T10:00:00Z"}` + "\n"))
		case strings.Contains(query, "FROM meta.source_registry FINAL"):
			_, _ = w.Write([]byte(`{"source_id":"seed:gdelt","bronze_table":"bronze.src_seed_gdelt_v1","crawl_enabled":1}` + "\n" + `{"source_id":"fixture:reliefweb","bronze_table":"bronze.src_fixture_reliefweb_v1","crawl_enabled":1}` + "\n"))
		case strings.Contains(query, "FROM bronze.src_seed_gdelt_v1"):
			_, _ = w.Write([]byte(sampleBronzePromoteRowJSONLine()))
		case strings.Contains(query, "FROM bronze.src_fixture_reliefweb_v1"):
			t.Fatalf("expected unchanged source bronze table to be skipped, got query %s", query)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	inputs, err := loadPromotionInputsFromBronze(context.Background(), migrate.NewHTTPRunner(server.URL))
	if err != nil {
		t.Fatalf("loadPromotionInputsFromBronze: %v", err)
	}
	if len(inputs) != 1 || inputs[0].SourceID != "seed:gdelt" {
		t.Fatalf("expected only changed source inputs, got %#v", inputs)
	}
	joined := strings.Join(queries, "\n")
	if !strings.Contains(joined, "WHERE parsed_at >= toDateTime64('2026-03-10T10:00:00Z'") {
		t.Fatalf("expected incremental parsed_at window query, got %s", joined)
	}
}

func TestLoadPromotionInputsFromBronzeSkipsWhenNoChangedSources(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		switch {
		case strings.Contains(query, "FROM ops.parse_checkpoint FINAL"):
			_, _ = w.Write([]byte(""))
		case strings.Contains(query, "FROM meta.source_registry FINAL"):
			_, _ = w.Write([]byte(`{"source_id":"seed:gdelt","bronze_table":"bronze.src_seed_gdelt_v1","crawl_enabled":1}` + "\n"))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	inputs, err := loadPromotionInputsFromBronze(context.Background(), migrate.NewHTTPRunner(server.URL))
	if err != nil {
		t.Fatalf("loadPromotionInputsFromBronze: %v", err)
	}
	if len(inputs) != 0 {
		t.Fatalf("expected no promotion inputs without changed parse checkpoints, got %#v", inputs)
	}
	for _, query := range queries {
		if strings.Contains(query, "FROM bronze.src_seed_gdelt_v1") {
			t.Fatalf("expected no bronze scan without changed sources, got %s", query)
		}
	}
}

func TestIncrementalPromoteSelectionUsesEarliestCheckpointWindow(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		switch {
		case strings.Contains(query, "FROM ops.parse_checkpoint FINAL"):
			_, _ = w.Write([]byte(
				`{"source_id":"seed:gdelt","bronze_table":"bronze.src_seed_gdelt_v1","changed_since":"2026-03-10T09:00:00Z"}` + "\n" +
					`{"source_id":"seed:gdelt","bronze_table":"bronze.src_seed_gdelt_v1","changed_since":"2026-03-10T10:00:00Z"}` + "\n"))
		case strings.Contains(query, "FROM meta.source_registry FINAL"):
			_, _ = w.Write([]byte(`{"source_id":"seed:gdelt","bronze_table":"bronze.src_seed_gdelt_v1","crawl_enabled":1}` + "\n"))
		case strings.Contains(query, "FROM bronze.src_seed_gdelt_v1"):
			_, _ = w.Write([]byte(sampleBronzePromoteRowJSONLine() + sampleBronzePromoteRowJSONLine()))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	_, err := loadPromotionInputsFromBronze(context.Background(), migrate.NewHTTPRunner(server.URL))
	if err != nil {
		t.Fatalf("loadPromotionInputsFromBronze: %v", err)
	}
	joined := strings.Join(queries, "\n")
	if !strings.Contains(joined, "WHERE parsed_at >= toDateTime64('2026-03-10T09:00:00Z'") {
		t.Fatalf("expected earliest checkpoint window to drive promote selection, got %s", joined)
	}
}

func TestIncrementalPromoteSelectionScopesWindowByBronzeTable(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		switch {
		case strings.Contains(query, "FROM ops.parse_checkpoint FINAL"):
			_, _ = w.Write([]byte(
				`{"source_id":"seed:gdelt","bronze_table":"bronze.src_seed_gdelt_v1","changed_since":"2026-03-10T09:00:00Z"}` + "\n" +
					`{"source_id":"seed:gdelt","bronze_table":"bronze.src_seed_gdelt_v2","changed_since":"2026-03-10T11:00:00Z"}` + "\n"))
		case strings.Contains(query, "FROM meta.source_registry FINAL"):
			_, _ = w.Write([]byte(
				`{"source_id":"seed:gdelt","bronze_table":"bronze.src_seed_gdelt_v1","crawl_enabled":1}` + "\n" +
					`{"source_id":"seed:gdelt","bronze_table":"bronze.src_seed_gdelt_v2","crawl_enabled":1}` + "\n"))
		case strings.Contains(query, "FROM bronze.src_seed_gdelt_v1"):
			_, _ = w.Write([]byte(sampleBronzePromoteRowJSONLine()))
		case strings.Contains(query, "FROM bronze.src_seed_gdelt_v2"):
			_, _ = w.Write([]byte(sampleBronzePromoteRowJSONLine()))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	_, err := loadPromotionInputsFromBronze(context.Background(), migrate.NewHTTPRunner(server.URL))
	if err != nil {
		t.Fatalf("loadPromotionInputsFromBronze: %v", err)
	}
	joined := strings.Join(queries, "\n")
	if !strings.Contains(joined, "FROM bronze.src_seed_gdelt_v1\nWHERE parsed_at >= toDateTime64('2026-03-10T09:00:00Z'") {
		t.Fatalf("expected v1 bronze window to use its own earliest checkpoint, got %s", joined)
	}
	if !strings.Contains(joined, "FROM bronze.src_seed_gdelt_v2\nWHERE parsed_at >= toDateTime64('2026-03-10T11:00:00Z'") {
		t.Fatalf("expected v2 bronze window to use its own checkpoint, got %s", joined)
	}
}

func sampleBronzePromoteRowJSONLine() string {
	return `{"raw_id":"raw:1","source_id":"seed:gdelt","parser_id":"parser:json","parser_version":"1.0.0","record_kind":"event","native_id":"native-1","source_url":"https://example.test/doc","canonical_url":"https://example.test/doc","fetched_at":"2026-03-10T09:00:00Z","parsed_at":"2026-03-10T10:05:00Z","occurred_at":"2026-03-10T08:00:00Z","published_at":"2026-03-10T08:05:00Z","status":"open","title":"Demo","summary":"Summary","place_hint":"Paris","lat":48.8566,"lon":2.3522,"severity":"medium","content_hash":"hash-1","schema_version":1,"record_version":1,"attrs":"{}","evidence":"[]","payload_json":"{\"record_kind\":\"event\",\"event_type\":\"demo\",\"place_id\":\"plc:demo\",\"parent_place_chain\":[\"plc:parent\"]}"}` + "\n"
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
