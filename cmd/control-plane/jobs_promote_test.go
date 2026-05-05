package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
		"SETTINGS async_insert=0, insert_deduplicate=1, deduplicate_blocks_in_dependent_materialized_views=1",
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

func TestReplayDoesNotDuplicateCanonicalRowsFromRetriedBronzeInputs(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		switch {
		case strings.Contains(query, "FROM ops.job_run FINAL"):
			_, _ = w.Write([]byte("{\"last_successful_promote_at\":null}\n"))
		case strings.Contains(query, "FROM ops.parse_checkpoint FINAL") && strings.Contains(query, "UNION ALL"):
			_, _ = w.Write([]byte(`{"source_id":"seed:gdelt","bronze_table":"bronze.src_seed_gdelt_v1","changed_since":"2026-03-10T10:00:00Z"}` + "\n"))
		case strings.Contains(query, "FROM meta.source_registry FINAL"):
			_, _ = w.Write([]byte(`{"source_id":"seed:gdelt","bronze_table":"bronze.src_seed_gdelt_v1","crawl_enabled":1}` + "\n"))
		case strings.Contains(query, "FROM bronze.src_seed_gdelt_v1"):
			_, _ = w.Write([]byte(sampleBronzePromoteRowJSONLine() + sampleBronzeRetryPromoteRowJSONLine()))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	inputs, err := loadPromotionInputsFromBronze(context.Background(), migrate.NewHTTPRunner(server.URL))
	if err != nil {
		t.Fatalf("loadPromotionInputsFromBronze: %v", err)
	}
	if len(inputs) != 2 {
		t.Fatalf("expected duplicate bronze retry inputs to load, got %#v", inputs)
	}
	plan, err := promote.NewPipeline(promote.Options{}).Prepare(inputs)
	if err != nil {
		t.Fatalf("prepare duplicated bronze inputs: %v", err)
	}
	if len(plan.Events) != 1 {
		t.Fatalf("expected retried bronze inputs to collapse to one canonical event, got %d", len(plan.Events))
	}
	if got := plan.Events[0].EventID; got == "" {
		t.Fatal("expected canonical event id to be populated")
	}
	joined := strings.Join(queries, "\n")
	if !strings.Contains(joined, "FROM bronze.src_seed_gdelt_v1") {
		t.Fatalf("expected bronze runtime query, got %s", joined)
	}
}

func TestIncrementalPromoteSelection(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		switch {
		case strings.Contains(query, "FROM ops.job_run FINAL"):
			_, _ = w.Write([]byte("{\"last_successful_promote_at\":null}\n"))
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
		case strings.Contains(query, "FROM ops.job_run FINAL"):
			_, _ = w.Write([]byte("{\"last_successful_promote_at\":null}\n"))
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
		case strings.Contains(query, "FROM ops.job_run FINAL"):
			_, _ = w.Write([]byte("{\"last_successful_promote_at\":null}\n"))
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
		case strings.Contains(query, "FROM ops.job_run FINAL"):
			_, _ = w.Write([]byte("{\"last_successful_promote_at\":null}\n"))
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
	return sampleBronzePromoteRowJSONLineForSource("seed:gdelt", "raw:1", "https://example.test/doc")
}

func sampleBronzeRetryPromoteRowJSONLine() string {
	return `{"raw_id":"raw:1:retry","source_id":"seed:gdelt","parser_id":"parser:json","parser_version":"1.0.0","record_kind":"event","native_id":"native-1","source_url":"https://example.test/doc","canonical_url":"https://example.test/doc","fetched_at":"2026-03-10T09:10:00Z","parsed_at":"2026-03-10T10:15:00Z","occurred_at":"2026-03-10T08:00:00Z","published_at":"2026-03-10T08:05:00Z","status":"open","title":"Demo retry","summary":"Summary","place_hint":"Paris","lat":48.8566,"lon":2.3522,"severity":"medium","content_hash":"hash-1","schema_version":1,"record_version":2,"attrs":"{}","evidence":"[]","payload_json":"{\"record_kind\":\"event\",\"event_type\":\"demo\",\"place_id\":\"plc:demo\",\"parent_place_chain\":[\"plc:parent\"]}"}` + "\n"
}

func sampleBronzePromoteRowJSONLineForSource(sourceID, rawID, sourceURL string) string {
	return fmt.Sprintf(`{"raw_id":%q,"source_id":%q,"parser_id":"parser:json","parser_version":"1.0.0","record_kind":"event","native_id":"native-1","source_url":%q,"canonical_url":%q,"fetched_at":"2026-03-10T09:00:00Z","parsed_at":"2026-03-10T08:30:00Z","occurred_at":"2026-03-10T08:00:00Z","published_at":"2026-03-10T08:05:00Z","status":"open","title":"Demo","summary":"Summary","place_hint":"Paris","lat":48.8566,"lon":2.3522,"severity":"medium","content_hash":"hash-1","schema_version":1,"record_version":1,"attrs":"{}","evidence":"[]","payload_json":"{\"record_kind\":\"event\",\"event_type\":\"demo\",\"place_id\":\"plc:demo\",\"parent_place_chain\":[\"plc:parent\"]}"}`+"\n", rawID, sourceID, sourceURL, sourceURL)
}

func mustPromoteTestTime(t *testing.T, raw string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		t.Fatalf("parse test time %s: %v", raw, err)
	}
	return parsed.UTC()
}

type promoteReplayHTTPState struct {
	windowStart   time.Time
	windowEnd     time.Time
	failZuluOnce  bool
	queries       []string
	checkpoints   map[string]promoteCheckpointRecord
	checkpointIDs map[string]string
}

func (s *promoteReplayHTTPState) handle(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("query")
	s.queries = append(s.queries, query)
	switch {
	case strings.Contains(query, "FROM ops.parse_checkpoint FINAL") && strings.Contains(query, "UNION ALL"):
		_, _ = w.Write([]byte(
			`{"source_id":"fixture:alpha","bronze_table":"bronze.src_fixture_alpha_v1","changed_since":"2026-03-10T08:15:00Z"}` + "\n" +
				`{"source_id":"fixture:zulu","bronze_table":"bronze.src_fixture_zulu_v1","changed_since":"2026-03-10T08:20:00Z"}` + "\n"))
	case strings.Contains(query, "FROM meta.source_registry FINAL"):
		_, _ = w.Write([]byte(
			`{"source_id":"fixture:alpha","bronze_table":"bronze.src_fixture_alpha_v1","crawl_enabled":1}` + "\n" +
				`{"source_id":"fixture:zulu","bronze_table":"bronze.src_fixture_zulu_v1","crawl_enabled":1}` + "\n"))
	case strings.Contains(query, "FROM ops.promote_checkpoint FINAL"):
		_, _ = w.Write([]byte(s.checkpointJSONLines()))
	case strings.Contains(query, "INSERT INTO ops.promote_checkpoint"):
		s.captureCheckpoint(query)
		w.WriteHeader(http.StatusOK)
	case s.failZuluOnce && strings.Contains(query, "FROM bronze.src_fixture_zulu_v1"):
		s.failZuluOnce = false
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("forced zulu bronze replay failure"))
	case strings.Contains(query, "FROM bronze.src_fixture_alpha_v1"):
		_, _ = w.Write([]byte(sampleBronzePromoteRowJSONLineForSource("fixture:alpha", "raw:alpha", "https://example.test/alpha")))
	case strings.Contains(query, "FROM bronze.src_fixture_zulu_v1"):
		_, _ = w.Write([]byte(sampleBronzePromoteRowJSONLineForSource("fixture:zulu", "raw:zulu", "https://example.test/zulu")))
	default:
		w.WriteHeader(http.StatusOK)
	}
}

func (s *promoteReplayHTTPState) checkpointJSONLines() string {
	lines := make([]string, 0, len(s.checkpoints))
	for _, sourceID := range []string{"fixture:alpha", "fixture:zulu"} {
		checkpoint, ok := s.checkpoints[sourceID]
		if !ok {
			continue
		}
		payload, err := json.Marshal(checkpoint)
		if err != nil {
			panic(err)
		}
		lines = append(lines, string(payload))
	}
	if len(lines) == 0 {
		return ""
	}
	return strings.Join(lines, "\n") + "\n"
}

func (s *promoteReplayHTTPState) captureCheckpoint(query string) {
	status := ""
	for _, candidate := range []string{promoteCheckpointRunning, promoteCheckpointSucceeded, promoteCheckpointFailed} {
		if strings.Contains(query, ","+sqlString(candidate)+",") {
			status = candidate
			break
		}
	}
	if status == "" {
		return
	}
	sourceID := ""
	bronzeTable := ""
	for candidateSource, candidateTable := range map[string]string{"fixture:alpha": "bronze.src_fixture_alpha_v1", "fixture:zulu": "bronze.src_fixture_zulu_v1"} {
		if strings.Contains(query, sqlString(candidateSource)) && strings.Contains(query, sqlString(candidateTable)) {
			sourceID = candidateSource
			bronzeTable = candidateTable
			break
		}
	}
	if sourceID == "" {
		return
	}
	attemptCount := uint16(1)
	if existing, ok := s.checkpoints[sourceID]; ok && existing.AttemptCount > 0 {
		attemptCount = existing.AttemptCount
		if existing.Status == promoteCheckpointFailed {
			attemptCount = existing.AttemptCount + 1
		}
	}
	checkpoint := promoteCheckpointRecord{
		CheckpointID:  s.checkpointIDs[sourceID],
		SourceID:      sourceID,
		BronzeTable:   bronzeTable,
		SelectionMode: promoteSelectionModeRange,
		WindowStart:   timePointer(s.windowStart),
		WindowEnd:     timePointer(s.windowEnd),
		Status:        status,
		AttemptCount:  attemptCount,
		StartedAt:     timePointer(s.windowStart),
		RecordVersion: uint64(len(s.checkpoints) + len(s.queries)),
		UpdatedAt:     s.windowEnd,
		Attrs:         `{}`,
		Evidence:      `[]`,
	}
	if status == promoteCheckpointSucceeded {
		checkpoint.FinishedAt = timePointer(s.windowEnd)
		checkpoint.InputRows = 1
	}
	if status == promoteCheckpointFailed {
		message := "forced zulu bronze replay failure"
		checkpoint.ErrorMessage = &message
		checkpoint.FinishedAt = timePointer(s.windowEnd)
	}
	s.checkpoints[sourceID] = checkpoint
}

func timePointer(value time.Time) *time.Time {
	v := value.UTC()
	return &v
}

func TestIncrementalPromoteSelectionUsesLastSuccessfulPromoteAndFetchLedger(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		switch {
		case strings.Contains(query, "FROM ops.job_run FINAL"):
			_, _ = w.Write([]byte(`{"last_successful_promote_at":"2026-03-10T10:30:00Z"}` + "\n"))
		case strings.Contains(query, "FROM ops.parse_checkpoint FINAL") && strings.Contains(query, "UNION ALL"):
			_, _ = w.Write([]byte(`{"source_id":"seed:gdelt","bronze_table":"bronze.src_seed_gdelt_v1","changed_since":"2026-03-10T11:15:00Z"}` + "\n"))
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
		t.Fatalf("expected only changed source inputs after last promote watermark, got %#v", inputs)
	}
	joined := strings.Join(queries, "\n")
	if !strings.Contains(joined, "WHERE changed_at >= toDateTime64('2026-03-10T10:30:00Z'") {
		t.Fatalf("expected last successful promote watermark in ledger query, got %s", joined)
	}
	if !strings.Contains(joined, "FROM ops.fetch_log FINAL AS fetch") {
		t.Fatalf("expected fetch ledger to drive promote selection, got %s", joined)
	}
	if !strings.Contains(joined, "WHERE parsed_at >= toDateTime64('2026-03-10T11:15:00Z'") {
		t.Fatalf("expected bronze scan to use ledger-derived changed window, got %s", joined)
	}
}

func TestLoadPromotionInputsFromBronzeUsesReplayBronzeOverrideTable(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		switch {
		case strings.Contains(query, "FROM ops.job_run FINAL"):
			_, _ = w.Write([]byte("{\"last_successful_promote_at\":null}\n"))
		case strings.Contains(query, "FROM ops.parse_checkpoint FINAL") && strings.Contains(query, "UNION ALL"):
			_, _ = w.Write([]byte(`{"source_id":"seed:gdelt","bronze_table":"bronze.src_seed_gdelt_v1","changed_since":"2026-03-10T10:00:00Z"}` + "\n"))
		case strings.Contains(query, "FROM meta.source_registry FINAL"):
			_, _ = w.Write([]byte(`{"source_id":"seed:gdelt","bronze_table":"bronze.src_seed_gdelt_v1","crawl_enabled":1}` + "\n"))
		case strings.Contains(query, "FROM bronze.src_seed_gdelt_v1__replay"):
			_, _ = w.Write([]byte(sampleBronzePromoteRowJSONLine()))
		case strings.Contains(query, "FROM bronze.src_seed_gdelt_v1"):
			t.Fatalf("expected replay override bronze table, got query %s", query)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	t.Setenv(promoteBronzeTableOverridesEnv, `{"seed:gdelt":"bronze.src_seed_gdelt_v1__replay"}`)
	inputs, err := loadPromotionInputsFromBronze(context.Background(), migrate.NewHTTPRunner(server.URL))
	if err != nil {
		t.Fatalf("loadPromotionInputsFromBronze: %v", err)
	}
	if len(inputs) != 1 || inputs[0].SourceID != "seed:gdelt" {
		t.Fatalf("expected replay bronze override to load gdelt, got %#v", inputs)
	}
	joined := strings.Join(queries, "\n")
	if !strings.Contains(joined, "FROM bronze.src_seed_gdelt_v1__replay") {
		t.Fatalf("expected replay bronze override table query, got %s", joined)
	}
}

func TestLoadPromotionWindowsUsesExplicitRangeSelection(t *testing.T) {
	queries := []string{}
	snapshotAt := mustPromoteTestTime(t, "2026-03-10T12:00:00Z")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		switch {
		case strings.Contains(query, "FROM ops.parse_checkpoint FINAL") && strings.Contains(query, "UNION ALL"):
			_, _ = w.Write([]byte(`{"source_id":"seed:gdelt","bronze_table":"bronze.src_seed_gdelt_v1","changed_since":"2026-03-10T08:15:00Z"}` + "\n"))
		case strings.Contains(query, "FROM meta.source_registry FINAL"):
			_, _ = w.Write([]byte(`{"source_id":"seed:gdelt","bronze_table":"bronze.src_seed_gdelt_v1","crawl_enabled":1}` + "\n"))
		case strings.Contains(query, "FROM ops.promote_checkpoint FINAL"):
			_, _ = w.Write([]byte(""))
		case strings.Contains(query, "FROM bronze.src_seed_gdelt_v1"):
			_, _ = w.Write([]byte(sampleBronzePromoteRowJSONLineForSource("seed:gdelt", "raw:range", "https://example.test/range")))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	windowStart := "2026-03-10T08:00:00Z"
	windowEnd := "2026-03-10T09:00:00Z"
	windows, err := loadPromotionWindows(context.Background(), migrate.NewHTTPRunner(server.URL), jobOptions{WindowStart: windowStart, WindowEnd: windowEnd}, snapshotAt)
	if err != nil {
		t.Fatalf("loadPromotionWindows: %v", err)
	}
	if len(windows) != 1 {
		t.Fatalf("expected one explicit replay window, got %#v", windows)
	}
	if windows[0].Mode != promoteSelectionModeRange {
		t.Fatalf("expected range replay mode, got %#v", windows[0])
	}
	if got := windows[0].WindowStart.UTC().Format(time.RFC3339Nano); got != windowStart {
		t.Fatalf("expected explicit range start %s, got %s", windowStart, got)
	}
	if got := windows[0].WindowEnd.UTC().Format(time.RFC3339Nano); got != windowEnd {
		t.Fatalf("expected explicit range end %s, got %s", windowEnd, got)
	}
	if _, err := loadPromotionInputsForWindow(context.Background(), migrate.NewHTTPRunner(server.URL), windows[0]); err != nil {
		t.Fatalf("loadPromotionInputsForWindow: %v", err)
	}
	joined := strings.Join(queries, "\n")
	if !strings.Contains(joined, "WHERE changed_at >= toDateTime64('2026-03-10T08:00:00Z'") {
		t.Fatalf("expected explicit range lower bound in changed-source query, got %s", joined)
	}
	if !strings.Contains(joined, "changed_at < toDateTime64('2026-03-10T09:00:00Z'") {
		t.Fatalf("expected explicit range upper bound in changed-source query, got %s", joined)
	}
	if !strings.Contains(joined, "WHERE parsed_at >= toDateTime64('2026-03-10T08:00:00Z', 3, 'UTC') AND parsed_at < toDateTime64('2026-03-10T09:00:00Z', 3, 'UTC')") {
		t.Fatalf("expected explicit bronze replay bounds, got %s", joined)
	}
}

func TestRunPromoteRangeResumeSkipsSucceededCheckpointWindows(t *testing.T) {
	windowStart := mustPromoteTestTime(t, "2026-03-10T08:00:00Z")
	windowEnd := mustPromoteTestTime(t, "2026-03-10T09:00:00Z")
	state := &promoteReplayHTTPState{
		windowStart:  windowStart,
		windowEnd:    windowEnd,
		failZuluOnce: true,
		checkpoints:  map[string]promoteCheckpointRecord{},
		checkpointIDs: map[string]string{
			"fixture:alpha": buildPromoteCheckpointID("fixture:alpha", "bronze.src_fixture_alpha_v1", promoteSelectionModeRange, windowStart, windowEnd),
			"fixture:zulu":  buildPromoteCheckpointID("fixture:zulu", "bronze.src_fixture_zulu_v1", promoteSelectionModeRange, windowStart, windowEnd),
		},
	}
	server := httptest.NewServer(http.HandlerFunc(state.handle))
	defer server.Close()
	runner := migrate.NewHTTPRunner(server.URL)
	ctx := context.WithValue(context.Background(), jobOptionsContextKey{}, jobOptions{WindowStart: windowStart.Format(time.RFC3339Nano), WindowEnd: windowEnd.Format(time.RFC3339Nano)})

	startedAt := mustPromoteTestTime(t, "2026-03-10T12:00:00Z")
	if _, err := runPromoteWithRunner(ctx, runner, startedAt); err == nil {
		t.Fatal("expected first promote replay to fail on second window")
	}
	if state.checkpoints["fixture:alpha"].Status != promoteCheckpointSucceeded {
		t.Fatalf("expected alpha replay window to succeed before failure, got %#v", state.checkpoints["fixture:alpha"])
	}
	if state.checkpoints["fixture:zulu"].Status != promoteCheckpointFailed {
		t.Fatalf("expected zulu replay window to fail, got %#v", state.checkpoints["fixture:zulu"])
	}

	state.queries = nil
	secondStartedAt := mustPromoteTestTime(t, "2026-03-10T12:05:00Z")
	if _, err := runPromoteWithRunner(ctx, runner, secondStartedAt); err != nil {
		t.Fatalf("second runPromoteWithRunner: %v", err)
	}
	joined := strings.Join(state.queries, "\n")
	if strings.Contains(joined, "FROM bronze.src_fixture_alpha_v1") {
		t.Fatalf("expected resume to skip succeeded alpha window, got %s", joined)
	}
	if !strings.Contains(joined, "FROM bronze.src_fixture_zulu_v1") {
		t.Fatalf("expected resume to replay failed zulu window, got %s", joined)
	}
	if state.checkpoints["fixture:zulu"].Status != promoteCheckpointSucceeded {
		t.Fatalf("expected zulu replay window to succeed on resume, got %#v", state.checkpoints["fixture:zulu"])
	}
	if state.checkpoints["fixture:zulu"].AttemptCount != 2 {
		t.Fatalf("expected zulu attempt count to increment on resume, got %#v", state.checkpoints["fixture:zulu"])
	}
}

func TestRunPromoteBackfillCutoverStagesAndBatchAppliesRows(t *testing.T) {
	payload, err := json.Marshal(promote.SampleInputs())
	if err != nil {
		t.Fatalf("marshal inputs: %v", err)
	}

	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query().Get("query"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	t.Setenv(promoteInputJSONEnv, string(payload))
	t.Setenv(promoteInputPathEnv, "")
	t.Setenv(promoteBackfillStagingTagEnv, "task14")

	if err := runPromote(context.Background()); err != nil {
		t.Fatalf("runPromote: %v", err)
	}
	joined := strings.Join(queries, "\n")
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS silver.dim_entity__backfill_task14 AS silver.dim_entity",
		"TRUNCATE TABLE silver.dim_entity__backfill_task14",
		"INSERT INTO silver.dim_entity__backfill_task14",
		"INSERT INTO silver.dim_entity SELECT * FROM silver.dim_entity__backfill_task14 SETTINGS async_insert=0, insert_deduplicate=1, deduplicate_blocks_in_dependent_materialized_views=1",
		"INSERT INTO silver.fact_event SELECT * FROM silver.fact_event__backfill_task14 SETTINGS async_insert=0, insert_deduplicate=1, deduplicate_blocks_in_dependent_materialized_views=1",
		"INSERT INTO ops.unresolved_location_queue SELECT * FROM ops.unresolved_location_queue__backfill_task14 SETTINGS async_insert=0, insert_deduplicate=1, deduplicate_blocks_in_dependent_materialized_views=1",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected backfill cutover query %q, got %s", want, joined)
		}
	}
}

func TestBuildPromoteExecutionStatementsReportsStagedCutoverMode(t *testing.T) {
	t.Setenv(promoteBackfillStagingTagEnv, "task14")
	statements, mode, err := buildPromoteExecutionStatements([]string{
		"INSERT INTO silver.dim_entity SELECT * FROM input_entities",
	})
	if err != nil {
		t.Fatalf("buildPromoteExecutionStatements: %v", err)
	}
	if mode != promoteBackfillCutoverMode {
		t.Fatalf("expected cutover mode %q, got %q", promoteBackfillCutoverMode, mode)
	}
	joined := strings.Join(statements, "\n")
	if !strings.Contains(joined, promoteBackfillStageMarker+"task14") {
		t.Fatalf("expected staged cutover marker %q in %s", promoteBackfillStageMarker+"task14", joined)
	}
}

func TestRunPromotePreservesMergedCanonicalEntitySourceLineage(t *testing.T) {
	inputs := []promote.Input{
		sharedPromoteEntityInput("fixture:registry-a", "raw:entity-a", "parse:entity-a", "frontier:entity-a"),
		sharedPromoteEntityInput("fixture:registry-b", "raw:entity-b", "parse:entity-b", "frontier:entity-b"),
	}
	payload, err := json.Marshal(inputs)
	if err != nil {
		t.Fatalf("marshal inputs: %v", err)
	}

	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query().Get("query"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	t.Setenv(promoteInputJSONEnv, string(payload))
	t.Setenv(promoteInputPathEnv, "")

	if err := runPromote(context.Background()); err != nil {
		t.Fatalf("runPromote: %v", err)
	}
	joined := strings.Join(queries, "\n")
	if !strings.Contains(joined, `"source_ids":["fixture:registry-a","fixture:registry-b"]`) {
		t.Fatalf("expected promote SQL to preserve merged source_ids lineage, got %s", joined)
	}
	if !strings.Contains(joined, `"source_id":"fixture:registry-a"`) {
		t.Fatalf("expected promote SQL to retain stable primary source_id, got %s", joined)
	}
}

func sharedPromoteEntityInput(sourceID, rawID, parseID, frontierID string) promote.Input {
	input := promote.SampleInputs()[2]
	input.SourceID = sourceID
	input.Discovery.FrontierID = frontierID
	input.Discovery.URL = "https://example.com/entities/" + sourceID
	input.Discovery.CanonicalURL = input.Discovery.URL
	input.Fetch.RawID = rawID
	input.Fetch.URL = input.Discovery.URL
	input.Parse.ParseID = parseID
	input.Parse.Candidate.SourceID = sourceID
	input.Parse.Candidate.RawID = rawID
	input.Parse.Candidate.NativeID = "shared-airport"
	input.Parse.Candidate.Data = map[string]any{
		"record_kind":       "entity",
		"entity_id":         "entity:shared-airport",
		"entity_type":       "airport",
		"canonical_name":    "Shared Airport",
		"status":            "active",
		"risk_band":         "low",
		"source_entity_key": "icao:shared",
	}
	return input
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
