package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"global-osint-backend/internal/migrate"
	"global-osint-backend/internal/promote"
)

func TestRunStoredPipelinePersistsDeterministicTransitionsAndOutputs(t *testing.T) {
	startedAt := time.Date(2026, 4, 25, 12, 0, 0, 0, time.UTC)
	definition := testStoredPromotePipelineDefinition(t, "pipeline:demo")
	ctx := context.WithValue(context.Background(), jobOptionsContextKey{}, jobOptions{PipelineID: definition.PipelineID})
	state := &pipelineExecuteHTTPState{definition: definition}
	server := httptest.NewServer(http.HandlerFunc(state.handle))
	defer server.Close()

	result, err := runStoredPipelineWithRunner(ctx, migrate.NewHTTPRunner(server.URL), startedAt)
	if err != nil {
		t.Fatalf("runStoredPipelineWithRunner: %v", err)
	}
	if result.Skipped {
		t.Fatal("expected first stored pipeline execution to run, got skipped")
	}
	if result.Run.Status != pipelineRunStatusSucceeded {
		t.Fatalf("expected succeeded run, got %q", result.Run.Status)
	}
	if !strings.Contains(result.Outputs, `"statement_count":4`) {
		t.Fatalf("expected durable outputs to include statement_count, got %s", result.Outputs)
	}
	if !strings.Contains(result.Outputs, `"pipeline_id":"pipeline:demo"`) {
		t.Fatalf("expected durable outputs to include pipeline_id, got %s", result.Outputs)
	}

	transitions := state.pipelineTransitionMessages()
	if got, want := strings.Join(transitions, " -> "), "planned stored pipeline execution -> claimed stored pipeline execution -> running stored pipeline execution -> stored pipeline execution succeeded"; got != want {
		t.Fatalf("unexpected pipeline transitions: got %s want %s", got, want)
	}
	joined := strings.Join(state.queries, "\n")
	for _, want := range []string{"INSERT INTO silver.dim_entity", "INSERT INTO silver.fact_observation", "INSERT INTO silver.fact_event", "INSERT INTO ops.unresolved_location_queue"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected promote execution query %q, got %s", want, joined)
		}
	}
	if state.latestRunLookupCount < 1 {
		t.Fatal("expected pipeline run lookup before execution")
	}
}

func TestRunStoredPipelineRerunIsIdempotent(t *testing.T) {
	startedAt := time.Date(2026, 4, 25, 12, 0, 0, 0, time.UTC)
	definition := testStoredPromotePipelineDefinition(t, "pipeline:demo")
	ctx := context.WithValue(context.Background(), jobOptionsContextKey{}, jobOptions{PipelineID: definition.PipelineID})
	state := &pipelineExecuteHTTPState{definition: definition}
	server := httptest.NewServer(http.HandlerFunc(state.handle))
	defer server.Close()
	runner := migrate.NewHTTPRunner(server.URL)

	first, err := runStoredPipelineWithRunner(ctx, runner, startedAt)
	if err != nil {
		t.Fatalf("first runStoredPipelineWithRunner: %v", err)
	}
	second, err := runStoredPipelineWithRunner(ctx, runner, startedAt.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("second runStoredPipelineWithRunner: %v", err)
	}
	if first.Skipped {
		t.Fatal("expected first invocation to execute")
	}
	if !second.Skipped {
		t.Fatal("expected second invocation to reuse stored succeeded run")
	}
	if second.Run.RunID != first.Run.RunID {
		t.Fatalf("expected deterministic rerun to reuse run id %q, got %q", first.Run.RunID, second.Run.RunID)
	}
	if got := state.promoteExecutionCount(); got != 4 {
		t.Fatalf("expected promote SQL to execute only once, got %d statements", got)
	}
	if got := len(state.pipelineRunInserts()); got != 4 {
		t.Fatalf("expected rerun to avoid extra pipeline state inserts, got %d", got)
	}
	if state.latestRunLookupCount < 2 {
		t.Fatalf("expected latest run lookup on rerun, got %d", state.latestRunLookupCount)
	}
}

func TestRunStoredPipelinePersistsFailureState(t *testing.T) {
	startedAt := time.Date(2026, 4, 25, 12, 0, 0, 0, time.UTC)
	definition := testStoredPromotePipelineDefinition(t, "pipeline:broken")
	ctx := context.WithValue(context.Background(), jobOptionsContextKey{}, jobOptions{PipelineID: definition.PipelineID})
	state := &pipelineExecuteHTTPState{definition: definition, failOnSilverEntityInsert: true}
	server := httptest.NewServer(http.HandlerFunc(state.handle))
	defer server.Close()

	_, err := runStoredPipelineWithRunner(ctx, migrate.NewHTTPRunner(server.URL), startedAt)
	if err == nil {
		t.Fatal("expected stored pipeline execution failure")
	}
	if !strings.Contains(err.Error(), "clickhouse http 500") {
		t.Fatalf("expected clickhouse failure to bubble up, got %v", err)
	}
	transitions := state.pipelineTransitionMessages()
	if got, want := strings.Join(transitions, " -> "), "planned stored pipeline execution -> claimed stored pipeline execution -> running stored pipeline execution -> stored pipeline execution failed"; got != want {
		t.Fatalf("unexpected pipeline failure transitions: got %s want %s", got, want)
	}
	if len(state.pipelineRunInserts()) != 4 {
		t.Fatalf("expected four persisted run states on failure, got %d", len(state.pipelineRunInserts()))
	}
	if strings.Contains(strings.Join(state.queries, "\n"), "stored pipeline execution succeeded") {
		t.Fatal("expected failure path to avoid succeeded snapshot")
	}
}

type pipelineExecuteHTTPState struct {
	definition               storedPipelineDefinition
	queries                  []string
	latestRun                *pipelineRunRecord
	latestRunLookupCount     int
	failOnSilverEntityInsert bool
}

func (s *pipelineExecuteHTTPState) handle(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("query")
	s.queries = append(s.queries, query)
	switch {
	case strings.Contains(query, "FROM meta.pipeline_registry FINAL"):
		_, _ = w.Write([]byte(s.definitionJSONLine()))
	case strings.Contains(query, "FROM ops.pipeline_run FINAL"):
		s.latestRunLookupCount++
		if s.latestRun != nil {
			_, _ = w.Write([]byte(s.pipelineRunJSONLine(*s.latestRun)))
		}
	case strings.Contains(query, "INSERT INTO ops.pipeline_run"):
		s.captureLatestRun(query)
		w.WriteHeader(http.StatusOK)
	case s.failOnSilverEntityInsert && strings.Contains(query, "INSERT INTO silver.dim_entity"):
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("forced entity insert failure"))
	default:
		w.WriteHeader(http.StatusOK)
	}
}

func (s *pipelineExecuteHTTPState) definitionJSONLine() string {
	line, err := json.Marshal(s.definition)
	if err != nil {
		panic(err)
	}
	return string(line) + "\n"
}

func (s *pipelineExecuteHTTPState) captureLatestRun(query string) {
	status := ""
	message := ""
	for _, candidate := range []string{pipelineRunStatusPlanned, pipelineRunStatusPending, pipelineRunStatusRunning, pipelineRunStatusSucceeded, pipelineRunStatusFailed} {
		needle := "," + sqlString(candidate) + ","
		if strings.Contains(query, needle) {
			status = candidate
			break
		}
	}
	for _, candidate := range []string{"planned stored pipeline execution", "claimed stored pipeline execution", "running stored pipeline execution", "stored pipeline execution succeeded", "stored pipeline execution failed"} {
		if strings.Contains(query, sqlString(candidate)) {
			message = candidate
			break
		}
	}
	if status == "" || message == "" {
		return
	}
	runKey := buildStoredPipelineRunKey(s.definition)
	runID := buildStoredPipelineRunID(runKey)
	outputs := defaultPipelineOutputsJSON
	if status == pipelineRunStatusSucceeded {
		outputs = `{"pipeline_id":"` + s.definition.PipelineID + `","statement_count":4}`
	}
	var finishedAt *time.Time
	if status == pipelineRunStatusSucceeded || status == pipelineRunStatusFailed {
		now := time.Date(2026, 4, 25, 12, 1, 0, 0, time.UTC)
		finishedAt = &now
	}
	startedAt := time.Date(2026, 4, 25, 12, 0, 0, 0, time.UTC)
	s.latestRun = &pipelineRunRecord{
		PipelineID:         s.definition.PipelineID,
		RunID:              runID,
		RunKey:             runKey,
		DefinitionChecksum: pipelineDefinitionChecksum(s.definition),
		Status:             status,
		AttemptCount:       1,
		StartedAt:          &startedAt,
		FinishedAt:         finishedAt,
		Message:            message,
		OutputsJSON:        outputs,
		SchemaVersion:      pipelineRunSchemaVersion,
		RecordVersion:      uint64(1 + len(s.pipelineRunInserts())),
		APIContractVersion: pipelineRunAPIContract,
		UpdatedAt:          startedAt,
		Attrs:              defaultPipelineAttrsJSON,
		Evidence:           defaultPipelineEvidenceJSON,
	}
}

func (s *pipelineExecuteHTTPState) pipelineRunJSONLine(record pipelineRunRecord) string {
	line, err := json.Marshal(record)
	if err != nil {
		panic(err)
	}
	return string(line) + "\n"
}

func (s *pipelineExecuteHTTPState) pipelineRunInserts() []string {
	insertQueries := make([]string, 0)
	for _, query := range s.queries {
		if strings.Contains(query, "INSERT INTO ops.pipeline_run") {
			insertQueries = append(insertQueries, query)
		}
	}
	return insertQueries
}

func (s *pipelineExecuteHTTPState) pipelineTransitionMessages() []string {
	messages := make([]string, 0)
	for _, query := range s.pipelineRunInserts() {
		for _, candidate := range []string{"planned stored pipeline execution", "claimed stored pipeline execution", "running stored pipeline execution", "stored pipeline execution succeeded", "stored pipeline execution failed"} {
			if strings.Contains(query, sqlString(candidate)) {
				messages = append(messages, candidate)
				break
			}
		}
	}
	return messages
}

func (s *pipelineExecuteHTTPState) promoteExecutionCount() int {
	count := 0
	for _, query := range s.queries {
		if strings.Contains(query, "INSERT INTO silver.dim_entity") || strings.Contains(query, "INSERT INTO silver.fact_observation") || strings.Contains(query, "INSERT INTO silver.fact_event") || strings.Contains(query, "INSERT INTO ops.unresolved_location_queue") {
			count++
		}
	}
	return count
}

func testStoredPromotePipelineDefinition(t *testing.T, pipelineID string) storedPipelineDefinition {
	t.Helper()
	definitionPayload, err := marshalJSONString(storedPromotePipelineDefinition{Inputs: promote.SampleInputs()})
	if err != nil {
		t.Fatalf("marshal stored promote pipeline definition: %v", err)
	}
	definition := storedPipelineDefinition{
		PipelineID:         pipelineID,
		PipelineName:       "demo-pipeline",
		PipelineKind:       storedPipelineKindPromote,
		DefinitionJSON:     definitionPayload,
		DefinitionChecksum: "",
		Enabled:            1,
		SchemaVersion:      pipelineRegistrySchemaVersion,
		RecordVersion:      1,
		APIContractVersion: pipelineRegistryAPIContract,
		Attrs:              defaultPipelineAttrsJSON,
		Evidence:           defaultPipelineEvidenceJSON,
	}
	definition.DefinitionChecksum = pipelineDefinitionChecksum(definition)
	return definition
}
