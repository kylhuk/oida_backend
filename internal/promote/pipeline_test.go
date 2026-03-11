package promote

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	promoteEvidencePath     = ".sisyphus/evidence/task-15-promote-plan.txt"
	promoteEdgeEvidencePath = ".sisyphus/evidence/task-15-promote-edge.txt"
)

func TestPipelinePreparePromotesResolvedAndQueuesUnresolved(t *testing.T) {
	pipeline := NewPipeline(Options{Now: func() time.Time { return time.Date(2026, 3, 10, 18, 0, 0, 0, time.UTC) }})
	inputs := append([]Input{}, SampleInputs()...)
	inputs = append(inputs, SampleInputs()[0])

	plan, err := pipeline.Prepare(inputs)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if got := plan.Stats.Inputs; got != 5 {
		t.Fatalf("expected 5 inputs, got %d", got)
	}
	if got := len(plan.Observations); got != 1 {
		t.Fatalf("expected 1 observation, got %d", got)
	}
	if got := len(plan.Events); got != 1 {
		t.Fatalf("expected 1 event, got %d", got)
	}
	if got := len(plan.Entities); got != 3 {
		t.Fatalf("expected 3 entities, got %d", got)
	}
	if got := len(plan.Unresolved); got != 1 {
		t.Fatalf("expected 1 unresolved row, got %d", got)
	}
	if got := plan.Observations[0].PlaceID; got != "plc:us-tx-paris" {
		t.Fatalf("expected observation place_id plc:us-tx-paris, got %s", got)
	}
	if got := plan.Events[0].ParentPlaceChain; len(got) != 3 || got[0] != "plc:fr-idf" {
		t.Fatalf("unexpected event place chain %#v", got)
	}
	if got := plan.Unresolved[0].FailureReason; got != "ambiguous_place_name" {
		t.Fatalf("expected unresolved failure reason ambiguous_place_name, got %s", got)
	}

	statements, err := plan.SQLStatements()
	if err != nil {
		t.Fatalf("sql statements: %v", err)
	}
	if len(statements) != 4 {
		t.Fatalf("expected 4 SQL statements, got %d", len(statements))
	}
	for _, want := range []string{
		"LEFT JOIN (SELECT DISTINCT entity_id, record_version FROM silver.dim_entity",
		"LEFT JOIN (SELECT DISTINCT observation_id FROM silver.fact_observation",
		"LEFT JOIN (SELECT DISTINCT event_id FROM silver.fact_event",
		"LEFT JOIN (SELECT DISTINCT queue_id FROM ops.unresolved_location_queue",
	} {
		if !strings.Contains(strings.Join(statements, "\n"), want) {
			t.Fatalf("expected anti-join marker %q in SQL", want)
		}
	}

	var evidence strings.Builder
	evidence.WriteString("kind\tid\tplace_id\textra\n")
	evidence.WriteString("observation\t" + plan.Observations[0].ObservationID + "\t" + plan.Observations[0].PlaceID + "\t" + plan.Observations[0].ObservationType + "\n")
	evidence.WriteString("event\t" + plan.Events[0].EventID + "\t" + plan.Events[0].PlaceID + "\t" + plan.Events[0].EventType + "\n")
	for _, entity := range plan.Entities {
		evidence.WriteString("entity\t" + entity.EntityID + "\t" + entity.PrimaryPlaceID + "\t" + entity.CanonicalName + "\n")
	}
	evidence.WriteString("unresolved\t" + plan.Unresolved[0].QueueID + "\t-\t" + plan.Unresolved[0].FailureReason + "\n")
	writeEvidenceFile(t, promoteEvidencePath, []byte(evidence.String()))
}

func TestPipelinePrepareRejectsBrokenStageChain(t *testing.T) {
	pipeline := NewPipeline(Options{})
	input := SampleInputs()[0]
	input.Fetch.RawID = ""

	_, err := pipeline.Prepare([]Input{input})
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "fetch.raw_id is required") {
		t.Fatalf("unexpected error: %v", err)
	}
	writeEvidenceFile(t, promoteEdgeEvidencePath, []byte(err.Error()+"\n"))
}

func TestCanonicalIDsIgnoreRawID(t *testing.T) {
	pipeline := NewPipeline(Options{Now: func() time.Time { return time.Date(2026, 3, 10, 18, 0, 0, 0, time.UTC) }})
	inputsA := append([]Input{}, SampleInputs()...)
	inputsB := append([]Input{}, SampleInputs()...)
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
	if planA.Observations[0].ObservationID != planB.Observations[0].ObservationID {
		t.Fatalf("expected observation ID stability across raw_id changes, got %q vs %q", planA.Observations[0].ObservationID, planB.Observations[0].ObservationID)
	}
	if planA.Events[0].EventID != planB.Events[0].EventID {
		t.Fatalf("expected event ID stability across raw_id changes, got %q vs %q", planA.Events[0].EventID, planB.Events[0].EventID)
	}
}

func TestReplayDoesNotDuplicateCanonicalRows(t *testing.T) {
	pipeline := NewPipeline(Options{Now: func() time.Time { return time.Date(2026, 3, 10, 18, 0, 0, 0, time.UTC) }})
	inputs := append([]Input{}, SampleInputs()...)

	planA, err := pipeline.Prepare(inputs)
	if err != nil {
		t.Fatalf("prepare first replay: %v", err)
	}
	planB, err := pipeline.Prepare(inputs)
	if err != nil {
		t.Fatalf("prepare second replay: %v", err)
	}

	if len(planA.Observations) != len(planB.Observations) || len(planA.Events) != len(planB.Events) || len(planA.Entities) != len(planB.Entities) {
		t.Fatalf("expected replay plan cardinality stability, got obs %d/%d events %d/%d entities %d/%d", len(planA.Observations), len(planB.Observations), len(planA.Events), len(planB.Events), len(planA.Entities), len(planB.Entities))
	}
	if len(planA.Observations) > 0 && planA.Observations[0].ObservationID != planB.Observations[0].ObservationID {
		t.Fatalf("expected stable observation ids across replay, got %q vs %q", planA.Observations[0].ObservationID, planB.Observations[0].ObservationID)
	}
	if len(planA.Events) > 0 && planA.Events[0].EventID != planB.Events[0].EventID {
		t.Fatalf("expected stable event ids across replay, got %q vs %q", planA.Events[0].EventID, planB.Events[0].EventID)
	}
}

func TestBackfillCutover(t *testing.T) {
	pipeline := NewPipeline(Options{Now: func() time.Time { return time.Date(2026, 3, 10, 18, 0, 0, 0, time.UTC) }})
	inputs := append([]Input{}, SampleInputs()...)

	planA, err := pipeline.Prepare(inputs)
	if err != nil {
		t.Fatalf("prepare plan A: %v", err)
	}
	statementsA, err := planA.SQLStatements()
	if err != nil {
		t.Fatalf("sql statements A: %v", err)
	}
	planB, err := pipeline.Prepare(inputs)
	if err != nil {
		t.Fatalf("prepare plan B: %v", err)
	}
	statementsB, err := planB.SQLStatements()
	if err != nil {
		t.Fatalf("sql statements B: %v", err)
	}
	if len(statementsA) != len(statementsB) {
		t.Fatalf("expected deterministic backfill statement count, got %d vs %d", len(statementsA), len(statementsB))
	}
}

func writeEvidenceFile(tb testing.TB, relativePath string, content []byte) {
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
