package promote

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"global-osint-backend/internal/parser"
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
	base := SampleInputs()[1]
	baselinePlan, err := pipeline.Prepare([]Input{base})
	if err != nil {
		t.Fatalf("prepare baseline replay: %v", err)
	}
	duplicate := cloneReplayInput(base)
	retried := cloneReplayInput(base)
	retried.Fetch.RawID = retried.Fetch.RawID + ":retry"
	retried.Parse.ParseID = retried.Parse.ParseID + ":retry"
	retried.Parse.Candidate.RawID = retried.Fetch.RawID
	retried.Parse.Candidate.RecordVersion++
	retried.Fetch.FetchedAt = retried.Fetch.FetchedAt.Add(10 * time.Minute)

	replayPlan, err := pipeline.Prepare([]Input{base, duplicate, retried})
	if err != nil {
		t.Fatalf("prepare duplicate/retry replay: %v", err)
	}
	if !reflect.DeepEqual(planCanonicalIDs(baselinePlan), planCanonicalIDs(replayPlan)) {
		t.Fatalf("expected duplicate/retried bronze-equivalent inputs to keep canonical plan stable, got %#v vs %#v", planCanonicalIDs(baselinePlan), planCanonicalIDs(replayPlan))
	}
}

func TestBackfillCutover(t *testing.T) {
	pipeline := NewPipeline(Options{Now: func() time.Time { return time.Date(2026, 3, 10, 18, 0, 0, 0, time.UTC) }})
	baseA := SampleInputs()[0]
	baseB := SampleInputs()[1]
	baselinePlan, err := pipeline.Prepare([]Input{baseA, baseB})
	if err != nil {
		t.Fatalf("prepare baseline plan: %v", err)
	}
	statementsA, err := baselinePlan.SQLStatements()
	if err != nil {
		t.Fatalf("sql statements baseline: %v", err)
	}
	retryA := cloneReplayInput(baseA)
	retryA.Fetch.RawID = retryA.Fetch.RawID + ":retry"
	retryA.Parse.ParseID = retryA.Parse.ParseID + ":retry"
	retryA.Parse.Candidate.RawID = retryA.Fetch.RawID
	retryA.Parse.Candidate.RecordVersion++
	retryA.Fetch.FetchedAt = retryA.Fetch.FetchedAt.Add(5 * time.Minute)
	retryB := cloneReplayInput(baseB)
	retryB.Fetch.RawID = retryB.Fetch.RawID + ":retry"
	retryB.Parse.ParseID = retryB.Parse.ParseID + ":retry"
	retryB.Parse.Candidate.RawID = retryB.Fetch.RawID
	retryB.Parse.Candidate.RecordVersion++
	retryB.Fetch.FetchedAt = retryB.Fetch.FetchedAt.Add(7 * time.Minute)

	replayPlan, err := pipeline.Prepare([]Input{baseA, retryA, baseB, retryB})
	if err != nil {
		t.Fatalf("prepare replay backfill plan: %v", err)
	}
	statementsB, err := replayPlan.SQLStatements()
	if err != nil {
		t.Fatalf("sql statements replay: %v", err)
	}
	if !reflect.DeepEqual(planCanonicalIDs(baselinePlan), planCanonicalIDs(replayPlan)) {
		t.Fatalf("expected replay backfill inputs to preserve canonical ids, got %#v vs %#v", planCanonicalIDs(baselinePlan), planCanonicalIDs(replayPlan))
	}
	if len(statementsA) != len(statementsB) {
		t.Fatalf("expected replay backfill SQL statement count to stay stable, got %d vs %d", len(statementsA), len(statementsB))
	}
}

func TestPrepareMergesCanonicalEntityLineageAcrossSources(t *testing.T) {
	pipeline := NewPipeline(Options{Now: func() time.Time { return time.Date(2026, 3, 10, 18, 0, 0, 0, time.UTC) }})
	inputs := []Input{sharedCanonicalEntityInput("fixture:registry-a", "raw:entity-a", "parse:entity-a", "frontier:entity-a"), sharedCanonicalEntityInput("fixture:registry-b", "raw:entity-b", "parse:entity-b", "frontier:entity-b")}

	plan, err := pipeline.Prepare(inputs)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if len(plan.Entities) != 1 {
		t.Fatalf("expected one canonical entity row, got %d", len(plan.Entities))
	}
	attrs := plan.Entities[0].Attrs
	if got := attrs["source_id"]; got != "fixture:registry-a" {
		t.Fatalf("expected stable primary source_id fixture:registry-a, got %#v", got)
	}
	sourceIDs, ok := attrs["source_ids"].([]string)
	if !ok {
		t.Fatalf("expected source_ids lineage array, got %#v", attrs["source_ids"])
	}
	if want := []string{"fixture:registry-a", "fixture:registry-b"}; !reflect.DeepEqual(sourceIDs, want) {
		t.Fatalf("expected merged source lineage %v, got %v", want, sourceIDs)
	}
	refs := map[string]bool{}
	for _, evidence := range plan.Entities[0].Evidence {
		refs[evidence.Ref] = true
	}
	for _, want := range []string{"frontier:entity-a", "frontier:entity-b", "raw:entity-a", "raw:entity-b", "parse:entity-a", "parse:entity-b"} {
		if !refs[want] {
			t.Fatalf("expected merged evidence ref %q in %#v", want, refs)
		}
	}
	statements, err := plan.SQLStatements()
	if err != nil {
		t.Fatalf("sql statements: %v", err)
	}
	joined := strings.Join(statements, "\n")
	if !strings.Contains(joined, `"source_ids":["fixture:registry-a","fixture:registry-b"]`) {
		t.Fatalf("expected SQL to preserve merged source_ids lineage, got %s", joined)
	}
}

func sharedCanonicalEntityInput(sourceID, rawID, parseID, frontierID string) Input {
	input := SampleInputs()[2]
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

func cloneReplayInput(input Input) Input {
	clone := input
	clone.Discovery = input.Discovery
	clone.Fetch = input.Fetch
	clone.Parse = input.Parse
	clone.Parse.Candidate = input.Parse.Candidate
	clone.Parse.Candidate.Data = cloneMap(input.Parse.Candidate.Data)
	clone.Parse.Candidate.Attrs = cloneMap(input.Parse.Candidate.Attrs)
	clone.Parse.Candidate.Evidence = append([]parser.Evidence(nil), input.Parse.Candidate.Evidence...)
	clone.Location = input.Location
	clone.Location.ParentPlaceChain = append([]string(nil), input.Location.ParentPlaceChain...)
	clone.Location.Attrs = cloneMap(input.Location.Attrs)
	return clone
}

func planCanonicalIDs(plan Plan) map[string][]string {
	ids := map[string][]string{
		"observation": make([]string, 0, len(plan.Observations)),
		"event":       make([]string, 0, len(plan.Events)),
		"entity":      make([]string, 0, len(plan.Entities)),
		"unresolved":  make([]string, 0, len(plan.Unresolved)),
	}
	for _, row := range plan.Observations {
		ids["observation"] = append(ids["observation"], row.ObservationID)
	}
	for _, row := range plan.Events {
		ids["event"] = append(ids["event"], row.EventID)
	}
	for _, row := range plan.Entities {
		ids["entity"] = append(ids["entity"], row.EntityID)
	}
	for _, row := range plan.Unresolved {
		ids["unresolved"] = append(ids["unresolved"], row.QueueID)
	}
	for _, key := range []string{"observation", "event", "entity", "unresolved"} {
		sort.Strings(ids[key])
	}
	return ids
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
