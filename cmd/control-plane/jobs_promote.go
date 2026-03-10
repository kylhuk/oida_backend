package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"global-osint-backend/internal/migrate"
	"global-osint-backend/internal/promote"
)

const (
	promoteJobName      = "promote"
	promoteInputPathEnv = "PROMOTE_PIPELINE_INPUT"
	promoteInputJSONEnv = "PROMOTE_PIPELINE_INPUT_JSON"
	promoteJobType      = "promote"
	promoteFailureRetry = 6 * time.Hour
)

func init() {
	jobRegistry[promoteJobName] = jobRunner{
		description: "Promote resolved canonical records into silver facts.",
		run:         runPromote,
	}
}

func runPromote(ctx context.Context) error {
	startedAt := time.Now().UTC().Truncate(time.Millisecond)
	runner := migrate.NewHTTPRunner(controlPlaneClickHouseURL())
	jobID := fmt.Sprintf("job:%s:%d", promoteJobName, startedAt.UnixMilli())

	recordFailure := func(err error, message string, stats map[string]any) error {
		if recordErr := recordJobRun(ctx, runner, jobID, promoteJobType, "failed", startedAt, time.Now().UTC().Truncate(time.Millisecond), message, stats); recordErr != nil {
			return fmt.Errorf("%w (job log failed: %v)", err, recordErr)
		}
		return err
	}

	inputs, err := loadPromotionInputs()
	if err != nil {
		return recordFailure(err, "load promotion inputs", map[string]any{"stage": "load_inputs"})
	}

	pipeline := promote.NewPipeline(promote.Options{Now: func() time.Time { return startedAt }})
	plan, err := pipeline.Prepare(inputs)
	if err != nil {
		return recordFailure(err, "prepare promotion plan", map[string]any{"stage": "prepare"})
	}
	statements, err := plan.SQLStatements()
	if err != nil {
		return recordFailure(err, "build promotion sql", map[string]any{"stage": "sql"})
	}
	for _, statement := range statements {
		if err := runner.ApplySQL(ctx, statement); err != nil {
			return recordFailure(err, "apply promotion sql", map[string]any{"stage": "apply"})
		}
	}

	stats := map[string]any{
		"input_rows":          plan.Stats.Inputs,
		"observation_rows":    len(plan.Observations),
		"event_rows":          len(plan.Events),
		"entity_rows":         len(plan.Entities),
		"unresolved_rows":     len(plan.Unresolved),
		"sql_statements":      len(statements),
		"retry_interval_h":    int(promoteFailureRetry.Hours()),
		"resolved_candidates": plan.Stats.ResolvedCandidates,
	}
	if err := recordJobRun(ctx, runner, jobID, promoteJobType, "success", startedAt, time.Now().UTC().Truncate(time.Millisecond), "promoted canonical records into silver", stats); err != nil {
		return err
	}
	return nil
}

func loadPromotionInputs() ([]promote.Input, error) {
	if raw := strings.TrimSpace(os.Getenv(promoteInputJSONEnv)); raw != "" {
		return decodePromotionInputs([]byte(raw))
	}
	if path := strings.TrimSpace(os.Getenv(promoteInputPathEnv)); path != "" {
		payload, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		return decodePromotionInputs(payload)
	}
	return promote.SampleInputs(), nil
}

func decodePromotionInputs(payload []byte) ([]promote.Input, error) {
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.UseNumber()
	var inputs []promote.Input
	if err := decoder.Decode(&inputs); err != nil {
		return nil, err
	}
	return inputs, nil
}
