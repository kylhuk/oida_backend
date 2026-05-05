package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"global-osint-backend/internal/migrate"
	"global-osint-backend/internal/promote"
)

const (
	pipelineExecuteJobName        = "pipeline-execute"
	pipelineRegistrySchemaVersion = 1
	pipelineRegistryAPIContract   = 1
	pipelineRunSchemaVersion      = 1
	pipelineRunAPIContract        = 1
	storedPipelineKindPromote     = "promote"
	pipelineRunStatusPlanned      = "planned"
	pipelineRunStatusPending      = "pending"
	pipelineRunStatusRunning      = "running"
	pipelineRunStatusSucceeded    = "succeeded"
	pipelineRunStatusFailed       = "failed"
	defaultPipelineOutputsJSON    = `{"outputs":[]}`
	defaultPipelineAttrsJSON      = `{}`
	defaultPipelineEvidenceJSON   = `[]`
)

func init() {
	jobRegistry[pipelineExecuteJobName] = jobRunner{
		description: "Execute one stored pipeline definition with durable run state.",
		run:         runStoredPipeline,
	}
}

type storedPipelineDefinition struct {
	PipelineID         string `json:"pipeline_id"`
	PipelineName       string `json:"pipeline_name"`
	PipelineKind       string `json:"pipeline_kind"`
	DefinitionJSON     string `json:"definition_json"`
	DefinitionChecksum string `json:"definition_checksum"`
	Enabled            uint8  `json:"enabled"`
	SchemaVersion      uint32 `json:"schema_version"`
	RecordVersion      uint64 `json:"record_version"`
	APIContractVersion uint32 `json:"api_contract_version"`
	Attrs              string `json:"attrs"`
	Evidence           string `json:"evidence"`
}

type storedPromotePipelineDefinition struct {
	Inputs []promote.Input `json:"inputs"`
}

type pipelineRunRecord struct {
	PipelineID         string     `json:"pipeline_id"`
	RunID              string     `json:"run_id"`
	RunKey             string     `json:"run_key"`
	DefinitionChecksum string     `json:"definition_checksum"`
	Status             string     `json:"status"`
	AttemptCount       uint16     `json:"attempt_count"`
	StartedAt          *time.Time `json:"started_at"`
	FinishedAt         *time.Time `json:"finished_at"`
	Message            string     `json:"message"`
	ErrorMessage       *string    `json:"error_message"`
	OutputsJSON        string     `json:"outputs_json"`
	SchemaVersion      uint32     `json:"schema_version"`
	RecordVersion      uint64     `json:"record_version"`
	APIContractVersion uint32     `json:"api_contract_version"`
	UpdatedAt          time.Time  `json:"updated_at"`
	Attrs              string     `json:"attrs"`
	Evidence           string     `json:"evidence"`
}

type pipelineRunSnapshot struct {
	PipelineID         string
	RunID              string
	RunKey             string
	DefinitionChecksum string
	Status             string
	AttemptCount       uint16
	StartedAt          *time.Time
	FinishedAt         *time.Time
	Message            string
	ErrorMessage       string
	OutputsJSON        string
	RecordVersion      uint64
	UpdatedAt          time.Time
	Attrs              string
	Evidence           string
}

type pipelineExecutionResult struct {
	Run     pipelineRunRecord
	Skipped bool
	Outputs string
}

func runStoredPipeline(ctx context.Context) error {
	startedAt := time.Now().UTC().Truncate(time.Millisecond)
	runner := migrate.NewHTTPRunner(controlPlaneClickHouseURL())
	_, err := runStoredPipelineWithRunner(ctx, runner, startedAt)
	return err
}

func runStoredPipelineWithRunner(ctx context.Context, runner *migrate.HTTPRunner, startedAt time.Time) (pipelineExecutionResult, error) {
	pipelineID := strings.TrimSpace(currentJobOptions(ctx).PipelineID)
	if pipelineID == "" {
		return pipelineExecutionResult{}, fmt.Errorf("pipeline-execute requires --pipeline-id")
	}

	definition, err := loadStoredPipelineDefinition(ctx, runner, pipelineID)
	if err != nil {
		return pipelineExecutionResult{}, err
	}
	runKey := buildStoredPipelineRunKey(definition)
	runID := buildStoredPipelineRunID(runKey)
	existing, err := loadLatestPipelineRun(ctx, runner, definition.PipelineID, runKey)
	if err != nil {
		return pipelineExecutionResult{}, err
	}
	if existing != nil && (existing.Status == pipelineRunStatusPending || existing.Status == pipelineRunStatusRunning || existing.Status == pipelineRunStatusSucceeded) {
		return pipelineExecutionResult{Run: *existing, Skipped: true, Outputs: defaultJSONOr(existing.OutputsJSON, defaultPipelineOutputsJSON)}, nil
	}

	attemptCount := uint16(1)
	if existing != nil && existing.AttemptCount > 0 {
		attemptCount = existing.AttemptCount + 1
	}
	attrs := defaultJSONOr(definition.Attrs, defaultPipelineAttrsJSON)
	evidence := defaultJSONOr(definition.Evidence, defaultPipelineEvidenceJSON)
	nextVersion := nextPipelineRunRecordVersion(existing, startedAt)

	plannedAt := startedAt
	planned := pipelineRunSnapshot{
		PipelineID:         definition.PipelineID,
		RunID:              runID,
		RunKey:             runKey,
		DefinitionChecksum: pipelineDefinitionChecksum(definition),
		Status:             pipelineRunStatusPlanned,
		AttemptCount:       attemptCount,
		StartedAt:          nil,
		FinishedAt:         nil,
		Message:            "planned stored pipeline execution",
		OutputsJSON:        defaultPipelineOutputsJSON,
		RecordVersion:      nextVersion,
		UpdatedAt:          plannedAt,
		Attrs:              attrs,
		Evidence:           evidence,
	}
	if err := persistPipelineRunSnapshot(ctx, runner, planned); err != nil {
		return pipelineExecutionResult{}, err
	}

	nextVersion++
	pending := planned
	pending.Status = pipelineRunStatusPending
	pending.StartedAt = &startedAt
	pending.Message = "claimed stored pipeline execution"
	pending.RecordVersion = nextVersion
	pending.UpdatedAt = startedAt
	if err := persistPipelineRunSnapshot(ctx, runner, pending); err != nil {
		return pipelineExecutionResult{}, err
	}

	nextVersion++
	running := pending
	running.Status = pipelineRunStatusRunning
	running.Message = "running stored pipeline execution"
	running.RecordVersion = nextVersion
	running.UpdatedAt = startedAt
	if err := persistPipelineRunSnapshot(ctx, runner, running); err != nil {
		return pipelineExecutionResult{}, err
	}

	outputsJSON, execErr := executeStoredPipelineDefinition(ctx, runner, startedAt, definition)
	finishedAt := time.Now().UTC().Truncate(time.Millisecond)
	nextVersion++
	terminal := running
	terminal.RecordVersion = nextVersion
	terminal.UpdatedAt = finishedAt
	terminal.FinishedAt = &finishedAt
	terminal.OutputsJSON = defaultJSONOr(outputsJSON, defaultPipelineOutputsJSON)
	if execErr != nil {
		terminal.Status = pipelineRunStatusFailed
		terminal.Message = "stored pipeline execution failed"
		terminal.ErrorMessage = execErr.Error()
		if err := persistPipelineRunSnapshot(ctx, runner, terminal); err != nil {
			return pipelineExecutionResult{}, fmt.Errorf("%w (pipeline run persistence failed: %v)", execErr, err)
		}
		return pipelineExecutionResult{}, execErr
	}
	terminal.Status = pipelineRunStatusSucceeded
	terminal.Message = "stored pipeline execution succeeded"
	if err := persistPipelineRunSnapshot(ctx, runner, terminal); err != nil {
		return pipelineExecutionResult{}, err
	}
	return pipelineExecutionResult{Run: pipelineRunRecord{PipelineID: terminal.PipelineID, RunID: terminal.RunID, RunKey: terminal.RunKey, DefinitionChecksum: terminal.DefinitionChecksum, Status: terminal.Status, AttemptCount: terminal.AttemptCount, StartedAt: terminal.StartedAt, FinishedAt: terminal.FinishedAt, Message: terminal.Message, OutputsJSON: terminal.OutputsJSON, SchemaVersion: pipelineRunSchemaVersion, RecordVersion: terminal.RecordVersion, APIContractVersion: pipelineRunAPIContract, UpdatedAt: terminal.UpdatedAt, Attrs: terminal.Attrs, Evidence: terminal.Evidence}, Outputs: terminal.OutputsJSON}, nil
}

func loadStoredPipelineDefinition(ctx context.Context, runner *migrate.HTTPRunner, pipelineID string) (storedPipelineDefinition, error) {
	query := fmt.Sprintf(`SELECT pipeline_id, pipeline_name, pipeline_kind, definition_json, definition_checksum, enabled, schema_version, record_version, api_contract_version, attrs, evidence
FROM meta.pipeline_registry FINAL
WHERE pipeline_id = %s
ORDER BY record_version DESC
LIMIT 1
FORMAT JSONEachRow`, sqlString(pipelineID))
	output, err := runner.Query(ctx, query)
	if err != nil {
		return storedPipelineDefinition{}, err
	}
	line := strings.TrimSpace(output)
	if line == "" {
		return storedPipelineDefinition{}, fmt.Errorf("stored pipeline %q not found", pipelineID)
	}
	var definition storedPipelineDefinition
	if err := json.Unmarshal([]byte(line), &definition); err != nil {
		return storedPipelineDefinition{}, err
	}
	if strings.TrimSpace(definition.PipelineID) == "" {
		return storedPipelineDefinition{}, fmt.Errorf("stored pipeline %q missing pipeline_id", pipelineID)
	}
	if definition.Enabled == 0 {
		return storedPipelineDefinition{}, fmt.Errorf("stored pipeline %q is disabled", pipelineID)
	}
	if strings.TrimSpace(definition.PipelineKind) == "" {
		return storedPipelineDefinition{}, fmt.Errorf("stored pipeline %q missing pipeline_kind", pipelineID)
	}
	if strings.TrimSpace(definition.DefinitionJSON) == "" {
		return storedPipelineDefinition{}, fmt.Errorf("stored pipeline %q missing definition_json", pipelineID)
	}
	return definition, nil
}

func loadLatestPipelineRun(ctx context.Context, runner *migrate.HTTPRunner, pipelineID, runKey string) (*pipelineRunRecord, error) {
	query := fmt.Sprintf(`SELECT pipeline_id, run_id, run_key, definition_checksum, status, attempt_count, started_at, finished_at, message, error_message, outputs_json, schema_version, record_version, api_contract_version, updated_at, attrs, evidence
FROM ops.pipeline_run FINAL
WHERE pipeline_id = %s AND run_key = %s
ORDER BY record_version DESC
LIMIT 1
FORMAT JSONEachRow`, sqlString(pipelineID), sqlString(runKey))
	output, err := runner.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	line := strings.TrimSpace(output)
	if line == "" {
		return nil, nil
	}
	var record pipelineRunRecord
	if err := json.Unmarshal([]byte(line), &record); err != nil {
		return nil, err
	}
	return &record, nil
}

func persistPipelineRunSnapshot(ctx context.Context, runner *migrate.HTTPRunner, snapshot pipelineRunSnapshot) error {
	query := fmt.Sprintf(`INSERT INTO ops.pipeline_run (pipeline_id, run_id, run_key, definition_checksum, status, attempt_count, started_at, finished_at, message, error_message, outputs_json, schema_version, record_version, api_contract_version, updated_at, attrs, evidence) VALUES (%s,%s,%s,%s,%s,%d,%s,%s,%s,%s,%s,%d,%d,%d,%s,%s,%s)`,
		sqlString(snapshot.PipelineID),
		sqlString(snapshot.RunID),
		sqlString(snapshot.RunKey),
		sqlString(snapshot.DefinitionChecksum),
		sqlString(snapshot.Status),
		snapshot.AttemptCount,
		nullablePipelineSQLTime(snapshot.StartedAt),
		nullablePipelineSQLTime(snapshot.FinishedAt),
		sqlString(snapshot.Message),
		nullableSQLString(snapshot.ErrorMessage),
		sqlString(defaultJSONOr(snapshot.OutputsJSON, defaultPipelineOutputsJSON)),
		pipelineRunSchemaVersion,
		snapshot.RecordVersion,
		pipelineRunAPIContract,
		sqlTime(snapshot.UpdatedAt),
		sqlString(defaultJSONOr(snapshot.Attrs, defaultPipelineAttrsJSON)),
		sqlString(defaultJSONOr(snapshot.Evidence, defaultPipelineEvidenceJSON)),
	)
	return runner.ApplySQL(ctx, query)
}

func executeStoredPipelineDefinition(ctx context.Context, runner *migrate.HTTPRunner, startedAt time.Time, definition storedPipelineDefinition) (string, error) {
	switch strings.TrimSpace(definition.PipelineKind) {
	case storedPipelineKindPromote:
		var promoteDefinition storedPromotePipelineDefinition
		if err := json.Unmarshal([]byte(definition.DefinitionJSON), &promoteDefinition); err != nil {
			return "", fmt.Errorf("decode promote pipeline definition: %w", err)
		}
		result, err := executePromoteInputsWithRunner(ctx, runner, startedAt, promoteDefinition.Inputs)
		if err != nil {
			return "", err
		}
		return buildPipelineOutputsJSON(definition, result)
	default:
		return "", fmt.Errorf("stored pipeline %q uses unsupported pipeline_kind %q", definition.PipelineID, definition.PipelineKind)
	}
}

func buildPipelineOutputsJSON(definition storedPipelineDefinition, result promoteExecutionResult) (string, error) {
	statementDigests := make([]string, 0, len(result.Statements))
	for _, statement := range result.Statements {
		digest := sha256.Sum256([]byte(statement))
		statementDigests = append(statementDigests, hex.EncodeToString(digest[:8]))
	}
	payload := map[string]any{
		"pipeline_id":         definition.PipelineID,
		"pipeline_kind":       definition.PipelineKind,
		"definition_checksum": pipelineDefinitionChecksum(definition),
		"source_ids":          result.SourceIDs,
		"statement_count":     len(result.Statements),
		"statement_digests":   statementDigests,
		"stats":               result.Stats,
		"finished_at":         result.FinishedAt.UTC().Format(time.RFC3339Nano),
	}
	encoded, err := marshalJSONString(payload)
	if err != nil {
		return "", err
	}
	return encoded, nil
}

func buildStoredPipelineRunKey(definition storedPipelineDefinition) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(definition.PipelineID) + "\n" + pipelineDefinitionChecksum(definition)))
	return "runkey:" + hex.EncodeToString(sum[:])
}

func buildStoredPipelineRunID(runKey string) string {
	sum := sha256.Sum256([]byte(runKey))
	return "run:" + hex.EncodeToString(sum[:12])
}

func pipelineDefinitionChecksum(definition storedPipelineDefinition) string {
	if strings.TrimSpace(definition.DefinitionChecksum) != "" {
		return strings.TrimSpace(definition.DefinitionChecksum)
	}
	sum := sha256.Sum256([]byte(definition.DefinitionJSON))
	return hex.EncodeToString(sum[:])
}

func nextPipelineRunRecordVersion(existing *pipelineRunRecord, now time.Time) uint64 {
	next := uint64(now.UTC().UnixMilli())
	if existing != nil && existing.RecordVersion >= next {
		return existing.RecordVersion + 1
	}
	return next
}

func nullablePipelineSQLTime(value *time.Time) string {
	if value == nil || value.IsZero() {
		return "NULL"
	}
	return sqlTime(value.UTC())
}

func defaultJSONOr(value, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}
