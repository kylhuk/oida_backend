package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"global-osint-backend/internal/migrate"
	"global-osint-backend/internal/parser"
	"global-osint-backend/internal/promote"
)

const (
	promoteJobName                 = "promote"
	promoteInputPathEnv            = "PROMOTE_PIPELINE_INPUT"
	promoteInputJSONEnv            = "PROMOTE_PIPELINE_INPUT_JSON"
	promoteBackfillStagingTagEnv   = "PROMOTE_BACKFILL_STAGING_TAG"
	promoteBronzeTableOverridesEnv = "PROMOTE_BRONZE_TABLE_OVERRIDES_JSON"
	promoteJobType                 = "promote"
	promoteFailureRetry            = 6 * time.Hour
	promoteCheckpointSchemaVersion = 1
	promoteCheckpointAPIContract   = 1
	promoteSelectionModeDelta      = "delta"
	promoteSelectionModeRange      = "range"
	promoteCheckpointRunning       = "running"
	promoteCheckpointSucceeded     = "succeeded"
	promoteCheckpointFailed        = "failed"
)

var bronzeTablePattern = regexp.MustCompile(`^bronze\.[a-z0-9_]+$`)

func init() {
	jobRegistry[promoteJobName] = jobRunner{
		description: "Promote resolved canonical records into silver facts.",
		run:         runPromote,
	}
}

func runPromote(ctx context.Context) error {
	startedAt := time.Now().UTC().Truncate(time.Millisecond)
	runner := migrate.NewHTTPRunner(controlPlaneClickHouseURL())
	_, err := runPromoteWithRunner(ctx, runner, startedAt)
	return err
}

func runPromoteWithRunner(ctx context.Context, runner *migrate.HTTPRunner, startedAt time.Time) (promoteExecutionResult, error) {
	jobID := fmt.Sprintf("job:%s:%d", promoteJobName, startedAt.UnixMilli())

	recordFailure := func(err error, message string, stats map[string]any) error {
		if recordErr := recordJobRun(ctx, runner, jobID, promoteJobType, "failed", startedAt, time.Now().UTC().Truncate(time.Millisecond), message, stats); recordErr != nil {
			return fmt.Errorf("%w (job log failed: %v)", err, recordErr)
		}
		return err
	}

	var (
		result promoteExecutionResult
		err    error
	)
	if hasPromotionInputOverride() {
		inputs, loadErr := loadPromotionInputs(ctx, runner)
		if loadErr != nil {
			return promoteExecutionResult{}, recordFailure(loadErr, "load promotion inputs", map[string]any{"stage": "load_inputs"})
		}
		result, err = executePromoteInputsWithRunner(ctx, runner, startedAt, inputs)
	} else {
		result, err = executePromoteWindowsWithRunner(ctx, runner, startedAt)
	}
	if err != nil {
		return promoteExecutionResult{}, recordFailure(err, "execute promotion plan", map[string]any{"stage": "execute"})
	}

	if err := recordJobRun(ctx, runner, jobID, promoteJobType, "success", startedAt, result.FinishedAt, "promoted canonical records into silver", result.Stats); err != nil {
		return promoteExecutionResult{}, err
	}
	return result, nil
}

func executePromoteInputsWithRunner(ctx context.Context, runner *migrate.HTTPRunner, startedAt time.Time, inputs []promote.Input) (promoteExecutionResult, error) {

	pipeline := promote.NewPipeline(promote.Options{Now: func() time.Time { return startedAt }})
	plan, err := pipeline.Prepare(inputs)
	if err != nil {
		return promoteExecutionResult{}, err
	}
	statements, err := plan.SQLStatements()
	if err != nil {
		return promoteExecutionResult{}, err
	}
	statements, backfillMode, err := buildPromoteExecutionStatements(statements)
	if err != nil {
		return promoteExecutionResult{}, err
	}
	for _, statement := range statements {
		if err := runner.ApplySQL(ctx, statement); err != nil {
			return promoteExecutionResult{}, err
		}
	}

	finishedAt := time.Now().UTC().Truncate(time.Millisecond)
	stats := map[string]any{
		"input_rows":          plan.Stats.Inputs,
		"observation_rows":    len(plan.Observations),
		"event_rows":          len(plan.Events),
		"entity_rows":         len(plan.Entities),
		"unresolved_rows":     len(plan.Unresolved),
		"sql_statements":      len(statements),
		"retry_interval_h":    int(promoteFailureRetry.Hours()),
		"resolved_candidates": plan.Stats.ResolvedCandidates,
		"insert_deduplicate":  1,
		"deduplicate_blocks_in_dependent_materialized_views": 1,
		"async_insert": 0,
	}
	if backfillMode != "" {
		stats["backfill_cutover_mode"] = backfillMode
	}
	return promoteExecutionResult{
		SourceIDs:  collectPromoteSourceIDs(inputs),
		Plan:       plan,
		Statements: statements,
		StartedAt:  startedAt,
		FinishedAt: finishedAt,
		Stats:      stats,
	}, nil
}

type bronzePromoteSource struct {
	SourceID     string  `json:"source_id"`
	BronzeTable  *string `json:"bronze_table"`
	CrawlEnabled uint8   `json:"crawl_enabled"`
}

type promoteSourceWindowKey struct {
	SourceID    string
	BronzeTable string
}

type bronzePromoteRow struct {
	RawID         string     `json:"raw_id"`
	SourceID      string     `json:"source_id"`
	ParserID      string     `json:"parser_id"`
	ParserVersion string     `json:"parser_version"`
	RecordKind    string     `json:"record_kind"`
	NativeID      *string    `json:"native_id"`
	SourceURL     string     `json:"source_url"`
	CanonicalURL  *string    `json:"canonical_url"`
	FetchedAt     time.Time  `json:"fetched_at"`
	ParsedAt      time.Time  `json:"parsed_at"`
	OccurredAt    *time.Time `json:"occurred_at"`
	PublishedAt   *time.Time `json:"published_at"`
	Status        *string    `json:"status"`
	Title         *string    `json:"title"`
	Summary       *string    `json:"summary"`
	PlaceHint     *string    `json:"place_hint"`
	Lat           *float64   `json:"lat"`
	Lon           *float64   `json:"lon"`
	Severity      *string    `json:"severity"`
	ContentHash   string     `json:"content_hash"`
	SchemaVersion uint32     `json:"schema_version"`
	RecordVersion uint64     `json:"record_version"`
	Attrs         string     `json:"attrs"`
	Evidence      string     `json:"evidence"`
	PayloadJSON   string     `json:"payload_json"`
}

type promoteExecutionResult struct {
	SourceIDs  []string
	Plan       promote.Plan
	Statements []string
	StartedAt  time.Time
	FinishedAt time.Time
	Stats      map[string]any
}

type promoteSelectionWindow struct {
	Mode                  string
	WindowStart           time.Time
	WindowEnd             time.Time
	SourceID              string
	ConfiguredBronzeTable string
	QueryBronzeTable      string
	CheckpointID          string
	LatestCheckpoint      *promoteCheckpointRecord
}

type promoteCheckpointRecord struct {
	CheckpointID  string     `json:"checkpoint_id"`
	SourceID      string     `json:"source_id"`
	BronzeTable   string     `json:"bronze_table"`
	SelectionMode string     `json:"selection_mode"`
	WindowStart   *time.Time `json:"window_start"`
	WindowEnd     *time.Time `json:"window_end"`
	Status        string     `json:"status"`
	AttemptCount  uint16     `json:"attempt_count"`
	StartedAt     *time.Time `json:"started_at"`
	FinishedAt    *time.Time `json:"finished_at"`
	InputRows     uint32     `json:"input_rows"`
	ErrorMessage  *string    `json:"error_message"`
	RecordVersion uint64     `json:"record_version"`
	UpdatedAt     time.Time  `json:"updated_at"`
	Attrs         string     `json:"attrs"`
	Evidence      string     `json:"evidence"`
}

func collectPromoteSourceIDs(inputs []promote.Input) []string {
	seen := make(map[string]struct{}, len(inputs))
	sourceIDs := make([]string, 0, len(inputs))
	for _, input := range inputs {
		sourceID := strings.TrimSpace(input.SourceID)
		if sourceID == "" {
			continue
		}
		if _, ok := seen[sourceID]; ok {
			continue
		}
		seen[sourceID] = struct{}{}
		sourceIDs = append(sourceIDs, sourceID)
	}
	sort.Strings(sourceIDs)
	return sourceIDs
}

func hasPromotionInputOverride() bool {
	return strings.TrimSpace(os.Getenv(promoteInputJSONEnv)) != "" || strings.TrimSpace(os.Getenv(promoteInputPathEnv)) != ""
}

func executePromoteWindowsWithRunner(ctx context.Context, runner *migrate.HTTPRunner, startedAt time.Time) (promoteExecutionResult, error) {
	options := currentJobOptions(ctx)
	windows, err := loadPromotionWindows(ctx, runner, options, startedAt)
	if err != nil {
		return promoteExecutionResult{}, err
	}
	result := promoteExecutionResult{
		StartedAt:  startedAt,
		FinishedAt: startedAt,
		Stats: map[string]any{
			"window_count":        len(windows),
			"executed_windows":    0,
			"skipped_windows":     0,
			"input_rows":          0,
			"observation_rows":    0,
			"event_rows":          0,
			"entity_rows":         0,
			"unresolved_rows":     0,
			"sql_statements":      0,
			"resolved_candidates": 0,
			"retry_interval_h":    int(promoteFailureRetry.Hours()),
			"insert_deduplicate":  1,
			"deduplicate_blocks_in_dependent_materialized_views": 1,
			"async_insert": 0,
		},
	}
	if len(windows) == 0 {
		result.FinishedAt = time.Now().UTC().Truncate(time.Millisecond)
		return result, nil
	}
	result.Stats["selection_mode"] = windows[0].Mode
	if windows[0].Mode == promoteSelectionModeRange {
		result.Stats["window_start"] = windows[0].WindowStart.UTC().Format(time.RFC3339Nano)
		result.Stats["window_end"] = windows[0].WindowEnd.UTC().Format(time.RFC3339Nano)
	}
	seenSources := map[string]struct{}{}
	for _, window := range windows {
		if window.LatestCheckpoint != nil && window.LatestCheckpoint.Status == promoteCheckpointSucceeded {
			result.Stats["skipped_windows"] = result.Stats["skipped_windows"].(int) + 1
			continue
		}
		attemptCount := uint16(1)
		if window.LatestCheckpoint != nil && window.LatestCheckpoint.AttemptCount > 0 {
			attemptCount = window.LatestCheckpoint.AttemptCount + 1
		}
		if err := persistPromoteCheckpointSnapshot(ctx, runner, window, promoteCheckpointRunning, attemptCount, startedAt, nil, 0, ""); err != nil {
			return promoteExecutionResult{}, err
		}
		window.LatestCheckpoint = &promoteCheckpointRecord{RecordVersion: uint64(startedAt.UTC().UnixMilli()), AttemptCount: attemptCount, Status: promoteCheckpointRunning}
		inputs, err := loadPromotionInputsForWindow(ctx, runner, window)
		if err != nil {
			finishedAt := time.Now().UTC().Truncate(time.Millisecond)
			if checkpointErr := persistPromoteCheckpointSnapshot(ctx, runner, window, promoteCheckpointFailed, attemptCount, startedAt, &finishedAt, 0, err.Error()); checkpointErr != nil {
				return promoteExecutionResult{}, fmt.Errorf("%w (promote checkpoint persistence failed: %v)", err, checkpointErr)
			}
			return promoteExecutionResult{}, err
		}
		if len(inputs) == 0 {
			finishedAt := time.Now().UTC().Truncate(time.Millisecond)
			if err := persistPromoteCheckpointSnapshot(ctx, runner, window, promoteCheckpointSucceeded, attemptCount, startedAt, &finishedAt, 0, ""); err != nil {
				return promoteExecutionResult{}, err
			}
			result.Stats["executed_windows"] = result.Stats["executed_windows"].(int) + 1
			continue
		}
		windowResult, err := executePromoteInputsWithRunner(ctx, runner, startedAt, inputs)
		if err != nil {
			finishedAt := time.Now().UTC().Truncate(time.Millisecond)
			if checkpointErr := persistPromoteCheckpointSnapshot(ctx, runner, window, promoteCheckpointFailed, attemptCount, startedAt, &finishedAt, uint32(len(inputs)), err.Error()); checkpointErr != nil {
				return promoteExecutionResult{}, fmt.Errorf("%w (promote checkpoint persistence failed: %v)", err, checkpointErr)
			}
			return promoteExecutionResult{}, err
		}
		if err := persistPromoteCheckpointSnapshot(ctx, runner, window, promoteCheckpointSucceeded, attemptCount, startedAt, &windowResult.FinishedAt, uint32(len(inputs)), ""); err != nil {
			return promoteExecutionResult{}, err
		}
		result.Stats["executed_windows"] = result.Stats["executed_windows"].(int) + 1
		result.Stats["input_rows"] = result.Stats["input_rows"].(int) + intValue(windowResult.Stats["input_rows"])
		result.Stats["observation_rows"] = result.Stats["observation_rows"].(int) + intValue(windowResult.Stats["observation_rows"])
		result.Stats["event_rows"] = result.Stats["event_rows"].(int) + intValue(windowResult.Stats["event_rows"])
		result.Stats["entity_rows"] = result.Stats["entity_rows"].(int) + intValue(windowResult.Stats["entity_rows"])
		result.Stats["unresolved_rows"] = result.Stats["unresolved_rows"].(int) + intValue(windowResult.Stats["unresolved_rows"])
		result.Stats["sql_statements"] = result.Stats["sql_statements"].(int) + intValue(windowResult.Stats["sql_statements"])
		result.Stats["resolved_candidates"] = result.Stats["resolved_candidates"].(int) + intValue(windowResult.Stats["resolved_candidates"])
		result.Statements = append(result.Statements, windowResult.Statements...)
		for _, sourceID := range windowResult.SourceIDs {
			if _, ok := seenSources[sourceID]; ok {
				continue
			}
			seenSources[sourceID] = struct{}{}
			result.SourceIDs = append(result.SourceIDs, sourceID)
		}
		result.FinishedAt = windowResult.FinishedAt
	}
	if result.FinishedAt.IsZero() || result.FinishedAt.Equal(startedAt) {
		result.FinishedAt = time.Now().UTC().Truncate(time.Millisecond)
	}
	sort.Strings(result.SourceIDs)
	return result, nil
}

func intValue(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case uint32:
		return int(typed)
	case uint64:
		return int(typed)
	case float64:
		return int(typed)
	default:
		return 0
	}
}

func loadPromotionInputs(ctx context.Context, runner *migrate.HTTPRunner) ([]promote.Input, error) {
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
	return loadPromotionInputsFromBronze(ctx, runner)
}

func loadPromotionInputsFromBronze(ctx context.Context, runner *migrate.HTTPRunner) ([]promote.Input, error) {
	windows, err := loadPromotionWindows(ctx, runner, jobOptions{}, time.Now().UTC().Truncate(time.Millisecond))
	if err != nil {
		return nil, err
	}
	inputs := make([]promote.Input, 0)
	for _, window := range windows {
		windowInputs, err := loadPromotionInputsForWindow(ctx, runner, window)
		if err != nil {
			return nil, err
		}
		inputs = append(inputs, windowInputs...)
	}
	sort.Slice(inputs, func(i, j int) bool {
		if inputs[i].SourceID != inputs[j].SourceID {
			return inputs[i].SourceID < inputs[j].SourceID
		}
		return inputs[i].Fetch.RawID < inputs[j].Fetch.RawID
	})
	return inputs, nil
}

func loadPromotionWindows(ctx context.Context, runner *migrate.HTTPRunner, options jobOptions, snapshotAt time.Time) ([]promoteSelectionWindow, error) {
	selection, err := resolvePromoteSelection(options, snapshotAt)
	if err != nil {
		return nil, err
	}
	query := `SELECT source_id, bronze_table, crawl_enabled
FROM meta.source_registry FINAL
WHERE transport_type = 'http' AND bronze_table IS NOT NULL AND enabled = 1
ORDER BY source_id
FORMAT JSONEachRow`
	output, err := runner.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSpace(output), "\n")
	sources := make([]bronzePromoteSource, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var source bronzePromoteSource
		if err := json.Unmarshal([]byte(line), &source); err != nil {
			return nil, err
		}
		if source.BronzeTable == nil || strings.TrimSpace(*source.BronzeTable) == "" {
			continue
		}
		sources = append(sources, source)
	}

	changedSources, err := loadChangedPromoteSources(ctx, runner, selection)
	if err != nil {
		return nil, err
	}
	tableOverrides, err := loadPromoteBronzeTableOverridesFromEnv()
	if err != nil {
		return nil, err
	}
	windows := make([]promoteSelectionWindow, 0)
	for _, source := range sources {
		configuredTable := configuredPromoteBronzeTable(source)
		queryTable := resolvePromoteBronzeTable(source, tableOverrides)
		if !bronzeTablePattern.MatchString(queryTable) {
			continue
		}
		changedSince, ok := changedSources[promoteSourceWindowKey{SourceID: source.SourceID, BronzeTable: configuredTable}]
		if !ok {
			continue
		}
		windowEnd := selection.WindowEnd
		windowStart := changedSince
		if selection.Mode == promoteSelectionModeRange {
			windowStart = selection.WindowStart
		}
		windows = append(windows, promoteSelectionWindow{
			Mode:                  selection.Mode,
			WindowStart:           windowStart,
			WindowEnd:             windowEnd,
			SourceID:              strings.TrimSpace(source.SourceID),
			ConfiguredBronzeTable: configuredTable,
			QueryBronzeTable:      queryTable,
			CheckpointID:          buildPromoteCheckpointID(strings.TrimSpace(source.SourceID), configuredTable, selection.Mode, windowStart, windowEnd),
		})
	}
	checkpoints, err := loadLatestPromoteCheckpoints(ctx, runner, collectPromoteCheckpointIDs(windows))
	if err != nil {
		return nil, err
	}
	for i := range windows {
		if checkpoint, ok := checkpoints[windows[i].CheckpointID]; ok {
			checkpointCopy := checkpoint
			windows[i].LatestCheckpoint = &checkpointCopy
		}
	}
	sort.Slice(windows, func(i, j int) bool {
		if windows[i].SourceID != windows[j].SourceID {
			return windows[i].SourceID < windows[j].SourceID
		}
		if windows[i].ConfiguredBronzeTable != windows[j].ConfiguredBronzeTable {
			return windows[i].ConfiguredBronzeTable < windows[j].ConfiguredBronzeTable
		}
		if !windows[i].WindowStart.Equal(windows[j].WindowStart) {
			return windows[i].WindowStart.Before(windows[j].WindowStart)
		}
		return windows[i].WindowEnd.Before(windows[j].WindowEnd)
	})
	return windows, nil
}

type promoteSelection struct {
	Mode        string
	WindowStart time.Time
	WindowEnd   time.Time
}

func resolvePromoteSelection(options jobOptions, snapshotAt time.Time) (promoteSelection, error) {
	startRaw := strings.TrimSpace(options.WindowStart)
	endRaw := strings.TrimSpace(options.WindowEnd)
	if options.DeltaOnly && (startRaw != "" || endRaw != "") {
		return promoteSelection{}, fmt.Errorf("promote replay cannot mix --delta-only with explicit window bounds")
	}
	if startRaw == "" && endRaw == "" {
		return promoteSelection{Mode: promoteSelectionModeDelta, WindowEnd: snapshotAt.UTC()}, nil
	}
	if startRaw == "" || endRaw == "" {
		return promoteSelection{}, fmt.Errorf("promote replay requires both --window-start and --window-end")
	}
	windowStart, err := parsePromoteWindowBound(startRaw)
	if err != nil {
		return promoteSelection{}, fmt.Errorf("parse --window-start: %w", err)
	}
	windowEnd, err := parsePromoteWindowBound(endRaw)
	if err != nil {
		return promoteSelection{}, fmt.Errorf("parse --window-end: %w", err)
	}
	if !windowEnd.After(windowStart) {
		return promoteSelection{}, fmt.Errorf("promote replay window end must be after window start")
	}
	return promoteSelection{Mode: promoteSelectionModeRange, WindowStart: windowStart, WindowEnd: windowEnd}, nil
}

func parsePromoteWindowBound(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05.000", "2006-01-02 15:04:05"} {
		parsed, err := time.Parse(layout, raw)
		if err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported timestamp %q", raw)
}

func loadPromotionInputsForWindow(ctx context.Context, runner *migrate.HTTPRunner, window promoteSelectionWindow) ([]promote.Input, error) {
	whereClauses := []string{fmt.Sprintf("parsed_at >= toDateTime64('%s', 3, 'UTC')", strings.TrimSpace(window.WindowStart.UTC().Format(time.RFC3339Nano)))}
	if !window.WindowEnd.IsZero() {
		whereClauses = append(whereClauses, fmt.Sprintf("parsed_at < toDateTime64('%s', 3, 'UTC')", strings.TrimSpace(window.WindowEnd.UTC().Format(time.RFC3339Nano))))
	}
	rowsQuery := fmt.Sprintf(`SELECT raw_id, source_id, parser_id, parser_version, record_kind, native_id, source_url, canonical_url, fetched_at, parsed_at, occurred_at, published_at, status, title, summary, place_hint, lat, lon, severity, content_hash, schema_version, record_version, attrs, evidence, payload_json
FROM %s
WHERE %s
ORDER BY parsed_at DESC, raw_id ASC, source_record_index ASC
FORMAT JSONEachRow`, window.QueryBronzeTable, strings.Join(whereClauses, " AND "))
	rowsOutput, err := runner.Query(ctx, rowsQuery)
	if err != nil {
		return nil, err
	}
	inputs := make([]promote.Input, 0)
	for _, line := range strings.Split(strings.TrimSpace(rowsOutput), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var row bronzePromoteRow
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, err
		}
		input, ok, err := bronzeRowToPromoteInput(row)
		if err != nil {
			return nil, err
		}
		if ok {
			inputs = append(inputs, input)
		}
	}
	return inputs, nil
}

func collectPromoteCheckpointIDs(windows []promoteSelectionWindow) []string {
	ids := make([]string, 0, len(windows))
	for _, window := range windows {
		if strings.TrimSpace(window.CheckpointID) == "" {
			continue
		}
		ids = append(ids, window.CheckpointID)
	}
	return ids
}

func loadChangedPromoteSources(ctx context.Context, runner *migrate.HTTPRunner, selection promoteSelection) (map[promoteSourceWindowKey]time.Time, error) {
	windowFilters := []string{}
	if selection.Mode == promoteSelectionModeRange {
		windowFilters = append(windowFilters,
			fmt.Sprintf("changed_at >= toDateTime64('%s', 3, 'UTC')", strings.TrimSpace(selection.WindowStart.UTC().Format(time.RFC3339Nano))),
			fmt.Sprintf("changed_at < toDateTime64('%s', 3, 'UTC')", strings.TrimSpace(selection.WindowEnd.UTC().Format(time.RFC3339Nano))),
		)
	} else {
		lastPromoteAt, err := loadLastSuccessfulPromoteAt(ctx, runner)
		if err != nil {
			return nil, err
		}
		if lastPromoteAt != nil && !lastPromoteAt.IsZero() {
			windowFilters = append(windowFilters, fmt.Sprintf("changed_at >= toDateTime64('%s', 3, 'UTC')", strings.TrimSpace(lastPromoteAt.UTC().Format(time.RFC3339Nano))))
		}
		if !selection.WindowEnd.IsZero() {
			windowFilters = append(windowFilters, fmt.Sprintf("changed_at < toDateTime64('%s', 3, 'UTC')", strings.TrimSpace(selection.WindowEnd.UTC().Format(time.RFC3339Nano))))
		}
	}
	windowFilter := ""
	if len(windowFilters) > 0 {
		windowFilter = "\nWHERE " + strings.Join(windowFilters, " AND ")
	}
	query := fmt.Sprintf(`SELECT source_id, bronze_table, min(changed_at) AS changed_since
FROM (
	SELECT source_id, bronze_table, parsed_at AS changed_at
	FROM ops.parse_checkpoint FINAL
	WHERE status = 'success'

	UNION ALL

	SELECT registry.source_id AS source_id, registry.bronze_table AS bronze_table, fetch.fetched_at AS changed_at
	FROM ops.fetch_log FINAL AS fetch
	INNER JOIN meta.source_registry FINAL AS registry ON registry.source_id = fetch.source_id
	WHERE fetch.success = 1
	  AND registry.transport_type = 'http'
	  AND registry.enabled = 1
	  AND registry.bronze_table IS NOT NULL
	  AND registry.bronze_table != ''
) changed%s
GROUP BY source_id, bronze_table
FORMAT JSONEachRow`, windowFilter)
	output, err := runner.Query(ctx, query)
	if err != nil {
		if strings.Contains(err.Error(), "UNKNOWN_TABLE") {
			return map[promoteSourceWindowKey]time.Time{}, nil
		}
		return nil, err
	}
	changed := map[promoteSourceWindowKey]time.Time{}
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var row struct {
			SourceID     string    `json:"source_id"`
			BronzeTable  string    `json:"bronze_table"`
			ChangedSince time.Time `json:"changed_since"`
		}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, err
		}
		key := promoteSourceWindowKey{SourceID: strings.TrimSpace(row.SourceID), BronzeTable: strings.TrimSpace(row.BronzeTable)}
		if key.SourceID == "" || key.BronzeTable == "" || row.ChangedSince.IsZero() {
			continue
		}
		if previous, ok := changed[key]; !ok || row.ChangedSince.Before(previous) {
			changed[key] = row.ChangedSince.UTC()
		}
	}
	return changed, nil
}

func buildPromoteCheckpointID(sourceID, bronzeTable, mode string, windowStart, windowEnd time.Time) string {
	return hashStrings(strings.TrimSpace(sourceID), strings.TrimSpace(bronzeTable), strings.TrimSpace(mode), windowStart.UTC().Format(time.RFC3339Nano), windowEnd.UTC().Format(time.RFC3339Nano))[:32]
}

func loadLatestPromoteCheckpoints(ctx context.Context, runner *migrate.HTTPRunner, checkpointIDs []string) (map[string]promoteCheckpointRecord, error) {
	if len(checkpointIDs) == 0 {
		return map[string]promoteCheckpointRecord{}, nil
	}
	query := fmt.Sprintf(`SELECT checkpoint_id, source_id, bronze_table, selection_mode, window_start, window_end, status, attempt_count, started_at, finished_at, input_rows, error_message, record_version, updated_at, attrs, evidence
FROM ops.promote_checkpoint FINAL
WHERE checkpoint_id IN (%s)
ORDER BY record_version DESC
FORMAT JSONEachRow`, sqlStringList(checkpointIDs))
	output, err := runner.Query(ctx, query)
	if err != nil {
		if strings.Contains(err.Error(), "UNKNOWN_TABLE") {
			return map[string]promoteCheckpointRecord{}, nil
		}
		return nil, err
	}
	checkpoints := map[string]promoteCheckpointRecord{}
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var checkpoint promoteCheckpointRecord
		if err := json.Unmarshal([]byte(line), &checkpoint); err != nil {
			return nil, err
		}
		if strings.TrimSpace(checkpoint.CheckpointID) == "" {
			continue
		}
		if _, exists := checkpoints[checkpoint.CheckpointID]; exists {
			continue
		}
		checkpoints[checkpoint.CheckpointID] = checkpoint
	}
	return checkpoints, nil
}

func persistPromoteCheckpointSnapshot(ctx context.Context, runner *migrate.HTTPRunner, window promoteSelectionWindow, status string, attemptCount uint16, startedAt time.Time, finishedAt *time.Time, inputRows uint32, errorMessage string) error {
	attrsJSON, err := marshalJSONString(map[string]any{
		"query_bronze_table":      window.QueryBronzeTable,
		"configured_bronze_table": window.ConfiguredBronzeTable,
		"window_start":            window.WindowStart.UTC().Format(time.RFC3339Nano),
		"window_end":              window.WindowEnd.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return err
	}
	recordVersion := uint64(startedAt.UTC().UnixMilli())
	if finishedAt != nil && !finishedAt.IsZero() {
		recordVersion = uint64(finishedAt.UTC().UnixMilli())
	}
	if window.LatestCheckpoint != nil && window.LatestCheckpoint.RecordVersion >= recordVersion {
		recordVersion = window.LatestCheckpoint.RecordVersion + 1
	}
	query := fmt.Sprintf(`INSERT INTO ops.promote_checkpoint (checkpoint_id, source_id, bronze_table, selection_mode, window_start, window_end, status, attempt_count, started_at, finished_at, input_rows, error_message, schema_version, record_version, api_contract_version, updated_at, attrs, evidence) VALUES (%s,%s,%s,%s,%s,%s,%s,%d,%s,%s,%d,%s,%d,%d,%d,%s,%s,%s)`,
		sqlString(window.CheckpointID),
		sqlString(window.SourceID),
		sqlString(window.ConfiguredBronzeTable),
		sqlString(window.Mode),
		sqlTime(window.WindowStart),
		sqlTime(window.WindowEnd),
		sqlString(status),
		attemptCount,
		sqlTime(startedAt),
		nullablePipelineSQLTime(finishedAt),
		inputRows,
		nullableSQLString(errorMessage),
		promoteCheckpointSchemaVersion,
		recordVersion,
		promoteCheckpointAPIContract,
		sqlTime(time.Now().UTC().Truncate(time.Millisecond)),
		sqlString(attrsJSON),
		sqlString(defaultPipelineEvidenceJSON),
	)
	return runner.ApplySQL(ctx, query)
}

func loadLastSuccessfulPromoteAt(ctx context.Context, runner *migrate.HTTPRunner) (*time.Time, error) {
	query := `SELECT max(coalesce(finished_at, started_at)) AS last_successful_promote_at
FROM ops.job_run FINAL
WHERE job_type = 'promote' AND status = 'success'
FORMAT JSONEachRow`
	output, err := runner.Query(ctx, query)
	if err != nil {
		if strings.Contains(err.Error(), "UNKNOWN_TABLE") {
			return nil, nil
		}
		return nil, err
	}
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var row struct {
			LastSuccessfulPromoteAt *time.Time `json:"last_successful_promote_at"`
		}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, err
		}
		if row.LastSuccessfulPromoteAt == nil || row.LastSuccessfulPromoteAt.IsZero() {
			return nil, nil
		}
		value := row.LastSuccessfulPromoteAt.UTC()
		return &value, nil
	}
	return nil, nil
}

func bronzeRowToPromoteInput(row bronzePromoteRow) (promote.Input, bool, error) {
	recordKind := strings.TrimSpace(row.RecordKind)
	if recordKind == "" {
		return promote.Input{}, false, nil
	}
	payload := map[string]any{}
	if strings.TrimSpace(row.PayloadJSON) != "" {
		if err := json.Unmarshal([]byte(row.PayloadJSON), &payload); err != nil {
			return promote.Input{}, false, err
		}
	}
	payload["record_kind"] = recordKind
	if row.OccurredAt != nil && !row.OccurredAt.IsZero() {
		payload["occurred_at"] = row.OccurredAt.UTC().Format(time.RFC3339)
		payload["observed_at"] = row.OccurredAt.UTC().Format(time.RFC3339)
		payload["starts_at"] = row.OccurredAt.UTC().Format(time.RFC3339)
	}
	if row.PublishedAt != nil && !row.PublishedAt.IsZero() {
		payload["published_at"] = row.PublishedAt.UTC().Format(time.RFC3339)
	}
	if row.Status != nil {
		payload["status"] = strings.TrimSpace(*row.Status)
	}
	if row.Title != nil {
		payload["title"] = strings.TrimSpace(*row.Title)
	}
	if row.Summary != nil {
		payload["summary"] = strings.TrimSpace(*row.Summary)
	}
	if row.Severity != nil {
		payload["severity"] = strings.TrimSpace(*row.Severity)
	}
	if row.Lat != nil {
		payload["lat"] = *row.Lat
	}
	if row.Lon != nil {
		payload["lon"] = *row.Lon
	}

	attrs := map[string]any{}
	if strings.TrimSpace(row.Attrs) != "" {
		if err := json.Unmarshal([]byte(row.Attrs), &attrs); err != nil {
			return promote.Input{}, false, err
		}
	}
	evidence := []parser.Evidence{}
	if strings.TrimSpace(row.Evidence) != "" {
		if err := json.Unmarshal([]byte(row.Evidence), &evidence); err != nil {
			return promote.Input{}, false, err
		}
	}
	nativeID := ""
	if row.NativeID != nil {
		nativeID = strings.TrimSpace(*row.NativeID)
	}
	canonicalURL := strings.TrimSpace(row.SourceURL)
	if row.CanonicalURL != nil && strings.TrimSpace(*row.CanonicalURL) != "" {
		canonicalURL = strings.TrimSpace(*row.CanonicalURL)
	}
	frontierID := hashStrings(strings.TrimSpace(row.SourceID), canonicalURL, "bronze")[:32]
	locationHint := ""
	if row.PlaceHint != nil {
		locationHint = strings.TrimSpace(*row.PlaceHint)
	}
	locationAttrs := map[string]any{}
	if row.Lat != nil {
		locationAttrs["lat"] = *row.Lat
	}
	if row.Lon != nil {
		locationAttrs["lon"] = *row.Lon
	}
	location := promote.LocationResolution{Resolved: false, PlaceID: "", ParentPlaceChain: []string{}, Confidence: 0, Method: "bronze", ResolvedAt: row.ParsedAt.UTC(), FailureReason: "location_resolution_pending", LocationHint: locationHint, Attrs: locationAttrs}
	if placeID, ok := payload["place_id"].(string); ok && strings.TrimSpace(placeID) != "" {
		location.Resolved = true
		location.PlaceID = strings.TrimSpace(placeID)
		location.Confidence = 1
		if chain, ok := payload["parent_place_chain"].([]any); ok {
			resolved := make([]string, 0, len(chain))
			for _, item := range chain {
				if text, ok := item.(string); ok && strings.TrimSpace(text) != "" {
					resolved = append(resolved, strings.TrimSpace(text))
				}
			}
			location.ParentPlaceChain = resolved
		}
	}

	input := promote.Input{
		SourceID: strings.TrimSpace(row.SourceID),
		Discovery: promote.DiscoveryRecord{
			FrontierID:   frontierID,
			URL:          strings.TrimSpace(row.SourceURL),
			CanonicalURL: canonicalURL,
			DiscoveredAt: row.ParsedAt.UTC(),
		},
		Fetch: promote.FetchRecord{
			RawID:       strings.TrimSpace(row.RawID),
			URL:         strings.TrimSpace(row.SourceURL),
			ContentType: "application/json",
			ContentHash: strings.TrimSpace(row.ContentHash),
			StatusCode:  200,
			FetchedAt:   row.FetchedAt.UTC(),
		},
		Parse: promote.ParseRecord{
			ParseID: "parse:" + strings.TrimSpace(row.RawID),
			Candidate: parser.Candidate{
				Kind:          "bronze_row",
				SchemaVersion: row.SchemaVersion,
				RecordVersion: row.RecordVersion,
				ParserID:      strings.TrimSpace(row.ParserID),
				ParserVersion: strings.TrimSpace(row.ParserVersion),
				SourceID:      strings.TrimSpace(row.SourceID),
				RawID:         strings.TrimSpace(row.RawID),
				NativeID:      nativeID,
				ContentHash:   strings.TrimSpace(row.ContentHash),
				Data:          payload,
				Attrs:         attrs,
				Evidence:      evidence,
			},
		},
		Location: location,
	}
	return input, true, nil
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

const (
	promoteInsertSettingsClause = " SETTINGS async_insert=0, insert_deduplicate=1, deduplicate_blocks_in_dependent_materialized_views=1"
	promoteBackfillCutoverMode  = "staging_batch_cutover"
	promoteBackfillStageMarker  = "__backfill_"
)

func buildPromoteExecutionStatements(planStatements []string) ([]string, string, error) {
	stagingTag := sanitizePromoteBackfillTag(strings.TrimSpace(os.Getenv(promoteBackfillStagingTagEnv)))
	if stagingTag == "" {
		result := make([]string, 0, len(planStatements))
		for _, statement := range planStatements {
			result = append(result, pinPromoteInsertSettings(statement))
		}
		return result, "", nil
	}

	targetTables := collectPromoteInsertTargets(planStatements)
	stagingTables := make(map[string]string, len(targetTables))
	result := make([]string, 0, len(planStatements)+(len(targetTables)*3))
	for _, target := range targetTables {
		staging := promoteBackfillStagingTable(target, stagingTag)
		stagingTables[target] = staging
		result = append(result,
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s AS %s", staging, target),
			fmt.Sprintf("TRUNCATE TABLE %s", staging),
		)
	}
	for _, statement := range planStatements {
		rewritten, err := rewritePromoteInsertTarget(statement, stagingTables)
		if err != nil {
			return nil, "", err
		}
		result = append(result, pinPromoteInsertSettings(rewritten))
	}
	for _, target := range targetTables {
		result = append(result, pinPromoteInsertSettings(fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", target, stagingTables[target])))
	}
	return result, promoteBackfillCutoverMode, nil
}

func pinPromoteInsertSettings(statement string) string {
	trimmed := strings.TrimSpace(statement)
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "INSERT INTO ") || strings.Contains(upper, " SETTINGS ") {
		return statement
	}
	return statement + promoteInsertSettingsClause
}

func collectPromoteInsertTargets(statements []string) []string {
	seen := map[string]struct{}{}
	targets := make([]string, 0)
	for _, statement := range statements {
		target, ok := promoteInsertTarget(statement)
		if !ok {
			continue
		}
		if _, exists := seen[target]; exists {
			continue
		}
		seen[target] = struct{}{}
		targets = append(targets, target)
	}
	return targets
}

func promoteInsertTarget(statement string) (string, bool) {
	trimmed := strings.TrimSpace(statement)
	if !strings.HasPrefix(strings.ToUpper(trimmed), "INSERT INTO ") {
		return "", false
	}
	remainder := strings.TrimSpace(trimmed[len("INSERT INTO "):])
	if remainder == "" {
		return "", false
	}
	fields := strings.Fields(remainder)
	if len(fields) == 0 {
		return "", false
	}
	return strings.TrimSpace(fields[0]), true
}

func rewritePromoteInsertTarget(statement string, replacements map[string]string) (string, error) {
	target, ok := promoteInsertTarget(statement)
	if !ok {
		return statement, nil
	}
	replacement, ok := replacements[target]
	if !ok {
		return statement, nil
	}
	prefix := "INSERT INTO " + target
	if !strings.HasPrefix(strings.TrimSpace(statement), prefix) {
		return "", fmt.Errorf("unexpected promote insert shape for target %s", target)
	}
	return strings.Replace(statement, prefix, "INSERT INTO "+replacement, 1), nil
}

func sanitizePromoteBackfillTag(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return ""
	}
	var builder strings.Builder
	lastUnderscore := false
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			builder.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			builder.WriteByte('_')
			lastUnderscore = true
		}
	}
	value := strings.Trim(builder.String(), "_")
	if value == "" {
		return ""
	}
	if len(value) > 40 {
		value = value[:40]
	}
	return value
}

func promoteBackfillStagingTable(target, tag string) string {
	return target + promoteBackfillStageMarker + tag
}

func loadPromoteBronzeTableOverridesFromEnv() (map[string]string, error) {
	raw := strings.TrimSpace(os.Getenv(promoteBronzeTableOverridesEnv))
	if raw == "" {
		return map[string]string{}, nil
	}
	overrides := map[string]string{}
	if err := json.Unmarshal([]byte(raw), &overrides); err != nil {
		return nil, err
	}
	resolved := make(map[string]string, len(overrides))
	for sourceID, table := range overrides {
		sourceID = strings.TrimSpace(sourceID)
		table = strings.TrimSpace(table)
		if sourceID == "" || table == "" {
			continue
		}
		if !bronzeTablePattern.MatchString(table) {
			return nil, fmt.Errorf("override bronze table %q for %s is invalid", table, sourceID)
		}
		resolved[sourceID] = table
	}
	return resolved, nil
}

func resolvePromoteBronzeTable(source bronzePromoteSource, overrides map[string]string) string {
	if table, ok := overrides[strings.TrimSpace(source.SourceID)]; ok && strings.TrimSpace(table) != "" {
		return strings.TrimSpace(table)
	}
	return configuredPromoteBronzeTable(source)
}

func configuredPromoteBronzeTable(source bronzePromoteSource) string {
	if source.BronzeTable == nil {
		return ""
	}
	return strings.TrimSpace(*source.BronzeTable)
}
