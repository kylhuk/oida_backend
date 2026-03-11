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
	promoteJobName      = "promote"
	promoteInputPathEnv = "PROMOTE_PIPELINE_INPUT"
	promoteInputJSONEnv = "PROMOTE_PIPELINE_INPUT_JSON"
	promoteJobType      = "promote"
	promoteFailureRetry = 6 * time.Hour
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
	jobID := fmt.Sprintf("job:%s:%d", promoteJobName, startedAt.UnixMilli())

	recordFailure := func(err error, message string, stats map[string]any) error {
		if recordErr := recordJobRun(ctx, runner, jobID, promoteJobType, "failed", startedAt, time.Now().UTC().Truncate(time.Millisecond), message, stats); recordErr != nil {
			return fmt.Errorf("%w (job log failed: %v)", err, recordErr)
		}
		return err
	}

	inputs, err := loadPromotionInputs(ctx, runner)
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

type bronzePromoteSource struct {
	SourceID     string     `json:"source_id"`
	BronzeTable  *string    `json:"bronze_table"`
	CrawlEnabled uint8      `json:"crawl_enabled"`
	ChangedSince *time.Time `json:"changed_since,omitempty"`
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

	changedSources, err := loadChangedPromoteSources(ctx, runner)
	if err != nil {
		return nil, err
	}
	inputs := make([]promote.Input, 0)
	for _, source := range sources {
		table := strings.TrimSpace(*source.BronzeTable)
		if !bronzeTablePattern.MatchString(table) {
			return nil, fmt.Errorf("invalid bronze table identifier %q for source %q", table, source.SourceID)
		}
		changedSince, ok := changedSources[promoteSourceWindowKey{SourceID: source.SourceID, BronzeTable: table}]
		if !ok {
			continue
		}
		rowsQuery := fmt.Sprintf(`SELECT raw_id, source_id, parser_id, parser_version, record_kind, native_id, source_url, canonical_url, fetched_at, parsed_at, occurred_at, published_at, status, title, summary, place_hint, lat, lon, severity, content_hash, schema_version, record_version, attrs, evidence, payload_json
FROM %s
WHERE parsed_at >= toDateTime64('%s', 3, 'UTC')
ORDER BY parsed_at DESC, raw_id ASC, source_record_index ASC
FORMAT JSONEachRow`, table, strings.TrimSpace(changedSince.UTC().Format(time.RFC3339Nano)))
		rowsOutput, err := runner.Query(ctx, rowsQuery)
		if err != nil {
			return nil, err
		}
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
	}

	sort.Slice(inputs, func(i, j int) bool {
		if inputs[i].SourceID != inputs[j].SourceID {
			return inputs[i].SourceID < inputs[j].SourceID
		}
		return inputs[i].Fetch.RawID < inputs[j].Fetch.RawID
	})
	return inputs, nil
}

func loadChangedPromoteSources(ctx context.Context, runner *migrate.HTTPRunner) (map[promoteSourceWindowKey]time.Time, error) {
	query := `SELECT source_id, bronze_table, min(parsed_at) AS changed_since
FROM ops.parse_checkpoint FINAL
WHERE status = 'success'
GROUP BY source_id, bronze_table
FORMAT JSONEachRow`
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
			FrontierID:   "",
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
