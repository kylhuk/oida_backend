package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"global-osint-backend/internal/migrate"
	"global-osint-backend/internal/observability"
	"global-osint-backend/internal/packs/aviation"
	"global-osint-backend/internal/parser"
	sharedretry "global-osint-backend/internal/retry"
)

const (
	defaultClickHouseURL = "http://localhost:8123"
	defaultMinIOEndpoint = "http://localhost:9000"
	defaultMinIORegion   = "us-east-1"
	defaultRawBucket     = "raw"
	defaultParseTimeout  = 30 * time.Second
	clickHouseTimeLayout = "2006-01-02 15:04:05.000"
)

type config struct {
	ClickHouseHTTP      string
	MinIOEndpoint       string
	MinIOAccessKey      string
	MinIOSecretKey      string
	MinIORegion         string
	RawBucket           string
	ParseTimeout        time.Duration
	RetryMaxAttempts    int
	RetryInitialBackoff time.Duration
	RetryMaxBackoff     time.Duration
}

type sourceParsePolicy struct {
	SourceID       string  `json:"source_id"`
	ParserID       string  `json:"parser_id"`
	FormatHint     string  `json:"format_hint"`
	ParseConfig    string  `json:"parse_config_json"`
	BronzeTable    *string `json:"bronze_table"`
	TransportType  string  `json:"transport_type"`
	CrawlEnabled   uint8   `json:"crawl_enabled"`
	PromoteProfile string  `json:"promote_profile"`
}

type automaticSourceRecord struct {
	SourceID          string  `json:"source_id"`
	RequestsPerMinute uint32  `json:"requests_per_minute"`
	BurstSize         uint16  `json:"burst_size"`
	RefreshStrategy   string  `json:"refresh_strategy"`
	NotModifiedRatio  float64 `json:"not_modified_ratio"`
}

type rawDocRow struct {
	RawID        string  `json:"raw_id"`
	FetchID      string  `json:"fetch_id"`
	SourceID     string  `json:"source_id"`
	URL          string  `json:"url"`
	FinalURL     string  `json:"final_url"`
	FetchedAt    string  `json:"fetched_at"`
	ContentType  string  `json:"content_type"`
	ObjectKey    *string `json:"object_key"`
	FetchMeta    string  `json:"fetch_metadata"`
	ContentHash  string  `json:"content_hash"`
	StorageClass string  `json:"storage_class"`
}

type parseMeta struct {
	InlineBodyBase64 string `json:"inline_body_base64"`
	ObjectKey        string `json:"object_key"`
	CorrelationID    string `json:"correlation_id"`
}

type parseStats struct {
	CorrelationID string `json:"correlation_id,omitempty"`
	SourceID      string `json:"source_id"`
	ProcessedDocs int    `json:"processed_docs"`
	SuccessDocs   int    `json:"success_docs"`
	FailedDocs    int    `json:"failed_docs"`
	BronzeRows    int    `json:"bronze_rows"`
}

type parseCheckpointRecord struct {
	CheckpointID     string  `json:"checkpoint_id"`
	SourceID         string  `json:"source_id"`
	RawID            string  `json:"raw_id"`
	ParserID         string  `json:"parser_id"`
	ParserVersion    string  `json:"parser_version"`
	ContentHash      string  `json:"content_hash"`
	BronzeTable      string  `json:"bronze_table"`
	Status           string  `json:"status"`
	AttemptCount     uint16  `json:"attempt_count"`
	NextAttemptAt    *string `json:"next_attempt_at"`
	LastErrorCode    *string `json:"last_error_code"`
	LastErrorMessage *string `json:"last_error_message"`
	DeadLetteredAt   *string `json:"dead_lettered_at"`
	RecordVersion    uint64  `json:"record_version"`
}

const (
	parseCheckpointStatusSuccess    = "success"
	parseCheckpointStatusRetry      = "retry"
	parseCheckpointStatusDeadLetter = "dead_letter"
)

type clickhouseStore struct {
	runner *migrate.HTTPRunner
}

type s3Client struct {
	endpoint  *url.URL
	accessKey string
	secretKey string
	region    string
	client    *http.Client
}

func main() { os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)) }

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintf(stderr, "load config: %v\n", err)
		return 1
	}
	if len(args) == 0 {
		serve()
		return 0
	}

	switch args[0] {
	case "help", "-h", "--help":
		printRootUsage(stdout)
		return 0
	case "list-parsers":
		return listParsers(stdout)
	case "parse":
		return parseOnce(args[1:], stdin, stdout, stderr)
	case "parse-source":
		return parseSource(cfg, args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n", args[0])
		printRootUsage(stderr)
		return 2
	}
}

func serve() {
	cfg, err := loadConfig()
	if err != nil {
		observability.LogEvent("worker-parse", "config_error", "", map[string]any{"error": err.Error()})
		return
	}
	registry := parser.DefaultRegistry()
	observability.LogEvent("worker-parse", "service_started", observability.NewCorrelationID("worker-parse"), map[string]any{"parser_routes": len(registry.Records())})
	store := clickhouseStore{runner: migrate.NewHTTPRunner(cfg.ClickHouseHTTP)}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ParseTimeout)
	sources, listErr := store.listAutomaticSources(ctx)
	cancel()
	if listErr != nil {
		observability.LogEvent("worker-parse", "automatic_source_list_failed", "", map[string]any{"error": listErr.Error()})
		for {
			time.Sleep(5 * time.Second)
		}
	}
	if len(sources) == 0 {
		observability.LogEvent("worker-parse", "automatic_sources_empty", "", nil)
		for {
			time.Sleep(30 * time.Second)
		}
	}
	for _, source := range sources {
		interval := suggestedParseInterval(source)
		batch := suggestedParseBatch(source)
		go runSourceParseLoop(cfg, registry, source.SourceID, interval, batch)
		observability.LogEvent("worker-parse", "source_worker_started", "", map[string]any{"source_id": source.SourceID, "interval": interval.Round(100 * time.Millisecond).String(), "batch": batch, "not_modified_ratio": source.NotModifiedRatio})
	}
	observability.LogEvent("worker-parse", "automatic_worker_pools_active", "", map[string]any{"sources": len(sources), "workers": len(sources)})
	select {}
}

func runSourceParseLoop(cfg config, registry *parser.Registry, sourceID string, interval time.Duration, batch int) {
	for {
		rc := parseSourceWithRegistry(cfg, []string{"--source-id", sourceID, "--limit", strconv.Itoa(batch)}, io.Discard, io.Discard, registry)
		if rc != 0 {
			observability.LogEvent("worker-parse", "source_loop_error", "", map[string]any{"source_id": sourceID, "code": rc})
		}
		time.Sleep(interval)
	}
}

func suggestedParseInterval(source automaticSourceRecord) time.Duration {
	seconds := 0.0
	if source.RequestsPerMinute > 0 {
		seconds = 60.0 / float64(source.RequestsPerMinute)
	}
	if seconds <= 0 {
		switch strings.TrimSpace(strings.ToLower(source.RefreshStrategy)) {
		case "frequent":
			seconds = 1.0
		case "scheduled":
			seconds = 30.0
		default:
			seconds = 60.0
		}
	}
	if source.NotModifiedRatio >= 0.90 {
		seconds *= 5.0
	} else if source.NotModifiedRatio >= 0.70 {
		seconds *= 3.0
	}
	if seconds < 0.1 {
		seconds = 0.1
	}
	if seconds > 300.0 {
		seconds = 300.0
	}
	return time.Duration(seconds * float64(time.Second))
}

func suggestedParseBatch(source automaticSourceRecord) int {
	batch := int(source.BurstSize) * 2
	if batch <= 0 {
		batch = 2
	}
	if batch > 128 {
		batch = 128
	}
	return batch
}

func listParsers(stdout io.Writer) int { return writeJSON(stdout, parser.DefaultRegistry().Records()) }

func parseSource(cfg config, args []string, stdout, stderr io.Writer) int {
	return parseSourceWithRegistry(cfg, args, stdout, stderr, parser.DefaultRegistry())
}

func parseSourceWithRegistry(cfg config, args []string, stdout, stderr io.Writer, registry *parser.Registry) int {
	fs := flag.NewFlagSet("parse-source", flag.ContinueOnError)
	fs.SetOutput(stderr)
	sourceID := fs.String("source-id", "", "Source registry id.")
	limit := fs.Int("limit", 1, "Maximum raw documents to parse.")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if strings.TrimSpace(*sourceID) == "" || *limit <= 0 {
		fmt.Fprintln(stderr, "Usage:\n  worker-parse parse-source --source-id <source-id> --limit <n>")
		return 2
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ParseTimeout)
	defer cancel()
	store := clickhouseStore{runner: migrate.NewHTTPRunner(cfg.ClickHouseHTTP)}
	policy, err := store.lookupSource(ctx, strings.TrimSpace(*sourceID))
	if err != nil {
		fmt.Fprintf(stderr, "lookup source: %v\n", err)
		return 1
	}
	if policy.BronzeTable == nil || strings.TrimSpace(*policy.BronzeTable) == "" {
		fmt.Fprintf(stderr, "source %s has no bronze table\n", policy.SourceID)
		return 1
	}
	resolvedSourceParser, ok := registry.Lookup(strings.TrimSpace(policy.ParserID))
	if !ok {
		fmt.Fprintf(stderr, "source %s parser %s is not registered\n", policy.SourceID, strings.TrimSpace(policy.ParserID))
		return 1
	}
	resolvedDescriptor := resolvedSourceParser.Descriptor()

	rows, err := store.loadRawDocuments(ctx, policy.SourceID, strings.TrimSpace(*policy.BronzeTable), strings.TrimSpace(policy.ParserID), strings.TrimSpace(resolvedDescriptor.Version), *limit)
	if err != nil {
		fmt.Fprintf(stderr, "load raw documents: %v\n", err)
		return 1
	}
	objectStore, err := newS3Client(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "init object store: %v\n", err)
		return 1
	}
	stats := parseStats{SourceID: policy.SourceID}
	retryPolicy := sharedretry.Policy{MaxAttempts: cfg.RetryMaxAttempts, InitialBackoff: cfg.RetryInitialBackoff, MaxBackoff: cfg.RetryMaxBackoff}.Normalize()

	for _, row := range rows {
		stats.ProcessedDocs++
		started := time.Now().UTC().Truncate(time.Millisecond)
		correlationID := extractCorrelationID(row)
		if correlationID == "" {
			correlationID = observability.NewCorrelationID("parse")
		}
		if stats.CorrelationID == "" {
			stats.CorrelationID = correlationID
		}
		checkpoint := buildParseCheckpoint(policy.SourceID, strings.TrimSpace(*policy.BronzeTable), row, strings.TrimSpace(resolvedDescriptor.ID), strings.TrimSpace(resolvedDescriptor.Version))
		existingCheckpoint, err := store.loadParseCheckpoint(ctx, checkpoint)
		if err != nil {
			fmt.Fprintf(stderr, "load parse checkpoint: %v\n", err)
			return 1
		}
		attemptCount := nextParseAttemptCount(existingCheckpoint)
		body, contentType, err := loadRawBody(ctx, objectStore, cfg.RawBucket, row)
		if err != nil {
			if err := persistParseFailure(ctx, store, policy, row, checkpoint, existingCheckpoint, started, attemptCount, retryPolicy, "load_body", err.Error(), true); err != nil {
				fmt.Fprintf(stderr, "persist parse retry state: %v\n", err)
				return 1
			}
			stats.FailedDocs++
			continue
		}
		input := parser.Input{
			ParserID:    strings.TrimSpace(policy.ParserID),
			SourceID:    policy.SourceID,
			RawID:       row.RawID,
			URL:         firstNonEmpty(row.FinalURL, row.URL),
			FormatHint:  strings.TrimSpace(policy.FormatHint),
			ContentType: firstNonEmpty(contentType, row.ContentType),
			Body:        body,
			FetchedAt:   parseTime(row.FetchedAt),
			Attrs:       fetchMetadataAttrs(row.FetchMeta),
		}
		resolvedParser, parseResolveErr := registry.Resolve(input)
		if parseResolveErr != nil {
			if err := persistParseFailure(ctx, store, policy, row, checkpoint, existingCheckpoint, started, attemptCount, retryPolicy, parseResolveErr.Code, parseResolveErr.Message, parseResolveErr.Retryable); err != nil {
				fmt.Fprintf(stderr, "persist parse retry state: %v\n", err)
				return 1
			}
			stats.FailedDocs++
			continue
		}
		descriptor := resolvedParser.Descriptor()
		checkpoint = buildParseCheckpoint(policy.SourceID, strings.TrimSpace(*policy.BronzeTable), row, descriptor.ID, descriptor.Version)
		existingCheckpoint, err = store.loadParseCheckpoint(ctx, checkpoint)
		if err != nil {
			fmt.Fprintf(stderr, "load parse checkpoint: %v\n", err)
			return 1
		}
		attemptCount = nextParseAttemptCount(existingCheckpoint)
		if existingCheckpoint != nil && existingCheckpoint.Status == parseCheckpointStatusSuccess {
			_ = store.insertParseLog(ctx, buildParseLog(policy, row, started, correlationID, "skipped", 0, "checkpoint", "raw document already parsed for current parser/content version"))
			continue
		}
		result, parseErr := registry.Parse(ctx, input)
		if parseErr != nil {
			if err := persistParseFailure(ctx, store, policy, row, checkpoint, existingCheckpoint, started, attemptCount, retryPolicy, parseErr.Code, parseErr.Message, parseErr.Retryable); err != nil {
				fmt.Fprintf(stderr, "persist parse retry state: %v\n", err)
				return 1
			}
			stats.FailedDocs++
			continue
		}
		candidates := normalizePhase1Candidates(policy.SourceID, firstNonEmpty(row.FinalURL, row.URL), result.Candidates, started)
		inserted := 0
		for idx, candidate := range candidates {
			rowSQL, err := buildBronzeInsertSQL(strings.TrimSpace(*policy.BronzeTable), row, candidate, idx, started)
			if err != nil {
				if err := persistParseFailure(ctx, store, policy, row, checkpoint, existingCheckpoint, started, attemptCount, retryPolicy, "bronze_insert_sql", err.Error(), false); err != nil {
					fmt.Fprintf(stderr, "persist parse retry state: %v\n", err)
					return 1
				}
				stats.FailedDocs++
				inserted = -1
				break
			}
			if err := store.runner.ApplySQL(ctx, rowSQL); err != nil {
				if err := persistParseFailure(ctx, store, policy, row, checkpoint, existingCheckpoint, started, attemptCount, retryPolicy, "bronze_insert", err.Error(), true); err != nil {
					fmt.Fprintf(stderr, "persist parse retry state: %v\n", err)
					return 1
				}
				stats.FailedDocs++
				inserted = -1
				break
			}
			inserted++
		}
		if inserted < 0 {
			continue
		}
		_ = store.insertParseLog(ctx, buildParseLog(policy, row, started, correlationID, "success", inserted, "", ""))
		observability.LogEvent("worker-parse", "parse_attempt_persisted", correlationID, map[string]any{"source_id": policy.SourceID, "raw_id": row.RawID, "rows": inserted, "status": "success"})
		checkpoint.Status = parseCheckpointStatusSuccess
		checkpoint.AttemptCount = attemptCount
		checkpoint.NextAttemptAt = nil
		checkpoint.LastErrorCode = nil
		checkpoint.LastErrorMessage = nil
		checkpoint.DeadLetteredAt = nil
		checkpoint.RecordVersion = nextParseRecordVersion(existingCheckpoint)
		if err := store.upsertParseCheckpoint(ctx, checkpoint, started); err != nil {
			fmt.Fprintf(stderr, "upsert parse checkpoint: %v\n", err)
			return 1
		}
		stats.SuccessDocs++
		stats.BronzeRows += inserted
	}
	return writeJSON(stdout, stats)
}

func parseOnce(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("parse", flag.ContinueOnError)
	fs.SetOutput(stderr)
	parserID := fs.String("parser-id", "", "Explicit parser ID to route to.")
	formatHint := fs.String("format", "", "Optional input format hint such as json, csv, rss, atom, or html.")
	contentType := fs.String("content-type", "", "Optional content type for parser routing.")
	sourceID := fs.String("source-id", "", "Source ID carried into candidate output.")
	rawID := fs.String("raw-id", "", "Raw document ID carried into candidate output.")
	urlValue := fs.String("url", "", "Optional source URL for evidence payloads.")
	profilePath := fs.String("profile", "", "Optional JSON file path for parser:html-profile selector definitions.")
	fs.Usage = func() { fmt.Fprint(fs.Output(), parseUsage()) }
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected arguments: %s\n\n", strings.Join(fs.Args(), " "))
		fs.Usage()
		return 2
	}

	body, err := io.ReadAll(stdin)
	if err != nil {
		fmt.Fprintf(stderr, "read stdin: %v\n", err)
		return 1
	}
	input := parser.Input{ParserID: strings.TrimSpace(*parserID), SourceID: strings.TrimSpace(*sourceID), RawID: strings.TrimSpace(*rawID), URL: strings.TrimSpace(*urlValue), FormatHint: strings.TrimSpace(*formatHint), ContentType: strings.TrimSpace(*contentType), Body: body, FetchedAt: time.Now().UTC()}
	if strings.TrimSpace(*profilePath) != "" {
		profile, err := loadProfile(*profilePath)
		if err != nil {
			fmt.Fprintf(stderr, "load profile: %v\n", err)
			return 1
		}
		input.Profile = profile
	}
	result, parseErr := parser.DefaultRegistry().Parse(context.Background(), input)
	if parseErr != nil {
		_ = writeJSON(stdout, map[string]any{"error": parseErr})
		return 1
	}
	return writeJSON(stdout, result)
}

func buildBronzeInsertSQL(table string, doc rawDocRow, candidate parser.Candidate, index int, parsedAt time.Time) (string, error) {
	payloadJSON, err := json.Marshal(candidate.Data)
	if err != nil {
		return "", err
	}
	attrsJSON, err := json.Marshal(candidate.Attrs)
	if err != nil {
		return "", err
	}
	evidenceJSON, err := json.Marshal(candidate.Evidence)
	if err != nil {
		return "", err
	}
	key := firstNonEmpty(strings.TrimSpace(candidate.NativeID), strings.TrimSpace(candidate.ContentHash))
	if key == "" {
		return "", fmt.Errorf("candidate missing native_id and content_hash")
	}
	recordKind := strings.TrimSpace(extractString(candidate.Data, "record_kind"))
	if recordKind == "" {
		recordKind = strings.TrimSpace(candidate.Kind)
	}
	occurredAt := firstNonEmpty(extractString(candidate.Data, "observed_at"), extractString(candidate.Data, "occurred_at"), extractString(candidate.Data, "starts_at"))
	publishedAt := extractString(candidate.Data, "published_at")
	sourceURL := firstNonEmpty(strings.TrimSpace(doc.FinalURL), strings.TrimSpace(doc.URL))
	canonicalURL := nullableString(strings.TrimSpace(doc.FinalURL))
	lat, hasLat := extractFloat(candidate.Data, "lat")
	lon, hasLon := extractFloat(candidate.Data, "lon")

	query := fmt.Sprintf(`INSERT INTO %s
	(raw_id, fetch_id, source_id, parser_id, parser_version, source_record_key, source_record_index, record_kind, native_id, source_url, canonical_url, fetched_at, parsed_at, occurred_at, published_at, title, summary, status, place_hint, lat, lon, severity, content_hash, schema_version, record_version, attrs, evidence, payload_json)
	VALUES ('%s','%s','%s','%s','%s','%s',%d,'%s',%s,'%s',%s,toDateTime64('%s', 3, 'UTC'),toDateTime64('%s', 3, 'UTC'),%s,%s,%s,%s,%s,%s,%s,%s,%s,'%s',%d,%d,'%s','%s','%s')`,
		quoteTableIdentifier(table),
		esc(doc.RawID),
		esc(doc.FetchID),
		esc(doc.SourceID),
		esc(candidate.ParserID),
		esc(candidate.ParserVersion),
		esc(key),
		index,
		esc(recordKind),
		nullableString(strings.TrimSpace(candidate.NativeID)),
		esc(sourceURL),
		canonicalURL,
		esc(formatClickHouseTime(parseTime(doc.FetchedAt))),
		esc(formatClickHouseTime(parsedAt)),
		nullableTime(occurredAt),
		nullableTime(publishedAt),
		nullableString(extractString(candidate.Data, "title")),
		nullableString(extractString(candidate.Data, "summary")),
		nullableString(extractString(candidate.Data, "status")),
		nullableString(extractString(candidate.Data, "place_hint")),
		nullableFloat(lat, hasLat),
		nullableFloat(lon, hasLon),
		nullableString(extractString(candidate.Data, "severity")),
		esc(candidate.ContentHash),
		candidate.SchemaVersion,
		candidate.RecordVersion,
		esc(string(attrsJSON)),
		esc(string(evidenceJSON)),
		esc(string(payloadJSON)),
	)
	return query, nil
}

func quoteTableIdentifier(table string) string {
	parts := strings.Split(strings.TrimSpace(table), ".")
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		quoted = append(quoted, "`"+strings.ReplaceAll(part, "`", "``")+"`")
	}
	return strings.Join(quoted, ".")
}

func buildParseLog(policy sourceParsePolicy, row rawDocRow, startedAt time.Time, correlationID, status string, extracted int, errClass, errMessage string) string {
	finished := time.Now().UTC().Truncate(time.Millisecond)
	duration := uint32(finished.Sub(startedAt).Milliseconds())
	if duration == 0 {
		duration = 1
	}
	parseID := fmt.Sprintf("parse:%s:%d", esc(row.RawID), startedAt.UnixMilli())
	errClassValue := ""
	if strings.TrimSpace(errClass) != "" {
		errClassValue = strings.TrimSpace(errClass)
	}
	return fmt.Sprintf(`INSERT INTO ops.parse_log
	(parse_id, correlation_id, job_id, source_id, parser_id, parser_family, raw_id, input_format, status, started_at, finished_at, duration_ms, extracted_rows, extracted_entities, error_class, error_message, attrs, evidence)
	VALUES ('%s',%s,'%s','%s','%s','%s','%s','%s','%s',toDateTime64('%s', 3, 'UTC'),toDateTime64('%s', 3, 'UTC'),%d,%d,0,'%s',%s,'{}','[]')`,
		parseID,
		nullableString(correlationID),
		fmt.Sprintf("job:parse-source:%s", esc(policy.SourceID)),
		esc(policy.SourceID),
		esc(policy.ParserID),
		esc(parserFamily(policy.ParserID)),
		esc(row.RawID),
		esc(policy.FormatHint),
		esc(status),
		esc(formatClickHouseTime(startedAt)),
		esc(formatClickHouseTime(finished)),
		duration,
		extracted,
		esc(errClassValue),
		nullableString(errMessage),
	)
}

func extractCorrelationID(row rawDocRow) string {
	if strings.TrimSpace(row.FetchMeta) == "" {
		return ""
	}
	var meta parseMeta
	if err := json.Unmarshal([]byte(row.FetchMeta), &meta); err != nil {
		return ""
	}
	return observability.NormalizeCorrelationID(meta.CorrelationID)
}

func fetchMetadataAttrs(raw string) map[string]any {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	attrs := map[string]any{}
	if err := json.Unmarshal([]byte(raw), &attrs); err != nil || len(attrs) == 0 {
		return nil
	}
	return attrs
}

func parserFamily(parserID string) string {
	value := strings.TrimSpace(parserID)
	if value == "" {
		return "unknown"
	}
	parts := strings.Split(value, ":")
	if len(parts) <= 1 {
		return value
	}
	return parts[len(parts)-1]
}

func loadRawBody(ctx context.Context, objectStore *s3Client, rawBucket string, row rawDocRow) ([]byte, string, error) {
	meta := parseMeta{}
	if strings.TrimSpace(row.FetchMeta) != "" {
		_ = json.Unmarshal([]byte(row.FetchMeta), &meta)
	}
	if strings.TrimSpace(meta.InlineBodyBase64) != "" {
		decoded, err := base64.StdEncoding.DecodeString(meta.InlineBodyBase64)
		if err != nil {
			return nil, "", err
		}
		return decoded, strings.TrimSpace(row.ContentType), nil
	}
	key := strings.TrimSpace(meta.ObjectKey)
	if key == "" && row.ObjectKey != nil {
		key = strings.TrimSpace(*row.ObjectKey)
	}
	if key == "" {
		return nil, "", fmt.Errorf("raw document %s has no retrievable payload", row.RawID)
	}
	body, contentType, err := objectStore.GetObject(ctx, rawBucket, key)
	if err != nil {
		return nil, "", err
	}
	return body, firstNonEmpty(contentType, row.ContentType), nil
}

func (s clickhouseStore) lookupSource(ctx context.Context, sourceID string) (sourceParsePolicy, error) {
	query := fmt.Sprintf(`SELECT source_id, parser_id, format_hint, parse_config_json, bronze_table, transport_type, crawl_enabled, promote_profile
FROM meta.source_registry FINAL
WHERE source_id = '%s'
LIMIT 1
FORMAT JSONEachRow`, esc(sourceID))
	output, err := s.runner.Query(ctx, query)
	if err != nil {
		return sourceParsePolicy{}, err
	}
	line := strings.TrimSpace(output)
	if line == "" {
		return sourceParsePolicy{}, fmt.Errorf("source %q not found", sourceID)
	}
	var record sourceParsePolicy
	if err := json.Unmarshal([]byte(line), &record); err != nil {
		return sourceParsePolicy{}, err
	}
	return record, nil
}

func (s clickhouseStore) listAutomaticSourceIDs(ctx context.Context) ([]string, error) {
	query := `SELECT source_id
FROM meta.source_registry FINAL
WHERE enabled = 1
  AND crawl_enabled = 1
  AND transport_type IN ('http','websocket')
  AND bronze_table IS NOT NULL
  AND parser_id != ''
ORDER BY source_id
FORMAT TabSeparated`
	output, err := s.runner.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	rows := []string{}
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		sourceID := strings.TrimSpace(line)
		if sourceID == "" {
			continue
		}
		rows = append(rows, sourceID)
	}
	return rows, nil
}

func (s clickhouseStore) listAutomaticSources(ctx context.Context) ([]automaticSourceRecord, error) {
	query := `SELECT s.source_id, s.requests_per_minute, s.burst_size, s.refresh_strategy,
if(count(f.status_code) = 0, 0.0, toFloat64(countIf(f.status_code = 304)) / toFloat64(count(f.status_code))) AS not_modified_ratio
FROM meta.source_registry s
LEFT JOIN ops.fetch_log f ON s.source_id = f.source_id AND f.fetched_at > now() - INTERVAL 30 MINUTE
WHERE s.enabled = 1
  AND s.crawl_enabled = 1
  AND s.transport_type = 'http'
  AND s.bronze_table IS NOT NULL
  AND s.parser_id != ''
GROUP BY s.source_id, s.requests_per_minute, s.burst_size, s.refresh_strategy
ORDER BY s.source_id
FORMAT JSONEachRow`
	out, err := s.runner.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	rows := []automaticSourceRecord{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var row automaticSourceRecord
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (s clickhouseStore) loadRawDocuments(ctx context.Context, sourceID, bronzeTable, parserID, parserVersion string, limit int) ([]rawDocRow, error) {
	query := buildRawDocumentsQuery(sourceID, bronzeTable, parserID, parserVersion, limit, true)
	out, err := s.runner.Query(ctx, query)
	if err != nil {
		if strings.Contains(err.Error(), "UNKNOWN_TABLE") {
			out, err = s.runner.Query(ctx, buildRawDocumentsQuery(sourceID, bronzeTable, parserID, parserVersion, limit, false))
		}
	}
	if err != nil {
		return nil, err
	}
	rows := []rawDocRow{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var row rawDocRow
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func buildRawDocumentsQuery(sourceID, bronzeTable, parserID, parserVersion string, limit int, useCheckpointLedger bool) string {
	if !useCheckpointLedger {
		return fmt.Sprintf(`SELECT raw_id, fetch_id, source_id, url, final_url, fetched_at, content_type, object_key, fetch_metadata, content_hash, storage_class
FROM bronze.raw_document
WHERE source_id = '%s' AND status_code IN (200, 204)
ORDER BY fetched_at DESC
LIMIT %d
FORMAT JSONEachRow`, esc(sourceID), limit)
	}
	return fmt.Sprintf(`SELECT raw.raw_id, raw.fetch_id, raw.source_id, raw.url, raw.final_url, raw.fetched_at, raw.content_type, raw.object_key, raw.fetch_metadata, raw.content_hash, raw.storage_class
FROM bronze.raw_document raw
LEFT JOIN (
	SELECT checkpoint_id, source_id, raw_id, parser_id, parser_version, content_hash, bronze_table, status, attempt_count, next_attempt_at
	FROM ops.parse_checkpoint FINAL
) checkpoint
	ON checkpoint.source_id = raw.source_id
	AND checkpoint.raw_id = raw.raw_id
	AND checkpoint.parser_id = '%s'
	AND checkpoint.parser_version = '%s'
	AND checkpoint.content_hash = raw.content_hash
	AND checkpoint.bronze_table = '%s'
WHERE raw.source_id = '%s'
	AND raw.status_code IN (200, 204)
	AND (
		checkpoint.checkpoint_id = ''
		OR isNull(checkpoint.checkpoint_id)
		OR (checkpoint.status = '%s' AND (checkpoint.next_attempt_at IS NULL OR checkpoint.next_attempt_at <= now64(3)))
	)
	AND ifNull(checkpoint.status, '') != '%s'
	AND ifNull(checkpoint.status, '') != '%s'
ORDER BY raw.fetched_at DESC
LIMIT %d
FORMAT JSONEachRow`, esc(parserID), esc(parserVersion), esc(bronzeTable), esc(sourceID), parseCheckpointStatusRetry, parseCheckpointStatusSuccess, parseCheckpointStatusDeadLetter, limit)
}

func (s clickhouseStore) insertParseLog(ctx context.Context, statement string) error {
	return s.runner.ApplySQL(ctx, statement)
}

func (s clickhouseStore) loadParseCheckpoint(ctx context.Context, checkpoint parseCheckpointRecord) (*parseCheckpointRecord, error) {
	query := fmt.Sprintf(`SELECT checkpoint_id, source_id, raw_id, parser_id, parser_version, content_hash, bronze_table, status, attempt_count, next_attempt_at, last_error_code, last_error_message, dead_lettered_at, record_version
FROM ops.parse_checkpoint FINAL
WHERE source_id = '%s' AND raw_id = '%s' AND parser_id = '%s' AND parser_version = '%s' AND content_hash = '%s' AND bronze_table = '%s'
LIMIT 1
FORMAT JSONEachRow`,
		esc(checkpoint.SourceID),
		esc(checkpoint.RawID),
		esc(checkpoint.ParserID),
		esc(checkpoint.ParserVersion),
		esc(checkpoint.ContentHash),
		esc(checkpoint.BronzeTable),
	)
	out, err := s.runner.Query(ctx, query)
	if err != nil {
		if strings.Contains(err.Error(), "UNKNOWN_TABLE") {
			return nil, nil
		}
		return nil, err
	}
	line := strings.TrimSpace(out)
	if line == "" {
		return nil, nil
	}
	var record parseCheckpointRecord
	if err := json.Unmarshal([]byte(line), &record); err != nil {
		return nil, err
	}
	return &record, nil
}

func (s clickhouseStore) upsertParseCheckpoint(ctx context.Context, checkpoint parseCheckpointRecord, parsedAt time.Time) error {
	query := fmt.Sprintf(`INSERT INTO ops.parse_checkpoint
	(checkpoint_id, source_id, raw_id, parser_id, parser_version, content_hash, bronze_table, status, attempt_count, parsed_at, next_attempt_at, last_error_code, last_error_message, dead_lettered_at, schema_version, record_version, api_contract_version, updated_at, attrs, evidence)
	VALUES ('%s','%s','%s','%s','%s','%s','%s','%s',%d,toDateTime64('%s', 3, 'UTC'),%s,%s,%s,%s,1,%d,1,toDateTime64('%s', 3, 'UTC'),'{}','[]')`,
		esc(checkpoint.CheckpointID),
		esc(checkpoint.SourceID),
		esc(checkpoint.RawID),
		esc(checkpoint.ParserID),
		esc(checkpoint.ParserVersion),
		esc(checkpoint.ContentHash),
		esc(checkpoint.BronzeTable),
		esc(checkpoint.Status),
		checkpoint.AttemptCount,
		esc(formatClickHouseTime(parsedAt)),
		sqlNullableTimeString(checkpoint.NextAttemptAt),
		sqlNullableString(checkpoint.LastErrorCode),
		sqlNullableString(checkpoint.LastErrorMessage),
		sqlNullableTimeString(checkpoint.DeadLetteredAt),
		maxUint64(checkpoint.RecordVersion, 1),
		esc(formatClickHouseTime(parsedAt)),
	)
	return s.runner.ApplySQL(ctx, query)
}

func loadConfig() (config, error) {
	endpoint, err := url.Parse(getenv("MINIO_ENDPOINT", defaultMinIOEndpoint))
	if err != nil {
		return config{}, err
	}
	if endpoint.Scheme == "" || endpoint.Host == "" {
		return config{}, fmt.Errorf("invalid MinIO endpoint %q", endpoint.String())
	}
	return config{
		ClickHouseHTTP:      getenv("CLICKHOUSE_HTTP_URL", defaultClickHouseURL),
		MinIOEndpoint:       endpoint.String(),
		MinIOAccessKey:      getenv("MINIO_ACCESS_KEY", getenv("MINIO_ROOT_USER", "minio")),
		MinIOSecretKey:      getenv("MINIO_SECRET_KEY", getenv("MINIO_ROOT_PASSWORD", "minio_change_me")),
		MinIORegion:         getenv("MINIO_REGION", defaultMinIORegion),
		RawBucket:           getenv("RAW_BUCKET", defaultRawBucket),
		ParseTimeout:        parseDurationEnv("PARSE_TIMEOUT", defaultParseTimeout),
		RetryMaxAttempts:    parseIntEnv("PARSE_RETRY_ATTEMPTS", 3),
		RetryInitialBackoff: parseDurationEnv("PARSE_RETRY_INITIAL_BACKOFF", 250*time.Millisecond),
		RetryMaxBackoff:     parseDurationEnv("PARSE_RETRY_MAX_BACKOFF", 3*time.Second),
	}, nil
}

func newS3Client(cfg config) (*s3Client, error) {
	endpoint, err := url.Parse(cfg.MinIOEndpoint)
	if err != nil {
		return nil, fmt.Errorf("parse MinIO endpoint: %w", err)
	}
	return &s3Client{endpoint: endpoint, accessKey: cfg.MinIOAccessKey, secretKey: cfg.MinIOSecretKey, region: cfg.MinIORegion, client: &http.Client{Timeout: cfg.ParseTimeout}}, nil
}

func (c *s3Client) GetObject(ctx context.Context, bucket, key string) ([]byte, string, error) {
	resp, body, err := c.do(ctx, http.MethodGet, "/"+bucket+"/"+key, nil, "")
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return body, strings.TrimSpace(resp.Header.Get("Content-Type")), nil
}

func (c *s3Client) do(ctx context.Context, method, rawPath string, body []byte, contentType string) (*http.Response, []byte, error) {
	u := *c.endpoint
	u.Path = rawPath
	payload := body
	if payload == nil {
		payload = []byte{}
	}
	payloadSum := sha256.Sum256(payload)
	payloadHash := hex.EncodeToString(payloadSum[:])
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	timestamp := time.Now().UTC()
	amzDate := timestamp.Format("20060102T150405Z")
	dateStamp := timestamp.Format("20060102")
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("x-amz-date", amzDate)
	if req.Header.Get("Host") == "" {
		req.Header.Set("Host", c.endpoint.Host)
	}
	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	canonicalHeaders := map[string]string{
		"host":                 c.endpoint.Host,
		"x-amz-content-sha256": payloadHash,
		"x-amz-date":           amzDate,
	}
	if contentType != "" {
		signedHeaders = append(signedHeaders, "content-type")
		canonicalHeaders["content-type"] = contentType
	}
	canonicalQuery := req.URL.Query().Encode()
	canonicalHeaderText := ""
	for _, key := range signedHeaders {
		canonicalHeaderText += key + ":" + strings.TrimSpace(canonicalHeaders[key]) + "\n"
	}
	canonicalRequest := strings.Join([]string{
		req.Method,
		req.URL.EscapedPath(),
		canonicalQuery,
		canonicalHeaderText,
		strings.Join(signedHeaders, ";"),
		payloadHash,
	}, "\n")
	credentialScope := dateStamp + "/" + c.region + "/s3/aws4_request"
	canonicalSum := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		hex.EncodeToString(canonicalSum[:]),
	}, "\n")
	signature := hex.EncodeToString(signV4(c.secretKey, dateStamp, c.region, "s3", stringToSign))
	authorization := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s", c.accessKey, credentialScope, strings.Join(signedHeaders, ";"), signature)
	req.Header.Set("Authorization", authorization)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	respBody, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return nil, nil, err
	}
	return resp, respBody, nil
}

func signV4(secret, dateStamp, region, service, stringToSign string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	return hmacSHA256(kSigning, stringToSign)
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(data))
	return h.Sum(nil)
}

func loadProfile(path string) (*parser.HTMLProfile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var profile parser.HTMLProfile
	if err := json.Unmarshal(b, &profile); err != nil {
		return nil, err
	}
	return &profile, nil
}

func writeJSON(w io.Writer, value any) int {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return 1
	}
	return 0
}

func printRootUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  worker-parse [command]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Commands:")
	fmt.Fprintln(w, "  list-parsers    Print built-in parser registry routes as JSON")
	fmt.Fprintln(w, "  parse           Parse stdin and emit canonical candidates as JSON")
	fmt.Fprintln(w, "  parse-source    Parse bronze.raw_document rows into source bronze tables")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Without a command the worker automatically parses eligible sources every 30s.")
	fmt.Fprintln(w, "Run `worker-parse parse --help` for the parser runtime contract.")
}

func parseUsage() string {
	var b strings.Builder
	b.WriteString("Usage:\n")
	b.WriteString("  worker-parse parse [options] < input\n\n")
	b.WriteString("Contract:\n")
	b.WriteString("  - Resolves the requested or inferred parser via the built-in parser registry.\n")
	b.WriteString("  - Reads the raw payload from stdin and emits structured canonical candidates as JSON.\n")
	b.WriteString("  - Emits machine-readable parser errors on stdout and exits non-zero on failure.\n\n")
	b.WriteString("Options:\n")
	b.WriteString("  --parser-id string\n")
	b.WriteString("        Explicit parser ID to route to.\n")
	b.WriteString("  --format string\n")
	b.WriteString("        Optional input format hint such as json, csv, rss, atom, or html.\n")
	b.WriteString("  --content-type string\n")
	b.WriteString("        Optional content type for parser routing.\n")
	b.WriteString("  --source-id string\n")
	b.WriteString("        Source ID carried into candidate output.\n")
	b.WriteString("  --raw-id string\n")
	b.WriteString("        Raw document ID carried into candidate output.\n")
	b.WriteString("  --url string\n")
	b.WriteString("        Optional source URL for evidence payloads.\n")
	b.WriteString("  --profile string\n")
	b.WriteString("        Optional JSON file path for parser:html-profile selector definitions.\n")
	return b.String()
}

func normalizePhase1Candidates(sourceID, sourceURL string, candidates []parser.Candidate, parsedAt time.Time) []parser.Candidate {
	switch strings.TrimSpace(sourceID) {
	case "catalog:auto:aviation-airports-drones-and-mobility-opensky-network":
		return normalizeOpenSkyCandidates(candidates, parsedAt)
	case "catalog:auto:aviation-airports-drones-and-mobility-airplanes-live", "catalog:auto:security-addendum-air-adsblol-api":
		return normalizeADSBCandidates(candidates, parsedAt)
	case "catalog:auto:maritime-ocean-and-coastal-sources-aishub":
		return normalizeAISHubCandidates(candidates, parsedAt)
	case "catalog:auto:aviation-airports-drones-and-mobility-openaip-core-api":
		return normalizeOpenAIPCandidates(sourceURL, candidates)
	default:
		return candidates
	}
}

func normalizeOpenSkyCandidates(candidates []parser.Candidate, parsedAt time.Time) []parser.Candidate {
	out := make([]parser.Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		states := asAnySlice(candidate.Data["states"])
		if len(states) == 0 {
			out = append(out, candidate)
			continue
		}

		payloadBytes, err := json.Marshal(candidate.Data)
		if err != nil {
			out = append(out, candidate)
			continue
		}
		vectors, err := aviation.DecodeStateVectors(bytes.NewReader(payloadBytes))
		if err != nil {
			out = append(out, candidate)
			continue
		}
		for _, vector := range vectors {
			if !vector.HasPosition {
				continue
			}
			icao24 := strings.ToLower(strings.TrimSpace(vector.ICAO24))
			data := map[string]any{
				"record_kind": "track_point",
				"icao24":      icao24,
				"lat":         vector.Latitude,
				"lon":         vector.Longitude,
				"observed_at": firstNonEmpty(vector.ObservedAt().Format(time.RFC3339), parsedAt.UTC().Format(time.RFC3339)),
			}
			if vector.VelocityMPS != nil {
				data["speed_kph"] = *vector.VelocityMPS * 3.6
			}
			if vector.TrueTrackDeg != nil {
				data["course_deg"] = *vector.TrueTrackDeg
			}
			if icao24 != "" {
				data["entity_id"] = "ent:aircraft:" + icao24
			}
			normalized := candidate
			normalized.Kind = "track_point"
			normalized.NativeID = icao24
			normalized.Data = data
			out = append(out, normalized)
		}
	}
	if len(out) == 0 {
		return candidates
	}
	return out
}

func normalizeADSBCandidates(candidates []parser.Candidate, parsedAt time.Time) []parser.Candidate {
	out := make([]parser.Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		records := asAnySlice(candidate.Data["ac"])
		if len(records) == 0 {
			records = asAnySlice(candidate.Data["aircraft"])
		}
		if len(records) == 0 {
			records = asAnySlice(candidate.Data["states"])
		}
		if len(records) == 0 {
			out = append(out, candidate)
			continue
		}
		for _, rawRecord := range records {
			record, ok := asStringAnyMap(rawRecord)
			if !ok {
				continue
			}
			icao24 := strings.ToLower(firstNonEmpty(extractString(record, "icao24"), extractString(record, "hex"), extractString(record, "icao")))
			lat, hasLat := firstFloat(record, "lat", "latitude")
			lon, hasLon := firstFloat(record, "lon", "lng", "longitude")
			if !hasLat || !hasLon {
				continue
			}
			data := map[string]any{
				"record_kind": "track_point",
				"icao24":      icao24,
				"lat":         lat,
				"lon":         lon,
				"observed_at": firstNonEmpty(extractString(record, "observed_at"), extractString(record, "timestamp"), anyToTimeString(record["seen_pos"], parsedAt), parsedAt.UTC().Format(time.RFC3339)),
			}
			if speedKnots, ok := firstFloat(record, "gs", "speed"); ok {
				data["speed_kph"] = speedKnots * 1.852
			}
			if courseDeg, ok := firstFloat(record, "track", "heading"); ok {
				data["course_deg"] = courseDeg
			}
			if icao24 != "" {
				data["entity_id"] = "ent:aircraft:" + icao24
			}
			normalized := candidate
			normalized.Kind = "track_point"
			normalized.NativeID = icao24
			normalized.Data = data
			out = append(out, normalized)
		}
	}
	if len(out) == 0 {
		return candidates
	}
	return out
}

func normalizeAISHubCandidates(candidates []parser.Candidate, parsedAt time.Time) []parser.Candidate {
	out := make([]parser.Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		record := candidate.Data
		if len(record) == 0 {
			out = append(out, candidate)
			continue
		}
		mmsi := firstNonEmpty(extractString(record, "MMSI"), extractString(record, "mmsi"))
		lat, hasLat := firstFloat(record, "LAT", "lat", "latitude")
		lon, hasLon := firstFloat(record, "LON", "lon", "longitude")
		if !hasLat || !hasLon {
			out = append(out, candidate)
			continue
		}
		data := map[string]any{
			"record_kind": "track_point",
			"mmsi":        mmsi,
			"lat":         lat,
			"lon":         lon,
			"observed_at": firstNonEmpty(extractString(record, "observed_at"), extractString(record, "TIME"), extractString(record, "time"), parsedAt.UTC().Format(time.RFC3339)),
		}
		if speedKnots, ok := firstFloat(record, "SOG", "sog"); ok {
			data["speed_kph"] = speedKnots * 1.852
		}
		if courseDeg, ok := firstFloat(record, "COG", "cog", "heading"); ok {
			data["course_deg"] = courseDeg
		}
		if mmsi != "" {
			data["entity_id"] = "ent:vessel:" + mmsi
		}
		normalized := candidate
		normalized.Kind = "track_point"
		normalized.NativeID = mmsi
		normalized.Data = data
		out = append(out, normalized)
	}
	if len(out) == 0 {
		return candidates
	}
	return out
}

func normalizeOpenAIPCandidates(sourceURL string, candidates []parser.Candidate) []parser.Candidate {
	entityType := "aeronautical_reference"
	urlLower := strings.ToLower(strings.TrimSpace(sourceURL))
	switch {
	case strings.Contains(urlLower, "/airports"):
		entityType = "airport"
	case strings.Contains(urlLower, "/airspaces"):
		entityType = "airspace"
	case strings.Contains(urlLower, "/navaids"):
		entityType = "navaid"
	case strings.Contains(urlLower, "/reporting-points"):
		entityType = "reporting_point"
	}

	out := make([]parser.Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		items := asAnySlice(candidate.Data["items"])
		if len(items) == 0 {
			items = []any{candidate.Data}
		}
		for _, rawItem := range items {
			item, ok := asStringAnyMap(rawItem)
			if !ok {
				continue
			}
			nativeID := firstNonEmpty(extractString(item, "_id"), extractString(item, "id"), extractString(item, "icaoCode"), extractString(item, "name"))
			if nativeID == "" {
				continue
			}
			entityID := firstNonEmpty(extractString(item, "entity_id"), "ent:openaip:"+nativeID)
			data := map[string]any{
				"record_kind": "entity",
				"entity_id":   entityID,
				"entity_type": entityType,
				"name":        firstNonEmpty(extractString(item, "name"), extractString(item, "title"), nativeID),
			}
			if placeID := firstNonEmpty(extractString(item, "place_id"), extractString(item, "country")); placeID != "" {
				data["place_id"] = placeID
			}
			normalized := candidate
			normalized.Kind = "entity"
			normalized.NativeID = nativeID
			normalized.Data = data
			out = append(out, normalized)
		}
	}
	if len(out) == 0 {
		return candidates
	}
	return out
}

func asAnySlice(value any) []any {
	switch typed := value.(type) {
	case []any:
		return typed
	default:
		return nil
	}
}

func asStringAnyMap(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		return typed, true
	default:
		return nil, false
	}
}

func anyToString(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text)
	}
	return strings.TrimSpace(fmt.Sprintf("%v", value))
}

func anyToFloat(value any) (float64, bool) {
	switch typed := value.(type) {
	case nil:
		return 0, false
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func rowAt(row []any, index int) any {
	if index < 0 || index >= len(row) {
		return nil
	}
	return row[index]
}

func firstNonNil(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func anyToTimeString(value any, fallback time.Time) string {
	if value == nil {
		return fallback.UTC().Format(time.RFC3339)
	}
	switch typed := value.(type) {
	case string:
		if parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(typed)); err == nil {
			return parsed.UTC().Format(time.RFC3339)
		}
		if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(typed)); err == nil {
			return parsed.UTC().Format(time.RFC3339)
		}
	case json.Number:
		if unix, err := typed.Int64(); err == nil && unix > 0 {
			return time.Unix(unix, 0).UTC().Format(time.RFC3339)
		}
	case float64:
		if typed > 0 {
			return time.Unix(int64(typed), 0).UTC().Format(time.RFC3339)
		}
	case int64:
		if typed > 0 {
			return time.Unix(typed, 0).UTC().Format(time.RFC3339)
		}
	case int:
		if typed > 0 {
			return time.Unix(int64(typed), 0).UTC().Format(time.RFC3339)
		}
	}
	return fallback.UTC().Format(time.RFC3339)
}

func firstFloat(data map[string]any, keys ...string) (float64, bool) {
	for _, key := range keys {
		if value, ok := extractFloat(data, key); ok {
			return value, true
		}
	}
	return 0, false
}

func extractString(data map[string]any, key string) string {
	if data == nil {
		return ""
	}
	value, ok := data[key]
	if !ok || value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text)
	}
	return strings.TrimSpace(fmt.Sprintf("%v", value))
}

func extractFloat(data map[string]any, key string) (float64, bool) {
	if data == nil {
		return 0, false
	}
	v, ok := data[key]
	if !ok || v == nil {
		return 0, false
	}
	switch typed := v.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func nullableString(value string) string {
	if strings.TrimSpace(value) == "" {
		return "NULL"
	}
	return fmt.Sprintf("'%s'", esc(value))
}

func nullableFloat(value float64, ok bool) string {
	if !ok {
		return "NULL"
	}
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func nullableTime(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "NULL"
	}
	t := parseTime(trimmed)
	if t.IsZero() {
		return "NULL"
	}
	return fmt.Sprintf("toDateTime64('%s', 3, 'UTC')", esc(formatClickHouseTime(t)))
}

func parseTime(value string) time.Time {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Now().UTC().Truncate(time.Millisecond)
	}
	if parsed, err := time.ParseInLocation(clickHouseTimeLayout, trimmed, time.UTC); err == nil {
		return parsed.UTC().Truncate(time.Millisecond)
	}
	if parsed, err := time.ParseInLocation("2006-01-02 15:04:05", trimmed, time.UTC); err == nil {
		return parsed.UTC().Truncate(time.Millisecond)
	}
	parsed, err := time.Parse(time.RFC3339Nano, trimmed)
	if err == nil {
		return parsed.UTC().Truncate(time.Millisecond)
	}
	parsed, err = time.Parse(time.RFC3339, trimmed)
	if err == nil {
		return parsed.UTC().Truncate(time.Millisecond)
	}
	return time.Now().UTC().Truncate(time.Millisecond)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func buildParseCheckpoint(sourceID, bronzeTable string, row rawDocRow, parserID, parserVersion string) parseCheckpointRecord {
	seed := strings.Join([]string{
		strings.TrimSpace(sourceID),
		strings.TrimSpace(row.RawID),
		strings.TrimSpace(parserID),
		strings.TrimSpace(parserVersion),
		strings.TrimSpace(row.ContentHash),
		strings.TrimSpace(bronzeTable),
	}, "|")
	digest := sha256.Sum256([]byte(seed))
	return parseCheckpointRecord{
		CheckpointID:  hex.EncodeToString(digest[:])[:32],
		SourceID:      strings.TrimSpace(sourceID),
		RawID:         strings.TrimSpace(row.RawID),
		ParserID:      strings.TrimSpace(parserID),
		ParserVersion: strings.TrimSpace(parserVersion),
		ContentHash:   strings.TrimSpace(row.ContentHash),
		BronzeTable:   strings.TrimSpace(bronzeTable),
		Status:        parseCheckpointStatusSuccess,
	}
}

func persistParseFailure(ctx context.Context, store clickhouseStore, policy sourceParsePolicy, row rawDocRow, checkpoint parseCheckpointRecord, existing *parseCheckpointRecord, startedAt time.Time, attemptCount uint16, retryPolicy sharedretry.Policy, errorCode, errorMessage string, retryable bool) error {
	status := parseCheckpointStatusDeadLetter
	var nextAttemptAt *string
	var deadLetteredAt *string
	if retryable {
		if retryAt, ok := retryPolicy.NextRetryAt(int(attemptCount), startedAt); ok {
			status = parseCheckpointStatusRetry
			formatted := formatClickHouseTime(*retryAt)
			nextAttemptAt = &formatted
		} else {
			formatted := formatClickHouseTime(startedAt)
			deadLetteredAt = &formatted
		}
	} else {
		formatted := formatClickHouseTime(startedAt)
		deadLetteredAt = &formatted
	}
	checkpoint.Status = status
	checkpoint.AttemptCount = attemptCount
	checkpoint.NextAttemptAt = nextAttemptAt
	checkpoint.LastErrorCode = optionalStringPointer(strings.TrimSpace(errorCode))
	checkpoint.LastErrorMessage = optionalStringPointer(strings.TrimSpace(errorMessage))
	checkpoint.DeadLetteredAt = deadLetteredAt
	checkpoint.RecordVersion = nextParseRecordVersion(existing)
	correlationID := extractCorrelationID(row)
	if correlationID == "" {
		correlationID = observability.NewCorrelationID("parse")
	}
	if err := store.insertParseLog(ctx, buildParseLog(policy, row, startedAt, correlationID, status, 0, errorCode, errorMessage)); err != nil {
		return err
	}
	return store.upsertParseCheckpoint(ctx, checkpoint, startedAt)
}

func nextParseAttemptCount(existing *parseCheckpointRecord) uint16 {
	if existing == nil || existing.AttemptCount == 0 {
		return 1
	}
	return existing.AttemptCount + 1
}

func nextParseRecordVersion(existing *parseCheckpointRecord) uint64 {
	if existing == nil || existing.RecordVersion == 0 {
		return 1
	}
	return existing.RecordVersion + 1
}

func esc(value string) string { return strings.ReplaceAll(strings.TrimSpace(value), "'", "''") }

func parseIntEnv(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func parseDurationEnv(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func formatClickHouseTime(value time.Time) string {
	return value.UTC().Format(clickHouseTimeLayout)
}

func sqlNullableTimeString(value *string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return "NULL"
	}
	return fmt.Sprintf("toDateTime64('%s', 3, 'UTC')", esc(*value))
}

func sqlNullableString(value *string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return "NULL"
	}
	return fmt.Sprintf("'%s'", esc(*value))
}

func maxUint64(value, fallback uint64) uint64 {
	if value == 0 {
		return fallback
	}
	return value
}

func optionalStringPointer(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}
