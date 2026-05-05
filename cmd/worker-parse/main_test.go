package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"global-osint-backend/internal/discovery"
	"global-osint-backend/internal/parser"
)

type testParser struct {
	version string
}

type testSourcePolicyRow struct {
	SourceID       string  `json:"source_id"`
	ParserID       string  `json:"parser_id"`
	FormatHint     string  `json:"format_hint"`
	ParseConfig    string  `json:"parse_config_json"`
	BronzeTable    *string `json:"bronze_table"`
	TransportType  string  `json:"transport_type"`
	CrawlEnabled   uint8   `json:"crawl_enabled"`
	PromoteProfile string  `json:"promote_profile"`
}

func (p testParser) Descriptor() parser.Descriptor {
	return parser.Descriptor{ID: "parser:test-json", Family: "structured", Version: p.version, RouteScope: "raw_document", SourceClass: "structured_document", HandlerRef: "cmd/worker-parse.testParser", SupportedFormats: []string{"json", "application/json"}}
}

func (p testParser) Parse(_ context.Context, input parser.Input) (parser.Result, *parser.ParseError) {
	data := map[string]any{}
	if err := json.Unmarshal(input.Body, &data); err != nil {
		return parser.Result{}, &parser.ParseError{Code: parser.CodeInvalidJSON, Message: "invalid test json"}
	}
	candidate := parser.Candidate{ParserID: p.Descriptor().ID, ParserVersion: p.version, NativeID: "native-1", ContentHash: firstNonEmpty(extractString(data, "content_hash"), "hash-1"), SchemaVersion: 1, RecordVersion: 1, Data: map[string]any{"record_kind": "event", "title": extractString(data, "title")}, Attrs: map[string]any{}, Evidence: []parser.Evidence{}}
	return parser.Result{ParserID: p.Descriptor().ID, ParserVersion: p.version, Candidates: []parser.Candidate{candidate}}, nil
}

func TestParseFailureLeavesFrontierFetchStateUnchanged(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	status := uint16(200)
	entry := discovery.FrontierEntry{SourceID: "seed:gdelt", State: discovery.FrontierStateFetched, AttemptCount: 1, LastAttemptAt: &now, LastStatusCode: &status, NextFetchAt: now.Add(6 * time.Hour)}
	parseErr := entry
	if parseErr.State != discovery.FrontierStateFetched {
		t.Fatalf("expected parse failure to keep state %q, got %q", discovery.FrontierStateFetched, parseErr.State)
	}
	if parseErr.LastStatusCode == nil || *parseErr.LastStatusCode != 200 {
		t.Fatalf("expected fetch status to remain intact, got %#v", parseErr.LastStatusCode)
	}
}

func TestBuildBronzeInsertSQLUsesNativeIDFallbackKey(t *testing.T) {
	doc := rawDocRow{RawID: "raw:1", FetchID: "fetch:1", SourceID: "seed:gdelt", URL: "https://example.test/doc", FinalURL: "https://example.test/doc", FetchedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	candidate := parser.Candidate{ParserID: "parser:json", ParserVersion: "1", NativeID: "native-42", ContentHash: "hash-42", SchemaVersion: 1, RecordVersion: 1, Data: map[string]any{"record_kind": "event", "title": "Demo"}, Attrs: map[string]any{}, Evidence: []parser.Evidence{}}
	sql, err := buildBronzeInsertSQL("bronze.src_seed_gdelt_v1", doc, candidate, 0, time.Now().UTC())
	if err != nil {
		t.Fatalf("build sql: %v", err)
	}
	if !strings.Contains(sql, "'native-42'") {
		t.Fatalf("expected source_record_key to include native id, got %s", sql)
	}
}

func TestBuildBronzeInsertSQLFallsBackToContentHashKey(t *testing.T) {
	doc := rawDocRow{RawID: "raw:1", FetchID: "fetch:1", SourceID: "seed:gdelt", URL: "https://example.test/doc", FinalURL: "https://example.test/doc", FetchedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	candidate := parser.Candidate{ParserID: "parser:json", ParserVersion: "1", ContentHash: "hash-only", SchemaVersion: 1, RecordVersion: 1, Data: map[string]any{"record_kind": "event"}, Attrs: map[string]any{}, Evidence: []parser.Evidence{}}
	sql, err := buildBronzeInsertSQL("bronze.src_seed_gdelt_v1", doc, candidate, 2, time.Now().UTC())
	if err != nil {
		t.Fatalf("build sql: %v", err)
	}
	if !strings.Contains(sql, "'hash-only'") {
		t.Fatalf("expected source_record_key to include content hash, got %s", sql)
	}
	if !strings.Contains(sql, ",2,") {
		t.Fatalf("expected source_record_index to match candidate index, got %s", sql)
	}
}

func TestBuildParseCheckpointIsStableForIdenticalInputs(t *testing.T) {
	row := rawDocRow{RawID: "raw:1", SourceID: "seed:gdelt", ContentHash: "content-v1"}
	checkpoint := buildParseCheckpoint("seed:gdelt", "bronze.src_seed_gdelt_v1", row, "parser:csv", "1.0.0")
	if checkpoint.CheckpointID == "" {
		t.Fatal("expected parse checkpoint id")
	}
	if checkpoint.ParserVersion != "1.0.0" {
		t.Fatalf("expected parser version to be tracked, got %q", checkpoint.ParserVersion)
	}
	repeated := buildParseCheckpoint("seed:gdelt", "bronze.src_seed_gdelt_v1", row, "parser:csv", "1.0.0")
	if checkpoint.CheckpointID != repeated.CheckpointID {
		t.Fatalf("expected identical raw/parser inputs to produce stable checkpoint ids, got %q vs %q", checkpoint.CheckpointID, repeated.CheckpointID)
	}
}

func TestBuildParseCheckpointChangesWhenVersionOrContentChanges(t *testing.T) {
	row := rawDocRow{RawID: "raw:1", SourceID: "seed:gdelt", ContentHash: "content-v1"}
	base := buildParseCheckpoint("seed:gdelt", "bronze.src_seed_gdelt_v1", row, "parser:csv", "1.0.0")
	versionBump := buildParseCheckpoint("seed:gdelt", "bronze.src_seed_gdelt_v1", row, "parser:csv", "1.1.0")
	if base.CheckpointID == versionBump.CheckpointID {
		t.Fatalf("expected parser version bump to produce a new checkpoint id, got %q", base.CheckpointID)
	}
	contentBump := buildParseCheckpoint("seed:gdelt", "bronze.src_seed_gdelt_v1", rawDocRow{RawID: "raw:1", SourceID: "seed:gdelt", ContentHash: "content-v2"}, "parser:csv", "1.0.0")
	if base.CheckpointID == contentBump.CheckpointID {
		t.Fatalf("expected content hash change to produce a new checkpoint id, got %q", base.CheckpointID)
	}
}

func TestParseCheckpointPreventsDuplicateBronzeWrites(t *testing.T) {
	bronzeInsertCount := 0
	parseLogCount := 0
	checkpointInsertCount := 0
	rawDocumentQueries := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		switch {
		case strings.Contains(query, "FROM meta.source_registry FINAL"):
			_, _ = w.Write([]byte(mustJSONLine(t, testSourcePolicyRow{SourceID: "seed:gdelt", ParserID: "parser:json", FormatHint: "json", ParseConfig: `{}`, BronzeTable: stringPointer("bronze.src_seed_gdelt_v1"), TransportType: "http", CrawlEnabled: 1, PromoteProfile: "default"})))
		case strings.Contains(query, "FROM bronze.raw_document raw"):
			rawDocumentQueries++
			if rawDocumentQueries == 1 {
				_, _ = w.Write([]byte(mustRawDocJSONLine(t, "content-v1")))
				return
			}
			_, _ = w.Write([]byte(""))
		case strings.Contains(query, "FROM bronze.raw_document"):
			t.Fatalf("expected parse-source raw document selection to use checkpoint-aware query, got %s", query)
		case strings.Contains(query, "FROM ops.parse_checkpoint FINAL"):
			_, _ = w.Write([]byte(""))
		case strings.Contains(query, "INSERT INTO bronze.src_seed_gdelt_v1"):
			bronzeInsertCount++
			w.WriteHeader(http.StatusOK)
		case strings.Contains(query, "INSERT INTO ops.parse_log"):
			parseLogCount++
			w.WriteHeader(http.StatusOK)
		case strings.Contains(query, "INSERT INTO ops.parse_checkpoint"):
			checkpointInsertCount++
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	cfg := config{ClickHouseHTTP: server.URL, MinIOEndpoint: "http://unused", MinIOAccessKey: "minio", MinIOSecretKey: "minio", MinIORegion: "us-east-1", RawBucket: "raw", ParseTimeout: 5 * time.Second}
	stdout := &strings.Builder{}
	stderr := &strings.Builder{}
	if code := parseSource(cfg, []string{"--source-id", "seed:gdelt", "--limit", "1"}, stdout, stderr); code != 0 {
		t.Fatalf("first parseSource failed: code=%d stderr=%s", code, stderr.String())
	}
	first := parseStats{}
	if err := json.Unmarshal([]byte(stdout.String()), &first); err != nil {
		t.Fatalf("decode first parse stats: %v", err)
	}
	stdout.Reset()
	stderr.Reset()
	if code := parseSource(cfg, []string{"--source-id", "seed:gdelt", "--limit", "1"}, stdout, stderr); code != 0 {
		t.Fatalf("second parseSource failed: code=%d stderr=%s", code, stderr.String())
	}
	second := parseStats{}
	if err := json.Unmarshal([]byte(stdout.String()), &second); err != nil {
		t.Fatalf("decode second parse stats: %v", err)
	}
	if bronzeInsertCount != 1 {
		t.Fatalf("expected exactly 1 bronze insert across repeated parse runs, got %d", bronzeInsertCount)
	}
	if checkpointInsertCount != 1 {
		t.Fatalf("expected exactly 1 checkpoint insert across repeated parse runs, got %d", checkpointInsertCount)
	}
	if rawDocumentQueries != 2 {
		t.Fatalf("expected two checkpoint-aware raw document queries across repeated runs, got %d", rawDocumentQueries)
	}
	if first.BronzeRows != 1 || second.BronzeRows != 0 {
		t.Fatalf("expected first run bronze rows=1 and second run bronze rows=0, got first=%d second=%d", first.BronzeRows, second.BronzeRows)
	}
	if parseLogCount != 1 {
		t.Fatalf("expected only the successful first run to log parsing work, got %d", parseLogCount)
	}
}

func TestParseSourcePropagatesCorrelationIDFromFetchMetadata(t *testing.T) {
	var parseLogQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		switch {
		case strings.Contains(query, "FROM meta.source_registry FINAL"):
			_, _ = w.Write([]byte(mustJSONLine(t, testSourcePolicyRow{SourceID: "seed:gdelt", ParserID: "parser:json", FormatHint: "json", ParseConfig: `{}`, BronzeTable: stringPointer("bronze.src_seed_gdelt_v1"), TransportType: "http", CrawlEnabled: 1, PromoteProfile: "default"})))
		case strings.Contains(query, "FROM bronze.raw_document raw"):
			_, _ = w.Write([]byte(`{"raw_id":"raw:1","fetch_id":"fetch:1","source_id":"seed:gdelt","url":"https://example.test/doc","final_url":"https://example.test/doc","fetched_at":"2026-03-10T09:00:00Z","content_type":"application/json","object_key":null,"fetch_metadata":"{\"inline_body_base64\":\"eyJ0aXRsZSI6IkRlbW8ifQ==\",\"correlation_id\":\"trace.fetch-123\"}","content_hash":"hash-1","storage_class":"inline"}` + "\n"))
		case strings.Contains(query, "FROM ops.parse_checkpoint FINAL"):
			_, _ = w.Write([]byte(""))
		case strings.Contains(query, "INSERT INTO bronze.src_seed_gdelt_v1"):
			w.WriteHeader(http.StatusOK)
		case strings.Contains(query, "INSERT INTO ops.parse_log"):
			parseLogQuery = query
			w.WriteHeader(http.StatusOK)
		case strings.Contains(query, "INSERT INTO ops.parse_checkpoint"):
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	cfg := config{ClickHouseHTTP: server.URL, MinIOEndpoint: "http://unused", MinIOAccessKey: "minio", MinIOSecretKey: "minio", MinIORegion: "us-east-1", RawBucket: "raw", ParseTimeout: 5 * time.Second}
	if code := parseSource(cfg, []string{"--source-id", "seed:gdelt", "--limit", "1"}, &strings.Builder{}, &strings.Builder{}); code != 0 {
		t.Fatalf("parseSource failed")
	}
	if !strings.Contains(parseLogQuery, "trace.fetch-123") {
		t.Fatalf("expected parse log query to contain propagated correlation id, got %s", parseLogQuery)
	}
}

func TestParserVersionBumpReprocessesRawDocs(t *testing.T) {
	t.Run("parser version change", func(t *testing.T) {
		registry, err := parser.NewRegistry(testParser{version: "2.0.0"})
		if err != nil {
			t.Fatalf("new registry: %v", err)
		}
		bronzeInsertCount := 0
		checkpointInsertCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			query := r.URL.Query().Get("query")
			switch {
			case strings.Contains(query, "FROM meta.source_registry FINAL"):
				_, _ = w.Write([]byte(mustJSONLine(t, testSourcePolicyRow{SourceID: "seed:gdelt", ParserID: "parser:test-json", FormatHint: "json", ParseConfig: `{}`, BronzeTable: stringPointer("bronze.src_seed_gdelt_v1"), TransportType: "http", CrawlEnabled: 1, PromoteProfile: "default"})))
			case strings.Contains(query, "FROM bronze.raw_document raw"):
				if !strings.Contains(query, "checkpoint.parser_version = '2.0.0'") {
					t.Fatalf("expected raw selection to filter on current parser version, got %s", query)
				}
				_, _ = w.Write([]byte(mustRawDocJSONLine(t, "content-v1")))
			case strings.Contains(query, "FROM bronze.raw_document"):
				t.Fatalf("expected checkpoint-aware raw selection query, got %s", query)
			case strings.Contains(query, "FROM ops.parse_checkpoint FINAL"):
				_, _ = w.Write([]byte(""))
			case strings.Contains(query, "INSERT INTO bronze.src_seed_gdelt_v1"):
				bronzeInsertCount++
				w.WriteHeader(http.StatusOK)
			case strings.Contains(query, "INSERT INTO ops.parse_checkpoint"):
				checkpointInsertCount++
				w.WriteHeader(http.StatusOK)
			default:
				w.WriteHeader(http.StatusOK)
			}
		}))
		defer server.Close()
		cfg := config{ClickHouseHTTP: server.URL, MinIOEndpoint: "http://unused", MinIOAccessKey: "minio", MinIOSecretKey: "minio", MinIORegion: "us-east-1", RawBucket: "raw", ParseTimeout: 5 * time.Second}
		if code := parseSourceWithRegistry(cfg, []string{"--source-id", "seed:gdelt", "--limit", "1"}, &strings.Builder{}, &strings.Builder{}, registry); code != 0 {
			t.Fatalf("parseSourceWithRegistry failed for parser version change")
		}
		if bronzeInsertCount != 1 || checkpointInsertCount != 1 {
			t.Fatalf("expected parser-version change to force 1 bronze insert and 1 checkpoint insert, got bronze=%d checkpoint=%d", bronzeInsertCount, checkpointInsertCount)
		}
	})

	t.Run("content hash change", func(t *testing.T) {
		bronzeInsertCount := 0
		checkpointInsertCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			query := r.URL.Query().Get("query")
			switch {
			case strings.Contains(query, "FROM meta.source_registry FINAL"):
				_, _ = w.Write([]byte(mustJSONLine(t, testSourcePolicyRow{SourceID: "seed:gdelt", ParserID: "parser:json", FormatHint: "json", ParseConfig: `{}`, BronzeTable: stringPointer("bronze.src_seed_gdelt_v1"), TransportType: "http", CrawlEnabled: 1, PromoteProfile: "default"})))
			case strings.Contains(query, "FROM bronze.raw_document raw"):
				if !strings.Contains(query, "checkpoint.content_hash = raw.content_hash") {
					t.Fatalf("expected raw selection to compare checkpoint content hash against raw content hash, got %s", query)
				}
				_, _ = w.Write([]byte(mustRawDocJSONLine(t, "content-v2")))
			case strings.Contains(query, "FROM bronze.raw_document"):
				t.Fatalf("expected checkpoint-aware raw selection query, got %s", query)
			case strings.Contains(query, "FROM ops.parse_checkpoint FINAL"):
				_, _ = w.Write([]byte(""))
			case strings.Contains(query, "INSERT INTO bronze.src_seed_gdelt_v1"):
				bronzeInsertCount++
				w.WriteHeader(http.StatusOK)
			case strings.Contains(query, "INSERT INTO ops.parse_checkpoint"):
				checkpointInsertCount++
				w.WriteHeader(http.StatusOK)
			default:
				w.WriteHeader(http.StatusOK)
			}
		}))
		defer server.Close()
		cfg := config{ClickHouseHTTP: server.URL, MinIOEndpoint: "http://unused", MinIOAccessKey: "minio", MinIOSecretKey: "minio", MinIORegion: "us-east-1", RawBucket: "raw", ParseTimeout: 5 * time.Second}
		if code := parseSource(cfg, []string{"--source-id", "seed:gdelt", "--limit", "1"}, &strings.Builder{}, &strings.Builder{}); code != 0 {
			t.Fatalf("parseSource failed for content hash change")
		}
		if bronzeInsertCount != 1 || checkpointInsertCount != 1 {
			t.Fatalf("expected content-hash change to force 1 bronze insert and 1 checkpoint insert, got bronze=%d checkpoint=%d", bronzeInsertCount, checkpointInsertCount)
		}
	})
}

func TestParseSourceRetriesTransientFailuresWithBackoff(t *testing.T) {
	checkpointByRaw := map[string]parseCheckpointRecord{}
	bronzeInsertCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		switch {
		case strings.Contains(query, "FROM meta.source_registry FINAL"):
			_, _ = w.Write([]byte(mustJSONLine(t, testSourcePolicyRow{SourceID: "seed:gdelt", ParserID: "parser:test-json", FormatHint: "json", ParseConfig: `{}`, BronzeTable: stringPointer("bronze.src_seed_gdelt_v1"), TransportType: "http", CrawlEnabled: 1, PromoteProfile: "default"})))
		case strings.Contains(query, "FROM bronze.raw_document raw"):
			if _, ok := checkpointByRaw["raw:1"]; ok {
				_, _ = w.Write([]byte(""))
				return
			}
			_, _ = w.Write([]byte(mustRawDocJSONLine(t, "content-v1")))
		case strings.Contains(query, "FROM ops.parse_checkpoint FINAL"):
			record, ok := checkpointByRaw["raw:1"]
			if !ok {
				_, _ = w.Write([]byte(""))
				return
			}
			_, _ = w.Write([]byte(mustJSONLine(t, record)))
		case strings.Contains(query, "INSERT INTO bronze.src_seed_gdelt_v1"):
			bronzeInsertCount++
			w.WriteHeader(http.StatusInternalServerError)
		case strings.Contains(query, "INSERT INTO ops.parse_checkpoint"):
			status := parseCheckpointStatusRetry
			if strings.Contains(query, "'dead_letter'") {
				status = parseCheckpointStatusDeadLetter
			}
			checkpointByRaw["raw:1"] = parseCheckpointRecord{CheckpointID: buildParseCheckpoint("seed:gdelt", "bronze.src_seed_gdelt_v1", rawDocRow{RawID: "raw:1", SourceID: "seed:gdelt", ContentHash: "content-v1"}, "parser:test-json", "v1").CheckpointID, SourceID: "seed:gdelt", RawID: "raw:1", ParserID: "parser:test-json", ParserVersion: "v1", ContentHash: "content-v1", BronzeTable: "bronze.src_seed_gdelt_v1", Status: status, AttemptCount: 1, RecordVersion: 1}
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	registry, err := parser.NewRegistry(testParser{version: "v1"})
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	cfg := config{ClickHouseHTTP: server.URL, MinIOEndpoint: "http://unused", MinIOAccessKey: "minio", MinIOSecretKey: "minio", MinIORegion: "us-east-1", RawBucket: "raw", ParseTimeout: 5 * time.Second, RetryMaxAttempts: 3, RetryInitialBackoff: time.Second, RetryMaxBackoff: 3 * time.Second}
	if code := parseSourceWithRegistry(cfg, []string{"--source-id", "seed:gdelt", "--limit", "1"}, &strings.Builder{}, &strings.Builder{}, registry); code != 0 {
		t.Fatalf("parseSourceWithRegistry failed on transient retry case")
	}
	if checkpointByRaw["raw:1"].Status != parseCheckpointStatusRetry {
		t.Fatalf("expected transient failure to be marked retry, got %q", checkpointByRaw["raw:1"].Status)
	}
	if code := parseSourceWithRegistry(cfg, []string{"--source-id", "seed:gdelt", "--limit", "1"}, &strings.Builder{}, &strings.Builder{}, registry); code != 0 {
		t.Fatalf("second parseSourceWithRegistry failed on transient retry case")
	}
	if bronzeInsertCount != 1 {
		t.Fatalf("expected immediate rerun to defer retried raw doc, got bronze inserts=%d", bronzeInsertCount)
	}
}

func TestParseSourceDeadLettersPoisonAndKeepsHealthyWorkFlowing(t *testing.T) {
	checkpointByRaw := map[string]parseCheckpointRecord{}
	bronzeInsertCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		switch {
		case strings.Contains(query, "FROM meta.source_registry FINAL"):
			_, _ = w.Write([]byte(mustJSONLine(t, testSourcePolicyRow{SourceID: "seed:gdelt", ParserID: "parser:test-json", FormatHint: "json", ParseConfig: `{}`, BronzeTable: stringPointer("bronze.src_seed_gdelt_v1"), TransportType: "http", CrawlEnabled: 1, PromoteProfile: "default"})))
		case strings.Contains(query, "FROM bronze.raw_document raw"):
			lines := []string{}
			if _, ok := checkpointByRaw["raw:poison"]; !ok {
				lines = append(lines, mustRawDocJSONLineWithTitle(t, "poison", "poison"))
			}
			if _, ok := checkpointByRaw["raw:healthy"]; !ok {
				lines = append(lines, mustRawDocJSONLineWithTitle(t, "healthy", "healthy"))
			}
			_, _ = w.Write([]byte(strings.Join(lines, "")))
		case strings.Contains(query, "FROM ops.parse_checkpoint FINAL"):
			if strings.Contains(query, "raw_id = 'raw:poison'") {
				if record, ok := checkpointByRaw["raw:poison"]; ok {
					_, _ = w.Write([]byte(mustJSONLine(t, record)))
					return
				}
			}
			if strings.Contains(query, "raw_id = 'raw:healthy'") {
				if record, ok := checkpointByRaw["raw:healthy"]; ok {
					_, _ = w.Write([]byte(mustJSONLine(t, record)))
					return
				}
			}
			_, _ = w.Write([]byte(""))
		case strings.Contains(query, "INSERT INTO bronze.src_seed_gdelt_v1"):
			bronzeInsertCount++
			w.WriteHeader(http.StatusOK)
		case strings.Contains(query, "INSERT INTO ops.parse_checkpoint"):
			if strings.Contains(query, "'raw:poison'") {
				checkpointByRaw["raw:poison"] = parseCheckpointRecord{CheckpointID: buildParseCheckpoint("seed:gdelt", "bronze.src_seed_gdelt_v1", rawDocRow{RawID: "raw:poison", SourceID: "seed:gdelt", ContentHash: "poison"}, "parser:test-json", "v1").CheckpointID, SourceID: "seed:gdelt", RawID: "raw:poison", ParserID: "parser:test-json", ParserVersion: "v1", ContentHash: "poison", BronzeTable: "bronze.src_seed_gdelt_v1", Status: parseCheckpointStatusDeadLetter, AttemptCount: 1, RecordVersion: 1}
			}
			if strings.Contains(query, "'raw:healthy'") {
				checkpointByRaw["raw:healthy"] = parseCheckpointRecord{CheckpointID: buildParseCheckpoint("seed:gdelt", "bronze.src_seed_gdelt_v1", rawDocRow{RawID: "raw:healthy", SourceID: "seed:gdelt", ContentHash: "healthy"}, "parser:test-json", "v1").CheckpointID, SourceID: "seed:gdelt", RawID: "raw:healthy", ParserID: "parser:test-json", ParserVersion: "v1", ContentHash: "healthy", BronzeTable: "bronze.src_seed_gdelt_v1", Status: parseCheckpointStatusSuccess, AttemptCount: 1, RecordVersion: 1}
			}
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	registry, err := parser.NewRegistry(poisonAwareParser{version: "v1"})
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	stdout := &strings.Builder{}
	cfg := config{ClickHouseHTTP: server.URL, MinIOEndpoint: "http://unused", MinIOAccessKey: "minio", MinIOSecretKey: "minio", MinIORegion: "us-east-1", RawBucket: "raw", ParseTimeout: 5 * time.Second, RetryMaxAttempts: 3, RetryInitialBackoff: time.Second, RetryMaxBackoff: 3 * time.Second}
	if code := parseSourceWithRegistry(cfg, []string{"--source-id", "seed:gdelt", "--limit", "2"}, stdout, &strings.Builder{}, registry); code != 0 {
		t.Fatalf("parseSourceWithRegistry failed on poison isolation case")
	}
	stats := parseStats{}
	if err := json.Unmarshal([]byte(stdout.String()), &stats); err != nil {
		t.Fatalf("decode parse stats: %v", err)
	}
	if checkpointByRaw["raw:poison"].Status != parseCheckpointStatusDeadLetter {
		t.Fatalf("expected poison payload to dead-letter, got %q", checkpointByRaw["raw:poison"].Status)
	}
	if checkpointByRaw["raw:healthy"].Status != parseCheckpointStatusSuccess {
		t.Fatalf("expected healthy payload to succeed, got %q", checkpointByRaw["raw:healthy"].Status)
	}
	if stats.SuccessDocs != 1 || stats.FailedDocs != 1 || bronzeInsertCount != 1 {
		t.Fatalf("expected poison isolation to keep healthy work flowing, got stats=%+v bronze=%d", stats, bronzeInsertCount)
	}
}

type poisonAwareParser struct {
	version string
}

func (p poisonAwareParser) Descriptor() parser.Descriptor {
	return parser.Descriptor{ID: "parser:test-json", Family: "structured", Version: p.version, RouteScope: "raw_document", SourceClass: "structured_document", HandlerRef: "cmd/worker-parse.poisonAwareParser", SupportedFormats: []string{"json", "application/json"}}
}

func (p poisonAwareParser) Parse(ctx context.Context, input parser.Input) (parser.Result, *parser.ParseError) {
	if strings.Contains(string(input.Body), "poison") {
		return parser.Result{}, &parser.ParseError{Code: parser.CodeInvalidJSON, Message: "poison payload"}
	}
	return testParser{version: p.version}.Parse(ctx, input)
}

func mustRawDocJSONLine(t *testing.T, contentHash string) string {
	t.Helper()
	metaJSON, err := json.Marshal(map[string]string{"inline_body_base64": base64.StdEncoding.EncodeToString([]byte(`{"title":"demo","content_hash":"` + contentHash + `"}`))})
	if err != nil {
		t.Fatalf("marshal fetch metadata: %v", err)
	}
	line, err := json.Marshal(rawDocRow{RawID: "raw:1", FetchID: "fetch:1", SourceID: "seed:gdelt", URL: "https://example.test/doc", FinalURL: "https://example.test/doc", FetchedAt: "2026-03-10T10:00:00Z", ContentType: "application/json", FetchMeta: string(metaJSON), ContentHash: contentHash, StorageClass: "inline"})
	if err != nil {
		t.Fatalf("marshal raw doc row: %v", err)
	}
	return string(line) + "\n"
}

func mustRawDocJSONLineWithTitle(t *testing.T, contentHash, title string) string {
	t.Helper()
	metaJSON, err := json.Marshal(map[string]string{"inline_body_base64": base64.StdEncoding.EncodeToString([]byte(`{"title":"` + title + `","content_hash":"` + contentHash + `"}`))})
	if err != nil {
		t.Fatalf("marshal fetch metadata: %v", err)
	}
	line, err := json.Marshal(rawDocRow{RawID: "raw:" + contentHash, FetchID: "fetch:" + contentHash, SourceID: "seed:gdelt", URL: "https://example.test/" + contentHash, FinalURL: "https://example.test/" + contentHash, FetchedAt: "2026-03-10T10:00:00Z", ContentType: "application/json", FetchMeta: string(metaJSON), ContentHash: contentHash, StorageClass: "inline"})
	if err != nil {
		t.Fatalf("marshal raw doc row: %v", err)
	}
	return string(line) + "\n"
}

func mustJSONLine(t *testing.T, value any) string {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal json line: %v", err)
	}
	return string(b) + "\n"
}

func stringPointer(value string) *string {
	return &value
}

func TestNormalizePhase1CandidatesOpenSky(t *testing.T) {
	candidates := []parser.Candidate{{
		ParserID:      "parser:json",
		ParserVersion: "1.0.0",
		SchemaVersion: 1,
		RecordVersion: 1,
		Data: map[string]any{
			"states": []any{
				[]any{"abc123", "TEST", "FR", float64(1710172790), float64(1710172800), 2.3522, 48.8566, 1000.0, false, 250.0, 90.0, 0.0, nil, 1100.0, "7000", false, 0, 0},
			},
		},
	}}
	normalized := normalizePhase1Candidates("catalog:auto:aviation-airports-drones-and-mobility-opensky-network", "https://opensky-network.org/api/states/all?extended=1", candidates, time.Unix(1710172800, 0).UTC())
	if len(normalized) != 1 {
		t.Fatalf("expected one normalized opensky candidate, got %d", len(normalized))
	}
	if got := extractString(normalized[0].Data, "record_kind"); got != "track_point" {
		t.Fatalf("expected opensky record_kind track_point, got %q", got)
	}
	if got := extractString(normalized[0].Data, "icao24"); got != "abc123" {
		t.Fatalf("expected opensky icao24 abc123, got %q", got)
	}
}

func TestNormalizePhase1CandidatesADSBSupplement(t *testing.T) {
	candidates := []parser.Candidate{{
		ParserID:      "parser:json",
		ParserVersion: "1.0.0",
		SchemaVersion: 1,
		RecordVersion: 1,
		Data: map[string]any{
			"ac": []any{
				map[string]any{"hex": "def456", "lat": 50.1109, "lon": 8.6821, "gs": 200.0, "track": 180.0},
			},
		},
	}}
	normalized := normalizePhase1Candidates("catalog:auto:aviation-airports-drones-and-mobility-airplanes-live", "https://api.airplanes.live/v2/mil", candidates, time.Unix(1710172800, 0).UTC())
	if len(normalized) != 1 {
		t.Fatalf("expected one normalized adsb candidate, got %d", len(normalized))
	}
	if got := extractString(normalized[0].Data, "record_kind"); got != "track_point" {
		t.Fatalf("expected adsb record_kind track_point, got %q", got)
	}
	if got := extractString(normalized[0].Data, "entity_id"); got == "" {
		t.Fatalf("expected adsb entity_id")
	}
}

func TestNormalizePhase1CandidatesAISHub(t *testing.T) {
	candidates := []parser.Candidate{{
		ParserID:      "parser:json",
		ParserVersion: "1.0.0",
		SchemaVersion: 1,
		RecordVersion: 1,
		Data:          map[string]any{"MMSI": "123456789", "LAT": 48.8566, "LON": 2.3522, "SOG": 12.0, "COG": 42.0},
	}}
	normalized := normalizePhase1Candidates("catalog:auto:maritime-ocean-and-coastal-sources-aishub", "https://data.aishub.net/ws.php", candidates, time.Unix(1710172800, 0).UTC())
	if len(normalized) != 1 {
		t.Fatalf("expected one normalized aishub candidate, got %d", len(normalized))
	}
	if got := extractString(normalized[0].Data, "record_kind"); got != "track_point" {
		t.Fatalf("expected aishub record_kind track_point, got %q", got)
	}
	if got := extractString(normalized[0].Data, "mmsi"); got != "123456789" {
		t.Fatalf("expected aishub mmsi 123456789, got %q", got)
	}
}

func TestNormalizePhase1CandidatesOpenAIP(t *testing.T) {
	candidates := []parser.Candidate{{
		ParserID:      "parser:json",
		ParserVersion: "1.0.0",
		SchemaVersion: 1,
		RecordVersion: 1,
		Data: map[string]any{
			"items": []any{
				map[string]any{"id": "apt-1", "name": "OpenAIP Airport"},
			},
		},
	}}
	normalized := normalizePhase1Candidates("catalog:auto:aviation-airports-drones-and-mobility-openaip-core-api", "https://api.core.openaip.net/api/airports", candidates, time.Unix(1710172800, 0).UTC())
	if len(normalized) != 1 {
		t.Fatalf("expected one normalized openaip candidate, got %d", len(normalized))
	}
	if got := extractString(normalized[0].Data, "record_kind"); got != "entity" {
		t.Fatalf("expected openaip record_kind entity, got %q", got)
	}
	if got := extractString(normalized[0].Data, "entity_type"); got != "airport" {
		t.Fatalf("expected openaip entity_type airport, got %q", got)
	}
}
