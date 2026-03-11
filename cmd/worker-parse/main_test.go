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

func TestParseCheckpointPreventsDuplicateBronzeWrites(t *testing.T) {
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

func TestParserVersionBumpReprocessesRawDocs(t *testing.T) {
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

func TestParseSourceSkipsCheckpointedRawDocs(t *testing.T) {
	bronzeInsertCount := 0
	parseLogCount := 0
	checkpointInsertCount := 0
	checkpointLookups := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		switch {
		case strings.Contains(query, "FROM meta.source_registry FINAL"):
			_, _ = w.Write([]byte(mustJSONLine(t, testSourcePolicyRow{SourceID: "seed:gdelt", ParserID: "parser:json", FormatHint: "json", ParseConfig: `{}`, BronzeTable: stringPointer("bronze.src_seed_gdelt_v1"), TransportType: "http", CrawlEnabled: 1, PromoteProfile: "default"})))
		case strings.Contains(query, "FROM bronze.raw_document"):
			_, _ = w.Write([]byte(mustRawDocJSONLine(t, "content-v1")))
		case strings.Contains(query, "SELECT count() FROM ops.parse_checkpoint"):
			checkpointLookups++
			if checkpointLookups == 1 {
				_, _ = w.Write([]byte("0\n"))
			} else {
				_, _ = w.Write([]byte("1\n"))
			}
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
	if first.BronzeRows != 1 || second.BronzeRows != 0 {
		t.Fatalf("expected first run bronze rows=1 and second run bronze rows=0, got first=%d second=%d", first.BronzeRows, second.BronzeRows)
	}
	if parseLogCount != 2 {
		t.Fatalf("expected parse logs for success then checkpoint skip, got %d", parseLogCount)
	}
}

func TestParseSourceReprocessesOnParserVersionAndContentHashChange(t *testing.T) {
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
			case strings.Contains(query, "FROM bronze.raw_document"):
				_, _ = w.Write([]byte(mustRawDocJSONLine(t, "content-v1")))
			case strings.Contains(query, "SELECT count() FROM ops.parse_checkpoint"):
				if strings.Contains(query, "parser_version = '2.0.0'") {
					_, _ = w.Write([]byte("0\n"))
				} else {
					_, _ = w.Write([]byte("1\n"))
				}
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
			case strings.Contains(query, "FROM bronze.raw_document"):
				_, _ = w.Write([]byte(mustRawDocJSONLine(t, "content-v2")))
			case strings.Contains(query, "SELECT count() FROM ops.parse_checkpoint"):
				if strings.Contains(query, "content_hash = 'content-v2'") {
					_, _ = w.Write([]byte("0\n"))
				} else {
					_, _ = w.Write([]byte("1\n"))
				}
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
