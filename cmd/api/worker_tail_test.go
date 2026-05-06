package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWorkerTailHandler(t *testing.T) {
	queries := []string{}
	server := serverWithTestAuth(&apiServer{version: "v1", queryTimeout: time.Second, clickhouse: statsStubQuerier{queryFn: func(query string) (string, error) {
		queries = append(queries, query)
		if !strings.Contains(query, "FROM ops.fetch_log") || !strings.Contains(query, "FROM ops.parse_log") || !strings.Contains(query, "FROM ops.job_run") {
			return "", errors.New("expected union tail query")
		}
		return strings.Join([]string{
			`{"activity_id":"parse:2","component":"worker-parse","activity_kind":"parse","correlation_id":"trace:demo","source_id":"seed:gdelt","status":"success","message":"extracted_rows=2","occurred_at":"2026-03-10T12:00:00.000Z","detail":"{}"}`,
			`{"activity_id":"fetch:1","component":"worker-fetch","activity_kind":"fetch","correlation_id":"trace:demo","source_id":"seed:gdelt","status":"success","message":"status_code=200","occurred_at":"2026-03-10T11:59:59.000Z","detail":"{}"}`,
		}, "\n") + "\n", nil
	}}})
	mux := newAPIMuxWithServer("v1", "", server)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/internal/worker-tail?limit=1&correlation_id=trace:demo", nil)
	req.Header.Set(apiKeyHeader, testAPIKey)
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d body=%s", rr.Code, rr.Body.String())
	}
	if len(queries) != 1 || !strings.Contains(queries[0], "correlation_id = 'trace:demo'") || !strings.Contains(queries[0], "LIMIT 2") {
		t.Fatalf("unexpected tail query %v", queries)
	}
	var payload map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	data := payload["data"].(map[string]any)
	items := data["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected one item, got %d", len(items))
	}
	if data["next_cursor"] == "" {
		t.Fatal("expected next_cursor for overflow page")
	}
}

func TestDecodeWorkerTailCursorRejectsInvalid(t *testing.T) {
	if _, err := decodeWorkerTailCursor("%%%"); err == nil {
		t.Fatal("expected invalid cursor error")
	}
	encoded := base64.RawURLEncoding.EncodeToString([]byte("broken"))
	if _, err := decodeWorkerTailCursor(encoded); err == nil {
		t.Fatal("expected invalid split cursor error")
	}
}
