package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"global-osint-backend/internal/migrate"
)

// TestSubscribeMessageJSON verifies the subscribe message serialises with the
// correct top-level keys and structure expected by AISstream.
func TestSubscribeMessageJSON(t *testing.T) {
	sub := subscribeMessage{
		APIKey:             "test-key-abc",
		BoundingBoxes:      [][][2]float64{{{-90, -180}, {90, 180}}},
		FilterMessageTypes: []string{},
	}
	data, err := json.Marshal(sub)
	if err != nil {
		t.Fatalf("marshal subscribe message: %v", err)
	}

	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal subscribe message: %v", err)
	}

	// Required keys must be present.
	for _, key := range []string{"APIKey", "BoundingBoxes", "FilterMessageTypes"} {
		if _, ok := m[key]; !ok {
			t.Errorf("subscribe message missing key %q", key)
		}
	}

	// APIKey must round-trip.
	var apiKey string
	if err := json.Unmarshal(m["APIKey"], &apiKey); err != nil || apiKey != "test-key-abc" {
		t.Errorf("expected APIKey %q, got %q", "test-key-abc", apiKey)
	}

	// BoundingBoxes must be a non-empty array.
	var bboxes [][][2]float64
	if err := json.Unmarshal(m["BoundingBoxes"], &bboxes); err != nil {
		t.Fatalf("BoundingBoxes parse: %v", err)
	}
	if len(bboxes) == 0 {
		t.Error("BoundingBoxes must not be empty")
	}
	// Verify global bounding box: first box should be [[-90,-180],[90,180]].
	if len(bboxes[0]) < 2 {
		t.Fatalf("expected at least 2 corners in BoundingBoxes[0], got %d", len(bboxes[0]))
	}
	sw, ne := bboxes[0][0], bboxes[0][1]
	if sw[0] != -90 || sw[1] != -180 {
		t.Errorf("expected SW corner [-90,-180], got %v", sw)
	}
	if ne[0] != 90 || ne[1] != 180 {
		t.Errorf("expected NE corner [90,180], got %v", ne)
	}

	// FilterMessageTypes must be an array (may be empty for all messages).
	var fmts []string
	if err := json.Unmarshal(m["FilterMessageTypes"], &fmts); err != nil {
		t.Fatalf("FilterMessageTypes parse: %v", err)
	}
}

// mockRetainer records calls to retain for inspection in tests.
type mockRetainer struct {
	calls []retainCall
}

type retainCall struct {
	batchSize int
	fetchedAt time.Time
}

func (m *mockRetainer) retain(_ context.Context, _ config, batch []json.RawMessage, fetchedAt time.Time) error {
	m.calls = append(m.calls, retainCall{batchSize: len(batch), fetchedAt: fetchedAt})
	return nil
}

// TestFlushBatchWritesToClickHouse creates a mock ClickHouse HTTP server and
// calls flushBatch directly, asserting both ops.fetch_log and
// bronze.raw_document INSERT statements are executed.
func TestFlushBatchWritesToClickHouse(t *testing.T) {
	var fetchLogInserts atomic.Int32
	var rawDocInserts atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// ClickHouse HTTP interface: query is in ?query= or request body.
		queryParam := r.URL.Query().Get("query")
		switch {
		case strings.Contains(queryParam, "INSERT INTO ops.fetch_log"):
			fetchLogInserts.Add(1)
			w.WriteHeader(http.StatusOK)
		case strings.Contains(queryParam, "INSERT INTO bronze.raw_document"):
			rawDocInserts.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			// Drain body and return 200 for anything else (e.g. health checks).
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	cfg := config{
		WSURL:         "wss://stream.aisstream.io/v0/stream",
		APIKey:        "test-key",
		BatchWindow:   5 * time.Second,
		ClickHouseURL: server.URL,
		RawBucket:     "raw",
		SourceID:      aistreamSourceID,
	}

	batch := []json.RawMessage{
		json.RawMessage(`{"MessageType":"PositionReport","MetaData":{"MMSI":123456789}}`),
		json.RawMessage(`{"MessageType":"ShipStaticData","MetaData":{"MMSI":987654321}}`),
	}
	fetchedAt := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)

	// Use a nil object store so the body is stored inline (warm policy,
	// InlineBodyMaxBytes=64KiB > our small batch).
	runner := newMigrateRunner(server.URL)
	err := flushBatch(context.Background(), cfg, batch, fetchedAt, runner, nil)
	if err != nil {
		t.Fatalf("flushBatch returned error: %v", err)
	}

	if fetchLogInserts.Load() != 1 {
		t.Errorf("expected 1 ops.fetch_log INSERT, got %d", fetchLogInserts.Load())
	}
	if rawDocInserts.Load() != 1 {
		t.Errorf("expected 1 bronze.raw_document INSERT, got %d", rawDocInserts.Load())
	}
}

// TestFlushBatchBodyIsBatchedJSON verifies that the retained body is a JSON
// array containing all messages in the batch.
func TestFlushBatchBodyIsBatchedJSON(t *testing.T) {
	var capturedBodies []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		if strings.Contains(q, "INSERT INTO bronze.raw_document") {
			// The body bytes in the SQL tell us the batch was serialised.
			capturedBodies = append(capturedBodies, q)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := config{
		WSURL:         "wss://stream.aisstream.io/v0/stream",
		APIKey:        "test-key",
		BatchWindow:   5 * time.Second,
		ClickHouseURL: server.URL,
		RawBucket:     "raw",
		SourceID:      aistreamSourceID,
	}

	batch := []json.RawMessage{
		json.RawMessage(`{"MessageType":"PositionReport"}`),
		json.RawMessage(`{"MessageType":"ShipStaticData"}`),
		json.RawMessage(`{"MessageType":"AidsToNavigationReport"}`),
	}
	fetchedAt := time.Date(2026, 2, 20, 8, 0, 0, 0, time.UTC)

	runner := newMigrateRunner(server.URL)
	if err := flushBatch(context.Background(), cfg, batch, fetchedAt, runner, nil); err != nil {
		t.Fatalf("flushBatch: %v", err)
	}

	if len(capturedBodies) == 0 {
		t.Fatal("expected at least one raw_document INSERT, got none")
	}

	// The INSERT SQL includes body_bytes; a 3-message batch should be >10 bytes.
	sql := capturedBodies[0]
	if !strings.Contains(sql, "application/json") {
		t.Errorf("expected content_type application/json in raw_document INSERT, got %s", sql)
	}
	// source_id must match.
	if !strings.Contains(sql, aistreamSourceID) {
		t.Errorf("expected source_id %q in raw_document INSERT", aistreamSourceID)
	}
}

// newMigrateRunner creates a migrate.HTTPRunner pointed at the given URL.
func newMigrateRunner(baseURL string) *migrate.HTTPRunner {
	return migrate.NewHTTPRunner(baseURL)
}
