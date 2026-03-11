package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type statsStubQuerier struct {
	queryFn func(query string) (string, error)
}

func (s statsStubQuerier) Query(_ context.Context, query string) (string, error) {
	if s.queryFn == nil {
		return "", errors.New("missing query stub")
	}
	return s.queryFn(query)
}

func TestInternalStatsContract(t *testing.T) {
	server := &apiServer{version: "v1", queryTimeout: time.Second, clickhouse: statsStubQuerier{queryFn: stubStatsQueries}}
	mux := newAPIMuxWithServer("v1", "", server)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/internal/stats", nil)
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d body=%s", rr.Code, rr.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	data := payload["data"].(map[string]any)
	for _, key := range []string{"summary", "storage", "quality", "outputs", "generated_at", "warnings"} {
		if _, ok := data[key]; !ok {
			t.Fatalf("missing key %q in data: %#v", key, data)
		}
	}
	quality := data["quality"].(map[string]any)
	parserSuccess := quality["parser_success"].(map[string]any)
	if parserSuccess["window_minutes"].(float64) != 15 {
		t.Fatalf("expected parser success window 15, got %#v", parserSuccess)
	}
}

func TestInternalStatsRejectsUnsupportedParams(t *testing.T) {
	server := &apiServer{version: "v1", queryTimeout: time.Second, clickhouse: statsStubQuerier{queryFn: stubStatsQueries}}
	mux := newAPIMuxWithServer("v1", "", server)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/internal/stats?bad=1", nil)
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestInternalStatsQueryFailure(t *testing.T) {
	server := &apiServer{version: "v1", queryTimeout: time.Second, clickhouse: statsStubQuerier{queryFn: func(_ string) (string, error) { return "", errors.New("boom") }}}
	mux := newAPIMuxWithServer("v1", "", server)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/internal/stats", nil)
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 got %d body=%s", rr.Code, rr.Body.String())
	}
}

func stubStatsQueries(query string) (string, error) {
	switch {
	case strings.Contains(query, "sources_total"):
		return `{"sources_total":7,"sources_enabled":6,"sources_disabled":1}` + "\n", nil
	case strings.Contains(query, "FROM ops.job_run"):
		return `{"jobs_running":1}` + "\n", nil
	case strings.Contains(query, "FROM ops.crawl_frontier"):
		return `{"frontier_pending":3,"frontier_retry":1}` + "\n", nil
	case strings.Contains(query, "FROM ops.unresolved_location_queue"):
		return `{"unresolved_open":2}` + "\n", nil
	case strings.Contains(query, "FROM ops.quality_incident"):
		return `{"quality_open":1}` + "\n", nil
	case strings.Contains(query, "FROM system.parts"):
		return `{"table_name":"bronze.raw_document","rows":120}` + "\n", nil
	case strings.Contains(query, "maxOrNull(r.fetched_at)"):
		return `{"source_id":"seed:gdelt","last_fetched_at":"2026-03-10T11:55:00Z"}` + "\n", nil
	case strings.Contains(query, "total_runs"):
		return `{"total_runs":4,"success_runs":3}` + "\n", nil
	case strings.Contains(query, "GROUP BY error_class"):
		return `{"error_class":"schema_drift","count":1,"example_source":"seed:gdelt"}` + "\n", nil
	case strings.Contains(query, "FROM ops.fetch_log"):
		return `{"success_count":10,"failed_count":2}` + "\n", nil
	case strings.Contains(query, "FROM meta.metric_registry"):
		return `{"metrics_total":5}` + "\n", nil
	case strings.Contains(query, "latest_snapshot_at"):
		return `{"latest_snapshot_at":"2026-03-10T11:50:00Z"}` + "\n", nil
	case strings.Contains(query, "FROM gold.hotspot_snapshot"):
		return `{"hotspots_total":3}` + "\n", nil
	case strings.Contains(query, "FROM gold.cross_domain_snapshot"):
		return `{"cross_domain_total":2}` + "\n", nil
	default:
		return "", errors.New("unexpected query")
	}
}
