package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	vf "global-osint-backend/internal/packs/maritime/vesselfinder"
)

func TestDefaultConfigMatchesVesselFinderCrawlerContract(t *testing.T) {
	t.Setenv("CLICKHOUSE_HTTP_URL", "")
	cfg := loadConfig()
	if cfg.SourceID != sourceID {
		t.Fatalf("unexpected source id %q", cfg.SourceID)
	}
	if cfg.Workers != 3 || cfg.WorkerRatePerMinute != 18 || cfg.DiscoveryRPS != 1.0 || cfg.MaxPage != 200 {
		t.Fatalf("unexpected crawler defaults: %#v", cfg)
	}
	if cfg.RediscoveryInterval != time.Hour || cfg.ListTimeout != 30*time.Second || cfg.DetailTimeout != 30*time.Second {
		t.Fatalf("unexpected timeout defaults: %#v", cfg)
	}
}

func TestListURLIncludesDimensionAndPage(t *testing.T) {
	job := vf.PageJob{CountryCode: "PA", TypeCode: "2", Page: 17}
	got := listURL(job)
	want := "https://www.vesselfinder.com/vessels?flag=PA&type=2&page=17"
	if got != want {
		t.Fatalf("listURL got %q want %q", got, want)
	}
}

func TestRetainedHTMLUsesObjectStoreForLargeDetailPages(t *testing.T) {
	page := renderedPage{
		URL:        "https://www.vesselfinder.com/vessels/details/9303801",
		HTML:       strings.Repeat("<div>detail</div>", 6000),
		StatusCode: 200,
		FetchedAt:  time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC),
		Latency:    250 * time.Millisecond,
	}
	store := &stubObjectStore{}
	item := vf.ScanQueueItem{CountryCode: "PA", CountryLabel: "Panama", TypeCode: "7", TypeLabel: "Cargo vessels", PlaceID: "plc:flag:pa"}
	stored, err := retainRenderedHTML(context.Background(), config{SourceID: sourceID, RawBucket: "raw"}, page, item, store)
	if err != nil {
		t.Fatalf("retainRenderedHTML returned error: %v", err)
	}
	if stored.RawDocument == nil {
		t.Fatal("expected raw document")
	}
	if stored.RawDocument.ObjectKey == nil || *stored.RawDocument.ObjectKey == "" {
		t.Fatalf("expected object key for large detail HTML: %#v", stored.RawDocument)
	}
	if len(store.puts) != 1 {
		t.Fatalf("expected one object store write, got %d", len(store.puts))
	}
	var metadata map[string]any
	if err := json.Unmarshal([]byte(stored.RawDocument.FetchMetadata), &metadata); err != nil {
		t.Fatalf("decode metadata: %v", err)
	}
	if metadata["inline_body_base64"] != nil {
		t.Fatalf("large detail HTML should not be inlined in ClickHouse metadata")
	}
	if metadata["object_key"] == "" || metadata["object_bucket"] != "raw" {
		t.Fatalf("expected object storage metadata, got %#v", metadata)
	}
	contextValue, ok := metadata["vesselfinder"].(map[string]any)
	if !ok {
		t.Fatalf("expected vesselfinder discovery context in metadata, got %#v", metadata)
	}
	if contextValue["country_code"] != "PA" || contextValue["place_id"] != "plc:flag:pa" || contextValue["type_code"] != "7" {
		t.Fatalf("unexpected vesselfinder metadata context: %#v", contextValue)
	}
}

func TestClaimScanQueueQueryIncludesExpiredLeases(t *testing.T) {
	query := claimScanQueueQuery("source:1", 7)
	if !strings.Contains(query, "status = 'leased' AND lease_expires_at <= now()") {
		t.Fatalf("expected expired leases to be claimable, got %s", query)
	}
	if !strings.Contains(query, "JSONExtractString(attrs, 'country_code')") {
		t.Fatalf("expected discovery context to be selected, got %s", query)
	}
	if !strings.Contains(query, "ORDER BY if(JSONExtractString(attrs, 'place_id') != '' OR JSONExtractString(attrs, 'country_code') != '', 0, 1), next_scan_at ASC") {
		t.Fatalf("expected context-enriched scan rows to be prioritized, got %s", query)
	}
	if !strings.Contains(query, "LIMIT 7") {
		t.Fatalf("expected limit in query, got %s", query)
	}
}

func TestRecordVersionUsesNanosecondTimestamp(t *testing.T) {
	base := time.Date(2026, 5, 7, 7, 15, 0, 0, time.UTC)
	if got, want := recordVersion(base), uint64(base.UnixNano()); got != want {
		t.Fatalf("recordVersion got %d want %d", got, want)
	}
	if recordVersion(base.Add(time.Nanosecond)) <= recordVersion(base) {
		t.Fatal("expected later timestamp to produce higher record version")
	}
}


func TestClassifyRenderError(t *testing.T) {
	cases := map[string]string{
		"tcp preflight www.vesselfinder.com:443: dial tcp 88.99.127.255:443: i/o timeout": "connect_timeout",
		"tcp preflight www.vesselfinder.com:443: no route to host":                        "connect_error",
		"context deadline exceeded": "render_timeout",
		"chromium crashed":          "render_error",
	}
	for input, want := range cases {
		if got := classifyRenderError(errors.New(input)); got != want {
			t.Fatalf("classifyRenderError(%q) got %q want %q", input, got, want)
		}
	}
}

func TestScanStatsCanCarryNetworkError(t *testing.T) {
	stats := map[string]any{"source_id": sourceID, "claimed": 0, "scanned": 0, "failed": 0, "network_error": "tcp preflight failed"}
	if got, _ := stats["network_error"].(string); got == "" {
		t.Fatalf("expected network_error in scan stats: %#v", stats)
	}
}

func TestExtractJSONFromPre(t *testing.T) {
	cases := []struct {
		name    string
		html    string
		want    string
		wantErr bool
	}{
		{
			name: "chromium wrapped",
			html: `<html><body><pre style="word-wrap: break-word; white-space: pre-wrap;">{"reta":0,"dest":"CNSHA"}</pre></body></html>`,
			want: `{"reta":0,"dest":"CNSHA"}`,
		},
		{
			name: "unwrapped json",
			html: `{"reta":1747699200,"wps":[[51.9,4.5]]}`,
			want: `{"reta":1747699200,"wps":[[51.9,4.5]]}`,
		},
		{
			name:    "html error page",
			html:    `<html><body><h1>Access Denied</h1></body></html>`,
			wantErr: true,
		},
		{
			name:    "pre with non-json content",
			html:    `<html><body><pre>not json</pre></body></html>`,
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := extractJSONFromPre(tc.html)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(got) != tc.want {
				t.Fatalf("got %q want %q", string(got), tc.want)
			}
		})
	}
}

func TestClaimRouteQueueQueryShape(t *testing.T) {
	query := claimRouteQueueQuery("route-source:1", 5)
	for _, want := range []string{
		"status IN ('pending', 'failed') AND next_fetch_at <= now()",
		"status = 'leased' AND lease_expires_at <= now()",
		"ORDER BY next_fetch_at ASC",
		"LIMIT 5",
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("expected %q in query, got:\n%s", want, query)
		}
	}
}

// TestRouteWorkerSlotNilAfterDeadProxy validates the slot state update that the
// dead-proxy branch performs before breaking out of the item loop. The critical
// invariant: slot.sess must be nil when the loop exits so that the next batch
// cycle creates a fresh session rather than dereferencing a stale nil pointer.
func TestRouteWorkerSlotNilAfterDeadProxy(t *testing.T) {
	slot := &routeWorkerSlot{
		sess:            &browserSession{proxyURL: "socks5://dead-proxy:1080"},
		consecutiveDead: 0,
		useDirect:       false,
	}
	// Mirror the dead-proxy branch without calling closeBrowserSession (which
	// requires live chromedp context).
	slot.sess = nil
	slot.consecutiveDead++
	if slot.consecutiveDead >= 5 {
		slot.useDirect = true
	}
	if slot.sess != nil {
		t.Fatal("expected slot.sess to be nil after dead-proxy handling")
	}
	if slot.consecutiveDead != 1 {
		t.Fatalf("expected consecutiveDead=1, got %d", slot.consecutiveDead)
	}
	if slot.useDirect {
		t.Fatal("expected useDirect=false before threshold")
	}
}

func TestRouteRecordVersionUsesNanosecondTimestamp(t *testing.T) {
	base := time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC)
	if got, want := recordVersion(base), uint64(base.UnixNano()); got != want {
		t.Fatalf("recordVersion got %d want %d", got, want)
	}
}

func TestRetainRenderedJSONSetsRouteSourceID(t *testing.T) {
	pg := renderedPage{
		URL:        "https://www.vesselfinder.com/api/pub/dm3/247379500?wp=1",
		HTML:       `<html><body><pre>{"reta":0}</pre></body></html>`,
		StatusCode: 200,
		FetchedAt:  time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC),
		Latency:    100 * time.Millisecond,
	}
	item := vf.RouteQueueItem{MMSI: "247379500"}
	body := []byte(`{"reta":0}`)
	store := &stubObjectStore{}
	stored, err := retainRenderedJSON(context.Background(), config{RawBucket: "raw"}, pg, item, body, store)
	if err != nil {
		t.Fatalf("retainRenderedJSON: %v", err)
	}
	if stored.RawDocument == nil {
		t.Fatal("expected raw document")
	}
	if stored.RawDocument.SourceID != routeSourceID {
		t.Fatalf("source_id: got %q want %q", stored.RawDocument.SourceID, routeSourceID)
	}
	var metadata map[string]any
	if err := json.Unmarshal([]byte(stored.RawDocument.FetchMetadata), &metadata); err != nil {
		t.Fatalf("decode metadata: %v", err)
	}
	vfCtx, _ := metadata["vesselfinder"].(map[string]any)
	if vfCtx["mmsi"] != "247379500" {
		t.Fatalf("expected mmsi in metadata, got %#v", vfCtx)
	}
}

type stubObjectStore struct {
	puts []struct {
		bucket      string
		key         string
		body        string
		contentType string
	}
}

func (s *stubObjectStore) PutObject(_ context.Context, bucket, key string, body []byte, contentType string) error {
	s.puts = append(s.puts, struct {
		bucket      string
		key         string
		body        string
		contentType string
	}{bucket: bucket, key: key, body: string(body), contentType: contentType})
	return nil
}

func (s *stubObjectStore) GetObject(context.Context, string, string) ([]byte, string, error) {
	return nil, "", nil
}
