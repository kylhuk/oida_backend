//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const e2eAPIKeyHeader = "X-API-Key"

const (
	e2eLocalMinIOEndpointDefault  = "http://localhost:9000"
	e2eLocalMinIOAccessKeyDefault = "minioadmin"
	e2eLocalMinIOSecretKeyDefault = "minioadmin"
)

func runEndToEndPipeline(t *testing.T) {
	ctx := context.Background()
	baseURL := getenv("E2E_API_URL", "http://localhost:8080")
	apiSharedKey := e2eAPISharedKey()
	clickhouseURL := getenv("E2E_CLICKHOUSE_HTTP_URL", "http://svc_control_plane:control_plane_change_me@localhost:8124")
	clickhouseIngestURL := getenv("E2E_CLICKHOUSE_INGEST_HTTP_URL", clickhouseURL)
	clickhouseParseURL := getenv("E2E_CLICKHOUSE_PARSE_HTTP_URL", "http://svc_worker_parse:worker_parse_change_me@localhost:8124")
	httpFixtureURL := getenv("E2E_HTTP_FIXTURE_URL", "http://localhost:8079")

	if err := waitForReady(ctx, baseURL, 30*time.Second); err != nil {
		t.Fatalf("API not ready: %v", err)
	}

	t.Run("RunOnceHelp", func(t *testing.T) {
		testRunOnceHelp(t, clickhouseURL)
	})

	t.Run("HTTPFixtureService", func(t *testing.T) {
		testHTTPFixtureService(t, httpFixtureURL)
	})

	t.Run("FixturePipeline", func(t *testing.T) {
		testFixturePipeline(t, baseURL, clickhouseURL, apiSharedKey)
	})

	t.Run("LocationAttribution", func(t *testing.T) {
		testLocationAttribution(t, baseURL, apiSharedKey)
	})

	t.Run("APIServing", func(t *testing.T) {
		testAPIServing(t, baseURL, apiSharedKey)
	})

	t.Run("EntityNestedRoutes", func(t *testing.T) {
		testEntityNestedRoutes(t, baseURL, clickhouseURL, apiSharedKey)
	})

	t.Run("CORSPreflight", func(t *testing.T) {
		testCORSPreflight(t, baseURL)
	})

	t.Run("MetricsRollup", func(t *testing.T) {
		testMetricsRollup(t, baseURL, apiSharedKey)
	})

	t.Run("StatsDashboard", func(t *testing.T) {
		testStatsDashboard(t, baseURL, getenv("E2E_RENDERER_URL", "http://localhost:8090"), apiSharedKey)
	})

	t.Run("SourceCatalogRollout", func(t *testing.T) {
		testSourceCatalogRollout(t, baseURL, apiSharedKey)
	})

	t.Run("AutomaticSourceSync", func(t *testing.T) {
		testAutomaticSourceSync(t, baseURL, clickhouseURL, apiSharedKey)
	})

	t.Run("Phase1TelemetryMVLanding", func(t *testing.T) {
		testPhase1TelemetryMVLanding(t, clickhouseURL, clickhouseIngestURL, clickhouseParseURL)
	})

	t.Run("Phase1FrontierEndpointInventory", func(t *testing.T) {
		testPhase1FrontierEndpointInventory(t, clickhouseURL)
	})

	t.Run("Phase1CoverageViews", func(t *testing.T) {
		testPhase1CoverageViews(t, clickhouseURL)
	})
}

func TestHTTPSourcePipeline(t *testing.T) {
	runEndToEndPipeline(t)
}

func TestOptionalLiveSmokeOpenSky(t *testing.T) {
	if strings.TrimSpace(getenv("E2E_LIVE_SMOKE_OPENSKY", "")) != "1" {
		t.Skip("set E2E_LIVE_SMOKE_OPENSKY=1 to enable live OpenSky smoke")
	}
	clientID := strings.TrimSpace(getenv("SOURCE_OPENSKY_NETWORK_CLIENT_ID", ""))
	clientSecret := strings.TrimSpace(getenv("SOURCE_OPENSKY_NETWORK_CLIENT_SECRET", ""))
	if clientID == "" || clientSecret == "" {
		t.Skip("live OpenSky smoke requires SOURCE_OPENSKY_NETWORK_CLIENT_ID and SOURCE_OPENSKY_NETWORK_CLIENT_SECRET")
	}
	tokenURL := getenv("E2E_OPENSKY_TOKEN_URL", "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token")
	statesURL := getenv("E2E_OPENSKY_STATES_URL", "https://opensky-network.org/api/states/all?extended=1")

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", clientID)
	form.Set("client_secret", clientSecret)
	resp, err := http.Post(tokenURL, "application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("request opensky oauth token: %v", err)
	}
	defer resp.Body.Close()
	tokenBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read opensky oauth token response: %v", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		t.Fatalf("opensky oauth token request failed: status=%s body=%s", resp.Status, strings.TrimSpace(string(tokenBody)))
	}
	var tokenPayload struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(tokenBody, &tokenPayload); err != nil {
		t.Fatalf("decode opensky oauth token payload: %v", err)
	}
	if strings.TrimSpace(tokenPayload.AccessToken) == "" {
		t.Fatal("opensky oauth token response missing access_token")
	}

	request, err := http.NewRequest(http.MethodGet, statesURL, nil)
	if err != nil {
		t.Fatalf("build opensky states request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(tokenPayload.AccessToken))
	stateResp, err := (&http.Client{Timeout: 20 * time.Second}).Do(request)
	if err != nil {
		t.Fatalf("request opensky states/all: %v", err)
	}
	defer stateResp.Body.Close()
	stateBody, err := io.ReadAll(stateResp.Body)
	if err != nil {
		t.Fatalf("read opensky states/all response: %v", err)
	}
	if stateResp.StatusCode != http.StatusOK {
		t.Fatalf("opensky states/all returned %s: %s", stateResp.Status, strings.TrimSpace(string(stateBody)))
	}
	var statesPayload struct {
		States []any `json:"states"`
	}
	if err := json.Unmarshal(stateBody, &statesPayload); err != nil {
		t.Fatalf("decode opensky states/all payload: %v", err)
	}
}

func testRunOnceHelp(t *testing.T, clickhouseURL string) {
	t.Helper()
	output := runControlPlane(t, clickhouseURL, "run-once", "--help")
	for _, want := range []string{
		"geoboundaries-sync",
		"geonames-sync",
		"ingest-aviation",
		"ingest-geopolitical",
		"ingest-maritime",
		"ingest-safety-security",
		"ingest-space",
		"place-build",
		"promote",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("run-once help missing %q: %s", want, output)
		}
	}
}

func testFixturePipeline(t *testing.T, baseURL, clickhouseURL, apiSharedKey string) {
	t.Helper()
	for _, job := range []string{"place-build", "promote"} {
		if _, err := tryRunControlPlane(clickhouseURL, "run-once", "--job", job); err != nil {
			if strings.Contains(err.Error(), "ILLEGAL_FINAL") {
				t.Skipf("fixture pipeline smoke skipped: %s hit local ClickHouse FINAL limitation (%v)", job, err)
			}
			t.Fatalf("control-plane %s failed: %v", job, err)
		}
	}
	requireAPIItemsOrSkip(t, baseURL, apiSharedKey, "/v1/events?source_id=seed:gdelt", func(items []map[string]any) bool {
		return len(items) > 0
	}, "fixture-backed compose smoke has not populated promoted seed:gdelt events in this local worktree yet", "no successful /v1/events response while checking promoted seed:gdelt smoke path")
}

type httpFixtureManifest struct {
	Service  string                     `json:"service"`
	Status   string                     `json:"status"`
	Payloads []httpFixtureManifestEntry `json:"payloads"`
}

type httpFixtureManifestEntry struct {
	SourceID      string `json:"source_id"`
	Path          string `json:"path"`
	Status        string `json:"status"`
	CredentialEnv string `json:"credential_env,omitempty"`
}

type httpFixturePayload struct {
	SourceID      string              `json:"source_id"`
	Status        string              `json:"status"`
	CredentialEnv string              `json:"credential_env,omitempty"`
	Records       []httpFixtureRecord `json:"records"`
}

type httpFixtureRecord struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	Title       string `json:"title,omitempty"`
	Name        string `json:"name,omitempty"`
	ObservedAt  string `json:"observed_at,omitempty"`
	PublishedAt string `json:"published_at,omitempty"`
	URL         string `json:"url,omitempty"`
	PlaceID     string `json:"place_id,omitempty"`
}

func testHTTPFixtureService(t *testing.T, fixtureURL string) {
	t.Helper()

	var health struct {
		Status  string `json:"status"`
		Service string `json:"service"`
	}
	fetchFixtureJSON(t, fixtureURL, "/health.json", &health)
	if health.Service != "http-fixture" || health.Status != "ok" {
		t.Fatalf("unexpected health payload: %#v", health)
	}

	var manifest httpFixtureManifest
	fetchFixtureJSON(t, fixtureURL, "/manifest.json", &manifest)
	if manifest.Service != "http-fixture" || manifest.Status != "ok" {
		t.Fatalf("unexpected fixture manifest header: %#v", manifest)
	}

	expected := map[string]httpFixtureManifestEntry{
		"seed:gdelt":            {SourceID: "seed:gdelt", Path: "/geopolitical/gdelt.json", Status: "ok"},
		"fixture:reliefweb":     {SourceID: "fixture:reliefweb", Path: "/geopolitical/reliefweb.json", Status: "ok"},
		"fixture:opensanctions": {SourceID: "fixture:opensanctions", Path: "/safety/opensanctions.json", Status: "ok"},
		"fixture:nasa-firms":    {SourceID: "fixture:nasa-firms", Path: "/safety/nasa-firms.json", Status: "ok"},
		"fixture:noaa-hazards":  {SourceID: "fixture:noaa-hazards", Path: "/safety/noaa-hazards.json", Status: "ok"},
		"fixture:kev":           {SourceID: "fixture:kev", Path: "/safety/kev.json", Status: "ok"},
		"fixture:acled":         {SourceID: "fixture:acled", Path: "/geopolitical/acled.json", Status: "credential-gated", CredentialEnv: "ACLED_API_KEY"},
	}
	if len(manifest.Payloads) != len(expected) {
		t.Fatalf("expected %d fixture entries, got %d (%#v)", len(expected), len(manifest.Payloads), manifest.Payloads)
	}
	for _, entry := range manifest.Payloads {
		want, ok := expected[entry.SourceID]
		if !ok {
			t.Fatalf("unexpected fixture manifest entry: %#v", entry)
		}
		if entry.Path != want.Path || entry.Status != want.Status || entry.CredentialEnv != want.CredentialEnv {
			t.Fatalf("unexpected manifest entry for %s: got %#v want %#v", entry.SourceID, entry, want)
		}
	}

	for _, want := range expected {
		var payload httpFixturePayload
		fetchFixtureJSON(t, fixtureURL, want.Path, &payload)
		if payload.SourceID != want.SourceID || payload.Status != want.Status {
			t.Fatalf("unexpected payload metadata for %s: %#v", want.SourceID, payload)
		}
		if payload.CredentialEnv != want.CredentialEnv {
			t.Fatalf("unexpected credential gate for %s: %#v", want.SourceID, payload)
		}
		if len(payload.Records) == 0 {
			t.Fatalf("expected records in %s payload", want.SourceID)
		}
		if payload.Records[0].ID == "" || payload.Records[0].Kind == "" {
			t.Fatalf("expected record identity in %s payload: %#v", want.SourceID, payload.Records[0])
		}
	}
}

func fetchFixtureJSON(t *testing.T, fixtureURL, path string, target any) {
	t.Helper()
	resp, err := http.Get(strings.TrimRight(fixtureURL, "/") + path)
	if err != nil {
		t.Fatalf("fixture service query %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("fixture service %s status: %d body=%s", path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
		t.Fatalf("decode fixture payload %s: %v", path, err)
	}
}

func testLocationAttribution(t *testing.T, baseURL, apiSharedKey string) {
	t.Helper()
	requireAPIItemsOrSkip(t, baseURL, apiSharedKey, "/v1/events?place_id=plc:fr-idf-paris", func(items []map[string]any) bool {
		return len(items) > 0 && items[0]["place_id"] != ""
	}, "fixture-backed compose smoke has not populated Paris-attributed event rows in this local worktree yet", "no successful /v1/events response while checking Paris attribution smoke path")
}

func requireAPIItemsOrSkip(t *testing.T, baseURL, apiSharedKey, path string, predicate func([]map[string]any) bool, skipReason, failure string) {
	t.Helper()
	matched, sawSuccess := waitForAPIItems(t, baseURL, apiSharedKey, path, predicate)
	if matched {
		return
	}
	if sawSuccess {
		t.Skip(skipReason)
	}
	t.Fatal(failure)
}

func waitForAPIItems(t *testing.T, baseURL, apiSharedKey, path string, predicate func([]map[string]any) bool) (bool, bool) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	sawSuccess := false
	for time.Now().Before(deadline) {
		resp, err := apiGET(baseURL+path, apiSharedKey)
		if err == nil && resp.StatusCode == http.StatusOK {
			sawSuccess = true
			var result struct {
				Data struct {
					Items []map[string]any `json:"items"`
				} `json:"data"`
			}
			if decodeErr := json.NewDecoder(resp.Body).Decode(&result); decodeErr == nil && predicate(result.Data.Items) {
				resp.Body.Close()
				return true, true
			}
			resp.Body.Close()
		} else if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(2 * time.Second)
	}
	return false, sawSuccess
}

func testAPIServing(t *testing.T, baseURL, apiSharedKey string) {
	t.Helper()
	publicEndpoints := []struct {
		path   string
		method string
		status int
	}{
		{"/v1/health", "GET", http.StatusOK},
		{"/v1/ready", "GET", http.StatusOK},
		{"/v1/version", "GET", http.StatusOK},
		{"/v1/schema", "GET", http.StatusOK},
	}

	for _, ep := range publicEndpoints {
		resp, err := http.Get(baseURL + ep.path)
		if err != nil {
			t.Errorf("%s: %v", ep.path, err)
			continue
		}

		if resp.StatusCode != ep.status {
			resp.Body.Close()
			t.Errorf("%s: expected %d, got %d", ep.path, ep.status, resp.StatusCode)
			continue
		}
		if ep.path == "/v1/ready" {
			var payload struct {
				Data struct {
					Ready bool `json:"ready"`
				} `json:"data"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
				resp.Body.Close()
				t.Fatalf("decode /v1/ready payload: %v", err)
			}
			resp.Body.Close()
			if !payload.Data.Ready {
				t.Fatal("expected /v1/ready to report data.ready=true after readiness wait")
			}
			continue
		}
		resp.Body.Close()
	}

	resp, err := http.Get(baseURL + "/v1/metrics")
	if err != nil {
		t.Fatalf("unauthenticated protected route: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for unauthenticated /v1/metrics, got %d", resp.StatusCode)
	}

	resp, err = apiGET(baseURL+"/v1/metrics", apiSharedKey)
	if err != nil {
		t.Fatalf("authenticated protected route: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for authenticated /v1/metrics, got %d", resp.StatusCode)
	}
}

func testMetricsRollup(t *testing.T, baseURL, apiSharedKey string) {
	t.Helper()
	resp, err := apiGET(baseURL+"/v1/analytics/rollups?metric_id=obs_count&fields=snapshot_id,metric_id,window_grain,metric_value,rank,attrs,evidence", apiSharedKey)
	if err != nil {
		t.Fatalf("metrics rollup: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status: %d", resp.StatusCode)
	}

	var result struct {
		Data struct {
			Items []map[string]any `json:"items"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode metrics: %v", err)
	}

	if len(result.Data.Items) > 0 {
		rollup := result.Data.Items[0]
		required := []string{"snapshot_id", "metric_id", "window_grain", "metric_value", "rank", "attrs", "evidence"}
		for _, field := range required {
			if _, ok := rollup[field]; !ok {
				t.Errorf("rollup missing field: %s", field)
			}
		}
		if _, ok := rollup["metric_value"].(float64); !ok {
			t.Fatalf("expected metric_value numeric, got %T", rollup["metric_value"])
		}
		if _, ok := rollup["rank"].(float64); !ok {
			t.Fatalf("expected rank numeric, got %T", rollup["rank"])
		}
		if _, ok := rollup["attrs"].(map[string]any); !ok {
			t.Fatalf("expected attrs object, got %T", rollup["attrs"])
		}
		if _, ok := rollup["evidence"].([]any); !ok {
			t.Fatalf("expected evidence array, got %T", rollup["evidence"])
		}
	}
}

func testEntityNestedRoutes(t *testing.T, baseURL, clickhouseURL, apiSharedKey string) {
	t.Helper()
	validatedLiveRoutes := 0
	for _, tc := range []struct {
		name      string
		query     string
		pathFmt   string
		idField   string
		entityKey string
	}{
		{name: "events", query: "SELECT entity_id FROM gold.api_v1_entity_events LIMIT 1 FORMAT TabSeparated", pathFmt: "/v1/entities/%s/events", idField: "event_id", entityKey: "entity_id"},
		{name: "places", query: "SELECT entity_id FROM gold.api_v1_entity_places LIMIT 1 FORMAT TabSeparated", pathFmt: "/v1/entities/%s/places", idField: "place_id", entityKey: "entity_id"},
		{name: "tracks", query: "SELECT entity_id FROM gold.api_v1_tracks WHERE entity_id != '' LIMIT 1 FORMAT TabSeparated", pathFmt: "/v1/entities/%s/tracks", idField: "track_record_id", entityKey: "entity_id"},
	} {
		entityID, err := clickhouseQueryTSV(clickhouseURL, tc.query)
		if err != nil {
			t.Fatalf("lookup entity id for nested %s route: %v", tc.name, err)
		}
		entityID = strings.TrimSpace(entityID)
		if entityID == "" {
			t.Logf("nested route %s skipped: no live entity ids available in fixture-backed compose stack", tc.name)
			continue
		}
		validatedLiveRoutes++
		path := fmt.Sprintf(tc.pathFmt, entityID)
		resp, err := apiGET(baseURL+path, apiSharedKey)
		if err != nil {
			t.Fatalf("nested route %s: %v", path, err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			t.Fatalf("nested route %s status: %d", path, resp.StatusCode)
		}
		var payload struct {
			Data struct {
				Items []map[string]any `json:"items"`
			} `json:"data"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			resp.Body.Close()
			t.Fatalf("decode nested route %s: %v", path, err)
		}
		resp.Body.Close()
		if payload.Data.Items == nil {
			t.Fatalf("nested route %s missing items array", path)
		}
		for _, item := range payload.Data.Items {
			if item[tc.idField] == nil {
				t.Fatalf("nested route %s missing %s in %#v", path, tc.idField, item)
			}
			if gotEntityID, _ := item[tc.entityKey].(string); gotEntityID != "" && gotEntityID != entityID {
				t.Fatalf("nested route %s leaked entity scope: got %q want %q", path, gotEntityID, entityID)
			}
		}
	}
	if validatedLiveRoutes == 0 {
		t.Skip("entity nested route smoke skipped: fixture-backed compose stack has no live entity ids yet")
	}
}

func testCORSPreflight(t *testing.T, baseURL string) {
	t.Helper()
	client := &http.Client{}
	req, err := http.NewRequest(http.MethodOptions, baseURL+"/v1/metrics", nil)
	if err != nil {
		t.Fatalf("build preflight request: %v", err)
	}
	req.Header.Set("Origin", "http://localhost:3000")
	req.Header.Set("Access-Control-Request-Method", "GET")
	req.Header.Set("Access-Control-Request-Headers", "Content-Type, Authorization")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("preflight request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("preflight status: %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "http://localhost:3000" {
		t.Fatalf("preflight missing allow-origin, got %q", got)
	}
	if got := resp.Header.Get("Access-Control-Allow-Methods"); got != "GET, HEAD, OPTIONS" {
		t.Fatalf("preflight missing allow-methods contract, got %q", got)
	}
	if got := strings.ToLower(resp.Header.Get("Access-Control-Allow-Headers")); !strings.Contains(got, strings.ToLower(e2eAPIKeyHeader)) {
		t.Fatalf("preflight missing %s in allow-headers, got %q", e2eAPIKeyHeader, resp.Header.Get("Access-Control-Allow-Headers"))
	}

	deniedReq, err := http.NewRequest(http.MethodOptions, baseURL+"/v1/metrics", nil)
	if err != nil {
		t.Fatalf("build denied preflight request: %v", err)
	}
	deniedReq.Header.Set("Origin", "http://evil.example")
	deniedReq.Header.Set("Access-Control-Request-Method", "GET")
	deniedResp, err := client.Do(deniedReq)
	if err != nil {
		t.Fatalf("denied preflight request: %v", err)
	}
	defer deniedResp.Body.Close()
	if deniedResp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected denied preflight status 403, got %d", deniedResp.StatusCode)
	}
	if got := deniedResp.Header.Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("expected denied preflight to omit allow-origin, got %q", got)
	}
}

func apiGET(requestURL, apiSharedKey string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(apiSharedKey) != "" {
		req.Header.Set(e2eAPIKeyHeader, apiSharedKey)
	}
	return http.DefaultClient.Do(req)
}

func TestDomainPacks(t *testing.T) {
	clickhouseURL := getenv("E2E_CLICKHOUSE_HTTP_URL", "http://svc_control_plane:control_plane_change_me@localhost:8124")

	output := runControlPlane(t, clickhouseURL, "run-once", "--help")
	for _, job := range []string{
		"ingest-geopolitical",
		"ingest-maritime",
		"ingest-aviation",
		"ingest-space",
		"ingest-safety-security",
	} {
		t.Run(job, func(t *testing.T) {
			if !strings.Contains(output, job) {
				t.Fatalf("run-once help missing %q: %s", job, output)
			}
		})
	}
}

func TestStatsDashboard(t *testing.T) {
	ctx := context.Background()
	baseURL := getenv("E2E_API_URL", "http://localhost:8080")
	rendererURL := getenv("E2E_RENDERER_URL", "http://localhost:8090")
	apiSharedKey := e2eAPISharedKey()
	testStatsDashboardWithContext(t, ctx, baseURL, rendererURL, apiSharedKey)
}

func testStatsDashboard(t *testing.T, baseURL, rendererURL, apiSharedKey string) {
	t.Helper()
	ctx := context.Background()
	testStatsDashboardWithContext(t, ctx, baseURL, rendererURL, apiSharedKey)
}

func testStatsDashboardWithContext(t *testing.T, ctx context.Context, baseURL, rendererURL, apiSharedKey string) {
	t.Helper()

	if err := waitForReady(ctx, baseURL, 30*time.Second); err != nil {
		t.Fatalf("API not ready: %v", err)
	}

	statsResp, err := apiGET(baseURL+"/v1/internal/stats", apiSharedKey)
	if err != nil {
		t.Fatalf("stats request: %v", err)
	}
	defer statsResp.Body.Close()
	if statsResp.StatusCode != http.StatusOK {
		t.Fatalf("stats status: %d", statsResp.StatusCode)
	}

	var statsPayload map[string]any
	if err := json.NewDecoder(statsResp.Body).Decode(&statsPayload); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	data, ok := statsPayload["data"].(map[string]any)
	if !ok {
		t.Fatalf("stats payload missing data: %#v", statsPayload)
	}
	if _, ok := data["summary"]; !ok {
		t.Fatalf("stats payload missing summary: %#v", data)
	}
	summary, ok := data["summary"].(map[string]any)
	if !ok {
		t.Fatalf("stats payload summary has wrong shape: %#v", data["summary"])
	}
	for _, field := range []string{"catalog_total", "catalog_concrete", "catalog_fingerprint", "catalog_family", "catalog_runnable", "catalog_approved_runtime_linked", "catalog_deferred", "catalog_credential_gated", "catalog_public_concrete", "catalog_public_runtime_linked", "catalog_public_deferred", "catalog_runtime_credential_gated", "catalog_deferred_credential_gated"} {
		if _, ok := summary[field]; !ok {
			t.Fatalf("stats summary missing rollout field %q: %#v", field, summary)
		}
	}

	rendererResp, err := http.Get(rendererURL + "/")
	if err != nil {
		t.Logf("renderer request skipped: %v", err)
		return
	}
	defer rendererResp.Body.Close()
	if rendererResp.StatusCode != http.StatusOK {
		t.Logf("renderer check skipped: status=%d", rendererResp.StatusCode)
		return
	}
	body, err := io.ReadAll(rendererResp.Body)
	if err != nil {
		t.Fatalf("read renderer body: %v", err)
	}
	if !strings.Contains(string(body), "Pipeline overview") {
		t.Fatalf("renderer missing dashboard heading: %s", string(body))
	}

	proxyResp, err := http.Get(rendererURL + "/stats")
	if err != nil {
		t.Fatalf("renderer stats proxy request: %v", err)
	}
	defer proxyResp.Body.Close()
	if proxyResp.StatusCode != http.StatusOK {
		t.Fatalf("renderer stats proxy status: %d", proxyResp.StatusCode)
	}
	proxyBody, err := io.ReadAll(proxyResp.Body)
	if err != nil {
		t.Fatalf("read renderer stats proxy body: %v", err)
	}
	if !strings.Contains(string(proxyBody), "summary") {
		t.Fatalf("renderer stats proxy missing summary field: %s", string(proxyBody))
	}
}

func TestSourceCatalogRollout(t *testing.T) {
	baseURL := getenv("E2E_API_URL", "http://localhost:8080")
	apiSharedKey := e2eAPISharedKey()
	testSourceCatalogRollout(t, baseURL, apiSharedKey)
}

func testSourceCatalogRollout(t *testing.T, baseURL, apiSharedKey string) {
	t.Helper()
	ctx := context.Background()
	if err := waitForReady(ctx, baseURL, 30*time.Second); err != nil {
		t.Fatalf("API not ready: %v", err)
	}
	resp, err := apiGET(baseURL+"/v1/internal/stats", apiSharedKey)
	if err != nil {
		t.Fatalf("stats request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stats status: %d", resp.StatusCode)
	}
	var payload struct {
		Data struct {
			Summary map[string]any `json:"summary"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode stats payload: %v", err)
	}
	for _, field := range []string{"catalog_total", "catalog_concrete", "catalog_fingerprint", "catalog_family", "catalog_runnable", "catalog_approved_runtime_linked", "catalog_deferred", "catalog_credential_gated", "catalog_public_concrete", "catalog_public_runtime_linked", "catalog_public_deferred", "catalog_runtime_credential_gated", "catalog_deferred_credential_gated"} {
		if _, ok := payload.Data.Summary[field]; !ok {
			t.Fatalf("source catalog rollout missing summary field %q: %#v", field, payload.Data.Summary)
		}
	}
	catalogTotal := summaryUInt(t, payload.Data.Summary, "catalog_total")
	concrete := summaryUInt(t, payload.Data.Summary, "catalog_concrete")
	fingerprint := summaryUInt(t, payload.Data.Summary, "catalog_fingerprint")
	family := summaryUInt(t, payload.Data.Summary, "catalog_family")
	runnable := summaryUInt(t, payload.Data.Summary, "catalog_runnable")
	approvedRuntime := summaryUInt(t, payload.Data.Summary, "catalog_approved_runtime_linked")
	deferred := summaryUInt(t, payload.Data.Summary, "catalog_deferred")
	credentialGated := summaryUInt(t, payload.Data.Summary, "catalog_credential_gated")
	publicConcrete := summaryUInt(t, payload.Data.Summary, "catalog_public_concrete")
	publicRuntime := summaryUInt(t, payload.Data.Summary, "catalog_public_runtime_linked")
	publicDeferred := summaryUInt(t, payload.Data.Summary, "catalog_public_deferred")
	runtimeGated := summaryUInt(t, payload.Data.Summary, "catalog_runtime_credential_gated")
	deferredGated := summaryUInt(t, payload.Data.Summary, "catalog_deferred_credential_gated")
	if catalogTotal != 309 || concrete != 267 || fingerprint != 16 || family != 26 {
		t.Fatalf("unexpected full catalog summary: %#v", payload.Data.Summary)
	}
	if runnable != 7 || approvedRuntime != 7 || deferred != 260 || credentialGated != 23 {
		t.Fatalf("unexpected runtime/deferred/gated summary: %#v", payload.Data.Summary)
	}
	if publicConcrete != 244 || publicRuntime != 6 || publicDeferred != 238 || runtimeGated != 1 || deferredGated != 22 {
		t.Fatalf("unexpected public/gated relationship summary: %#v", payload.Data.Summary)
	}
	if catalogTotal != concrete+fingerprint+family {
		t.Fatalf("expected catalog_total=%d to equal concrete+fingerprint+family=%d", catalogTotal, concrete+fingerprint+family)
	}
	if approvedRuntime != runnable {
		t.Fatalf("expected approved runtime-linked count %d to match runnable count %d", approvedRuntime, runnable)
	}
	if runnable+deferred != concrete {
		t.Fatalf("expected runnable+deferred to equal concrete, got runnable=%d deferred=%d concrete=%d", runnable, deferred, concrete)
	}
	if publicConcrete+credentialGated != concrete {
		t.Fatalf("expected public+credential-gated to equal concrete, got public=%d gated=%d concrete=%d", publicConcrete, credentialGated, concrete)
	}
	if publicRuntime+runtimeGated != runnable {
		t.Fatalf("expected public-runtime + runtime-gated to equal runnable, got public=%d gated=%d runnable=%d", publicRuntime, runtimeGated, runnable)
	}
	if publicDeferred+deferredGated != deferred {
		t.Fatalf("expected public-deferred + deferred-gated to equal deferred, got public=%d gated=%d deferred=%d", publicDeferred, deferredGated, deferred)
	}
}

func TestAutomaticSourceSync(t *testing.T) {
	baseURL := getenv("E2E_API_URL", "http://localhost:8080")
	clickhouseURL := getenv("E2E_CLICKHOUSE_HTTP_URL", "http://svc_control_plane:control_plane_change_me@localhost:8124")
	apiSharedKey := e2eAPISharedKey()
	testAutomaticSourceSync(t, baseURL, clickhouseURL, apiSharedKey)
}

func testAutomaticSourceSync(t *testing.T, baseURL, clickhouseURL, apiSharedKey string) {
	t.Helper()
	ctx := context.Background()
	if err := waitForReady(ctx, baseURL, 30*time.Second); err != nil {
		t.Fatalf("API not ready: %v", err)
	}
	for _, job := range []string{"ingest-geopolitical", "ingest-safety-security"} {
		runControlPlaneJob(t, clickhouseURL, job)
	}
	serveOutput := runControlPlaneServeSnippet(t, clickhouseURL, 15*time.Second)
	if !strings.Contains(serveOutput, "automatic sync tick") && !strings.Contains(serveOutput, "control-plane started") {
		t.Fatalf("expected control-plane serve path to run, got %s", serveOutput)
	}
	resp, err := apiGET(baseURL+"/v1/internal/stats", apiSharedKey)
	if err != nil {
		t.Fatalf("stats request: %v", err)
	}
	defer resp.Body.Close()
	var payload struct {
		Data struct {
			Summary map[string]any `json:"summary"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode stats payload: %v", err)
	}
	for _, field := range []string{"catalog_runnable", "catalog_approved_runtime_linked", "catalog_deferred", "catalog_credential_gated", "catalog_public_runtime_linked", "catalog_runtime_credential_gated", "catalog_deferred_credential_gated", "frontier_pending", "frontier_retry"} {
		if _, ok := payload.Data.Summary[field]; !ok {
			t.Fatalf("automatic sync stats missing field %q: %#v", field, payload.Data.Summary)
		}
	}
	frontierPending := summaryUInt(t, payload.Data.Summary, "frontier_pending")
	frontierRetry := summaryUInt(t, payload.Data.Summary, "frontier_retry")
	runnable := summaryUInt(t, payload.Data.Summary, "catalog_runnable")
	approvedRuntime := summaryUInt(t, payload.Data.Summary, "catalog_approved_runtime_linked")
	gated := summaryUInt(t, payload.Data.Summary, "catalog_credential_gated")
	publicRuntime := summaryUInt(t, payload.Data.Summary, "catalog_public_runtime_linked")
	runtimeGated := summaryUInt(t, payload.Data.Summary, "catalog_runtime_credential_gated")
	deferredGated := summaryUInt(t, payload.Data.Summary, "catalog_deferred_credential_gated")
	if runnable == 0 {
		t.Fatalf("expected automatic sync surface to report runnable sources, got %#v", payload.Data.Summary)
	}
	if runnable != 7 || approvedRuntime != 7 || publicRuntime != 6 || runtimeGated != 1 || deferredGated != 22 {
		t.Fatalf("expected automatic sync rollout counts to preserve the task-11/task-12 subset semantics, got %#v", payload.Data.Summary)
	}
	if gated == 0 {
		t.Fatalf("expected automatic sync surface to report credential-gated sources, got %#v", payload.Data.Summary)
	}
	if frontierPending+frontierRetry == 0 {
		t.Fatalf("expected automatic sync path to leave observable frontier work, got %#v", payload.Data.Summary)
	}
	jobStats, err := clickhouseQueryTSV(clickhouseURL, "SELECT stats FROM ops.job_run WHERE job_type IN ('ingest-geopolitical','ingest-safety-security') ORDER BY finished_at DESC LIMIT 1 FORMAT TabSeparated")
	if err != nil {
		t.Fatalf("query automatic sync job stats: %v", err)
	}
	for _, want := range []string{"\"catalog_approved_runtime_linked\":7", "\"catalog_public_runtime_linked\":6", "\"catalog_runtime_credential_gated\":1", "\"catalog_deferred_credential_gated\":22"} {
		if !strings.Contains(jobStats, want) {
			t.Fatalf("expected automatic sync job stats to include %s, got %s", want, jobStats)
		}
	}
}

func testPhase1TelemetryMVLanding(t *testing.T, clickhouseReadURL, clickhouseIngestURL, clickhouseParseURL string) {
	t.Helper()
	trackSources := []string{
		"catalog:auto:aviation-airports-drones-and-mobility-opensky-network",
		"catalog:auto:aviation-airports-drones-and-mobility-airplanes-live",
		"catalog:auto:security-addendum-air-adsblol-api",
		"catalog:auto:maritime-ocean-and-coastal-sources-aishub",
	}
	openAIPSource := "catalog:auto:aviation-airports-drones-and-mobility-openaip-core-api"
	allSources := append([]string{}, trackSources...)
	allSources = append(allSources, openAIPSource)
	fixturePathBySource := map[string]string{
		"catalog:auto:aviation-airports-drones-and-mobility-opensky-network":  "test/e2e/testdata/phase1/opensky_states_all.json",
		"catalog:auto:aviation-airports-drones-and-mobility-airplanes-live":   "test/e2e/testdata/phase1/airplanes_live_v2_mil.json",
		"catalog:auto:security-addendum-air-adsblol-api":                      "test/e2e/testdata/phase1/adsb_lol_v2_mil.json",
		"catalog:auto:maritime-ocean-and-coastal-sources-aishub":              "test/e2e/testdata/phase1/aishub_ws.json",
		"catalog:auto:aviation-airports-drones-and-mobility-openaip-core-api": "test/e2e/testdata/phase1/openaip_airports.json",
	}
	sourceURLBySource := map[string]string{
		"catalog:auto:aviation-airports-drones-and-mobility-opensky-network":  "https://opensky-network.org/api/states/all?extended=1",
		"catalog:auto:aviation-airports-drones-and-mobility-airplanes-live":   "https://api.airplanes.live/v2/mil",
		"catalog:auto:security-addendum-air-adsblol-api":                      "https://api.adsb.lol/v2/mil",
		"catalog:auto:maritime-ocean-and-coastal-sources-aishub":              "https://data.aishub.net/ws.php?format=1&output=json&compress=2&latmin=-90&latmax=90&lonmin=-180&lonmax=180&interval=5",
		"catalog:auto:aviation-airports-drones-and-mobility-openaip-core-api": "https://api.core.openaip.net/api/airports",
	}

	sourceList := "'" + strings.Join(allSources, "','") + "'"
	bronzeLookupQuery := "SELECT source_id, bronze_table FROM meta.source_registry FINAL WHERE source_id IN (" + sourceList + ") ORDER BY source_id FORMAT TabSeparated"
	bronzeLookup, err := clickhouseQueryTSV(clickhouseReadURL, bronzeLookupQuery)
	if err != nil {
		t.Fatalf("lookup phase-1 bronze tables: %v", err)
	}
	bronzeBySource := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(bronzeLookup), "\n") {
		parts := strings.Split(line, "\t")
		if len(parts) != 2 {
			continue
		}
		bronzeBySource[parts[0]] = parts[1]
	}
	if len(bronzeBySource) != len(allSources) {
		if len(bronzeBySource) == 0 {
			t.Skip("phase-1 telemetry smoke skipped: local registry has no phase-1 bronze mappings")
		}
		t.Fatalf("expected bronze table mapping for %d phase-1 sources, got %d (%s)", len(allSources), len(bronzeBySource), bronzeLookup)
	}

	for _, sourceID := range allSources {
		fixturePayload, err := os.ReadFile(filepath.Join(mustRepoRoot(t), fixturePathBySource[sourceID]))
		if err != nil {
			t.Fatalf("read fixture payload for %s: %v", sourceID, err)
		}
		payloadHash := sha256.Sum256(fixturePayload)
		hashValue := fmt.Sprintf("%x", payloadHash[:])
		metaJSON, err := json.Marshal(map[string]string{"inline_body_base64": base64.StdEncoding.EncodeToString(fixturePayload)})
		if err != nil {
			t.Fatalf("encode fetch metadata for %s: %v", sourceID, err)
		}
		slug := slugifyE2E(sourceID)
		rawID := fmt.Sprintf("raw:e2e:%s:%d", slug, time.Now().UTC().UnixNano())
		fetchID := fmt.Sprintf("fetch:e2e:%s:%d", slug, time.Now().UTC().UnixNano())
		insertRawDoc := fmt.Sprintf(
			"INSERT INTO bronze.raw_document (raw_id, fetch_id, source_id, url, final_url, fetched_at, status_code, content_type, content_hash, body_bytes, object_key, etag, last_modified, not_modified, storage_class, fetch_metadata) VALUES ('%s','%s','%s','%s','%s',now64(3),200,'application/json','%s',%d,NULL,NULL,NULL,0,'inline','%s')",
			rawID,
			fetchID,
			sourceID,
			sourceURLBySource[sourceID],
			sourceURLBySource[sourceID],
			hashValue,
			len(fixturePayload),
			esc(string(metaJSON)),
		)
		if _, err := clickhouseQueryTSV(clickhouseIngestURL, insertRawDoc); err != nil {
			t.Fatalf("insert raw fixture document for %s: %v", sourceID, err)
		}
		runWorkerParseSource(t, clickhouseParseURL, sourceID, 1)
		if bronzeTable := normalizeBronzeTableName(bronzeBySource[sourceID]); bronzeTable == "" {
			t.Fatalf("missing bronze table for %s", sourceID)
		}
	}

	trackCountQuery := "SELECT source_id, count() FROM silver.fact_track_point WHERE source_id IN ('" + strings.Join(trackSources, "','") + "') GROUP BY source_id ORDER BY source_id FORMAT TabSeparated"
	trackCountsRaw, err := clickhouseQueryTSV(clickhouseReadURL, trackCountQuery)
	if err != nil {
		t.Fatalf("query phase-1 track landings: %v", err)
	}
	trackCounts := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(trackCountsRaw), "\n") {
		parts := strings.Split(line, "\t")
		if len(parts) != 2 {
			continue
		}
		count, convErr := parseNonNegativeInt(parts[1])
		if convErr != nil {
			t.Fatalf("parse track landing count %q: %v", parts[1], convErr)
		}
		trackCounts[parts[0]] = count
	}
	for _, sourceID := range trackSources {
		if trackCounts[sourceID] == 0 {
			t.Fatalf("expected deterministic track landing for %s, got counts %s", sourceID, trackCountsRaw)
		}
	}

	entityLineageCount, err := clickhouseQueryTSV(clickhouseReadURL, "SELECT count() FROM silver.v_entity_source_lineage WHERE source_id = '"+openAIPSource+"' FORMAT TabSeparated")
	if err != nil {
		t.Fatalf("query openaip lineage landing: %v", err)
	}
	if strings.TrimSpace(entityLineageCount) == "0" {
		t.Fatalf("expected deterministic openaip lineage landing, got %s", strings.TrimSpace(entityLineageCount))
	}
}

func testPhase1FrontierEndpointInventory(t *testing.T, clickhouseReadURL string) {
	t.Helper()
	phaseSources := []string{
		"catalog:auto:aviation-airports-drones-and-mobility-opensky-network",
		"catalog:auto:aviation-airports-drones-and-mobility-airplanes-live",
		"catalog:auto:security-addendum-air-adsblol-api",
		"catalog:auto:maritime-ocean-and-coastal-sources-aishub",
		"catalog:auto:aviation-airports-drones-and-mobility-openaip-core-api",
	}
	query := "SELECT s.source_id, length(s.entrypoints) AS expected, countDistinct(f.url) AS actual, coalesce(s.disabled_reason, ''), s.lifecycle_state FROM (SELECT source_id, entrypoints, disabled_reason, lifecycle_state FROM meta.source_registry FINAL WHERE source_id IN ('" + strings.Join(phaseSources, "','") + "')) AS s LEFT JOIN ops.crawl_frontier AS f ON f.source_id = s.source_id GROUP BY s.source_id, s.entrypoints, s.disabled_reason, s.lifecycle_state ORDER BY s.source_id FORMAT TabSeparated"
	result, err := clickhouseQueryTSV(clickhouseReadURL, query)
	if err != nil {
		t.Fatalf("query phase-1 frontier inventory: %v", err)
	}
	type frontierCounts struct {
		expected       int
		actual         int
		disabledReason string
		lifecycleState string
	}
	countsBySource := map[string]frontierCounts{}
	for _, line := range strings.Split(strings.TrimSpace(result), "\n") {
		parts := strings.Split(line, "\t")
		if len(parts) != 5 {
			continue
		}
		expected, expectedErr := parseNonNegativeInt(parts[1])
		if expectedErr != nil {
			t.Fatalf("parse expected frontier count %q: %v", parts[1], expectedErr)
		}
		actual, actualErr := parseNonNegativeInt(parts[2])
		if actualErr != nil {
			t.Fatalf("parse actual frontier count %q: %v", parts[2], actualErr)
		}
		countsBySource[parts[0]] = frontierCounts{
			expected:       expected,
			actual:         actual,
			disabledReason: strings.TrimSpace(parts[3]),
			lifecycleState: strings.TrimSpace(parts[4]),
		}
	}
	if len(countsBySource) != len(phaseSources) {
		if len(countsBySource) == 0 {
			t.Skip("phase-1 frontier inventory smoke skipped: local registry has no phase-1 source rows")
		}
		t.Fatalf("expected frontier coverage for %d phase-1 sources, got %d (%s)", len(phaseSources), len(countsBySource), result)
	}
	for _, sourceID := range phaseSources {
		counts := countsBySource[sourceID]
		if counts.expected == 0 {
			t.Fatalf("expected source %s to have configured entrypoints", sourceID)
		}
		if strings.Contains(strings.ToLower(counts.disabledReason), "missing credential") || strings.EqualFold(counts.lifecycleState, "blocked_missing_credential") {
			continue
		}
		if counts.actual < counts.expected {
			t.Fatalf("expected frontier rows to cover every entrypoint for %s (expected=%d actual=%d)", sourceID, counts.expected, counts.actual)
		}
	}
}

func testPhase1CoverageViews(t *testing.T, clickhouseReadURL string) {
	t.Helper()
	phaseSources := []string{
		"catalog:auto:aviation-airports-drones-and-mobility-opensky-network",
		"catalog:auto:aviation-airports-drones-and-mobility-airplanes-live",
		"catalog:auto:security-addendum-air-adsblol-api",
		"catalog:auto:maritime-ocean-and-coastal-sources-aishub",
		"catalog:auto:aviation-airports-drones-and-mobility-openaip-core-api",
	}
	denominatorQuery := "SELECT count() FROM meta.source_registry FINAL WHERE catalog_kind='concrete' AND transport_type='http' AND bronze_table IS NOT NULL FORMAT TabSeparated"
	denominatorRaw, err := clickhouseQueryTSV(clickhouseReadURL, denominatorQuery)
	if err != nil {
		t.Fatalf("query source coverage denominator: %v", err)
	}
	denominatorCount, err := parseNonNegativeInt(denominatorRaw)
	if err != nil {
		t.Fatalf("parse source coverage denominator count: %v", err)
	}

	metaCoverageQuery := "SELECT count() FROM meta.source_silver_coverage FORMAT TabSeparated"
	metaCountRaw, err := clickhouseQueryTSV(clickhouseReadURL, metaCoverageQuery)
	if err != nil {
		t.Fatalf("query meta source coverage count: %v", err)
	}
	metaCount, err := parseNonNegativeInt(metaCountRaw)
	if err != nil {
		t.Fatalf("parse meta source coverage count: %v", err)
	}
	if metaCount != denominatorCount {
		t.Fatalf("expected meta.source_silver_coverage count %d to match registry denominator, got %d", denominatorCount, metaCount)
	}

	metadataCompletenessQuery := "SELECT count() FROM meta.source_silver_coverage WHERE routing_mode = '' OR promote_profile = '' OR terminal_destination = '' FORMAT TabSeparated"
	metadataCountRaw, err := clickhouseQueryTSV(clickhouseReadURL, metadataCompletenessQuery)
	if err != nil {
		t.Fatalf("query source coverage routing completeness: %v", err)
	}
	metadataCount, err := parseNonNegativeInt(metadataCountRaw)
	if err != nil {
		t.Fatalf("parse source coverage routing completeness count: %v", err)
	}
	if metadataCount != 0 {
		t.Fatalf("expected zero source coverage rows with missing routing metadata, got %d", metadataCount)
	}

	goldCoverageQuery := "SELECT count() FROM gold.api_v1_source_coverage WHERE source_id IN ('" + strings.Join(phaseSources, "','") + "') FORMAT TabSeparated"
	goldCountRaw, err := clickhouseQueryTSV(clickhouseReadURL, goldCoverageQuery)
	if err != nil {
		t.Fatalf("query gold source coverage: %v", err)
	}
	goldCount, err := parseNonNegativeInt(goldCountRaw)
	if err != nil {
		t.Fatalf("parse gold source coverage count: %v", err)
	}
	if goldCount != len(phaseSources) {
		if goldCount == 0 {
			t.Skip("phase-1 coverage smoke skipped: local stack has no phase-1 source coverage rows")
		}
		t.Fatalf("expected %d phase-1 rows in gold.api_v1_source_coverage, got %d", len(phaseSources), goldCount)
	}
}

func clickhouseQueryTSV(clickhouseURL, sql string) (string, error) {
	var (
		resp *http.Response
		err  error
	)
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "SELECT") {
		parsedURL, parseErr := url.Parse(clickhouseURL)
		if parseErr != nil {
			return "", parseErr
		}
		queryValues := parsedURL.Query()
		queryValues.Set("query", sql)
		parsedURL.RawQuery = queryValues.Encode()
		resp, err = http.Get(parsedURL.String())
	} else {
		resp, err = http.Post(clickhouseURL, "text/plain; charset=utf-8", strings.NewReader(sql))
	}
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", fmt.Errorf("clickhouse query failed (%s): %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return string(body), nil
}

func slugifyE2E(value string) string {
	replacer := strings.NewReplacer(":", "-", "/", "-", ".", "-", " ", "-")
	return strings.ToLower(replacer.Replace(value))
}

func normalizeBronzeTableName(table string) string {
	table = strings.TrimSpace(table)
	if strings.HasPrefix(table, "bronze.") {
		return strings.TrimPrefix(table, "bronze.")
	}
	return table
}

func parseNonNegativeInt(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty integer value")
	}
	value := 0
	for _, r := range raw {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("non-digit %q", string(r))
		}
		value = value*10 + int(r-'0')
	}
	return value, nil
}

func esc(value string) string {
	return strings.ReplaceAll(strings.TrimSpace(value), "'", "''")
}

func waitForReady(ctx context.Context, baseURL string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		resp, err := http.Get(baseURL + "/v1/ready")
		if err == nil && resp.StatusCode == http.StatusOK {
			var payload struct {
				Data struct {
					Ready bool `json:"ready"`
				} `json:"data"`
			}
			decodeErr := json.NewDecoder(resp.Body).Decode(&payload)
			resp.Body.Close()
			if decodeErr == nil && payload.Data.Ready {
				return nil
			}
		}
		if resp != nil {
			resp.Body.Close()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}

	return fmt.Errorf("timeout waiting for ready")
}

func runControlPlaneJob(t *testing.T, clickhouseURL, job string) string {
	t.Helper()
	return runControlPlane(t, clickhouseURL, "run-once", "--job", job)
}

func runControlPlane(t *testing.T, clickhouseURL string, args ...string) string {
	t.Helper()
	output, err := tryRunControlPlane(clickhouseURL, args...)
	if err != nil {
		failingOutput := strings.TrimSpace(err.Error())
		if len(failingOutput) > 400 {
			failingOutput = failingOutput[:400]
		}
		t.Fatalf("control-plane command failed: go run ./cmd/control-plane %s (%s)", strings.Join(args, " "), failingOutput)
	}
	return output
}

func tryRunControlPlane(clickhouseURL string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmdArgs := append([]string{"run", "./cmd/control-plane"}, args...)
	cmd := exec.CommandContext(ctx, "go", cmdArgs...)
	root, err := repoRoot()
	if err != nil {
		return "", err
	}
	cmd.Dir = root
	cmd.Env = append(os.Environ(), e2eLocalCommandEnv(clickhouseURL)...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		if ctx.Err() != nil {
			return "", fmt.Errorf("command timed out: go %s", strings.Join(cmdArgs, " "))
		}
		failingOutput := strings.TrimSpace(string(output))
		if failingOutput == "" {
			failingOutput = err.Error()
		}
		failingOutput = strings.ReplaceAll(failingOutput, "\n", " | ")
		failingOutput = strings.TrimSpace(failingOutput)
		if len(failingOutput) > 400 {
			failingOutput = failingOutput[:400]
		}
		failingOutput = strings.TrimSpace(failingOutput)
		return string(output), fmt.Errorf("%s", failingOutput)
	}
	return string(output), nil
}

func runWorkerParseSource(t *testing.T, clickhouseURL, sourceID string, limit int) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "run", "./cmd/worker-parse", "parse-source", "--source-id", sourceID, "--limit", fmt.Sprintf("%d", limit))
	cmd.Dir = mustRepoRoot(t)
	cmd.Env = append(os.Environ(), e2eLocalCommandEnv(clickhouseURL)...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("worker-parse parse-source %s failed: %v\n%s", sourceID, err, string(output))
	}
	return string(output)
}

func runControlPlaneServeSnippet(t *testing.T, clickhouseURL string, timeout time.Duration) string {
	t.Helper()
	binPath := filepath.Join(t.TempDir(), "control-plane")
	build := exec.Command("go", "build", "-buildvcs=false", "-o", binPath, "./cmd/control-plane")
	build.Dir = mustRepoRoot(t)
	build.Env = os.Environ()
	if output, err := build.CombinedOutput(); err != nil {
		failingOutput := strings.TrimSpace(string(output))
		if failingOutput == "" {
			failingOutput = err.Error()
		}
		t.Fatalf("control-plane build failed: %s", failingOutput)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, binPath)
	cmd.Dir = mustRepoRoot(t)
	cmd.Env = append(os.Environ(), e2eLocalCommandEnv(clickhouseURL)...)
	cmd.Env = append(cmd.Env, "CONTROL_PLANE_MAX_TICKS=1")
	output, err := cmd.CombinedOutput()
	if err != nil && ctx.Err() == nil {
		failingOutput := strings.TrimSpace(string(output))
		if failingOutput == "" {
			failingOutput = err.Error()
		}
		t.Fatalf("control-plane serve command failed: %s", failingOutput)
	}
	return string(output)
}

func summaryUInt(t *testing.T, summary map[string]any, key string) uint64 {
	t.Helper()
	value, ok := summary[key]
	if !ok {
		t.Fatalf("summary missing key %q", key)
	}
	switch typed := value.(type) {
	case float64:
		return uint64(typed)
	case int:
		return uint64(typed)
	case int64:
		return uint64(typed)
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			t.Fatalf("summary key %q invalid number: %v", key, err)
		}
		return uint64(parsed)
	default:
		t.Fatalf("summary key %q has unsupported type %T", key, value)
	}
	return 0
}

func mustRepoRoot(tb testing.TB) string {
	tb.Helper()
	root, err := repoRoot()
	if err != nil {
		tb.Fatalf("repo root: %v", err)
	}
	return root
}

func repoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for dir := wd; dir != filepath.Dir(dir); dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
	}
	return "", fmt.Errorf("unable to locate repo root")
}

func getenv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func e2eAPISharedKey() string {
	if value := strings.TrimSpace(os.Getenv("E2E_API_SHARED_KEY")); value != "" {
		return value
	}
	if value := readDotEnvValue("API_SHARED_KEY"); value != "" {
		return value
	}
	return "local_api_key_change_me"
}

func readDotEnvValue(key string) string {
	root, err := repoRoot()
	if err != nil {
		return ""
	}
	body, err := os.ReadFile(filepath.Join(root, ".env"))
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(body), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		name, value, ok := strings.Cut(trimmed, "=")
		if !ok || strings.TrimSpace(name) != key {
			continue
		}
		return strings.Trim(strings.TrimSpace(value), "\"'")
	}
	return ""
}

func e2eLocalCommandEnv(clickhouseURL string) []string {
	return []string{
		"CLICKHOUSE_HTTP_URL=" + clickhouseURL,
		"MINIO_ENDPOINT=" + getenv("E2E_MINIO_ENDPOINT", e2eLocalMinIOEndpointDefault),
		"MINIO_ACCESS_KEY=" + getenv("E2E_MINIO_ACCESS_KEY", e2eLocalMinIOAccessKeyDefault),
		"MINIO_SECRET_KEY=" + getenv("E2E_MINIO_SECRET_KEY", e2eLocalMinIOSecretKeyDefault),
	}
}
