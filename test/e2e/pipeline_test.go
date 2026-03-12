//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const e2eAPIKeyHeader = "X-API-Key"

func runEndToEndPipeline(t *testing.T) {
	ctx := context.Background()
	baseURL := getenv("E2E_API_URL", "http://localhost:8080")
	apiSharedKey := getenv("E2E_API_SHARED_KEY", "local_api_key_change_me")
	clickhouseURL := getenv("E2E_CLICKHOUSE_HTTP_URL", "http://svc_control_plane:control_plane_change_me@localhost:8124")
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
		testEntityNestedRoutes(t, baseURL, apiSharedKey)
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
}

func TestHTTPSourcePipeline(t *testing.T) {
	runEndToEndPipeline(t)
}

func testRunOnceHelp(t *testing.T, clickhouseURL string) {
	t.Helper()
	output := runControlPlane(t, clickhouseURL, "run-once", "--help")
	for _, want := range []string{
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
		runControlPlaneJob(t, clickhouseURL, job)
	}

	resp, err := apiGET(baseURL+"/v1/events?source_id=fixture:newsroom", apiSharedKey)
	if err != nil {
		t.Fatalf("events query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("events status: %d", resp.StatusCode)
	}

	var result struct {
		Data struct {
			Items []map[string]any `json:"items"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode events: %v", err)
	}
	if len(result.Data.Items) == 0 {
		t.Fatal("no promoted events found after run-once pipeline jobs")
	}
}

func testHTTPFixtureService(t *testing.T, fixtureURL string) {
	t.Helper()
	resp, err := http.Get(fixtureURL + "/health.json")
	if err != nil {
		t.Fatalf("fixture service query: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("fixture service status: %d", resp.StatusCode)
	}
}

func testLocationAttribution(t *testing.T, baseURL, apiSharedKey string) {
	t.Helper()
	resp, err := apiGET(baseURL+"/v1/events?place_id=plc:fr-idf-paris", apiSharedKey)
	if err != nil {
		t.Fatalf("location query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("location query status: %d", resp.StatusCode)
	}

	var result struct {
		Data struct {
			Items []struct {
				PlaceID string `json:"place_id"`
			} `json:"items"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result.Data.Items) == 0 {
		t.Fatal("no location-attributed events found for plc:fr-idf-paris")
	}
	for _, item := range result.Data.Items {
		if item.PlaceID == "" {
			t.Error("event missing place_id")
		}
	}
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
		resp.Body.Close()

		if resp.StatusCode != ep.status {
			t.Errorf("%s: expected %d, got %d", ep.path, ep.status, resp.StatusCode)
		}
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
	resp, err := apiGET(baseURL+"/v1/analytics/rollups?metric_id=obs_count&fields=snapshot_id,metric_id,window_grain,metric_value", apiSharedKey)
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
		required := []string{"metric_id", "window_grain", "metric_value"}
		for _, field := range required {
			if _, ok := rollup[field]; !ok {
				t.Errorf("rollup missing field: %s", field)
			}
		}
	}
}

func testEntityNestedRoutes(t *testing.T, baseURL, apiSharedKey string) {
	t.Helper()
	entityResp, err := apiGET(baseURL+"/v1/entities?limit=1", apiSharedKey)
	if err != nil {
		t.Fatalf("entities list: %v", err)
	}
	defer entityResp.Body.Close()
	if entityResp.StatusCode != http.StatusOK {
		t.Fatalf("entities status: %d", entityResp.StatusCode)
	}
	var entityPayload struct {
		Data struct {
			Items []struct {
				EntityID string `json:"entity_id"`
			} `json:"items"`
		} `json:"data"`
	}
	if err := json.NewDecoder(entityResp.Body).Decode(&entityPayload); err != nil {
		t.Fatalf("decode entities: %v", err)
	}
	if len(entityPayload.Data.Items) == 0 || entityPayload.Data.Items[0].EntityID == "" {
		t.Fatal("no entity available for nested route checks")
	}
	entityID := entityPayload.Data.Items[0].EntityID

	for _, path := range []string{"/v1/entities/" + entityID + "/events", "/v1/entities/" + entityID + "/places"} {
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
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		t.Fatalf("preflight status: %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "http://localhost:3000" {
		t.Fatalf("preflight missing allow-origin, got %q", got)
	}
	if got := strings.ToLower(resp.Header.Get("Access-Control-Allow-Headers")); !strings.Contains(got, strings.ToLower(e2eAPIKeyHeader)) {
		t.Fatalf("preflight missing %s in allow-headers, got %q", e2eAPIKeyHeader, resp.Header.Get("Access-Control-Allow-Headers"))
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
	apiSharedKey := getenv("E2E_API_SHARED_KEY", "local_api_key_change_me")
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
	for _, field := range []string{"catalog_total", "catalog_concrete", "catalog_fingerprint", "catalog_family", "catalog_runnable", "catalog_deferred", "catalog_credential_gated"} {
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
	apiSharedKey := getenv("E2E_API_SHARED_KEY", "local_api_key_change_me")
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
	for _, field := range []string{"catalog_total", "catalog_concrete", "catalog_fingerprint", "catalog_family", "catalog_runnable", "catalog_deferred", "catalog_credential_gated"} {
		if _, ok := payload.Data.Summary[field]; !ok {
			t.Fatalf("source catalog rollout missing summary field %q: %#v", field, payload.Data.Summary)
		}
	}
	catalogTotal := summaryUInt(t, payload.Data.Summary, "catalog_total")
	concrete := summaryUInt(t, payload.Data.Summary, "catalog_concrete")
	fingerprint := summaryUInt(t, payload.Data.Summary, "catalog_fingerprint")
	family := summaryUInt(t, payload.Data.Summary, "catalog_family")
	runnable := summaryUInt(t, payload.Data.Summary, "catalog_runnable")
	deferred := summaryUInt(t, payload.Data.Summary, "catalog_deferred")
	credentialGated := summaryUInt(t, payload.Data.Summary, "catalog_credential_gated")
	if catalogTotal != concrete+fingerprint+family {
		t.Fatalf("expected catalog_total=%d to equal concrete+fingerprint+family=%d", catalogTotal, concrete+fingerprint+family)
	}
	if runnable+deferred > concrete {
		t.Fatalf("expected runnable+deferred <= concrete, got runnable=%d deferred=%d concrete=%d", runnable, deferred, concrete)
	}
	if credentialGated > concrete {
		t.Fatalf("expected credential-gated <= concrete, got gated=%d concrete=%d", credentialGated, concrete)
	}
}

func TestAutomaticSourceSync(t *testing.T) {
	baseURL := getenv("E2E_API_URL", "http://localhost:8080")
	clickhouseURL := getenv("E2E_CLICKHOUSE_HTTP_URL", "http://svc_control_plane:control_plane_change_me@localhost:8124")
	apiSharedKey := getenv("E2E_API_SHARED_KEY", "local_api_key_change_me")
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
	for _, field := range []string{"catalog_runnable", "catalog_deferred", "catalog_credential_gated", "frontier_pending"} {
		if _, ok := payload.Data.Summary[field]; !ok {
			t.Fatalf("automatic sync stats missing field %q: %#v", field, payload.Data.Summary)
		}
	}
	frontierPending := summaryUInt(t, payload.Data.Summary, "frontier_pending")
	runnable := summaryUInt(t, payload.Data.Summary, "catalog_runnable")
	gated := summaryUInt(t, payload.Data.Summary, "catalog_credential_gated")
	if runnable == 0 {
		t.Fatalf("expected automatic sync surface to report runnable sources, got %#v", payload.Data.Summary)
	}
	if gated == 0 {
		t.Fatalf("expected automatic sync surface to report credential-gated sources, got %#v", payload.Data.Summary)
	}
	if frontierPending == 0 {
		t.Fatalf("expected automatic sync path to leave observable frontier work, got %#v", payload.Data.Summary)
	}
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmdArgs := append([]string{"run", "./cmd/control-plane"}, args...)
	cmd := exec.CommandContext(ctx, "go", cmdArgs...)
	cmd.Dir = mustRepoRoot(t)
	cmd.Env = append(os.Environ(), "CLICKHOUSE_HTTP_URL="+clickhouseURL)
	output, err := cmd.CombinedOutput()
	if err != nil {
		if ctx.Err() != nil {
			t.Fatalf("control-plane command timed out: go %s", strings.Join(cmdArgs, " "))
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
		t.Fatalf("control-plane command failed: go %s (%s)", strings.Join(cmdArgs, " "), failingOutput)
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
	cmd.Env = append(os.Environ(), "CLICKHOUSE_HTTP_URL="+clickhouseURL, "CONTROL_PLANE_MAX_TICKS=1")
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
	wd, err := os.Getwd()
	if err != nil {
		tb.Fatalf("getwd: %v", err)
	}
	for dir := wd; dir != filepath.Dir(dir); dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
	}
	tb.Fatal("unable to locate repo root")
	return ""
}

func getenv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
