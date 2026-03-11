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

func runEndToEndPipeline(t *testing.T) {
	ctx := context.Background()
	baseURL := getenv("E2E_API_URL", "http://localhost:8080")
	clickhouseURL := getenv("E2E_CLICKHOUSE_HTTP_URL", "http://localhost:8123")
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
		testFixturePipeline(t, baseURL, clickhouseURL)
	})

	t.Run("LocationAttribution", func(t *testing.T) {
		testLocationAttribution(t, baseURL)
	})

	t.Run("APIServing", func(t *testing.T) {
		testAPIServing(t, baseURL)
	})

	t.Run("MetricsRollup", func(t *testing.T) {
		testMetricsRollup(t, baseURL)
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

func testFixturePipeline(t *testing.T, baseURL, clickhouseURL string) {
	t.Helper()
	for _, job := range []string{"place-build", "promote"} {
		runControlPlaneJob(t, clickhouseURL, job)
	}

	resp, err := http.Get(baseURL + "/v1/events?source_id=fixture:newsroom")
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

func testLocationAttribution(t *testing.T, baseURL string) {
	t.Helper()
	resp, err := http.Get(baseURL + "/v1/events?place_id=plc:fr-idf-paris")
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

func testAPIServing(t *testing.T, baseURL string) {
	t.Helper()
	endpoints := []struct {
		path   string
		method string
		status int
	}{
		{"/v1/health", "GET", http.StatusOK},
		{"/v1/ready", "GET", http.StatusOK},
		{"/v1/sources", "GET", http.StatusOK},
		{"/v1/places", "GET", http.StatusOK},
		{"/v1/events", "GET", http.StatusOK},
		{"/v1/entities", "GET", http.StatusOK},
		{"/v1/metrics", "GET", http.StatusOK},
	}

	for _, ep := range endpoints {
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
}

func testMetricsRollup(t *testing.T, baseURL string) {
	t.Helper()
	resp, err := http.Get(baseURL + "/v1/analytics/rollups?metric_id=obs_count&fields=snapshot_id,metric_id,grain,place_id,value")
	if err != nil {
		t.Fatalf("metrics rollup: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status: %d", resp.StatusCode)
	}

	var result struct {
		Data struct {
			Rollups []map[string]any `json:"rollups"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode metrics: %v", err)
	}

	if len(result.Data.Rollups) > 0 {
		rollup := result.Data.Rollups[0]
		required := []string{"metric_id", "grain", "place_id", "value"}
		for _, field := range required {
			if _, ok := rollup[field]; !ok {
				t.Errorf("rollup missing field: %s", field)
			}
		}
	}
}

func TestDomainPacks(t *testing.T) {
	clickhouseURL := getenv("E2E_CLICKHOUSE_HTTP_URL", "http://localhost:8123")

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

	if err := waitForReady(ctx, baseURL, 30*time.Second); err != nil {
		t.Fatalf("API not ready: %v", err)
	}

	statsResp, err := http.Get(baseURL + "/v1/internal/stats")
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
		t.Fatalf("renderer request: %v", err)
	}
	defer rendererResp.Body.Close()
	if rendererResp.StatusCode != http.StatusOK {
		t.Fatalf("renderer status: %d", rendererResp.StatusCode)
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
	ctx := context.Background()
	baseURL := getenv("E2E_API_URL", "http://localhost:8080")
	if err := waitForReady(ctx, baseURL, 30*time.Second); err != nil {
		t.Fatalf("API not ready: %v", err)
	}
	resp, err := http.Get(baseURL + "/v1/internal/stats")
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
	ctx := context.Background()
	baseURL := getenv("E2E_API_URL", "http://localhost:8080")
	clickhouseURL := getenv("E2E_CLICKHOUSE_HTTP_URL", "http://localhost:8123")
	if err := waitForReady(ctx, baseURL, 30*time.Second); err != nil {
		t.Fatalf("API not ready: %v", err)
	}
	for _, job := range []string{"ingest-geopolitical", "ingest-safety-security"} {
		runControlPlaneJob(t, clickhouseURL, job)
	}
	serveOutput := runControlPlaneServeSnippet(t, clickhouseURL, 5*time.Second)
	if !strings.Contains(serveOutput, "automatic sync tick") {
		t.Fatalf("expected control-plane serve path to execute automatic sync tick, got %s", serveOutput)
	}
	resp, err := http.Get(baseURL + "/v1/internal/stats")
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
	deferred := summaryUInt(t, payload.Data.Summary, "catalog_deferred")
	gated := summaryUInt(t, payload.Data.Summary, "catalog_credential_gated")
	if runnable == 0 {
		t.Fatalf("expected automatic sync surface to report runnable sources, got %#v", payload.Data.Summary)
	}
	if deferred == 0 {
		t.Fatalf("expected automatic sync surface to report deferred sources, got %#v", payload.Data.Summary)
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
			resp.Body.Close()
			return nil
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
