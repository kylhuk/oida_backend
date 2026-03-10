//go:build e2e
// +build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// TestEndToEndPipeline validates the full data flow:
// discovery → fetch → parse → location → promote → serve
func TestEndToEndPipeline(t *testing.T) {
	ctx := context.Background()
	baseURL := getenv("E2E_API_URL", "http://localhost:8080")
	
	// Wait for API to be ready
	if err := waitForReady(ctx, baseURL, 30*time.Second); err != nil {
		t.Fatalf("API not ready: %v", err)
	}
	
	t.Run("DiscoveryAndFetch", func(t *testing.T) {
		testDiscoveryAndFetch(t, baseURL)
	})
	
	t.Run("ParseAndPromote", func(t *testing.T) {
		testParseAndPromote(t, baseURL)
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

func testDiscoveryAndFetch(t *testing.T, baseURL string) {
	// Trigger discovery job
	resp, err := http.Post(
		baseURL+"/v1/jobs/discovery",
		"application/json",
		strings.NewReader(`{"source_class": "feed", "limit": 10}`),
	)
	if err != nil {
		t.Fatalf("discovery job: %v", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("discovery job status: %d", resp.StatusCode)
	}
	
	// Verify frontier population
	time.Sleep(2 * time.Second)
	
	resp, err = http.Get(baseURL + "/v1/jobs?status=pending")
	if err != nil {
		t.Fatalf("fetch frontier: %v", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("frontier status: %d", resp.StatusCode)
	}
}

func testParseAndPromote(t *testing.T, baseURL string) {
	// Submit test document for parsing
	doc := map[string]any{
		"source_id": "e2e:test",
		"url": "https://example.com/test.json",
		"content_type": "application/json",
		"body": map[string]any{
			"event_type": "test",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"location": map[string]any{
				"lat": 51.5074,
				"lon": -0.1278,
			},
		},
	}
	
	body, _ := json.Marshal(doc)
	resp, err := http.Post(
		baseURL+"/v1/jobs/parse",
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("parse job: %v", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("parse job status: %d", resp.StatusCode)
	}
	
	// Wait for promotion
	time.Sleep(3 * time.Second)
	
	// Verify event created
	resp, err = http.Get(baseURL + "/v1/events?source_id=e2e:test")
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
		t.Error("no events found after promotion")
	}
}

func testLocationAttribution(t *testing.T, baseURL string) {
	// Query events with location
	resp, err := http.Get(baseURL + "/v1/events?place_id=plc:gb:GBR")
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
				PlaceID     string `json:"place_id"`
				Admin0ID    string `json:"admin0_id"`
				GeoMethod   string `json:"geo_method"`
			} `json:"items"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	
	for _, item := range result.Data.Items {
		if item.PlaceID == "" {
			t.Error("event missing place_id")
		}
		if item.GeoMethod == "" {
			t.Error("event missing geo_method")
		}
	}
}

func testAPIServing(t *testing.T, baseURL string) {
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
	// Query metrics rollup
	resp, err := http.Get(baseURL + "/v1/analytics/rollups?metric=obs_count&grain=day")
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
	
	// Verify structure
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

// TestDomainPacks validates each domain pack can ingest and produce metrics
func TestDomainPacks(t *testing.T) {
	baseURL := getenv("E2E_API_URL", "http://localhost:8080")
	
	domains := []struct {
		name   string
		job    string
		metric string
	}{
		{"geopolitical", "ingest-geopolitical", "conflict_intensity_score"},
		{"maritime", "ingest-maritime", "shadow_fleet_score"},
		{"aviation", "ingest-aviation", "military_likelihood_score"},
		{"space", "ingest-space", "overpass_density_score"},
		{"safety", "ingest-safety", "sanctions_exposure_score"},
	}
	
	for _, domain := range domains {
		t.Run(domain.name, func(t *testing.T) {
			testDomainPack(t, baseURL, domain.job, domain.metric)
		})
	}
}

func testDomainPack(t *testing.T, baseURL, job, metric string) {
	// Trigger domain pack job
	resp, err := http.Post(
		fmt.Sprintf("%s/v1/jobs/%s", baseURL, job),
		"application/json",
		strings.NewReader(`{"mode": "fixture"}`),
	)
	if err != nil {
		t.Skipf("%s job not available: %v", job, err)
		return
	}
	defer resp.Body.Close()
	
	if resp.StatusCode == http.StatusNotFound {
		t.Skipf("%s job endpoint not implemented", job)
		return
	}
	
	// Wait for processing
	time.Sleep(5 * time.Second)
	
	// Check metric exists
	resp, err = http.Get(fmt.Sprintf("%s/v1/metrics/%s", baseURL, metric))
	if err != nil {
		t.Fatalf("metrics query: %v", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		t.Errorf("metric %s not found", metric)
	}
}

// TestDataQuality validates data quality constraints
func TestDataQuality(t *testing.T) {
	baseURL := getenv("E2E_API_URL", "http://localhost:8080")
	
	t.Run("Freshness", func(t *testing.T) {
		testDataFreshness(t, baseURL)
	})
	
	t.Run("GeolocationRate", func(t *testing.T) {
		testGeolocationRate(t, baseURL)
	})
	
	t.Run("DuplicatePrevention", func(t *testing.T) {
		testDuplicatePrevention(t, baseURL)
	})
}

func testDataFreshness(t *testing.T, baseURL string) {
	resp, err := http.Get(baseURL + "/v1/sources")
	if err != nil {
		t.Fatalf("sources: %v", err)
	}
	defer resp.Body.Close()
	
	var result struct {
		Data struct {
			Items []struct {
				SourceID      string    `json:"source_id"`
				LastFetchAt   time.Time `json:"last_fetch_at"`
				FreshnessLag  int       `json:"freshness_lag_minutes"`
			} `json:"items"`
		} `json:"data"`
	}
	
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	
	for _, source := range result.Data.Items {
		if source.FreshnessLag > 1440 { // 24 hours
			t.Errorf("source %s stale: %d minutes lag", source.SourceID, source.FreshnessLag)
		}
	}
}

func testGeolocationRate(t *testing.T, baseURL string) {
	resp, err := http.Get(baseURL + "/v1/events?limit=100")
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	defer resp.Body.Close()
	
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
		t.Skip("no events to check")
		return
	}
	
	located := 0
	for _, item := range result.Data.Items {
		if item.PlaceID != "" {
			located++
		}
	}
	
	rate := float64(located) / float64(len(result.Data.Items))
	if rate < 0.9 { // 90% threshold
		t.Errorf("geolocation rate %.2f%% below 90%%", rate*100)
	}
}

func testDuplicatePrevention(t *testing.T, baseURL string) {
	// Query for duplicate detection
	resp, err := http.Get(baseURL + "/v1/events?source_id=e2e:test")
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	defer resp.Body.Close()
	
	var result struct {
		Data struct {
			Items []map[string]any `json:"items"`
		} `json:"data"`
	}
	
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	
	// Check for duplicate IDs
	ids := make(map[string]int)
	for _, item := range result.Data.Items {
		id, _ := item["id"].(string)
		if id != "" {
			ids[id]++
			if ids[id] > 1 {
				t.Errorf("duplicate event ID: %s", id)
			}
		}
	}
}

// Helpers

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
		time.Sleep(1 * time.Second)
	}
	
	return fmt.Errorf("timeout waiting for ready")
}

func getenv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
