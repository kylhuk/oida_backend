package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRespond(t *testing.T) {
	rr := httptest.NewRecorder()
	respond(rr, "v1", envelope{"status": "ok"})
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", rr.Code)
	}
	if rr.Body.Len() == 0 {
		t.Fatal("expected body")
	}
}

func TestListStub(t *testing.T) {
	h := listStub("v1", "sources")
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/sources", nil)
	h(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", rr.Code)
	}
}

func TestReady(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "bootstrap.ready")
	h := readyHandler("v1", marker)

	t.Run("false before marker exists", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/ready", nil)
		h(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 got %d", rr.Code)
		}
		if ready := decodeReady(t, rr.Body.Bytes()); ready {
			t.Fatal("expected readiness false before bootstrap marker exists")
		}
	})

	t.Run("true after marker exists", func(t *testing.T) {
		if err := os.WriteFile(marker, []byte("ready\n"), 0o644); err != nil {
			t.Fatalf("write marker: %v", err)
		}

		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/ready", nil)
		h(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 got %d", rr.Code)
		}
		if ready := decodeReady(t, rr.Body.Bytes()); !ready {
			t.Fatal("expected readiness true after bootstrap marker exists")
		}
	})
}

func decodeReady(t *testing.T, body []byte) bool {
	t.Helper()

	var payload struct {
		Data struct {
			Ready bool `json:"ready"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return payload.Data.Ready
}

func TestCORSPreflightAllowedAndDenied(t *testing.T) {
	t.Setenv("API_CORS_ALLOW_ORIGINS", "http://localhost:3000,http://localhost:5173")
	t.Setenv("API_SHARED_KEY", "test_api_key")
	mux := newAPIMuxWithServer("v1", "", &apiServer{
		version: "v1",
		clickhouse: stubQuerier{queryFn: func(_ context.Context, _ string) (string, error) {
			return `{"metric_id":"obs_count","metric_family":"activity","subject_grain":"place","unit":"count","value_type":"count","rollup_engine":"snapshot","rollup_rule":"sum","enabled":1,"updated_at":"2026-03-10T08:30:00Z","attrs":"{}","evidence":"[]"}` + "\n", nil
		}},
		queryTimeout: time.Second,
	})

	t.Run("allowed preflight", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodOptions, "/v1/metrics", nil)
		req.Header.Set("Origin", "http://localhost:3000")
		req.Header.Set("Access-Control-Request-Method", "GET")
		req.Header.Set("Access-Control-Request-Headers", "Content-Type, Authorization")
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusNoContent {
			t.Fatalf("expected 204 got %d", rr.Code)
		}
		if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:3000" {
			t.Fatalf("unexpected allow origin %q", got)
		}
		if got := rr.Header().Get("Access-Control-Allow-Methods"); got != "GET, HEAD, OPTIONS" {
			t.Fatalf("unexpected allow methods %q", got)
		}
		if got := rr.Header().Get("Access-Control-Allow-Headers"); got != "Content-Type, Authorization, X-API-Key" {
			t.Fatalf("unexpected allow headers %q", got)
		}
	})

	t.Run("denied preflight", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodOptions, "/v1/metrics", nil)
		req.Header.Set("Origin", "http://evil.example")
		req.Header.Set("Access-Control-Request-Method", "GET")
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusForbidden {
			t.Fatalf("expected 403 got %d", rr.Code)
		}
		if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "" {
			t.Fatalf("expected no allow origin header, got %q", got)
		}
	})

	t.Run("allowed get includes CORS header", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/metrics", nil)
		req.Header.Set("Origin", "http://localhost:3000")
		req.Header.Set(apiKeyHeader, "test_api_key")
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 got %d", rr.Code)
		}
		if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:3000" {
			t.Fatalf("unexpected allow origin %q", got)
		}
	})
}

func TestAPIKeyAuthProtectedAndPublicRoutes(t *testing.T) {
	t.Setenv("API_SHARED_KEY", "test_api_key")
	mux := newAPIMuxWithServer("v1", "", &apiServer{
		version: "v1",
		clickhouse: stubQuerier{queryFn: func(_ context.Context, _ string) (string, error) {
			return `{"metric_id":"obs_count","metric_family":"activity","subject_grain":"place","unit":"count","value_type":"count","rollup_engine":"snapshot","rollup_rule":"sum","enabled":1,"updated_at":"2026-03-10T08:30:00Z","attrs":"{}","evidence":"[]"}` + "\n", nil
		}},
		queryTimeout: time.Second,
	})

	t.Run("public route does not require key", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 got %d", rr.Code)
		}
	})

	t.Run("protected route without key returns 401", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/metrics", nil)
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 got %d", rr.Code)
		}
		var payload struct {
			Data struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			} `json:"data"`
		}
		if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if payload.Data.Error.Code != "unauthorized" {
			t.Fatalf("expected unauthorized code got %q", payload.Data.Error.Code)
		}
	})

	t.Run("protected route with key returns 200", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/metrics", nil)
		req.Header.Set(apiKeyHeader, "test_api_key")
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 got %d", rr.Code)
		}
	})

	t.Run("HEAD follows protected auth behavior", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodHead, "/v1/metrics", nil)
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 got %d", rr.Code)
		}

		rr = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodHead, "/v1/metrics", nil)
		req.Header.Set(apiKeyHeader, "test_api_key")
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 got %d", rr.Code)
		}
	})
}
