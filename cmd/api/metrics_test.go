package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsEndpointRequiresBearerAndEmitsPrometheus(t *testing.T) {
	t.Setenv("METRICS_SHARED_KEY", "metrics-secret")
	mux := newAPIMuxWithServer("v1", "", serverWithTestAuth(&apiServer{version: "v1"}))

	t.Run("missing bearer is rejected", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 got %d", rr.Code)
		}
	})

	t.Run("valid bearer returns prometheus text", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		req.Header.Set("Authorization", "Bearer metrics-secret")
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 got %d body=%s", rr.Code, rr.Body.String())
		}
		if contentType := rr.Header().Get("Content-Type"); !strings.Contains(contentType, "text/plain") {
			t.Fatalf("expected prometheus text content type, got %q", contentType)
		}
		for _, want := range []string{"oida_ready", "oida_http_requests_total", "oida_auth_failures_total"} {
			if !strings.Contains(rr.Body.String(), want) {
				t.Fatalf("expected metrics body to contain %q, got %s", want, rr.Body.String())
			}
		}
	})
}
