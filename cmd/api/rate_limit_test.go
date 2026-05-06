package main

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func TestClientRateLimiterAllowsBurstThenRefills(t *testing.T) {
	limiter := newClientRateLimiter(2, 1)
	now := time.Unix(1000, 0)
	limiter.now = func() time.Time { return now }

	if allowed, retryAfter := limiter.allow("client-a"); !allowed || retryAfter != 0 {
		t.Fatalf("expected first request to be allowed, allowed=%v retry=%s", allowed, retryAfter)
	}
	if allowed, retryAfter := limiter.allow("client-a"); allowed || retryAfter <= 0 {
		t.Fatalf("expected second immediate request to be limited, allowed=%v retry=%s", allowed, retryAfter)
	}

	now = now.Add(500 * time.Millisecond)
	if allowed, retryAfter := limiter.allow("client-a"); !allowed || retryAfter != 0 {
		t.Fatalf("expected request after refill to be allowed, allowed=%v retry=%s", allowed, retryAfter)
	}
}

func TestRateLimitMiddlewareReturns429WithHeaders(t *testing.T) {
	limiter := newClientRateLimiter(1, 1)
	now := time.Unix(1000, 0)
	limiter.now = func() time.Time { return now }

	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := withRateLimit(next, "v1", limiter)

	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	req.Header.Set(apiKeyHeader, testAPIKey)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected first request 200 got %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("expected second request 429 got %d body=%s", rr.Code, rr.Body.String())
	}
	if retry := rr.Header().Get("Retry-After"); retry == "" {
		t.Fatal("expected Retry-After header")
	} else if seconds, err := strconv.Atoi(retry); err != nil || seconds < 1 {
		t.Fatalf("expected positive integer Retry-After, got %q", retry)
	}
	if got := rr.Header().Get("X-RateLimit-Limit"); got != "1" {
		t.Fatalf("expected X-RateLimit-Limit=1, got %q", got)
	}
	if got := rr.Header().Get("X-RateLimit-Burst"); got != "1" {
		t.Fatalf("expected X-RateLimit-Burst=1, got %q", got)
	}
}
