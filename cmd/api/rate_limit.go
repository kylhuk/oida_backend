package main

import (
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type clientRateLimiter struct {
	mu      sync.Mutex
	rps     float64
	burst   int
	now     func() time.Time
	buckets map[string]*rateLimitBucket
}

type rateLimitBucket struct {
	tokens  float64
	updated time.Time
}

func newClientRateLimiter(rps float64, burst int) *clientRateLimiter {
	if rps <= 0 || burst <= 0 {
		return nil
	}
	return &clientRateLimiter{
		rps:     rps,
		burst:   burst,
		now:     time.Now,
		buckets: make(map[string]*rateLimitBucket),
	}
}

func newClientRateLimiterFromEnv() *clientRateLimiter {
	return newClientRateLimiter(getenvFloat("API_RATE_LIMIT_RPS", 0), getenvInt("API_RATE_LIMIT_BURST", 0))
}

func (l *clientRateLimiter) allow(client string) (bool, time.Duration) {
	if l == nil {
		return true, 0
	}
	now := l.now().UTC()
	client = strings.TrimSpace(client)
	if client == "" {
		client = "unknown"
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	bucket, ok := l.buckets[client]
	if !ok {
		bucket = &rateLimitBucket{tokens: float64(l.burst), updated: now}
		l.buckets[client] = bucket
	}
	if elapsed := now.Sub(bucket.updated).Seconds(); elapsed > 0 {
		bucket.tokens = math.Min(float64(l.burst), bucket.tokens+(elapsed*l.rps))
		bucket.updated = now
	}
	if bucket.tokens >= 1 {
		bucket.tokens--
		return true, 0
	}
	missing := 1 - bucket.tokens
	retrySeconds := missing / l.rps
	return false, time.Duration(math.Ceil(retrySeconds)) * time.Second
}

func withRateLimit(next http.Handler, apiVersion string, limiter *clientRateLimiter) http.Handler {
	if limiter == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" || r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}
		allowed, retryAfter := limiter.allow(rateLimitClientID(r))
		if allowed {
			w.Header().Set("X-RateLimit-Limit", strconv.FormatFloat(limiter.rps, 'f', -1, 64))
			w.Header().Set("X-RateLimit-Burst", strconv.Itoa(limiter.burst))
			next.ServeHTTP(w, r)
			return
		}
		if retryAfter <= 0 {
			retryAfter = time.Second
		}
		w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(retryAfter.Seconds()))))
		w.Header().Set("X-RateLimit-Limit", strconv.FormatFloat(limiter.rps, 'f', -1, 64))
		w.Header().Set("X-RateLimit-Burst", strconv.Itoa(limiter.burst))
		respondError(w, apiVersion, http.StatusTooManyRequests, "rate_limited", "rate limit exceeded", r.URL.Path)
	})
}

func rateLimitClientID(r *http.Request) string {
	if rawKey := strings.TrimSpace(r.Header.Get(apiKeyHeader)); rawKey != "" {
		return "api-key:" + sha256Hex(rawKey)
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err == nil && host != "" {
		return "ip:" + host
	}
	if r.RemoteAddr != "" {
		return "ip:" + r.RemoteAddr
	}
	return "ip:unknown"
}

func getenvFloat(key string, defaultValue float64) float64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return defaultValue
	}
	return parsed
}

func getenvInt(key string, defaultValue int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return parsed
}
