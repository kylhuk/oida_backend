package main

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var apiMetrics = newAPIMetrics()

type apiMetricsCollector struct {
	mu          sync.Mutex
	requests    map[string]uint64
	authFailure uint64
	jobFailures uint64
}

func newAPIMetrics() *apiMetricsCollector {
	return &apiMetricsCollector{requests: map[string]uint64{}}
}

func (m *apiMetricsCollector) recordRequest(status int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests[strconv.Itoa(status)]++
}

func (m *apiMetricsCollector) recordAuthFailure() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.authFailure++
}

func (m *apiMetricsCollector) render(ready bool) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var b strings.Builder
	b.WriteString("# HELP oida_ready API readiness state from the bootstrap marker.\n")
	b.WriteString("# TYPE oida_ready gauge\n")
	if ready {
		b.WriteString("oida_ready 1\n")
	} else {
		b.WriteString("oida_ready 0\n")
	}
	b.WriteString("# HELP oida_http_requests_total Total API HTTP requests by status code.\n")
	b.WriteString("# TYPE oida_http_requests_total counter\n")
	statuses := make([]string, 0, len(m.requests))
	for status := range m.requests {
		statuses = append(statuses, status)
	}
	sort.Strings(statuses)
	for _, status := range statuses {
		fmt.Fprintf(&b, "oida_http_requests_total{status=%q} %d\n", status, m.requests[status])
	}
	b.WriteString("# HELP oida_auth_failures_total Total API authentication or authorization failures.\n")
	b.WriteString("# TYPE oida_auth_failures_total counter\n")
	fmt.Fprintf(&b, "oida_auth_failures_total %d\n", m.authFailure)
	b.WriteString("# HELP oida_control_plane_jobs_total Control-plane job outcomes observed by the API process.\n")
	b.WriteString("# TYPE oida_control_plane_jobs_total counter\n")
	fmt.Fprintf(&b, "oida_control_plane_jobs_total{status=%q} %d\n", "failed", m.jobFailures)
	return b.String()
}

func metricsHandler(readyMarker, sharedKey string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if sharedKey == "" || !validMetricsBearer(r.Header.Get("Authorization"), sharedKey) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(apiMetrics.render(bootstrapReady(readyMarker))))
	}
}

func validMetricsBearer(header, sharedKey string) bool {
	token := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(header), "Bearer "))
	if token == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(sharedKey)) == 1
}
