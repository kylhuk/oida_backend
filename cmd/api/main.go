package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"global-osint-backend/internal/observability"
)

type envelope map[string]any

type corsConfig struct {
	allowedOrigins map[string]struct{}
}

func main() {
	version := getenv("API_VERSION", "v1")
	readyMarker := getenv("BOOTSTRAP_READY_MARKER", "/tmp/bootstrap.ready")
	handler := newAPIMux(version, readyMarker)

	addr := ":" + getenv("PORT", "8080")
	log.Printf("api listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, handler))
}

func newAPIMux(version, readyMarker string) http.Handler {
	return newAPIMuxWithServer(version, readyMarker, newAPIServer(version))
}

func newAPIMuxWithServer(version, readyMarker string, server *apiServer) http.Handler {
	mux := http.NewServeMux()
	routes := buildRouteContracts(version, readyMarker, server)
	mux.HandleFunc(http.MethodGet+" /metrics", metricsHandler(readyMarker, getenv("METRICS_SHARED_KEY", "")))
	registerRouteContracts(mux, routes)
	config := parseCORSConfig(getenv("API_CORS_ALLOW_ORIGINS", "http://localhost:3000,http://localhost:5173"))
	authenticator := server.authenticator
	if authenticator == nil {
		authenticator = denyAPIKeyAuthenticator{}
	}
	auth := withAPIKeyAuth(mux, version, routes, authenticator)
	rateLimited := withRateLimit(auth, version, newClientRateLimiterFromEnv())
	return withRequestObservability(withCORS(rateLimited, config))
}

type responseStatusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *responseStatusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func withRequestObservability(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		correlationID := observability.CorrelationIDFromRequest(r, "api")
		w.Header().Set(observability.RequestIDHeader, correlationID)
		start := time.Now()
		recorder := &responseStatusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r.WithContext(observability.WithCorrelationID(r.Context(), correlationID)))
		apiMetrics.recordRequest(recorder.status)
		observability.LogEvent("api", "request_completed", correlationID, map[string]any{"method": r.Method, "path": r.URL.Path, "status": recorder.status, "duration_ms": time.Since(start).Milliseconds()})
	})
}

func registerRouteContracts(mux *http.ServeMux, contracts []apiRouteContract) {
	for _, contract := range contracts {
		mux.HandleFunc(contract.Method+" "+contract.Path, contract.handler)
	}
}

func parseCORSConfig(raw string) corsConfig {
	allowed := make(map[string]struct{})
	for _, part := range strings.Split(raw, ",") {
		origin := strings.TrimSpace(part)
		if origin == "" {
			continue
		}
		allowed[origin] = struct{}{}
	}
	return corsConfig{allowedOrigins: allowed}
}

func withCORS(next http.Handler, config corsConfig) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin == "" {
			next.ServeHTTP(w, r)
			return
		}

		_, allowed := config.allowedOrigins[origin]
		if isPreflightRequest(r) {
			if !allowed || !allowedPreflightMethod(r.Header.Get("Access-Control-Request-Method")) {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			setCORSHeaders(w, origin, preflightAllowHeaders(r.Header.Get("Access-Control-Request-Headers")))
			w.WriteHeader(http.StatusNoContent)
			return
		}

		if allowed {
			setCORSHeaders(w, origin, "")
		}
		next.ServeHTTP(w, r)
	})
}

func isPreflightRequest(r *http.Request) bool {
	if r.Method != http.MethodOptions {
		return false
	}
	return strings.TrimSpace(r.Header.Get("Access-Control-Request-Method")) != ""
}

func allowedPreflightMethod(method string) bool {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodOptions:
		return true
	default:
		return false
	}
}

func preflightAllowHeaders(requestHeaders string) string {
	headers := strings.TrimSpace(requestHeaders)
	if headers == "" {
		return "Content-Type, Authorization, X-API-Key, " + observability.RequestIDHeader
	}
	parts := strings.Split(headers, ",")
	normalized := make([]string, 0, len(parts)+1)
	seen := make(map[string]struct{}, len(parts)+1)
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		key := strings.ToLower(name)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		normalized = append(normalized, name)
	}
	if _, ok := seen[strings.ToLower(apiKeyHeader)]; !ok {
		normalized = append(normalized, apiKeyHeader)
	}
	if _, ok := seen[strings.ToLower(observability.RequestIDHeader)]; !ok {
		normalized = append(normalized, observability.RequestIDHeader)
	}
	return strings.Join(normalized, ", ")
}

func withAPIKeyAuth(next *http.ServeMux, apiVersion string, contracts []apiRouteContract, authenticator apiKeyAuthenticator) http.Handler {
	contractsByPattern := make(map[string]apiRouteContract, len(contracts))
	for _, contract := range contracts {
		contractsByPattern[contract.Method+" "+contract.Path] = contract
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}

		_, pattern := next.Handler(r)
		contract, ok := contractsByPattern[pattern]
		if !ok {
			next.ServeHTTP(w, r)
			return
		}
		if !contract.Auth.Required {
			next.ServeHTTP(w, r)
			return
		}

		provided := strings.TrimSpace(r.Header.Get(apiKeyHeader))
		if provided == "" {
			apiMetrics.recordAuthFailure()
			respondError(w, apiVersion, http.StatusUnauthorized, "unauthorized", "missing or invalid api key", r.URL.Path)
			return
		}
		if _, err := authenticator.AuthenticateAPIKey(r.Context(), provided, contract.Auth.Scopes); err != nil {
			apiMetrics.recordAuthFailure()
			if errors.Is(err, errAPIKeyForbidden) {
				respondError(w, apiVersion, http.StatusForbidden, "forbidden", "api key is missing required scope", r.URL.Path)
				return
			}
			respondError(w, apiVersion, http.StatusUnauthorized, "unauthorized", "missing or invalid api key", r.URL.Path)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func setCORSHeaders(w http.ResponseWriter, origin, allowHeaders string) {
	w.Header().Add("Vary", "Origin")
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, POST, OPTIONS")
	w.Header().Set("Access-Control-Expose-Headers", observability.RequestIDHeader)
	if allowHeaders != "" {
		w.Header().Set("Access-Control-Allow-Headers", allowHeaders)
	}
}

func listStub(apiVersion, kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		respond(w, apiVersion, envelope{"kind": kind, "items": []any{}, "path": r.URL.Path})
	}
}

func readyHandler(apiVersion, markerPath string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		respond(w, apiVersion, envelope{"ready": bootstrapReady(markerPath)})
	}
}

func bootstrapReady(markerPath string) bool {
	if markerPath == "" {
		return false
	}
	info, err := os.Stat(filepath.Clean(markerPath))
	if err != nil {
		return false
	}
	return !info.IsDir()
}

func respond(w http.ResponseWriter, apiVersion string, data envelope) {
	respondStatus(w, http.StatusOK, apiVersion, data)
}

func respondStatus(w http.ResponseWriter, status int, apiVersion string, data envelope) {
	payload := envelope{"api_version": apiVersion, "schema_version": 1, "generated_at": time.Now().UTC().Format(time.RFC3339), "data": data}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
