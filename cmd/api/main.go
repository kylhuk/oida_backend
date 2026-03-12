package main

import (
	"crypto/subtle"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
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
	contracts := buildRouteContracts(version, readyMarker, server)
	for _, contract := range contracts {
		mux.HandleFunc(contract.Method+" "+contract.Path, contract.handler)
	}
	config := parseCORSConfig(getenv("API_CORS_ALLOW_ORIGINS", "http://localhost:3000,http://localhost:5173"))
	auth := withAPIKeyAuth(mux, version, contracts, getenv("API_SHARED_KEY", ""))
	return withCORS(auth, config)
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
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	default:
		return false
	}
}

func preflightAllowHeaders(requestHeaders string) string {
	headers := strings.TrimSpace(requestHeaders)
	if headers == "" {
		return "Content-Type, Authorization, X-API-Key"
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
	return strings.Join(normalized, ", ")
}

func withAPIKeyAuth(next http.Handler, apiVersion string, contracts []apiRouteContract, sharedKey string) http.Handler {
	publicPaths := make(map[string]struct{}, len(contracts))
	for _, contract := range contracts {
		if contract.Method != http.MethodGet {
			continue
		}
		if !contract.Auth.Required {
			publicPaths[contract.Path] = struct{}{}
		}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/v1/") {
			next.ServeHTTP(w, r)
			return
		}
		if _, ok := publicPaths[r.URL.Path]; ok {
			next.ServeHTTP(w, r)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			next.ServeHTTP(w, r)
			return
		}
		provided := strings.TrimSpace(r.Header.Get(apiKeyHeader))
		if sharedKey == "" || provided == "" || subtle.ConstantTimeCompare([]byte(sharedKey), []byte(provided)) != 1 {
			respondError(w, apiVersion, http.StatusUnauthorized, "unauthorized", "missing or invalid api key", r.URL.Path)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func setCORSHeaders(w http.ResponseWriter, origin, allowHeaders string) {
	w.Header().Add("Vary", "Origin")
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
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
