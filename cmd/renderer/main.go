package main

import (
	"embed"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"global-osint-backend/internal/observability"
)

//go:embed assets/dist/* templates/*
var uiAssets embed.FS

const apiKeyHeader = "X-API-Key"

func main() {
	apiBaseURL := strings.TrimRight(getenv("API_BASE_URL", "http://api:8080"), "/")
	apiSharedKey := strings.TrimSpace(getenv("API_SHARED_KEY", ""))
	port := getenv("PORT", "8090")
	mux := newMux(apiBaseURL, apiSharedKey, &http.Client{Timeout: 8 * time.Second})
	observability.LogEvent("renderer", "service_started", observability.NewCorrelationID("renderer"), map[string]any{"port": port})
	_ = http.ListenAndServe(":"+port, mux)
}

func newMux(apiBaseURL, apiSharedKey string, client *http.Client) http.Handler {
	if client == nil {
		client = &http.Client{Timeout: 8 * time.Second}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		html, err := uiAssets.ReadFile("templates/index.html")
		if err != nil {
			http.Error(w, "failed to load renderer template", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(html)
	})
	mux.HandleFunc("GET /static/", func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/static/")
		if name == "" || strings.Contains(name, "..") {
			http.NotFound(w, r)
			return
		}
		b, err := uiAssets.ReadFile(path.Join("assets/dist", name))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		if strings.HasSuffix(name, ".css") {
			w.Header().Set("Content-Type", "text/css; charset=utf-8")
		}
		if strings.HasSuffix(name, ".js") {
			w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
		}
		_, _ = w.Write(b)
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		proxyAPIRequest(w, r, client, apiBaseURL+"/v1/internal/stats", apiSharedKey)
	})
	mux.HandleFunc("GET /tail", func(w http.ResponseWriter, r *http.Request) {
		proxyAPIRequest(w, r, client, apiBaseURL+"/v1/internal/worker-tail", apiSharedKey)
	})
	return withRendererObservability(mux)
}

func withRendererObservability(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		correlationID := observability.CorrelationIDFromRequest(r, "renderer")
		w.Header().Set(observability.RequestIDHeader, correlationID)
		observability.LogEvent("renderer", "request_received", correlationID, map[string]any{"method": r.Method, "path": r.URL.Path})
		next.ServeHTTP(w, r.WithContext(observability.WithCorrelationID(r.Context(), correlationID)))
	})
}

func proxyAPIRequest(w http.ResponseWriter, r *http.Request, client *http.Client, upstreamURL, apiSharedKey string) {
	upstream, err := http.NewRequestWithContext(r.Context(), http.MethodGet, upstreamURL, nil)
	if err != nil {
		http.Error(w, "invalid upstream request", http.StatusInternalServerError)
		return
	}
	upstream.URL.RawQuery = r.URL.RawQuery
	if apiSharedKey != "" {
		upstream.Header.Set(apiKeyHeader, apiSharedKey)
	}
	if correlationID := observability.CorrelationID(r.Context()); correlationID != "" {
		upstream.Header.Set(observability.RequestIDHeader, correlationID)
	}
	resp, err := client.Do(upstream)
	if err != nil {
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	if contentType := strings.TrimSpace(resp.Header.Get("Content-Type")); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	if requestID := strings.TrimSpace(resp.Header.Get(observability.RequestIDHeader)); requestID != "" {
		w.Header().Set(observability.RequestIDHeader, requestID)
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
