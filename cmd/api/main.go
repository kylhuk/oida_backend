package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

type envelope map[string]any

func main() {
	mux := http.NewServeMux()
	version := getenv("API_VERSION", "v1")
	readyMarker := getenv("BOOTSTRAP_READY_MARKER", "/tmp/bootstrap.ready")

	routes := []string{
		"/v1/health", "/v1/ready", "/v1/version", "/v1/schema",
		"/v1/jobs", "/v1/sources", "/v1/places", "/v1/entities", "/v1/events", "/v1/observations", "/v1/metrics",
		"/v1/analytics/rollups", "/v1/analytics/time-series", "/v1/analytics/hotspots", "/v1/analytics/cross-domain",
		"/v1/search", "/v1/search/places", "/v1/search/entities",
	}

	mux.HandleFunc("/v1/health", func(w http.ResponseWriter, r *http.Request) { respond(w, version, envelope{"status": "ok"}) })
	mux.HandleFunc("/v1/ready", readyHandler(version, readyMarker))
	mux.HandleFunc("/v1/version", func(w http.ResponseWriter, r *http.Request) { respond(w, version, envelope{"service": "api", "api_version": version}) })
	mux.HandleFunc("/v1/schema", func(w http.ResponseWriter, r *http.Request) { respond(w, version, envelope{"endpoints": routes}) })
	mux.HandleFunc("/v1/jobs", listStub(version, "jobs"))
	mux.HandleFunc("/v1/sources", listStub(version, "sources"))
	mux.HandleFunc("/v1/places", listStub(version, "places"))
	mux.HandleFunc("/v1/entities", listStub(version, "entities"))
	mux.HandleFunc("/v1/events", listStub(version, "events"))
	mux.HandleFunc("/v1/observations", listStub(version, "observations"))
	mux.HandleFunc("/v1/metrics", listStub(version, "metrics"))
	mux.HandleFunc("/v1/analytics/rollups", listStub(version, "rollups"))
	mux.HandleFunc("/v1/analytics/time-series", listStub(version, "time_series"))
	mux.HandleFunc("/v1/analytics/hotspots", listStub(version, "hotspots"))
	mux.HandleFunc("/v1/analytics/cross-domain", listStub(version, "cross_domain"))
	mux.HandleFunc("/v1/search", listStub(version, "search"))
	mux.HandleFunc("/v1/search/places", listStub(version, "search_places"))
	mux.HandleFunc("/v1/search/entities", listStub(version, "search_entities"))

	addr := ":" + getenv("PORT", "8080")
	log.Printf("api listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
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
	payload := envelope{"api_version": apiVersion, "schema_version": 1, "generated_at": time.Now().UTC().Format(time.RFC3339), "data": data}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(payload)
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
