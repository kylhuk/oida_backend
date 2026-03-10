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
	version := getenv("API_VERSION", "v1")
	readyMarker := getenv("BOOTSTRAP_READY_MARKER", "/tmp/bootstrap.ready")
	mux := newAPIMux(version, readyMarker)

	addr := ":" + getenv("PORT", "8080")
	log.Printf("api listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func newAPIMux(version, readyMarker string) *http.ServeMux {
	return newAPIMuxWithServer(version, readyMarker, newAPIServer(version))
}

func newAPIMuxWithServer(version, readyMarker string, server *apiServer) *http.ServeMux {
	mux := http.NewServeMux()
	routes := []string{
		"/v1/health", "/v1/ready", "/v1/version", "/v1/schema",
		"/v1/jobs", "/v1/jobs/{jobId}",
		"/v1/sources", "/v1/sources/{sourceId}", "/v1/sources/{sourceId}/coverage",
		"/v1/places", "/v1/places/{placeId}", "/v1/places/{placeId}/children", "/v1/places/{placeId}/metrics", "/v1/places/{placeId}/events", "/v1/places/{placeId}/observations",
		"/v1/entities", "/v1/entities/{entityId}", "/v1/entities/{entityId}/tracks", "/v1/entities/{entityId}/events", "/v1/entities/{entityId}/places",
		"/v1/events", "/v1/events/{eventId}", "/v1/observations", "/v1/observations/{recordId}", "/v1/metrics", "/v1/metrics/{metricId}",
		"/v1/analytics/rollups", "/v1/analytics/time-series", "/v1/analytics/hotspots", "/v1/analytics/cross-domain",
		"/v1/search", "/v1/search/places", "/v1/search/entities",
	}

	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, r *http.Request) { respond(w, version, envelope{"status": "ok"}) })
	mux.HandleFunc("GET /v1/ready", readyHandler(version, readyMarker))
	mux.HandleFunc("GET /v1/version", func(w http.ResponseWriter, r *http.Request) {
		respond(w, version, envelope{"service": "api", "api_version": version})
	})
	mux.HandleFunc("GET /v1/schema", func(w http.ResponseWriter, r *http.Request) { respond(w, version, envelope{"endpoints": routes}) })
	mux.HandleFunc("GET /v1/jobs", server.listHandler(jobResource))
	mux.HandleFunc("GET /v1/jobs/{jobId}", server.detailHandler(jobResource))
	mux.HandleFunc("GET /v1/sources", server.listHandler(sourceResource))
	mux.HandleFunc("GET /v1/sources/{sourceId}", server.detailHandler(sourceResource))
	mux.HandleFunc("GET /v1/sources/{sourceId}/coverage", server.listHandler(sourceCoverageResource))
	mux.HandleFunc("GET /v1/places", server.listHandler(placeResource))
	mux.HandleFunc("GET /v1/places/{placeId}", server.detailHandler(placeResource))
	mux.HandleFunc("GET /v1/places/{placeId}/children", server.listHandler(placeChildResource))
	mux.HandleFunc("GET /v1/places/{placeId}/metrics", server.listHandler(placeMetricResource))
	mux.HandleFunc("GET /v1/places/{placeId}/events", server.listHandler(placeEventResource))
	mux.HandleFunc("GET /v1/places/{placeId}/observations", server.listHandler(placeObservationResource))
	mux.HandleFunc("GET /v1/entities", server.listHandler(entityResource))
	mux.HandleFunc("GET /v1/entities/{entityId}", server.detailHandler(entityResource))
	mux.HandleFunc("GET /v1/entities/{entityId}/tracks", server.listHandler(entityTrackResource))
	mux.HandleFunc("GET /v1/entities/{entityId}/events", server.listHandler(entityEventResource))
	mux.HandleFunc("GET /v1/entities/{entityId}/places", server.listHandler(entityPlaceResource))
	mux.HandleFunc("GET /v1/events", server.listHandler(eventResource))
	mux.HandleFunc("GET /v1/events/{eventId}", server.detailHandler(eventResource))
	mux.HandleFunc("GET /v1/observations", server.listHandler(observationResource))
	mux.HandleFunc("GET /v1/observations/{recordId}", server.detailHandler(observationResource))
	mux.HandleFunc("GET /v1/metrics", server.listHandler(metricResource))
	mux.HandleFunc("GET /v1/metrics/{metricId}", server.detailHandler(metricResource))
	mux.HandleFunc("GET /v1/analytics/rollups", server.listHandler(rollupResource))
	mux.HandleFunc("GET /v1/analytics/time-series", server.listHandler(timeSeriesResource))
	mux.HandleFunc("GET /v1/analytics/hotspots", server.listHandler(hotspotResource))
	mux.HandleFunc("GET /v1/analytics/cross-domain", server.listHandler(crossDomainResource))
	mux.HandleFunc("GET /v1/search", server.combinedSearchHandler())
	mux.HandleFunc("GET /v1/search/places", server.listHandler(searchPlaceResource))
	mux.HandleFunc("GET /v1/search/entities", server.listHandler(searchEntityResource))

	return mux
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
