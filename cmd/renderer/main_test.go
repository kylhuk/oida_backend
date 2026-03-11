package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRendererHealth(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"api_version":"v1","data":{}}`))
	}))
	defer api.Close()

	ts := httptest.NewServer(newMux(api.URL, api.Client()))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("health request: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if strings.TrimSpace(string(body)) != "ok" {
		t.Fatalf("expected ok, got %s", string(body))
	}
}

func TestRendererRootServesDashboard(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"api_version":"v1","data":{}}`))
	}))
	defer api.Close()

	ts := httptest.NewServer(newMux(api.URL, api.Client()))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("root request: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "Pipeline overview") {
		t.Fatalf("expected dashboard heading, got %s", string(body))
	}
}

func TestRendererStatsProxy(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/internal/stats" {
			t.Fatalf("unexpected upstream path: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"api_version":"v1","data":{"summary":{"sources_total":7}}}`))
	}))
	defer api.Close()

	ts := httptest.NewServer(newMux(api.URL, api.Client()))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/stats")
	if err != nil {
		t.Fatalf("stats request: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "sources_total") {
		t.Fatalf("expected proxied stats payload, got %s", string(body))
	}
}
