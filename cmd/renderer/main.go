package main

import (
	"embed"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
	"time"
)

//go:embed assets/dist/* templates/*
var uiAssets embed.FS

func main() {
	apiBaseURL := strings.TrimRight(getenv("API_BASE_URL", "http://api:8080"), "/")
	port := getenv("PORT", "8090")
	mux := newMux(apiBaseURL, &http.Client{Timeout: 8 * time.Second})
	log.Printf("renderer dashboard listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func newMux(apiBaseURL string, client *http.Client) *http.ServeMux {
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
		upstream, err := http.NewRequestWithContext(r.Context(), http.MethodGet, apiBaseURL+"/v1/internal/stats", nil)
		if err != nil {
			http.Error(w, "invalid stats upstream request", http.StatusInternalServerError)
			return
		}
		resp, err := client.Do(upstream)
		if err != nil {
			http.Error(w, "stats upstream unavailable", http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	})
	return mux
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
