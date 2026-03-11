package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSafetyAliasExpandsConcreteSourcesOnly(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		if strings.Contains(query, "FROM meta.source_registry FINAL") {
			_, _ = w.Write([]byte(mockSourceRegistryJSONLines(extractQuotedValues(query))))
			return
		}
		if strings.Contains(query, "SELECT count() FROM ops.crawl_frontier") {
			_, _ = w.Write([]byte("0\n"))
			return
		}
		if strings.Contains(query, "SELECT max(next_fetch_at) FROM ops.crawl_frontier") {
			_, _ = w.Write([]byte("\\N\n"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	ctx := context.WithValue(context.Background(), jobOptionsContextKey{}, jobOptions{SourceID: "fixture:safety"})
	if err := runIngestSafetySecurity(ctx); err != nil {
		t.Fatalf("runIngestSafetySecurity: %v", err)
	}
	joined := strings.Join(queries, "\n")
	for _, want := range []string{
		"fixture:opensanctions",
		"fixture:nasa-firms",
		"fixture:noaa-hazards",
		"fixture:kev",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected expanded concrete safety source %q, got %s", want, joined)
		}
	}
	if strings.Contains(joined, "orchestrated fetch stage for fixture:safety") {
		t.Fatalf("fixture:safety alias should not fetch directly, got %s", joined)
	}
}
