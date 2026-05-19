package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestClickhouseClient(server *httptest.Server) *clickhouseClient {
	return &clickhouseClient{
		baseURL: server.URL,
		client:  server.Client(),
	}
}

func TestExecFormatJSON(t *testing.T) {
	const responseBody = `{"data":[{"entity_id":"ent:001"}],"rows":1,"rows_before_limit_at_least":42}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(responseBody))
	}))
	defer srv.Close()

	client := newTestClickhouseClient(srv)
	resp, err := client.Exec(context.Background(), ExecRequest{
		SQL:    "SELECT entity_id FROM gold.entities LIMIT 1",
		Format: "JSON",
	})
	if err != nil {
		t.Fatalf("Exec returned error: %v", err)
	}
	if len(resp.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(resp.Rows))
	}
	if got, want := resp.Rows[0]["entity_id"], "ent:001"; got != want {
		t.Errorf("entity_id: got %q, want %q", got, want)
	}
	if resp.RowsBeforeLimitAtLeast == nil {
		t.Fatal("RowsBeforeLimitAtLeast is nil")
	}
	if got, want := *resp.RowsBeforeLimitAtLeast, uint64(42); got != want {
		t.Errorf("RowsBeforeLimitAtLeast: got %d, want %d", got, want)
	}
}

func TestExecParamBinding(t *testing.T) {
	var capturedURL string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedURL = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := newTestClickhouseClient(srv)
	_, err := client.Exec(context.Background(), ExecRequest{
		SQL:    "SELECT 1",
		Params: map[string]string{"name": "hello"},
	})
	if err != nil {
		t.Fatalf("Exec returned error: %v", err)
	}
	if !strings.Contains(capturedURL, "param_name=hello") {
		t.Errorf("expected param_name=hello in URL query, got: %s", capturedURL)
	}
}

func TestExecSettingBinding(t *testing.T) {
	var capturedURL string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedURL = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := newTestClickhouseClient(srv)
	_, err := client.Exec(context.Background(), ExecRequest{
		SQL:      "SELECT 1",
		Settings: map[string]string{"max_execution_time": "5"},
	})
	if err != nil {
		t.Fatalf("Exec returned error: %v", err)
	}
	if !strings.Contains(capturedURL, "max_execution_time=5") {
		t.Errorf("expected max_execution_time=5 in URL query, got: %s", capturedURL)
	}
}
