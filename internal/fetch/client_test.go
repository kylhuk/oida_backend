package fetch

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	brotli "github.com/andybalholm/brotli"
)

func TestFetchWorker(t *testing.T) {
	t.Run("conditional get returns not modified", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if got := r.Header.Get("If-None-Match"); got != `"v1"` {
				t.Fatalf("expected If-None-Match header, got %q", got)
			}
			w.Header().Set("ETag", `"v1"`)
			w.WriteHeader(http.StatusNotModified)
		}))
		defer server.Close()

		client := NewClient(Config{})
		resp, err := client.Fetch(context.Background(), Request{
			URL:         server.URL,
			Conditional: ConditionalRequest{ETag: `"v1"`},
			Source:      SourcePolicy{SourceID: "fixture:site", RetentionClass: "warm", SupportsLiveGET: true},
		})
		if err != nil {
			t.Fatalf("fetch: %v", err)
		}
		if !resp.NotModified || resp.StatusCode != http.StatusNotModified {
			t.Fatalf("expected 304 not-modified, got status=%d notModified=%v", resp.StatusCode, resp.NotModified)
		}
		if !resp.Success {
			t.Fatal("expected conditional 304 to count as a successful fetch")
		}
	})

	t.Run("gzip and brotli bodies are decompressed and sniffed", func(t *testing.T) {
		cases := []struct {
			name     string
			encoding string
			encode   func([]byte) []byte
		}{
			{name: "gzip", encoding: "gzip", encode: gzipEncode},
			{name: "brotli", encoding: "br", encode: brotliEncode},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				payload := []byte("fixture text body")
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/octet-stream")
					w.Header().Set("Content-Encoding", tc.encoding)
					_, _ = w.Write(tc.encode(payload))
				}))
				defer server.Close()

				client := NewClient(Config{})
				resp, err := client.Fetch(context.Background(), Request{
					URL:    server.URL,
					Source: SourcePolicy{SourceID: "fixture:site", RetentionClass: "warm", SupportsLiveGET: true},
				})
				if err != nil {
					t.Fatalf("fetch: %v", err)
				}
				if !bytes.Equal(resp.Body, payload) {
					t.Fatalf("expected decoded payload %q, got %q", payload, resp.Body)
				}
				if resp.ContentType != "text/plain; charset=utf-8" {
					t.Fatalf("expected sniffed text/plain content type, got %q", resp.ContentType)
				}
			})
		}
	})

	t.Run("retries with exponential backoff", func(t *testing.T) {
		attempts := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts++
			if attempts < 3 {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("ok"))
		}))
		defer server.Close()

		var sleeps []time.Duration
		client := NewClient(Config{
			RetryPolicy: RetryPolicy{MaxAttempts: 3, InitialBackoff: 10 * time.Millisecond, MaxBackoff: 40 * time.Millisecond},
			Sleep: func(_ context.Context, delay time.Duration) error {
				sleeps = append(sleeps, delay)
				return nil
			},
		})
		resp, err := client.Fetch(context.Background(), Request{
			URL:    server.URL,
			Source: SourcePolicy{SourceID: "fixture:site", RetentionClass: "warm", SupportsLiveGET: true},
		})
		if err != nil {
			t.Fatalf("fetch: %v", err)
		}
		if attempts != 3 || resp.Attempts != 3 {
			t.Fatalf("expected 3 attempts, server=%d response=%d", attempts, resp.Attempts)
		}
		if !slices.Equal(sleeps, []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}) {
			t.Fatalf("unexpected backoff sequence: %v", sleeps)
		}
		if len(resp.RetryReasons) != 2 {
			t.Fatalf("expected two retry reasons, got %d", len(resp.RetryReasons))
		}
	})

	t.Run("max body guardrail rejects oversized payloads", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("0123456789abcdef"))
		}))
		defer server.Close()

		client := NewClient(Config{MaxBodyBytes: 8})
		_, err := client.Fetch(context.Background(), Request{
			URL:    server.URL,
			Source: SourcePolicy{SourceID: "fixture:site", RetentionClass: "warm", SupportsLiveGET: true},
		})
		if !errors.Is(err, ErrBodyTooLarge) {
			t.Fatalf("expected ErrBodyTooLarge, got %v", err)
		}
	})

	t.Run("disabled sources are blocked before fetch", func(t *testing.T) {
		client := NewClient(Config{})
		_, err := client.Fetch(context.Background(), Request{
			URL:    "https://example.com",
			Source: SourcePolicy{SourceID: "fixture:disabled", Disabled: true, DisabledReason: "legal hold", RetentionClass: "warm", SupportsLiveGET: true},
		})
		if !errors.Is(err, ErrSourceBlocked) {
			t.Fatalf("expected ErrSourceBlocked, got %v", err)
		}
	})
}

func gzipEncode(body []byte) []byte {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	_, _ = writer.Write(body)
	_ = writer.Close()
	return buf.Bytes()
}

func brotliEncode(body []byte) []byte {
	var buf bytes.Buffer
	writer := brotli.NewWriter(&buf)
	_, _ = writer.Write(body)
	_ = writer.Close()
	return buf.Bytes()
}
