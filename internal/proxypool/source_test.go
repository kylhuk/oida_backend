package proxypool

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseProxyLines_BareIPPort(t *testing.T) {
	data := "1.2.3.4:8080\n5.6.7.8:3128\n"
	got := ParseProxyLines(data, "http")
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d: %v", len(got), got)
	}
	if got[0] != "http://1.2.3.4:8080" {
		t.Fatalf("unexpected %q", got[0])
	}
}

func TestParseProxyLines_WithProtocol(t *testing.T) {
	data := "socks5://1.2.3.4:1080\nhttp://9.8.7.6:3128"
	got := ParseProxyLines(data, "")
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
	if got[0] != "socks5://1.2.3.4:1080" {
		t.Fatalf("unexpected %q", got[0])
	}
}

func TestParseProxyLines_Comments(t *testing.T) {
	data := "# this is a comment\n1.2.3.4:8080\n"
	got := ParseProxyLines(data, "http")
	if len(got) != 1 {
		t.Fatalf("expected 1, got %d", len(got))
	}
}

func TestParseProxyLines_Dedup(t *testing.T) {
	data := "1.2.3.4:8080\n1.2.3.4:8080\n"
	got := ParseProxyLines(data, "http")
	if len(got) != 1 {
		t.Fatalf("expected 1 after dedup, got %d", len(got))
	}
}

func TestParseProxyLines_DefaultProtocol(t *testing.T) {
	data := "1.2.3.4:1080"
	got := ParseProxyLines(data, "socks5")
	if len(got) != 1 || got[0] != "socks5://1.2.3.4:1080" {
		t.Fatalf("unexpected %v", got)
	}
}

func TestParseSourcesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "proxy_sources.txt")
	content := "# header\nhttp https://example.com/list.txt\nhttps://other.com/proxies\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	records, err := parseSourcesFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if records[0].protocol != "http" || records[0].url != "https://example.com/list.txt" {
		t.Fatalf("record[0] wrong: %+v", records[0])
	}
	if records[1].protocol != "" || records[1].url != "https://other.com/proxies" {
		t.Fatalf("record[1] wrong: %+v", records[1])
	}
}

func TestRefresher_FetchesAndPopulatesPool(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("1.2.3.4:8080\n5.6.7.8:3128\n"))
	}))
	defer srv.Close()

	dir := t.TempDir()
	path := filepath.Join(dir, "proxy_sources.txt")
	os.WriteFile(path, []byte("http "+srv.URL+"\n"), 0644)

	pool := New()
	r := NewRefresher(path, pool, time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	_, total := pool.Stats()
	if total != 2 {
		t.Fatalf("expected 2 proxies, got %d", total)
	}
}
