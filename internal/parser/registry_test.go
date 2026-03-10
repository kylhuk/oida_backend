package parser

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestParserRegistry(t *testing.T) {
	registry := DefaultRegistry()
	records := registry.Records()
	if len(records) < 6 {
		t.Fatalf("expected parser registry records, got %d", len(records))
	}

	cases := []struct {
		name  string
		input Input
		want  string
	}{
		{name: "explicit_json", input: Input{ParserID: "parser:json"}, want: "parser:json"},
		{name: "content_type_json", input: Input{ContentType: "application/json"}, want: "parser:json"},
		{name: "format_tsv", input: Input{FormatHint: "tsv"}, want: "parser:csv"},
		{name: "rss", input: Input{ContentType: "application/rss+xml"}, want: "parser:rss"},
		{name: "atom", input: Input{Body: []byte("<feed xmlns=\"http://www.w3.org/2005/Atom\"></feed>")}, want: "parser:atom"},
		{name: "html_profile", input: Input{ParserID: "parser:html-profile", Profile: &HTMLProfile{}}, want: "parser:html-profile"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resolved, err := registry.Resolve(tc.input)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if got := resolved.Descriptor().ID; got != tc.want {
				t.Fatalf("expected %s, got %s", tc.want, got)
			}
			if resolved.Descriptor().Version == "" {
				t.Fatal("expected versioned parser descriptor")
			}
		})
	}

	if _, err := registry.Resolve(Input{ParserID: "parser:missing"}); err == nil || err.Code != CodeParserNotRegistered {
		t.Fatalf("expected missing parser contract, got %#v", err)
	}

	result, err := registry.Parse(context.Background(), Input{ContentType: "application/json", Body: []byte(`{"id":"demo"}`)})
	if err != nil {
		t.Fatalf("parse via registry: %v", err)
	}
	if result.ParserID != "parser:json" {
		t.Fatalf("expected registry parse to route to parser:json, got %s", result.ParserID)
	}
}

func writeEvidenceFile(tb testing.TB, relativePath string, content []byte) {
	tb.Helper()
	artifactPath := filepath.Join(mustRepoRoot(tb), relativePath)
	if err := os.MkdirAll(filepath.Dir(artifactPath), 0o755); err != nil {
		tb.Fatalf("mkdir evidence dir: %v", err)
	}
	if err := os.WriteFile(artifactPath, content, 0o644); err != nil {
		tb.Fatalf("write evidence file: %v", err)
	}
}

func mustRepoRoot(tb testing.TB) string {
	tb.Helper()
	wd, err := os.Getwd()
	if err != nil {
		tb.Fatalf("getwd: %v", err)
	}
	for dir := wd; dir != filepath.Dir(dir); dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
	}
	tb.Fatal("unable to locate repo root")
	return ""
}
