package parser

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

const (
	parseEvidencePath     = ".sisyphus/evidence/task-12-parse.txt"
	parseEdgeEvidencePath = ".sisyphus/evidence/task-12-parse-edge.txt"
)

func TestStructuredParsers(t *testing.T) {
	registry := DefaultRegistry()
	htmlProfile := &HTMLProfile{
		Name: "fixture-profile",
		Fields: []HTMLField{
			{Name: "name", Selector: "article.profile > h1.name", Required: true},
			{Name: "website", Selector: "article.profile a.site", Attr: "href", Required: true},
			{Name: "data_id", XPath: "//article[@data-id='person-42']", Attr: "data-id", Required: true},
			{Name: "tags", XPath: "//ul[@class='tags']/li", All: true},
		},
	}

	cases := []struct {
		name       string
		input      Input
		wantParser string
		wantCount  int
		assert     func(t *testing.T, result Result)
	}{
		{
			name:       "json",
			input:      Input{SourceID: "fixture:site", RawID: "raw:json", ContentType: "application/json", Body: []byte(`[{"id":"json-1","title":"Alpha"},{"id":"json-2","title":"Bravo"}]`)},
			wantParser: "parser:json",
			wantCount:  2,
			assert: func(t *testing.T, result Result) {
				if got := result.Candidates[0].Data["title"]; got != "Alpha" {
					t.Fatalf("expected Alpha, got %v", got)
				}
			},
		},
		{
			name:       "csv",
			input:      Input{SourceID: "fixture:site", RawID: "raw:csv", FormatHint: "csv", Body: []byte("id,name\n1,Alice\n2,Bob\n")},
			wantParser: "parser:csv",
			wantCount:  2,
			assert: func(t *testing.T, result Result) {
				if got := result.Candidates[1].Data["name"]; got != "Bob" {
					t.Fatalf("expected Bob, got %v", got)
				}
			},
		},
		{
			name:       "tsv",
			input:      Input{SourceID: "fixture:site", RawID: "raw:tsv", FormatHint: "tsv", Body: []byte("id\tstatus\n10\tactive\n")},
			wantParser: "parser:csv",
			wantCount:  1,
			assert: func(t *testing.T, result Result) {
				if got := result.Candidates[0].Data["status"]; got != "active" {
					t.Fatalf("expected active, got %v", got)
				}
			},
		},
		{
			name:       "xml",
			input:      Input{SourceID: "fixture:site", RawID: "raw:xml", ContentType: "application/xml", Body: []byte(`<people><person id="person-1"><name>Alice</name></person></people>`)},
			wantParser: "parser:xml",
			wantCount:  1,
			assert: func(t *testing.T, result Result) {
				if got := result.Candidates[0].Data["name"]; got != "people" {
					t.Fatalf("expected xml root people, got %v", got)
				}
			},
		},
		{
			name:       "rss",
			input:      Input{SourceID: "fixture:site", RawID: "raw:rss", ContentType: "application/rss+xml", Body: []byte(`<rss version="2.0"><channel><title>Feed</title><link>https://example.com</link><item><title>One</title><link>https://example.com/one</link><guid>item-1</guid><pubDate>Tue, 10 Mar 2026 12:00:00 GMT</pubDate></item></channel></rss>`)},
			wantParser: "parser:rss",
			wantCount:  1,
			assert: func(t *testing.T, result Result) {
				if got := result.Candidates[0].Data["feed_type"]; got != "rss" {
					t.Fatalf("expected rss feed_type, got %v", got)
				}
			},
		},
		{
			name:       "atom",
			input:      Input{SourceID: "fixture:site", RawID: "raw:atom", ContentType: "application/atom+xml", Body: []byte(`<feed xmlns="http://www.w3.org/2005/Atom"><title>Atom Feed</title><id>feed-1</id><updated>2026-03-10T12:00:00Z</updated><entry><title>Entry</title><id>atom-1</id><updated>2026-03-10T12:00:00Z</updated><summary>Example</summary><link rel="alternate" href="https://example.com/atom-1"></link></entry></feed>`)},
			wantParser: "parser:atom",
			wantCount:  1,
			assert: func(t *testing.T, result Result) {
				if got := result.Candidates[0].Data["feed_type"]; got != "atom" {
					t.Fatalf("expected atom feed_type, got %v", got)
				}
			},
		},
		{
			name:       "html_profile",
			input:      Input{ParserID: "parser:html-profile", SourceID: "fixture:site", RawID: "raw:html", ContentType: "text/html", Profile: htmlProfile, Body: []byte(`<html><body><article class="profile" data-id="person-42"><h1 class="name">Jane Doe</h1><a class="site" href="https://example.com/profile">Profile</a><ul class="tags"><li>analyst</li><li>osint</li></ul></article></body></html>`)},
			wantParser: "parser:html-profile",
			wantCount:  1,
			assert: func(t *testing.T, result Result) {
				candidate := result.Candidates[0]
				if got := candidate.Data["name"]; got != "Jane Doe" {
					t.Fatalf("expected Jane Doe, got %v", got)
				}
				tags, ok := candidate.Data["tags"].([]any)
				if !ok || len(tags) != 2 {
					t.Fatalf("expected two tags, got %#v", candidate.Data["tags"])
				}
			},
		},
	}

	var evidence strings.Builder
	evidence.WriteString("case\tparser_id\tcandidates\tnative_id\tkind\n")
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := registry.Parse(context.Background(), tc.input)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if result.ParserID != tc.wantParser {
				t.Fatalf("expected parser %s, got %s", tc.wantParser, result.ParserID)
			}
			if len(result.Candidates) != tc.wantCount {
				t.Fatalf("expected %d candidates, got %d", tc.wantCount, len(result.Candidates))
			}
			tc.assert(t, result)
			first := result.Candidates[0]
			evidence.WriteString(fmt.Sprintf("%s\t%s\t%d\t%s\t%s\n", tc.name, result.ParserID, len(result.Candidates), first.NativeID, first.Kind))
		})
	}

	writeEvidenceFile(t, parseEvidencePath, []byte(evidence.String()))
}

func TestStructuredParsersEdgeCases(t *testing.T) {
	registry := DefaultRegistry()
	htmlProfile := &HTMLProfile{Fields: []HTMLField{{Name: "missing", Selector: ".nope", Required: true}}}
	cases := []struct {
		name  string
		input Input
		want  string
	}{
		{name: "invalid_json", input: Input{ContentType: "application/json", Body: []byte(`{"id":`)}, want: CodeInvalidJSON},
		{name: "schema_drift_csv", input: Input{FormatHint: "csv", Body: []byte("id,name\n1\n")}, want: CodeSchemaDrift},
		{name: "invalid_xml", input: Input{ContentType: "application/xml", Body: []byte(`<people><name>Alice</people>`)}, want: CodeInvalidXML},
		{name: "invalid_rss", input: Input{ContentType: "application/rss+xml", Body: []byte(`<rss version="2.0"><channel><title>Feed</title></channel></rss>`)}, want: CodeInvalidFeed},
		{name: "missing_selector", input: Input{ParserID: "parser:html-profile", ContentType: "text/html", Profile: htmlProfile, Body: []byte(`<html><body><div class="profile"></div></body></html>`)}, want: CodeSelectorNotFound},
	}

	var evidence strings.Builder
	evidence.WriteString("case\tcode\tmessage\n")
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := registry.Parse(context.Background(), tc.input)
			if err == nil {
				t.Fatal("expected parser error")
			}
			if err.Code != tc.want {
				t.Fatalf("expected error code %s, got %s", tc.want, err.Code)
			}
			evidence.WriteString(fmt.Sprintf("%s\t%s\t%s\n", tc.name, err.Code, err.Message))
		})
	}

	writeEvidenceFile(t, parseEdgeEvidencePath, []byte(evidence.String()))
}
