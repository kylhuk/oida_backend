package discovery

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDiscoveryRobotsPolicy(t *testing.T) {
	t.Run("unavailable 4xx allows crawling", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.NotFound(w, r)
		}))
		defer server.Close()

		fetcher := NewRobotsFetcher(server.Client(), "ExampleBot")
		result, err := fetcher.Fetch(context.Background(), server.URL)
		if err != nil {
			t.Fatalf("fetch robots: %v", err)
		}
		if result.Status != RobotsStatusUnavailable {
			t.Fatalf("expected unavailable, got %s", result.Status)
		}
		if !result.Allowed(server.URL + "/private") {
			t.Fatal("expected 4xx robots to allow crawling")
		}
	})

	t.Run("unreachable 5xx disallows crawling", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "boom", http.StatusServiceUnavailable)
		}))
		defer server.Close()

		fetcher := NewRobotsFetcher(server.Client(), "ExampleBot")
		result, err := fetcher.Fetch(context.Background(), server.URL)
		if err == nil {
			t.Fatal("expected unreachable robots fetch to return error")
		}
		if result.Status != RobotsStatusUnreachable {
			t.Fatalf("expected unreachable, got %s", result.Status)
		}
		if result.Allowed(server.URL + "/anything") {
			t.Fatal("expected 5xx robots to deny crawling")
		}
	})

	t.Run("redirects preserve rules and allow/disallow precedence", func(t *testing.T) {
		final := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "User-agent: *\nDisallow: /private\nAllow: /private/public\nSitemap: "+r.Host+"/sitemap.xml\n")
		}))
		defer final.Close()

		redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, final.URL+"/robots-final.txt", http.StatusFound)
		}))
		defer redirect.Close()

		fetcher := NewRobotsFetcher(redirect.Client(), "ExampleBot")
		result, err := fetcher.Fetch(context.Background(), redirect.URL)
		if err != nil {
			t.Fatalf("fetch robots via redirect: %v", err)
		}
		if result.Status != RobotsStatusSuccessful {
			t.Fatalf("expected successful redirect fetch, got %s", result.Status)
		}
		privateURL := mustURL(t, redirect.URL+"/private/report")
		publicURL := mustURL(t, redirect.URL+"/private/public/post")
		if result.Document.Allowed("ExampleBot", privateURL) {
			t.Fatal("expected disallow rule to block /private/report")
		}
		if !result.Document.Allowed("ExampleBot", publicURL) {
			t.Fatal("expected longer allow rule to win for /private/public/post")
		}
	})

	t.Run("cache reuses robots response", func(t *testing.T) {
		var hits atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.Header().Set("Cache-Control", "max-age=3600")
			fmt.Fprint(w, "User-agent: *\nDisallow: /cached\n")
		}))
		defer server.Close()

		fetcher := NewRobotsFetcher(server.Client(), "ExampleBot")
		first, err := fetcher.Fetch(context.Background(), server.URL)
		if err != nil {
			t.Fatalf("first fetch: %v", err)
		}
		second, err := fetcher.Fetch(context.Background(), server.URL)
		if err != nil {
			t.Fatalf("second fetch: %v", err)
		}
		if hits.Load() != 1 {
			t.Fatalf("expected one network fetch, got %d", hits.Load())
		}
		if second.Cached != true {
			t.Fatal("expected second robots response to be served from cache")
		}
		if second.Document.Allowed("ExampleBot", mustURL(t, server.URL+"/cached")) {
			t.Fatal("expected cached disallow rule to be enforced")
		}
		if first.ExpiresAt.IsZero() || !first.ExpiresAt.After(first.FetchedAt) {
			t.Fatal("expected cache expiry to be populated")
		}
	})
}

func TestDiscoverySitemapsAndFeeds(t *testing.T) {
	var serverURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sitemap.xml":
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>%s/section/posts.xml.gz</loc></sitemap>
  <sitemap><loc>%s/feeds/rss.xml</loc></sitemap>
  <sitemap><loc>%s/feeds/atom.xml</loc></sitemap>
</sitemapindex>`, serverURL, serverURL, serverURL)
		case "/section/posts.xml.gz":
			w.Header().Set("Content-Type", "application/gzip")
			_, _ = w.Write(gzipFixture(t, fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>%s/section/news/alpha?utm_source=test&amp;b=2&amp;a=1</loc>
    <lastmod>2026-03-09T09:00:00Z</lastmod>
    <changefreq>daily</changefreq>
    <priority>0.9</priority>
  </url>
  <url>
    <loc>%s/section/news/alpha?a=1&amp;b=2</loc>
    <lastmod>2026-03-09T10:00:00Z</lastmod>
    <priority>0.8</priority>
  </url>
  <url>
    <loc>%s/section/archive/beta</loc>
    <lastmod>2026-02-01</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.5</priority>
  </url>
  <url>
    <loc>https://offsite.example.com/escape</loc>
  </url>
</urlset>`, serverURL, serverURL, serverURL)))
		case "/feeds/rss.xml":
			w.Header().Set("Content-Type", "application/rss+xml")
			fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Fixture RSS</title>
    <item>
      <guid>rss-1</guid>
      <title>Gamma</title>
      <link>%s/section/news/gamma</link>
      <pubDate>Mon, 10 Mar 2026 10:00:00 +0000</pubDate>
    </item>
  </channel>
</rss>`, serverURL)
		case "/feeds/atom.xml":
			w.Header().Set("Content-Type", "application/atom+xml")
			fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Fixture Atom</title>
  <entry>
    <id>atom-1</id>
    <title>Delta</title>
    <updated>2026-03-08T09:30:00Z</updated>
    <link rel="alternate" href="%s/section/briefs/delta" />
  </entry>
</feed>`, serverURL)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	serverURL = server.URL

	fetcher := NewSitemapFetcher(server.Client())
	result, err := fetcher.Fetch(context.Background(), server.URL+"/sitemap.xml")
	if err != nil {
		t.Fatalf("fetch sitemap index: %v", err)
	}
	if len(result.URLs) != 2 {
		t.Fatalf("expected 2 deduplicated sitemap urls, got %d", len(result.URLs))
	}
	if result.URLs[0].Loc != server.URL+"/section/archive/beta" {
		t.Fatalf("expected archive url to remain, got %q", result.URLs[0].Loc)
	}
	if result.URLs[1].Loc != server.URL+"/section/news/alpha?a=1&b=2" {
		t.Fatalf("expected normalized alpha url, got %q", result.URLs[1].Loc)
	}
	if len(result.Feeds) != 2 {
		t.Fatalf("expected 2 feed entries, got %d", len(result.Feeds))
	}
	if result.Feeds[0].Kind != "atom" || result.Feeds[1].Kind != "rss" {
		t.Fatalf("expected atom and rss feed entries, got %#v", result.Feeds)
	}
	if len(result.Files) != 4 {
		t.Fatalf("expected sitemap index plus three children, got %d files", len(result.Files))
	}
}

func TestFrontierRankingDeterministic(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	policy := SourcePolicy{
		SourceID:           "fixture:site",
		Domain:             "example.com",
		RobotsPolicy:       "respect",
		Priority:           90,
		ConfidenceBaseline: 0.8,
		Enabled:            true,
		ReviewStatus:       "approved",
	}
	robots := RobotsResult{
		Status: RobotsStatusSuccessful,
		Document: ParseRobots([]byte(strings.Join([]string{
			"User-agent: *",
			"Disallow: /section/archive/",
			"Allow: /section/archive/public/",
		}, "\n"))),
	}
	recent := now.Add(-2 * time.Hour)
	moderate := now.Add(-12 * time.Hour)
	older := now.Add(-48 * time.Hour)

	entries := BuildFrontier(policy, robots, []DiscoveryCandidate{
		{URL: "https://example.com/section/news/alpha?utm_source=x&a=1&b=2", DiscoveredAt: now, Freshness: &recent, QualityHint: 0.95, ChangeFrequency: "daily", Kind: "sitemap"},
		{URL: "https://example.com/section/news/alpha?b=2&a=1", DiscoveredAt: now, Freshness: &moderate, QualityHint: 0.85, Kind: "feed"},
		{URL: "https://example.com/section/archive/beta", DiscoveredAt: now, Freshness: &older, QualityHint: 0.5, Kind: "sitemap"},
		{URL: "https://example.com/section/archive/public/brief", DiscoveredAt: now, Freshness: &moderate, QualityHint: 0.7, Kind: "sitemap"},
		{URL: "https://example.com/section/news/gamma", DiscoveredAt: now, Freshness: &older, QualityHint: 0.65, Kind: "rss"},
		{URL: "https://other.example.net/escape", DiscoveredAt: now, Freshness: &recent, QualityHint: 0.99, Kind: "sitemap"},
	}, now)

	if len(entries) != 3 {
		t.Fatalf("expected 3 frontier entries after host policy, dedupe, and robots filtering, got %d", len(entries))
	}
	got := []string{entries[0].CanonicalURL, entries[1].CanonicalURL, entries[2].CanonicalURL}
	want := []string{
		"https://example.com/section/news/alpha?a=1&b=2",
		"https://example.com/section/archive/public/brief",
		"https://example.com/section/news/gamma",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("unexpected frontier ranking order:\nwant=%v\n got=%v", want, got)
	}
	if entries[0].Priority <= entries[1].Priority || entries[1].Priority <= entries[2].Priority {
		t.Fatalf("expected descending deterministic priorities, got %#v", entries)
	}
	if entries[0].NextFetchAt.After(entries[1].NextFetchAt) {
		t.Fatal("expected freshest item to be scheduled first")
	}
	if entries[0].URLHash == "" {
		t.Fatal("expected frontier entry hash to be populated")
	}
	if entries[0].Domain != "example.com" {
		t.Fatalf("expected policy domain to be carried into frontier, got %q", entries[0].Domain)
	}
	if ranked := RankFrontier(entries); ranked[0].CanonicalURL != want[0] || ranked[1].CanonicalURL != want[1] || ranked[2].CanonicalURL != want[2] {
		t.Fatalf("expected RankFrontier to preserve deterministic ordering, got %#v", ranked)
	}
}

func gzipFixture(t *testing.T, content string) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	if _, err := writer.Write([]byte(content)); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse url %q: %v", raw, err)
	}
	return parsed
}
