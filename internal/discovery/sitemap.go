package discovery

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"
)

const (
	defaultSitemapMaxBytes = 50 * 1024 * 1024
	defaultSitemapDepth    = 4
)

type SitemapURL struct {
	Loc        string
	LastMod    *time.Time
	ChangeFreq string
	Priority   float64
}

type SitemapResult struct {
	URLs  []SitemapURL
	Feeds []FeedEntry
	Files []string
}

type SitemapFetcher struct {
	Client   *http.Client
	MaxBytes int64
	MaxDepth int
}

func NewSitemapFetcher(client *http.Client) *SitemapFetcher {
	if client == nil {
		client = http.DefaultClient
	}
	return &SitemapFetcher{Client: client, MaxBytes: defaultSitemapMaxBytes, MaxDepth: defaultSitemapDepth}
}

func (f *SitemapFetcher) Fetch(ctx context.Context, raw string) (SitemapResult, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return SitemapResult{}, fmt.Errorf("parse sitemap url: %w", err)
	}
	return f.fetch(ctx, parsed, 0, map[string]struct{}{})
}

func (f *SitemapFetcher) fetch(ctx context.Context, target *url.URL, depth int, seen map[string]struct{}) (SitemapResult, error) {
	if depth > f.maxDepth() {
		return SitemapResult{}, fmt.Errorf("sitemap recursion exceeds max depth %d", f.maxDepth())
	}
	canonical := target.String()
	if _, ok := seen[canonical]; ok {
		return SitemapResult{}, nil
	}
	seen[canonical] = struct{}{}

	body, contentType, err := fetchBody(ctx, f.Client, canonical, f.maxBytes())
	if err != nil {
		return SitemapResult{}, err
	}
	body, err = maybeGunzip(body, contentType, canonical)
	if err != nil {
		return SitemapResult{}, err
	}

	kind, urls, children, err := parseSitemapDocument(body, target)
	if err != nil {
		return SitemapResult{}, err
	}

	result := SitemapResult{Files: []string{canonical}}
	if kind == "feed" {
		feed, err := ParseFeed(body, target)
		if err != nil {
			return SitemapResult{}, err
		}
		result.Feeds = feed.Entries
		return result, nil
	}

	for _, item := range urls {
		if !sitemapAllowsURL(target, item.Loc) {
			continue
		}
		result.URLs = append(result.URLs, item)
	}

	for _, child := range children {
		childURL, err := url.Parse(strings.TrimSpace(child))
		if err != nil {
			continue
		}
		childURL = target.ResolveReference(childURL)
		if !sitemapAllowsChild(target, childURL) {
			continue
		}
		childResult, err := f.fetch(ctx, childURL, depth+1, seen)
		if err != nil {
			return SitemapResult{}, err
		}
		result.URLs = append(result.URLs, childResult.URLs...)
		result.Feeds = append(result.Feeds, childResult.Feeds...)
		result.Files = append(result.Files, childResult.Files...)
	}

	result.URLs = dedupeSitemapURLs(result.URLs)
	result.Feeds = dedupeFeedEntries(result.Feeds)
	result.Files = dedupeStrings(result.Files)
	return result, nil
}

func parseSitemapDocument(data []byte, base *url.URL) (string, []SitemapURL, []string, error) {
	var probe struct {
		XMLName xml.Name
	}
	if err := xml.Unmarshal(data, &probe); err != nil {
		return "", nil, nil, fmt.Errorf("decode sitemap xml: %w", err)
	}

	switch probe.XMLName.Local {
	case "urlset":
		var doc struct {
			URLs []struct {
				Loc        string `xml:"loc"`
				LastMod    string `xml:"lastmod"`
				ChangeFreq string `xml:"changefreq"`
				Priority   string `xml:"priority"`
			} `xml:"url"`
		}
		if err := xml.Unmarshal(data, &doc); err != nil {
			return "", nil, nil, fmt.Errorf("decode sitemap urlset: %w", err)
		}
		urls := make([]SitemapURL, 0, len(doc.URLs))
		for _, item := range doc.URLs {
			loc, err := resolveURL(base, item.Loc)
			if err != nil {
				continue
			}
			urls = append(urls, SitemapURL{
				Loc:        loc,
				LastMod:    parseFlexibleTime(item.LastMod),
				ChangeFreq: strings.TrimSpace(item.ChangeFreq),
				Priority:   parseFloat(item.Priority),
			})
		}
		return "urlset", urls, nil, nil
	case "sitemapindex":
		var doc struct {
			Sitemaps []struct {
				Loc string `xml:"loc"`
			} `xml:"sitemap"`
		}
		if err := xml.Unmarshal(data, &doc); err != nil {
			return "", nil, nil, fmt.Errorf("decode sitemap index: %w", err)
		}
		children := make([]string, 0, len(doc.Sitemaps))
		for _, item := range doc.Sitemaps {
			loc, err := resolveURL(base, item.Loc)
			if err != nil {
				continue
			}
			children = append(children, loc)
		}
		return "index", nil, children, nil
	case "rss", "feed":
		return "feed", nil, nil, nil
	default:
		return "", nil, nil, fmt.Errorf("unsupported sitemap root element %q", probe.XMLName.Local)
	}
}

func fetchBody(ctx context.Context, client *http.Client, raw string, maxBytes int64) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("fetch %s: unexpected status %s", raw, resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return nil, "", err
	}
	if int64(len(body)) > maxBytes {
		return nil, "", fmt.Errorf("fetch %s: body exceeds %d bytes", raw, maxBytes)
	}
	return body, resp.Header.Get("Content-Type"), nil
}

func maybeGunzip(body []byte, contentType, raw string) ([]byte, error) {
	if !looksGzipped(body, contentType, raw) {
		return body, nil
	}
	reader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("gunzip %s: %w", raw, err)
	}
	defer reader.Close()
	decoded, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read gzip %s: %w", raw, err)
	}
	return decoded, nil
}

func looksGzipped(body []byte, contentType, raw string) bool {
	if len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b {
		return true
	}
	contentType = strings.ToLower(contentType)
	return strings.Contains(contentType, "gzip") || strings.HasSuffix(strings.ToLower(raw), ".gz")
}

func resolveURL(base *url.URL, raw string) (string, error) {
	child, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	return base.ResolveReference(child).String(), nil
}

func sitemapAllowsChild(parent, child *url.URL) bool {
	return sameSite(parent, child)
}

func sitemapAllowsURL(sitemapURL *url.URL, raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil {
		return false
	}
	if !sameSite(sitemapURL, parsed) {
		return false
	}
	dir := path.Dir(sitemapURL.EscapedPath())
	if dir == "." || dir == "/" {
		return true
	}
	allowedPrefix := strings.TrimSuffix(dir, "/") + "/"
	return strings.HasPrefix(parsed.EscapedPath(), allowedPrefix)
}

func sameSite(left, right *url.URL) bool {
	return strings.EqualFold(left.Scheme, right.Scheme) && strings.EqualFold(left.Host, right.Host)
}

func parseFlexibleTime(raw string) *time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	for _, layout := range []string{time.RFC3339, time.RFC3339Nano, time.RFC1123Z, time.RFC1123, time.RFC822Z, time.RFC822, "2006-01-02", time.RFC850} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			parsed = parsed.UTC()
			return &parsed
		}
	}
	return nil
}

func dedupeSitemapURLs(urls []SitemapURL) []SitemapURL {
	seen := map[string]SitemapURL{}
	for _, item := range urls {
		canonical, err := NormalizeURL(item.Loc)
		if err != nil {
			continue
		}
		item.Loc = canonical
		current, ok := seen[canonical]
		if !ok {
			seen[canonical] = item
			continue
		}
		if item.LastMod != nil && (current.LastMod == nil || item.LastMod.After(*current.LastMod)) {
			seen[canonical] = item
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	items := make([]SitemapURL, 0, len(keys))
	for _, key := range keys {
		items = append(items, seen[key])
	}
	return items
}

func (f *SitemapFetcher) maxBytes() int64 {
	if f.MaxBytes > 0 {
		return f.MaxBytes
	}
	return defaultSitemapMaxBytes
}

func (f *SitemapFetcher) maxDepth() int {
	if f.MaxDepth > 0 {
		return f.MaxDepth
	}
	return defaultSitemapDepth
}
