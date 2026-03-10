package discovery

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

type FeedEntry struct {
	URL       string
	ID        string
	Title     string
	UpdatedAt *time.Time
	Kind      string
}

type FeedResult struct {
	Entries []FeedEntry
	Source  string
}

type FeedFetcher struct {
	Client   *http.Client
	MaxBytes int64
}

func NewFeedFetcher(client *http.Client) *FeedFetcher {
	if client == nil {
		client = http.DefaultClient
	}
	return &FeedFetcher{Client: client, MaxBytes: defaultSitemapMaxBytes}
}

func (f *FeedFetcher) Fetch(ctx context.Context, raw string) (FeedResult, error) {
	body, contentType, err := fetchBody(ctx, f.Client, raw, f.maxBytes())
	if err != nil {
		return FeedResult{}, err
	}
	body, err = maybeGunzip(body, contentType, raw)
	if err != nil {
		return FeedResult{}, err
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return FeedResult{}, fmt.Errorf("parse feed url: %w", err)
	}
	result, err := ParseFeed(body, parsed)
	if err != nil {
		return FeedResult{}, err
	}
	result.Source = raw
	return result, nil
}

func ParseFeed(data []byte, base *url.URL) (FeedResult, error) {
	var probe struct {
		XMLName xml.Name
	}
	if err := xml.Unmarshal(data, &probe); err != nil {
		return FeedResult{}, fmt.Errorf("decode feed xml: %w", err)
	}

	switch probe.XMLName.Local {
	case "rss":
		return parseRSSFeed(data, base)
	case "feed":
		return parseAtomFeed(data, base)
	default:
		return FeedResult{}, fmt.Errorf("unsupported feed root element %q", probe.XMLName.Local)
	}
}

func parseRSSFeed(data []byte, base *url.URL) (FeedResult, error) {
	var doc struct {
		Channel struct {
			Items []struct {
				GUID    string `xml:"guid"`
				Title   string `xml:"title"`
				Link    string `xml:"link"`
				PubDate string `xml:"pubDate"`
			} `xml:"item"`
		} `xml:"channel"`
	}
	if err := xml.Unmarshal(data, &doc); err != nil {
		return FeedResult{}, fmt.Errorf("decode rss feed: %w", err)
	}

	entries := make([]FeedEntry, 0, len(doc.Channel.Items))
	for _, item := range doc.Channel.Items {
		resolved, err := resolveURL(base, item.Link)
		if err != nil {
			continue
		}
		canonical, err := NormalizeURL(resolved)
		if err != nil {
			continue
		}
		entries = append(entries, FeedEntry{
			URL:       canonical,
			ID:        strings.TrimSpace(item.GUID),
			Title:     strings.TrimSpace(item.Title),
			UpdatedAt: parseFlexibleTime(item.PubDate),
			Kind:      "rss",
		})
	}
	return FeedResult{Entries: dedupeFeedEntries(entries)}, nil
}

func parseAtomFeed(data []byte, base *url.URL) (FeedResult, error) {
	var doc struct {
		Entries []struct {
			ID      string `xml:"id"`
			Title   string `xml:"title"`
			Updated string `xml:"updated"`
			Links   []struct {
				Href string `xml:"href,attr"`
				Rel  string `xml:"rel,attr"`
			} `xml:"link"`
		} `xml:"entry"`
	}
	if err := xml.Unmarshal(data, &doc); err != nil {
		return FeedResult{}, fmt.Errorf("decode atom feed: %w", err)
	}

	entries := make([]FeedEntry, 0, len(doc.Entries))
	for _, item := range doc.Entries {
		link := atomEntryLink(item.Links)
		if link == "" {
			continue
		}
		resolved, err := resolveURL(base, link)
		if err != nil {
			continue
		}
		canonical, err := NormalizeURL(resolved)
		if err != nil {
			continue
		}
		entries = append(entries, FeedEntry{
			URL:       canonical,
			ID:        strings.TrimSpace(item.ID),
			Title:     strings.TrimSpace(item.Title),
			UpdatedAt: parseFlexibleTime(item.Updated),
			Kind:      "atom",
		})
	}
	return FeedResult{Entries: dedupeFeedEntries(entries)}, nil
}

func atomEntryLink(links []struct {
	Href string `xml:"href,attr"`
	Rel  string `xml:"rel,attr"`
}) string {
	for _, link := range links {
		rel := strings.TrimSpace(strings.ToLower(link.Rel))
		if rel == "" || rel == "alternate" {
			return strings.TrimSpace(link.Href)
		}
	}
	if len(links) == 0 {
		return ""
	}
	return strings.TrimSpace(links[0].Href)
}

func dedupeFeedEntries(entries []FeedEntry) []FeedEntry {
	seen := map[string]FeedEntry{}
	for _, entry := range entries {
		if entry.URL == "" {
			continue
		}
		current, ok := seen[entry.URL]
		if !ok {
			seen[entry.URL] = entry
			continue
		}
		if entry.UpdatedAt != nil && (current.UpdatedAt == nil || entry.UpdatedAt.After(*current.UpdatedAt)) {
			seen[entry.URL] = entry
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	items := make([]FeedEntry, 0, len(keys))
	for _, key := range keys {
		items = append(items, seen[key])
	}
	return items
}

func (f *FeedFetcher) maxBytes() int64 {
	if f.MaxBytes > 0 {
		return f.MaxBytes
	}
	return defaultSitemapMaxBytes
}
