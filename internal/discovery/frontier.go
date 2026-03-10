package discovery

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"net"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

type SourcePolicy struct {
	SourceID            string
	Domain              string
	AllowedHosts        []string
	RobotsPolicy        string
	RequestsPerMinute   uint32
	BurstSize           uint16
	Priority            uint16
	ConfidenceBaseline  float64
	Enabled             bool
	ReviewStatus        string
	BackfillPriority    uint16
	SupportsHistorical  bool
	SupportsDelta       bool
	AttributionRequired bool
}

type DiscoveryCandidate struct {
	URL             string
	DiscoveredAt    time.Time
	Freshness       *time.Time
	QualityHint     float64
	ChangeFrequency string
	Kind            string
}

type FrontierScore struct {
	Freshness float64
	Quality   float64
	Diversity float64
	Source    float64
	Total     float64
}

type FrontierEntry struct {
	SourceID      string
	Domain        string
	URL           string
	CanonicalURL  string
	URLHash       string
	Priority      int32
	State         string
	DiscoveredAt  time.Time
	NextFetchAt   time.Time
	DiscoveryKind string
	Score         FrontierScore
}

func NormalizeURL(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("parse url %q: %w", raw, err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("url %q must include scheme and host", raw)
	}

	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("unsupported url scheme %q", parsed.Scheme)
	}
	parsed.Scheme = scheme
	parsed.Host = normalizeHost(parsed, scheme)
	parsed.Fragment = ""
	parsed.User = nil

	cleanPath := parsed.EscapedPath()
	if cleanPath == "" {
		cleanPath = "/"
	}
	originalTrailingSlash := strings.HasSuffix(cleanPath, "/")
	cleanPath = path.Clean(cleanPath)
	if !strings.HasPrefix(cleanPath, "/") {
		cleanPath = "/" + cleanPath
	}
	if originalTrailingSlash && cleanPath != "/" {
		cleanPath += "/"
	}
	parsed.RawPath = cleanPath
	parsed.Path = cleanPath

	parsed.RawQuery = normalizeQuery(parsed.Query())

	return parsed.String(), nil
}

func normalizeHost(parsed *url.URL, scheme string) string {
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	port := parsed.Port()
	if port == "" {
		return host
	}
	if (scheme == "http" && port == "80") || (scheme == "https" && port == "443") {
		return host
	}
	return net.JoinHostPort(host, port)
}

func normalizeQuery(values url.Values) string {
	if len(values) == 0 {
		return ""
	}

	type pair struct {
		key   string
		value string
	}
	pairs := make([]pair, 0, len(values))
	for key, items := range values {
		lower := strings.ToLower(key)
		if strings.HasPrefix(lower, "utm_") || lower == "fbclid" || lower == "gclid" || lower == "mc_cid" || lower == "mc_eid" {
			continue
		}
		sort.Strings(items)
		for _, item := range items {
			pairs = append(pairs, pair{key: key, value: item})
		}
	}

	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].key == pairs[j].key {
			return pairs[i].value < pairs[j].value
		}
		return pairs[i].key < pairs[j].key
	})

	encoded := url.Values{}
	for _, pair := range pairs {
		encoded.Add(pair.key, pair.value)
	}
	return encoded.Encode()
}

func (p SourcePolicy) AllowsURL(raw string) bool {
	if !p.Enabled {
		return false
	}
	if p.ReviewStatus != "" && p.ReviewStatus != "approved" {
		return false
	}

	canonical, err := NormalizeURL(raw)
	if err != nil {
		return false
	}
	parsed, err := url.Parse(canonical)
	if err != nil {
		return false
	}
	host := parsed.Hostname()
	allowedHosts := append([]string{}, p.AllowedHosts...)
	if p.Domain != "" {
		allowedHosts = append(allowedHosts, p.Domain)
	}
	if len(allowedHosts) == 0 {
		return true
	}

	for _, allowed := range allowedHosts {
		allowed = strings.ToLower(strings.TrimSpace(allowed))
		if allowed == "" {
			continue
		}
		if host == allowed || strings.HasSuffix(host, "."+allowed) {
			return true
		}
	}
	return false
}

func BuildFrontier(policy SourcePolicy, robots RobotsResult, candidates []DiscoveryCandidate, now time.Time) []FrontierEntry {
	if !policy.Enabled {
		return nil
	}

	type aggregated struct {
		entry FrontierEntry
		fresh *time.Time
		best  DiscoveryCandidate
	}

	aggregatedByURL := map[string]aggregated{}
	for _, candidate := range candidates {
		canonical, err := NormalizeURL(candidate.URL)
		if err != nil {
			continue
		}
		if !policy.AllowsURL(canonical) {
			continue
		}
		if strings.ToLower(policy.RobotsPolicy) != "ignore" && !robots.Allowed(canonical) {
			continue
		}

		discoveredAt := candidate.DiscoveredAt.UTC()
		if discoveredAt.IsZero() {
			discoveredAt = now.UTC()
		}

		existing, ok := aggregatedByURL[canonical]
		if !ok {
			aggregatedByURL[canonical] = aggregated{
				entry: FrontierEntry{
					SourceID:      policy.SourceID,
					Domain:        sourceDomain(policy, canonical),
					URL:           candidate.URL,
					CanonicalURL:  canonical,
					URLHash:       hashURL(canonical),
					State:         "discovered",
					DiscoveredAt:  discoveredAt,
					DiscoveryKind: candidate.Kind,
				},
				fresh: candidate.Freshness,
				best:  candidate,
			}
			continue
		}

		if candidate.QualityHint > existing.best.QualityHint {
			existing.best = candidate
			existing.entry.URL = candidate.URL
			existing.entry.DiscoveryKind = candidate.Kind
		}
		if candidate.Freshness != nil && (existing.fresh == nil || candidate.Freshness.After(*existing.fresh)) {
			existing.fresh = candidate.Freshness
		}
		if discoveredAt.Before(existing.entry.DiscoveredAt) {
			existing.entry.DiscoveredAt = discoveredAt
		}
		aggregatedByURL[canonical] = existing
	}

	if len(aggregatedByURL) == 0 {
		return nil
	}

	bucketCounts := map[string]int{}
	entries := make([]FrontierEntry, 0, len(aggregatedByURL))
	keys := make([]string, 0, len(aggregatedByURL))
	for canonical := range aggregatedByURL {
		keys = append(keys, canonical)
	}
	sort.Strings(keys)
	for _, canonical := range keys {
		bucketCounts[diversityBucket(canonical)]++
	}

	for _, canonical := range keys {
		candidate := aggregatedByURL[canonical]
		score := scoreFrontier(policy, candidate.best, candidate.fresh, bucketCounts[diversityBucket(canonical)], now)
		candidate.entry.Score = score
		candidate.entry.Priority = int32(math.Round(score.Total * 1000))
		candidate.entry.NextFetchAt = nextFetchAt(candidate.best, candidate.fresh, now)
		entries = append(entries, candidate.entry)
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Priority != entries[j].Priority {
			return entries[i].Priority > entries[j].Priority
		}
		if !entries[i].NextFetchAt.Equal(entries[j].NextFetchAt) {
			return entries[i].NextFetchAt.Before(entries[j].NextFetchAt)
		}
		return entries[i].CanonicalURL < entries[j].CanonicalURL
	})

	return entries
}

func RankFrontier(entries []FrontierEntry) []FrontierEntry {
	cloned := append([]FrontierEntry(nil), entries...)
	sort.Slice(cloned, func(i, j int) bool {
		if cloned[i].Priority != cloned[j].Priority {
			return cloned[i].Priority > cloned[j].Priority
		}
		if !cloned[i].NextFetchAt.Equal(cloned[j].NextFetchAt) {
			return cloned[i].NextFetchAt.Before(cloned[j].NextFetchAt)
		}
		return cloned[i].CanonicalURL < cloned[j].CanonicalURL
	})
	return cloned
}

func sourceDomain(policy SourcePolicy, canonical string) string {
	if policy.Domain != "" {
		return policy.Domain
	}
	parsed, err := url.Parse(canonical)
	if err != nil {
		return ""
	}
	return parsed.Hostname()
}

func diversityBucket(canonical string) string {
	parsed, err := url.Parse(canonical)
	if err != nil {
		return canonical
	}
	segment := firstPathSegment(parsed.EscapedPath())
	return parsed.Hostname() + ":" + segment
}

func firstPathSegment(rawPath string) string {
	trimmed := strings.Trim(strings.TrimSpace(rawPath), "/")
	if trimmed == "" {
		return "root"
	}
	parts := strings.Split(trimmed, "/")
	return parts[0]
}

func scoreFrontier(policy SourcePolicy, candidate DiscoveryCandidate, fresh *time.Time, bucketCount int, now time.Time) FrontierScore {
	sourceScore := clamp01((float64(policy.Priority)/100.0)*0.55 + policy.ConfidenceBaseline*0.45)
	quality := clamp01(candidate.QualityHint)
	if quality == 0 {
		quality = clamp01(sourceScore * 0.75)
	}
	freshness := freshnessScore(fresh, now)
	diversity := clamp01(1 / float64(bucketCount))
	total := clamp01(freshness*0.4 + quality*0.3 + diversity*0.15 + sourceScore*0.15)
	return FrontierScore{
		Freshness: freshness,
		Quality:   quality,
		Diversity: diversity,
		Source:    sourceScore,
		Total:     total,
	}
}

func freshnessScore(fresh *time.Time, now time.Time) float64 {
	if fresh == nil || fresh.IsZero() {
		return 0.45
	}
	age := now.Sub(fresh.UTC())
	if age < 0 {
		age = 0
	}
	hours := age.Hours()
	return clamp01(1 / (1 + hours/72.0))
}

func nextFetchAt(candidate DiscoveryCandidate, fresh *time.Time, now time.Time) time.Time {
	interval := 24 * time.Hour
	switch strings.ToLower(candidate.ChangeFrequency) {
	case "always", "hourly":
		interval = time.Hour
	case "daily":
		interval = 6 * time.Hour
	case "weekly":
		interval = 24 * time.Hour
	case "monthly":
		interval = 7 * 24 * time.Hour
	case "yearly", "never":
		interval = 30 * 24 * time.Hour
	}
	if fresh != nil && !fresh.IsZero() {
		age := now.Sub(fresh.UTC())
		switch {
		case age < 6*time.Hour:
			interval = minDuration(interval, 2*time.Hour)
		case age < 24*time.Hour:
			interval = minDuration(interval, 6*time.Hour)
		case age > 30*24*time.Hour:
			interval = maxDuration(interval, 7*24*time.Hour)
		}
	}
	return now.UTC().Add(interval)
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func clamp01(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func hashURL(canonical string) string {
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}

func parseFloat(raw string) float64 {
	v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0
	}
	return v
}
