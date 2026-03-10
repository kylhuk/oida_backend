package discovery

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultRobotsCacheTTL   = 24 * time.Hour
	defaultUnreachableTTL   = 5 * time.Minute
	defaultRobotsUserAgent  = "global-osint-backend"
	defaultRobotsMaxBytes   = 500 * 1024
	defaultRobotsRedirects  = 5
	robotsDirectiveSitemap  = "sitemap"
	robotsDirectiveAllow    = "allow"
	robotsDirectiveDisallow = "disallow"
	robotsDirectiveAgent    = "user-agent"
)

type RobotsStatus string

const (
	RobotsStatusSuccessful  RobotsStatus = "successful"
	RobotsStatusUnavailable RobotsStatus = "unavailable"
	RobotsStatusUnreachable RobotsStatus = "unreachable"
)

type RobotsRule struct {
	Allow   bool
	Pattern string
}

type RobotsGroup struct {
	Agents []string
	Rules  []RobotsRule
}

type RobotsDocument struct {
	Groups   []RobotsGroup
	Sitemaps []string
}

type RobotsResult struct {
	Status    RobotsStatus
	Document  RobotsDocument
	FetchedAt time.Time
	ExpiresAt time.Time
	SourceURL string
	UserAgent string
	Cached    bool
	Err       error
}

type RobotsFetcher struct {
	Client       *http.Client
	UserAgent    string
	MaxBytes     int64
	MaxRedirects int
	Now          func() time.Time

	mu    sync.Mutex
	cache map[string]RobotsResult
}

func NewRobotsFetcher(client *http.Client, userAgent string) *RobotsFetcher {
	if client == nil {
		client = http.DefaultClient
	}
	if strings.TrimSpace(userAgent) == "" {
		userAgent = defaultRobotsUserAgent
	}
	return &RobotsFetcher{
		Client:       client,
		UserAgent:    userAgent,
		MaxBytes:     defaultRobotsMaxBytes,
		MaxRedirects: defaultRobotsRedirects,
		Now:          time.Now,
		cache:        map[string]RobotsResult{},
	}
}

func ParseRobots(data []byte) RobotsDocument {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	current := []string{}
	groups := []RobotsGroup{}
	sitemaps := []string{}

	for scanner.Scan() {
		line := strings.TrimSpace(stripRobotsComment(scanner.Text()))
		if line == "" {
			continue
		}
		key, value, ok := splitDirective(line)
		if !ok {
			continue
		}
		switch key {
		case robotsDirectiveAgent:
			agent := normalizeRobotAgent(value)
			if agent == "" {
				continue
			}
			if len(groups) == 0 || len(groups[len(groups)-1].Rules) > 0 {
				groups = append(groups, RobotsGroup{})
			}
			groups[len(groups)-1].Agents = append(groups[len(groups)-1].Agents, agent)
			current = groups[len(groups)-1].Agents
		case robotsDirectiveAllow, robotsDirectiveDisallow:
			if len(current) == 0 || len(groups) == 0 {
				continue
			}
			groups[len(groups)-1].Rules = append(groups[len(groups)-1].Rules, RobotsRule{
				Allow:   key == robotsDirectiveAllow,
				Pattern: strings.TrimSpace(value),
			})
		case robotsDirectiveSitemap:
			if strings.TrimSpace(value) != "" {
				sitemaps = append(sitemaps, strings.TrimSpace(value))
			}
		}
	}

	return RobotsDocument{Groups: groups, Sitemaps: dedupeStrings(sitemaps)}
}

func (f *RobotsFetcher) Fetch(ctx context.Context, raw string) (RobotsResult, error) {
	base, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return RobotsResult{}, fmt.Errorf("parse robots base url: %w", err)
	}
	key := robotsAuthorityKey(base)
	now := f.now().UTC()

	f.mu.Lock()
	if cached, ok := f.cache[key]; ok && now.Before(cached.ExpiresAt) {
		cached.Cached = true
		f.mu.Unlock()
		return cached, nil
	}
	stale, hasStale := f.cache[key]
	f.mu.Unlock()

	result, err := f.fetchFresh(ctx, base, stale, hasStale)
	if err == nil || result.Status != RobotsStatusUnreachable {
		f.mu.Lock()
		f.cache[key] = result
		f.mu.Unlock()
		return result, err
	}

	if hasStale && stale.Status == RobotsStatusSuccessful {
		stale.Cached = true
		stale.Err = err
		stale.ExpiresAt = now.Add(defaultUnreachableTTL)
		f.mu.Lock()
		f.cache[key] = stale
		f.mu.Unlock()
		return stale, nil
	}

	f.mu.Lock()
	f.cache[key] = result
	f.mu.Unlock()
	return result, err
}

func (f *RobotsFetcher) fetchFresh(ctx context.Context, base *url.URL, stale RobotsResult, hasStale bool) (RobotsResult, error) {
	client := *f.Client
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}

	current := robotsURL(base)
	for redirects := 0; redirects <= f.maxRedirects(); redirects++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, current.String(), nil)
		if err != nil {
			return f.unreachableResult(base, current.String(), err), err
		}
		req.Header.Set("User-Agent", f.userAgent())

		resp, err := client.Do(req)
		if err != nil {
			return f.unreachableResult(base, current.String(), err), err
		}

		if isRedirectStatus(resp.StatusCode) {
			location, err := resp.Location()
			_ = resp.Body.Close()
			if err != nil {
				return f.unreachableResult(base, current.String(), err), err
			}
			if redirects == f.maxRedirects() {
				result := f.unavailableResult(base, current.String(), resp)
				return result, nil
			}
			current = current.ResolveReference(location)
			continue
		}

		result, err := f.handleResponse(base, current.String(), resp)
		return result, err
	}

	result := f.unavailableResult(base, current.String(), nil)
	return result, nil
}

func (f *RobotsFetcher) handleResponse(base *url.URL, source string, resp *http.Response) (RobotsResult, error) {
	defer resp.Body.Close()
	now := f.now().UTC()
	result := RobotsResult{FetchedAt: now, SourceURL: source}
	result.UserAgent = f.userAgent()
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		body, err := io.ReadAll(io.LimitReader(resp.Body, f.maxBytes()+1))
		if err != nil {
			result = f.unreachableResult(base, source, err)
			return result, err
		}
		if int64(len(body)) > f.maxBytes() {
			body = body[:f.maxBytes()]
		}
		result.Status = RobotsStatusSuccessful
		result.Document = ParseRobots(body)
		result.ExpiresAt = now.Add(cacheTTL(resp.Header, defaultRobotsCacheTTL))
		return result, nil
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		result = f.unavailableResult(base, source, resp)
		return result, nil
	case resp.StatusCode >= 500 && resp.StatusCode < 600:
		err := fmt.Errorf("robots server error: %s", resp.Status)
		result = f.unreachableResult(base, source, err)
		return result, err
	default:
		err := fmt.Errorf("unexpected robots status: %s", resp.Status)
		result = f.unreachableResult(base, source, err)
		return result, err
	}
}

func (f *RobotsFetcher) Allowed(raw string) bool {
	result, err := f.Fetch(context.Background(), raw)
	if err != nil {
		return result.Allowed(raw)
	}
	return result.Allowed(raw)
}

func (r RobotsResult) Allowed(raw string) bool {
	switch r.Status {
	case RobotsStatusUnavailable:
		return true
	case RobotsStatusUnreachable:
		return false
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return false
	}
	if isRobotsTXT(parsed) {
		return true
	}
	userAgent := r.UserAgent
	if strings.TrimSpace(userAgent) == "" {
		userAgent = defaultRobotsUserAgent
	}
	return r.Document.Allowed(userAgent, parsed)
}

func (d RobotsDocument) Allowed(userAgent string, target *url.URL) bool {
	rules := d.rulesForAgent(userAgent)
	if len(rules) == 0 {
		return true
	}
	path := normalizeRobotsTarget(target)
	bestLength := -1
	allowed := true
	for _, rule := range rules {
		matched, length := matchRobotsPattern(rule.Pattern, path)
		if !matched {
			continue
		}
		if length > bestLength || (length == bestLength && rule.Allow) {
			bestLength = length
			allowed = rule.Allow
		}
	}
	if bestLength == -1 {
		return true
	}
	return allowed
}

func (d RobotsDocument) rulesForAgent(userAgent string) []RobotsRule {
	token := normalizeRobotAgent(userAgent)
	if token == "" {
		token = "*"
	}

	matched := []RobotsRule{}
	for _, group := range d.Groups {
		for _, agent := range group.Agents {
			if agent == token {
				matched = append(matched, group.Rules...)
				break
			}
		}
	}
	if len(matched) > 0 {
		return dedupeRules(matched)
	}

	for _, group := range d.Groups {
		for _, agent := range group.Agents {
			if agent == "*" {
				matched = append(matched, group.Rules...)
				break
			}
		}
	}
	return dedupeRules(matched)
}

func dedupeRules(rules []RobotsRule) []RobotsRule {
	seen := map[string]struct{}{}
	filtered := make([]RobotsRule, 0, len(rules))
	for _, rule := range rules {
		key := strconv.FormatBool(rule.Allow) + "\x00" + rule.Pattern
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		filtered = append(filtered, rule)
	}
	return filtered
}

func normalizeRobotAgent(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return ""
	}
	fields := strings.Fields(raw)
	if len(fields) > 0 {
		raw = fields[0]
	}
	if idx := strings.Index(raw, "/"); idx >= 0 {
		raw = raw[:idx]
	}
	var b strings.Builder
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '*' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func splitDirective(line string) (string, string, bool) {
	idx := strings.Index(line, ":")
	if idx <= 0 {
		return "", "", false
	}
	key := strings.ToLower(strings.TrimSpace(line[:idx]))
	value := strings.TrimSpace(line[idx+1:])
	return key, value, true
}

func stripRobotsComment(line string) string {
	idx := strings.Index(line, "#")
	if idx < 0 {
		return line
	}
	return line[:idx]
}

func matchRobotsPattern(pattern, target string) (bool, int) {
	pattern = normalizeRobotsPattern(pattern)
	target = normalizeRobotsPath(target)
	endAnchored := strings.HasSuffix(pattern, "$")
	if endAnchored {
		pattern = strings.TrimSuffix(pattern, "$")
	}
	parts := strings.Split(pattern, "*")
	var expr strings.Builder
	expr.WriteString("^")
	for i, part := range parts {
		if i > 0 {
			expr.WriteString(".*")
		}
		expr.WriteString(regexp.QuoteMeta(part))
	}
	if endAnchored {
		expr.WriteString("$")
	}
	re := regexp.MustCompile(expr.String())
	if !re.MatchString(target) {
		return false, 0
	}
	return true, len(strings.ReplaceAll(pattern, "*", ""))
}

func normalizeRobotsPattern(raw string) string {
	return normalizeRobotsPath(raw)
}

func normalizeRobotsTarget(target *url.URL) string {
	value := target.EscapedPath()
	if value == "" {
		value = "/"
	}
	if target.RawQuery != "" {
		value += "?" + target.RawQuery
	}
	return normalizeRobotsPath(value)
}

func normalizeRobotsPath(raw string) string {
	var b strings.Builder
	for i := 0; i < len(raw); i++ {
		if raw[i] == '%' && i+2 < len(raw) {
			hex := raw[i+1 : i+3]
			decoded, err := strconv.ParseUint(hex, 16, 8)
			if err == nil {
				ch := byte(decoded)
				if isUnreserved(ch) {
					b.WriteByte(ch)
				} else {
					b.WriteByte('%')
					b.WriteString(strings.ToUpper(hex))
				}
				i += 2
				continue
			}
		}
		if raw[i] > 127 || isReserved(raw[i]) {
			b.WriteString(url.QueryEscape(string(raw[i])))
			continue
		}
		b.WriteByte(raw[i])
	}
	return b.String()
}

func isReserved(ch byte) bool {
	return strings.ContainsRune(":/?#[]@!$&'()*+,;=", rune(ch))
}

func isUnreserved(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '.' || ch == '_' || ch == '~'
}

func dedupeStrings(values []string) []string {
	seen := map[string]struct{}{}
	items := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		items = append(items, value)
	}
	return items
}

func robotsURL(base *url.URL) *url.URL {
	clone := *base
	clone.Path = "/robots.txt"
	clone.RawPath = "/robots.txt"
	clone.RawQuery = ""
	clone.Fragment = ""
	return &clone
}

func robotsAuthorityKey(base *url.URL) string {
	clone := *base
	clone.Host = normalizeHost(&clone, clone.Scheme)
	clone.Path = ""
	clone.RawPath = ""
	clone.RawQuery = ""
	clone.Fragment = ""
	return clone.Scheme + "://" + clone.Host
}

func cacheTTL(header http.Header, fallback time.Duration) time.Duration {
	if cc := header.Get("Cache-Control"); cc != "" {
		for _, directive := range strings.Split(cc, ",") {
			directive = strings.TrimSpace(directive)
			if !strings.HasPrefix(strings.ToLower(directive), "max-age=") {
				continue
			}
			seconds, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(strings.ToLower(directive), "max-age=")))
			if err == nil && seconds >= 0 {
				ttl := time.Duration(seconds) * time.Second
				if ttl > defaultRobotsCacheTTL {
					return defaultRobotsCacheTTL
				}
				return ttl
			}
		}
	}
	if expires := header.Get("Expires"); expires != "" {
		if at, err := http.ParseTime(expires); err == nil {
			ttl := time.Until(at)
			if ttl > 0 {
				if ttl > defaultRobotsCacheTTL {
					return defaultRobotsCacheTTL
				}
				return ttl
			}
		}
	}
	return fallback
}

func isRedirectStatus(code int) bool {
	return code == http.StatusMovedPermanently || code == http.StatusFound || code == http.StatusSeeOther || code == http.StatusTemporaryRedirect || code == http.StatusPermanentRedirect
}

func isRobotsTXT(target *url.URL) bool {
	return target != nil && target.EscapedPath() == "/robots.txt"
}

func (f *RobotsFetcher) unavailableResult(base *url.URL, source string, resp *http.Response) RobotsResult {
	now := f.now().UTC()
	result := RobotsResult{
		Status:    RobotsStatusUnavailable,
		FetchedAt: now,
		ExpiresAt: now.Add(defaultRobotsCacheTTL),
		SourceURL: source,
		UserAgent: f.userAgent(),
	}
	if resp != nil {
		result.ExpiresAt = now.Add(cacheTTL(resp.Header, defaultRobotsCacheTTL))
	}
	_ = base
	return result
}

func (f *RobotsFetcher) unreachableResult(base *url.URL, source string, err error) RobotsResult {
	now := f.now().UTC()
	_ = base
	return RobotsResult{
		Status:    RobotsStatusUnreachable,
		FetchedAt: now,
		ExpiresAt: now.Add(defaultUnreachableTTL),
		SourceURL: source,
		UserAgent: f.userAgent(),
		Err:       err,
	}
}

func (f *RobotsFetcher) now() time.Time {
	if f.Now != nil {
		return f.Now()
	}
	return time.Now()
}

func (f *RobotsFetcher) maxBytes() int64 {
	if f.MaxBytes > 0 {
		return f.MaxBytes
	}
	return defaultRobotsMaxBytes
}

func (f *RobotsFetcher) maxRedirects() int {
	if f.MaxRedirects > 0 {
		return f.MaxRedirects
	}
	return defaultRobotsRedirects
}

func (f *RobotsFetcher) userAgent() string {
	if strings.TrimSpace(f.UserAgent) != "" {
		return f.UserAgent
	}
	return defaultRobotsUserAgent
}
