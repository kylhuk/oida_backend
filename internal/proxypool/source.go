package proxypool

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"global-osint-backend/internal/observability"
)

var (
	protoIPPortRE = regexp.MustCompile(`(?i)(https?|socks[45])://(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})`)
	bareIPPortRE  = regexp.MustCompile(`\b(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})\b`)
)

// ParseProxyLines extracts proxy URLs from arbitrary text.
// Lines containing protocol://IP:PORT use the detected protocol.
// Bare IP:PORT lines use defaultProtocol (defaults to "http" if empty).
// Comments (lines starting with #) and duplicates are ignored.
func ParseProxyLines(data, defaultProtocol string) []string {
	if defaultProtocol == "" {
		defaultProtocol = "http"
	}
	seen := make(map[string]struct{})
	var result []string
	for _, line := range strings.Split(data, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if m := protoIPPortRE.FindStringSubmatch(line); m != nil {
			u := strings.ToLower(m[1]) + "://" + m[2] + ":" + m[3]
			if _, ok := seen[u]; !ok {
				seen[u] = struct{}{}
				result = append(result, u)
			}
			continue
		}
		if m := bareIPPortRE.FindStringSubmatch(line); m != nil {
			u := defaultProtocol + "://" + m[1] + ":" + m[2]
			if _, ok := seen[u]; !ok {
				seen[u] = struct{}{}
				result = append(result, u)
			}
		}
	}
	return result
}

type sourceRecord struct {
	protocol string
	url      string
}

func parseSourcesFile(path string) ([]sourceRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var records []sourceRecord
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 4096), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		switch len(fields) {
		case 1:
			records = append(records, sourceRecord{url: fields[0]})
		default:
			records = append(records, sourceRecord{protocol: strings.ToLower(fields[0]), url: fields[1]})
		}
	}
	return records, sc.Err()
}

// Refresher periodically fetches proxy lists from upstream sources into the pool.
type Refresher struct {
	sourcesFile string
	pool        *Pool
	client      *http.Client
	interval    time.Duration
}

func NewRefresher(sourcesFile string, pool *Pool, interval time.Duration) *Refresher {
	return &Refresher{
		sourcesFile: sourcesFile,
		pool:        pool,
		client:      &http.Client{Timeout: 30 * time.Second},
		interval:    interval,
	}
}

// Run fetches immediately, then repeats every interval. Blocks until ctx is cancelled.
func (r *Refresher) Run(ctx context.Context) {
	if err := r.refresh(ctx); err != nil {
		observability.LogEvent("proxy-pool", "refresh_error", "", map[string]any{"error": err.Error()})
	}
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.refresh(ctx); err != nil {
				observability.LogEvent("proxy-pool", "refresh_error", "", map[string]any{"error": err.Error()})
			}
		}
	}
}

func (r *Refresher) refresh(ctx context.Context) error {
	records, err := parseSourcesFile(r.sourcesFile)
	if err != nil {
		return err
	}
	fetched := 0
	for _, rec := range records {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rec.url, nil)
		if err != nil {
			continue
		}
		resp, err := r.client.Do(req)
		if err != nil {
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			continue
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
		resp.Body.Close()
		if err != nil {
			continue
		}
		proxies := ParseProxyLines(string(body), rec.protocol)
		r.pool.Add(proxies)
		fetched += len(proxies)
	}
	active, total := r.pool.Stats()
	observability.LogEvent("proxy-pool", "refresh_complete", "", map[string]any{
		"fetched": fetched, "active": active, "total": total,
	})
	return nil
}
