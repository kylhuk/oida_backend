# VesselFinder Anti-Block Overhaul Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix VesselFinder ingestion blocking by adding a rotating proxy pool with auto-fetch and health tracking, a persistent stealth Chromium browser session, real HTTP status detection, and adaptive rate throttling.

**Architecture:** A new `internal/proxypool` package manages an in-memory proxy pool fed by auto-fetched upstream lists; a new `internal/throttle` package provides adaptive pacing that ramps from slow to fast when clean and resets on any block. The worker's `render()` function is replaced by a persistent browser session (recycled every 50 requests or on block) that injects a new proxy at each recycle, captures real HTTP status codes via CDP network events, and detects bot/captcha pages at fetch time.

**Tech Stack:** Go 1.23, `github.com/chromedp/chromedp v0.10.1` (already vendored), `github.com/chromedp/cdproto` (already vendored), stdlib `net/http`, `sync`, `math/rand`.

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `internal/proxypool/pool.go` | Thread-safe proxy pool: state, pick, disable, reactivate |
| Create | `internal/proxypool/pool_test.go` | Unit tests for pool |
| Create | `internal/proxypool/source.go` | Parse proxy lines; Refresher goroutine that fetches upstream lists |
| Create | `internal/proxypool/source_test.go` | Unit tests for parser + Refresher |
| Create | `internal/proxypool/validator.go` | Background validator: re-probes disabled proxies after cooldown |
| Create | `internal/proxypool/validator_test.go` | Unit tests for Validator |
| Create | `internal/throttle/adaptive.go` | Adaptive rate controller (floor→ceiling ramp, block reset, jitter) |
| Create | `internal/throttle/adaptive_test.go` | Unit tests for Adaptive |
| Create | `internal/packs/maritime/vesselfinder/botpage.go` | Standalone `IsBotPage(html string) bool` helper |
| Modify | `internal/packs/maritime/vesselfinder/vesselfinder.go:173-181` | Call `IsBotPage()` instead of inline check in `ParseDetail` |
| Modify | `cmd/worker-vesselfinder/main.go` | Browser session mgmt, renderPage, scan/discovery rewrites, config additions, serve() wiring |
| Create | `config/proxies/proxy_sources.txt` | User-editable upstream proxy list sources |
| Modify | `docker-compose.yml` | New env vars, volume mounts for proxy sources + chrome profile |

---

### Task 1: Proxy Pool Core (`internal/proxypool/pool.go`)

**Files:**
- Create: `internal/proxypool/pool.go`
- Create: `internal/proxypool/pool_test.go`

- [ ] **Step 1.1: Write failing tests**

```go
// internal/proxypool/pool_test.go
package proxypool

import (
	"testing"
	"time"
)

func TestPool_PickEmpty(t *testing.T) {
	p := New()
	_, ok := p.Pick()
	if ok {
		t.Fatal("expected no proxy from empty pool")
	}
}

func TestPool_AddAndPick(t *testing.T) {
	p := New()
	p.Add([]string{"http://1.2.3.4:8080"})
	got, ok := p.Pick()
	if !ok {
		t.Fatal("expected a proxy")
	}
	if got != "http://1.2.3.4:8080" {
		t.Fatalf("unexpected proxy %q", got)
	}
}

func TestPool_InvalidURLSkipped(t *testing.T) {
	p := New()
	p.Add([]string{"not-a-url", "http://1.2.3.4:8080"})
	_, ok := p.Pick()
	if !ok {
		t.Fatal("expected valid proxy to be added")
	}
	_, total := p.Stats()
	if total != 1 {
		t.Fatalf("expected 1 entry, got %d", total)
	}
}

func TestPool_Disable(t *testing.T) {
	p := New()
	p.Add([]string{"http://1.2.3.4:8080"})
	p.Disable("http://1.2.3.4:8080")
	_, ok := p.Pick()
	if ok {
		t.Fatal("disabled proxy should not be picked")
	}
	active, total := p.Stats()
	if active != 0 || total != 1 {
		t.Fatalf("expected active=0 total=1, got %d/%d", active, total)
	}
}

func TestPool_Reactivate(t *testing.T) {
	p := New()
	p.Add([]string{"http://1.2.3.4:8080"})
	p.Disable("http://1.2.3.4:8080")
	p.Reactivate("http://1.2.3.4:8080")
	_, ok := p.Pick()
	if !ok {
		t.Fatal("reactivated proxy should be pickable")
	}
}

func TestPool_ProbeDue(t *testing.T) {
	p := New()
	p.Add([]string{"http://1.2.3.4:8080"})
	// Artificially set disabledUntil in the past
	p.mu.Lock()
	p.entries["http://1.2.3.4:8080"].disabledUntil = time.Now().Add(-time.Minute)
	p.mu.Unlock()
	due := p.ProbeDue()
	if len(due) != 1 || due[0] != "http://1.2.3.4:8080" {
		t.Fatalf("expected 1 due entry, got %v", due)
	}
}

func TestPool_AddDedup(t *testing.T) {
	p := New()
	p.Add([]string{"http://1.2.3.4:8080", "http://1.2.3.4:8080"})
	_, total := p.Stats()
	if total != 1 {
		t.Fatalf("expected 1 deduped entry, got %d", total)
	}
}
```

- [ ] **Step 1.2: Run tests to verify they fail**

```bash
cd /home/hal9000/docker/oida_backend
go test ./internal/proxypool/... 2>&1
```
Expected: build errors — package does not exist yet.

- [ ] **Step 1.3: Implement `internal/proxypool/pool.go`**

```go
package proxypool

import (
	"math/rand"
	"net/url"
	"sync"
	"time"
)

const disableDuration = 24 * time.Hour

type entry struct {
	rawURL        string
	disabledUntil time.Time
}

func (e *entry) active() bool {
	return e.disabledUntil.IsZero() || time.Now().Before(e.disabledUntil)
}

// Pool is a thread-safe in-memory proxy pool.
type Pool struct {
	mu      sync.RWMutex
	entries map[string]*entry
	rng     *rand.Rand
}

func New() *Pool {
	return &Pool{
		entries: make(map[string]*entry),
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Add merges proxy URLs into the pool; invalid URLs and duplicates are silently skipped.
func (p *Pool) Add(proxies []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, raw := range proxies {
		if _, err := url.ParseRequestURI(raw); err != nil {
			continue
		}
		if _, exists := p.entries[raw]; !exists {
			p.entries[raw] = &entry{rawURL: raw}
		}
	}
}

// Pick returns a random active proxy URL. Returns ("", false) if none are active.
func (p *Pool) Pick() (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	active := make([]*entry, 0, len(p.entries))
	for _, e := range p.entries {
		if e.active() {
			active = append(active, e)
		}
	}
	if len(active) == 0 {
		return "", false
	}
	return active[p.rng.Intn(len(active))].rawURL, true
}

// Disable marks the given proxy as disabled for 24 hours.
func (p *Pool) Disable(proxyURL string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.entries[proxyURL]; ok {
		e.disabledUntil = time.Now().Add(disableDuration)
	}
}

// Reactivate re-enables a previously disabled proxy.
func (p *Pool) Reactivate(proxyURL string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.entries[proxyURL]; ok {
		e.disabledUntil = time.Time{}
	}
}

// ProbeDue returns proxy URLs whose 24h cooldown has expired and should be re-tested.
func (p *Pool) ProbeDue() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	var due []string
	for _, e := range p.entries {
		if !e.disabledUntil.IsZero() && now.After(e.disabledUntil) {
			due = append(due, e.rawURL)
		}
	}
	return due
}

// Stats returns (active, total) proxy counts.
func (p *Pool) Stats() (active, total int) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	total = len(p.entries)
	for _, e := range p.entries {
		if e.active() {
			active++
		}
	}
	return
}
```

- [ ] **Step 1.4: Run tests to verify they pass**

```bash
cd /home/hal9000/docker/oida_backend
go test ./internal/proxypool/... -run TestPool -v 2>&1
```
Expected: All `TestPool_*` tests PASS.

- [ ] **Step 1.5: Commit**

```bash
git add internal/proxypool/pool.go internal/proxypool/pool_test.go
git commit -m "feat(proxypool): add thread-safe proxy pool with disable/reactivate lifecycle"
```

---

### Task 2: Proxy Line Parser + Source Refresher (`internal/proxypool/source.go`)

**Files:**
- Create: `internal/proxypool/source.go`
- Create: `internal/proxypool/source_test.go`

- [ ] **Step 2.1: Write failing tests**

```go
// internal/proxypool/source_test.go
package proxypool

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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
	active, total := pool.Stats()
	if total != 2 {
		t.Fatalf("expected 2 proxies, got %d", total)
	}
	_ = active
	_ = strings.Contains // suppress import
}
```

- [ ] **Step 2.2: Run tests to verify they fail**

```bash
cd /home/hal9000/docker/oida_backend
go test ./internal/proxypool/... -run "TestParse|TestSource|TestRefresher" 2>&1
```
Expected: build error — source.go missing.

- [ ] **Step 2.3: Implement `internal/proxypool/source.go`**

```go
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
// Lines containing protocol://IP:PORT use the given protocol.
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
		body, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
		resp.Body.Close()
		if err != nil || resp.StatusCode != 200 {
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
```

- [ ] **Step 2.4: Run tests to verify they pass**

```bash
cd /home/hal9000/docker/oida_backend
go test ./internal/proxypool/... -run "TestParse|TestSource|TestRefresher" -v 2>&1
```
Expected: All tests PASS.

- [ ] **Step 2.5: Commit**

```bash
git add internal/proxypool/source.go internal/proxypool/source_test.go
git commit -m "feat(proxypool): add proxy line parser and upstream source refresher"
```

---

### Task 3: Background Validator (`internal/proxypool/validator.go`)

**Files:**
- Create: `internal/proxypool/validator.go`
- Create: `internal/proxypool/validator_test.go`

- [ ] **Step 3.1: Write failing tests**

```go
// internal/proxypool/validator_test.go
package proxypool

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestValidator_ReactivatesReachableProxy(t *testing.T) {
	// Start a real local TCP listener to simulate a reachable proxy
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	proxyURL := "http://" + ln.Addr().String()
	pool := New()
	pool.Add([]string{proxyURL})

	// Manually disable the proxy with an expired cooldown
	pool.mu.Lock()
	pool.entries[proxyURL].disabledUntil = time.Now().Add(-time.Minute)
	pool.mu.Unlock()

	v := NewValidator(pool, time.Hour)
	v.probe(context.Background())

	active, _ := pool.Stats()
	if active != 1 {
		t.Fatalf("expected proxy to be reactivated, got active=%d", active)
	}
}

func TestValidator_KeepsUnreachableDisabled(t *testing.T) {
	proxyURL := "http://127.0.0.1:1" // port 1 is never listening
	pool := New()
	pool.Add([]string{proxyURL})

	pool.mu.Lock()
	pool.entries[proxyURL].disabledUntil = time.Now().Add(-time.Minute)
	pool.mu.Unlock()

	v := NewValidator(pool, time.Hour)
	v.probe(context.Background())

	active, _ := pool.Stats()
	if active != 0 {
		t.Fatalf("expected proxy to remain disabled, got active=%d", active)
	}
	// Cooldown should be extended (disabledUntil reset to ~24h from now)
	pool.mu.RLock()
	due := pool.entries[proxyURL].disabledUntil
	pool.mu.RUnlock()
	if time.Until(due) < 23*time.Hour {
		t.Fatalf("expected extended cooldown, got disabledUntil=%v", due)
	}
}
```

- [ ] **Step 3.2: Run tests to verify they fail**

```bash
cd /home/hal9000/docker/oida_backend
go test ./internal/proxypool/... -run "TestValidator" 2>&1
```
Expected: build error — validator.go missing.

- [ ] **Step 3.3: Implement `internal/proxypool/validator.go`**

```go
package proxypool

import (
	"context"
	"net"
	"net/url"
	"time"

	"global-osint-backend/internal/observability"
)

const validatorDialTimeout = 5 * time.Second

// Validator re-probes disabled pool entries when their cooldown expires.
// It uses a TCP dial to check if the proxy host is reachable; actual
// proxy functionality is confirmed on first real use by the worker.
type Validator struct {
	pool     *Pool
	interval time.Duration
}

func NewValidator(pool *Pool, interval time.Duration) *Validator {
	return &Validator{pool: pool, interval: interval}
}

// Run checks for due probes every interval. Blocks until ctx is cancelled.
func (v *Validator) Run(ctx context.Context) {
	ticker := time.NewTicker(v.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			v.probe(ctx)
		}
	}
}

func (v *Validator) probe(_ context.Context) {
	for _, proxyURL := range v.pool.ProbeDue() {
		if v.reachable(proxyURL) {
			v.pool.Reactivate(proxyURL)
			observability.LogEvent("proxy-pool", "proxy_reactivated", "", map[string]any{"proxy": proxyURL})
		} else {
			v.pool.Disable(proxyURL) // extends cooldown another 24h
			observability.LogEvent("proxy-pool", "proxy_still_dead", "", map[string]any{"proxy": proxyURL})
		}
	}
}

func (v *Validator) reachable(proxyURL string) bool {
	parsed, err := url.Parse(proxyURL)
	if err != nil || parsed.Host == "" {
		return false
	}
	conn, err := net.DialTimeout("tcp", parsed.Host, validatorDialTimeout)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}
```

- [ ] **Step 3.4: Run all proxypool tests**

```bash
cd /home/hal9000/docker/oida_backend
go test ./internal/proxypool/... -v 2>&1
```
Expected: All tests PASS.

- [ ] **Step 3.5: Commit**

```bash
git add internal/proxypool/validator.go internal/proxypool/validator_test.go
git commit -m "feat(proxypool): add background validator that re-probes disabled proxies after cooldown"
```

---

### Task 4: Adaptive Rate Throttle (`internal/throttle/adaptive.go`)

**Files:**
- Create: `internal/throttle/adaptive.go`
- Create: `internal/throttle/adaptive_test.go`

- [ ] **Step 4.1: Write failing tests**

```go
// internal/throttle/adaptive_test.go
package throttle

import (
	"testing"
	"time"
)

func TestAdaptive_StartsAtFloor(t *testing.T) {
	a := New(30*time.Second, 10*time.Second, time.Hour)
	d := a.currentDelay() // unexported call for testing
	if d != 30*time.Second {
		t.Fatalf("expected floor 30s at start, got %v", d)
	}
}

func TestAdaptive_RampsTowardCeiling(t *testing.T) {
	a := New(30*time.Second, 10*time.Second, time.Hour)
	a.RecordSuccess()
	// Manually set cleanSince to half the ramp duration ago
	a.mu.Lock()
	a.cleanSince = time.Now().Add(-30 * time.Minute)
	a.mu.Unlock()
	d := a.currentDelay()
	// At t=0.5 we expect 30s + 0.5*(10-30) = 30-10 = 20s
	if d < 19*time.Second || d > 21*time.Second {
		t.Fatalf("expected ~20s at midpoint, got %v", d)
	}
}

func TestAdaptive_ReachesFullCeiling(t *testing.T) {
	a := New(30*time.Second, 10*time.Second, time.Hour)
	a.RecordSuccess()
	a.mu.Lock()
	a.cleanSince = time.Now().Add(-2 * time.Hour)
	a.mu.Unlock()
	d := a.currentDelay()
	if d != 10*time.Second {
		t.Fatalf("expected ceiling 10s, got %v", d)
	}
}

func TestAdaptive_RecordBlockResetsToFloor(t *testing.T) {
	a := New(30*time.Second, 10*time.Second, time.Hour)
	a.RecordSuccess()
	a.mu.Lock()
	a.cleanSince = time.Now().Add(-2 * time.Hour)
	a.mu.Unlock()
	a.RecordBlock()
	d := a.currentDelay()
	if d != 30*time.Second {
		t.Fatalf("expected floor after block, got %v", d)
	}
}

func TestAdaptive_DelayIncludesJitter(t *testing.T) {
	a := New(30*time.Second, 10*time.Second, time.Hour)
	// Run 50 samples and verify all are within ±30% of floor (30s)
	for i := 0; i < 50; i++ {
		d := a.Delay()
		if d < 21*time.Second || d > 39*time.Second {
			t.Fatalf("delay %v out of jitter range [21s, 39s]", d)
		}
	}
}
```

- [ ] **Step 4.2: Run tests to verify they fail**

```bash
cd /home/hal9000/docker/oida_backend
go test ./internal/throttle/... 2>&1
```
Expected: build error — package does not exist.

- [ ] **Step 4.3: Implement `internal/throttle/adaptive.go`**

```go
package throttle

import (
	"math/rand"
	"sync"
	"time"
)

// Adaptive adjusts crawl delay between a slow floor and a fast ceiling.
// After RecordSuccess() is called the first time, the delay linearly
// decreases from floorDelay to ceilingDelay over rampDuration.
// RecordBlock() resets to floorDelay immediately.
type Adaptive struct {
	mu           sync.Mutex
	floorDelay   time.Duration
	ceilingDelay time.Duration
	rampDuration time.Duration
	cleanSince   time.Time
	hasClean     bool
	rng          *rand.Rand
}

// New creates an Adaptive throttle.
// floorDelay: slowest delay (used at start and after any block).
// ceilingDelay: fastest delay (reached after rampDuration of clean requests).
// rampDuration: how long it takes to ramp from floor to ceiling.
func New(floorDelay, ceilingDelay, rampDuration time.Duration) *Adaptive {
	return &Adaptive{
		floorDelay:   floorDelay,
		ceilingDelay: ceilingDelay,
		rampDuration: rampDuration,
		rng:          rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Delay returns how long to sleep before the next request, with ±30% random jitter.
func (a *Adaptive) Delay() time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	base := a.currentDelay()
	factor := 0.7 + 0.6*a.rng.Float64() // uniform [0.7, 1.3]
	return time.Duration(float64(base) * factor)
}

func (a *Adaptive) currentDelay() time.Duration {
	if !a.hasClean {
		return a.floorDelay
	}
	elapsed := time.Since(a.cleanSince)
	if elapsed >= a.rampDuration {
		return a.ceilingDelay
	}
	t := float64(elapsed) / float64(a.rampDuration)
	return a.floorDelay + time.Duration(t*float64(a.ceilingDelay-a.floorDelay))
}

// RecordSuccess marks a clean request. Starts the ramp timer on first call.
func (a *Adaptive) RecordSuccess() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.hasClean {
		a.cleanSince = time.Now()
		a.hasClean = true
	}
}

// RecordBlock resets the throttle to floorDelay and clears the ramp timer.
func (a *Adaptive) RecordBlock() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.hasClean = false
	a.cleanSince = time.Time{}
}
```

- [ ] **Step 4.4: Run tests to verify they pass**

```bash
cd /home/hal9000/docker/oida_backend
go test ./internal/throttle/... -v 2>&1
```
Expected: All tests PASS.

- [ ] **Step 4.5: Commit**

```bash
git add internal/throttle/adaptive.go internal/throttle/adaptive_test.go
git commit -m "feat(throttle): add adaptive rate controller with floor/ceiling ramp and block reset"
```

---

### Task 5: Standalone `IsBotPage` Helper in VF Package

**Files:**
- Create: `internal/packs/maritime/vesselfinder/botpage.go`
- Modify: `internal/packs/maritime/vesselfinder/vesselfinder.go:173-181`

- [ ] **Step 5.1: Write the test (add to existing test file)**

Add this function to `internal/packs/maritime/vesselfinder/vesselfinder_test.go`:

```go
func TestIsBotPage(t *testing.T) {
	cases := []struct {
		html string
		want bool
	}{
		{"<html><body>checking if the site connection is secure</body></html>", true},
		{"<html><body>verify you are human</body></html>", true},
		{"<html><body>cf-challenge</body></html>", true},
		{"<html><body>g-recaptcha</body></html>", true},
		{"<html><body>h-captcha</body></html>", true},
		{"<html><body><h1>MMSI: 123456789</h1></body></html>", false},
		{"", false},
	}
	for _, c := range cases {
		got := IsBotPage(c.html)
		if got != c.want {
			t.Errorf("IsBotPage(%q) = %v, want %v", c.html[:min(len(c.html), 40)], got, c.want)
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
```

- [ ] **Step 5.2: Run test to verify it fails**

```bash
cd /home/hal9000/docker/oida_backend
go test ./internal/packs/maritime/vesselfinder/... -run TestIsBotPage 2>&1
```
Expected: build error — IsBotPage undefined.

- [ ] **Step 5.3: Create `internal/packs/maritime/vesselfinder/botpage.go`**

```go
package vesselfinder

import "strings"

// IsBotPage returns true if the HTML looks like a Cloudflare challenge or captcha page.
func IsBotPage(html string) bool {
	lower := strings.ToLower(html)
	return strings.Contains(lower, "checking if the site connection is secure") ||
		strings.Contains(lower, "verify you are human") ||
		strings.Contains(lower, "cf-challenge") ||
		strings.Contains(lower, "g-recaptcha") ||
		strings.Contains(lower, "h-captcha")
}
```

- [ ] **Step 5.4: Refactor the inline check in `vesselfinder.go`**

In `internal/packs/maritime/vesselfinder/vesselfinder.go`, replace lines 173-181:

Old (lines 173-181):
```go
	lower := strings.ToLower(body)
	if strings.Contains(lower, "checking if the site connection is secure") ||
		strings.Contains(lower, "verify you are human") ||
		strings.Contains(lower, "cf-challenge") ||
		strings.Contains(lower, "g-recaptcha") ||
		strings.Contains(lower, "h-captcha") {
		return VesselMetadata{}, &ParseError{Code: ErrorBotPage, Message: "vesselfinder returned a bot or captcha page"}
	}
```

New:
```go
	if IsBotPage(body) {
		return VesselMetadata{}, &ParseError{Code: ErrorBotPage, Message: "vesselfinder returned a bot or captcha page"}
	}
	lower := strings.ToLower(body)
```

- [ ] **Step 5.5: Run tests**

```bash
cd /home/hal9000/docker/oida_backend
go test ./internal/packs/maritime/vesselfinder/... -v 2>&1
```
Expected: All tests PASS including `TestIsBotPage`.

- [ ] **Step 5.6: Commit**

```bash
git add internal/packs/maritime/vesselfinder/botpage.go internal/packs/maritime/vesselfinder/vesselfinder.go internal/packs/maritime/vesselfinder/vesselfinder_test.go
git commit -m "refactor(vf): extract IsBotPage helper for early bot detection at fetch time"
```

---

### Task 6: Config Additions to Worker

**Files:**
- Modify: `cmd/worker-vesselfinder/main.go` (constants block, config struct, loadConfig)

- [ ] **Step 6.1: Add new constants** (add after existing constants block at line 30-49)

```go
const (
	// existing constants stay unchanged ...

	defaultProxySourcesFile     = "/config/proxies/proxy_sources.txt"
	defaultProxyRefreshInterval = time.Hour
	defaultProxyValidateInterval = 5 * time.Minute
	defaultBrowserRecycleAfter  = 50
	defaultProfileDir           = "/chrome-profile"
	defaultRateFloorPerMin      = 2.0
	defaultRateCeilPerMin       = 6.0
	defaultRateRampDuration     = 30 * time.Minute
	defaultDiscoveryFloorRPS    = 0.1
	defaultDiscoveryCeilRPS     = 0.3
)
```

- [ ] **Step 6.2: Extend the config struct** (add fields after `RawBucket string` at line 68)

```go
type config struct {
	// ... existing fields ...
	ProxySourcesFile     string
	ProxyRefreshInterval time.Duration
	ProxyValidateInterval time.Duration
	BrowserRecycleAfter  int
	ProfileDir           string
	RateFloorPerMin      float64
	RateCeilPerMin       float64
	RateRampDuration     time.Duration
	DiscoveryFloorRPS    float64
	DiscoveryCeilRPS     float64
}
```

- [ ] **Step 6.3: Extend loadConfig()** (add new fields to the return statement at line 405-424)

```go
func loadConfig() config {
	return config{
		// existing fields stay unchanged ...
		ProxySourcesFile:      getenv("PROXY_SOURCES_FILE", defaultProxySourcesFile),
		ProxyRefreshInterval:  getenvDuration("PROXY_REFRESH_INTERVAL", defaultProxyRefreshInterval),
		ProxyValidateInterval: getenvDuration("PROXY_VALIDATE_INTERVAL", defaultProxyValidateInterval),
		BrowserRecycleAfter:   getenvInt("BROWSER_RECYCLE_AFTER", defaultBrowserRecycleAfter),
		ProfileDir:            getenv("CHROME_PROFILE_DIR", defaultProfileDir),
		RateFloorPerMin:       getenvFloat("RATE_FLOOR_PER_MIN", defaultRateFloorPerMin),
		RateCeilPerMin:        getenvFloat("RATE_CEIL_PER_MIN", defaultRateCeilPerMin),
		RateRampDuration:      getenvDuration("RATE_RAMP_DURATION", defaultRateRampDuration),
		DiscoveryFloorRPS:     getenvFloat("DISCOVERY_FLOOR_RPS", defaultDiscoveryFloorRPS),
		DiscoveryCeilRPS:      getenvFloat("DISCOVERY_CEIL_RPS", defaultDiscoveryCeilRPS),
	}
}
```

- [ ] **Step 6.4: Build to verify no errors**

```bash
cd /home/hal9000/docker/oida_backend
go build ./cmd/worker-vesselfinder/... 2>&1
```
Expected: compiles cleanly.

- [ ] **Step 6.5: Commit**

```bash
git add cmd/worker-vesselfinder/main.go
git commit -m "feat(worker-vf): add proxy pool and adaptive throttle config fields"
```

---

### Task 7: Stealth Browser Session Management

**Files:**
- Modify: `cmd/worker-vesselfinder/main.go` — add `browserSession` type, `newBrowserSession()`, `closeBrowserSession()`, replace `render()` with `renderPage()`

- [ ] **Step 7.1: Add imports**

Add to the imports block in `cmd/worker-vesselfinder/main.go`:

```go
import (
	// ... existing imports ...

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"global-osint-backend/internal/observability"
	"global-osint-backend/internal/proxypool"
	"global-osint-backend/internal/throttle"
)
```

(Note: `observability` may already be imported. Check and deduplicate.)

- [ ] **Step 7.2: Add `browserSession` type and lifecycle functions**

Add these functions to `cmd/worker-vesselfinder/main.go` (after the `renderedPage` type at line 70):

```go
type browserSession struct {
	allocCtx    context.Context
	cancelAlloc context.CancelFunc
	browserCtx  context.Context
	cancelBrowser context.CancelFunc
	reqCount    int
	proxyURL    string
}

func newBrowserSession(parent context.Context, cfg config, proxyURL string) (*browserSession, error) {
	opts := []chromedp.ExecAllocatorOption{
		chromedp.Flag("no-first-run", true),
		chromedp.Flag("no-default-browser-check", true),
		chromedp.Flag("headless", true),
		chromedp.Flag("disable-gpu", true),
		chromedp.Flag("no-sandbox", true),
		chromedp.Flag("disable-dev-shm-usage", true),
		chromedp.Flag("disable-crash-reporter", true),
		chromedp.Flag("disable-crashpad", true),
		chromedp.Flag("disable-extensions", true),
		chromedp.Flag("disable-popup-blocking", true),
		chromedp.Flag("disable-background-networking", true),
		chromedp.Flag("disable-renderer-backgrounding", true),
		chromedp.Flag("disable-blink-features", "AutomationControlled"),
		chromedp.Flag("exclude-switches", "enable-automation"),
		chromedp.Flag("disable-infobars", true),
		chromedp.Flag("lang", "en-US"),
		chromedp.UserDataDir(cfg.ProfileDir),
		chromedp.UserAgent(cfg.UserAgent),
	}
	if proxyURL != "" {
		opts = append(opts, chromedp.Flag("proxy-server", proxyURL))
	}
	allocCtx, cancelAlloc := chromedp.NewExecAllocator(parent, opts...)
	browserCtx, cancelBrowser := chromedp.NewContext(allocCtx)

	// Inject stealth JS that runs before every page load
	if err := chromedp.Run(browserCtx,
		chromedp.ActionFunc(func(ctx context.Context) error {
			_, err := page.AddScriptToEvaluateOnNewDocument(
				`Object.defineProperty(navigator,'webdriver',{get:()=>undefined});` +
					`window.chrome={runtime:{}};`,
			).Do(ctx)
			return err
		}),
	); err != nil {
		cancelBrowser()
		cancelAlloc()
		return nil, fmt.Errorf("browser stealth init: %w", err)
	}

	return &browserSession{
		allocCtx:      allocCtx,
		cancelAlloc:   cancelAlloc,
		browserCtx:    browserCtx,
		cancelBrowser: cancelBrowser,
		proxyURL:      proxyURL,
	}, nil
}

func closeBrowserSession(sess *browserSession) {
	if sess == nil {
		return
	}
	sess.cancelBrowser()
	sess.cancelAlloc()
}
```

- [ ] **Step 7.3: Add `renderPage()` — replaces `render()`**

Add `renderPage()` after `closeBrowserSession()`:

```go
// renderPage renders target using the given persistent session.
// It captures the real HTTP status code via CDP network events.
// On success, sess.reqCount is incremented.
func renderPage(sess *browserSession, target string, timeout time.Duration) (renderedPage, error) {
	started := time.Now().UTC()

	statusCh := make(chan int, 10)
	listenerCtx, cancelListener := context.WithCancel(sess.browserCtx)
	chromedp.ListenTarget(listenerCtx, func(ev interface{}) {
		if e, ok := ev.(*network.EventResponseReceived); ok {
			if e.Type == network.ResourceTypeDocument {
				select {
				case statusCh <- int(e.Response.Status):
				default:
				}
			}
		}
	})

	timeoutCtx, cancelTimeout := context.WithTimeout(sess.browserCtx, timeout)
	defer cancelTimeout()

	var html string
	err := chromedp.Run(timeoutCtx,
		network.Enable(),
		chromedp.Navigate(target),
		chromedp.WaitReady("body", chromedp.ByQuery),
		chromedp.OuterHTML("html", &html, chromedp.ByQuery),
	)
	cancelListener()

	if err != nil {
		return renderedPage{}, err
	}

	statusCode := 200
	select {
	case code := <-statusCh:
		statusCode = code
	default:
	}

	sess.reqCount++
	return renderedPage{
		URL:        target,
		HTML:       html,
		StatusCode: statusCode,
		FetchedAt:  started,
		Latency:    time.Since(started),
	}, nil
}
```

- [ ] **Step 7.4: Build to verify**

```bash
cd /home/hal9000/docker/oida_backend
go build ./cmd/worker-vesselfinder/... 2>&1
```
Expected: compiles cleanly. (The old `render()` still exists and is still called — we'll remove it in the next task.)

- [ ] **Step 7.5: Commit**

```bash
git add cmd/worker-vesselfinder/main.go
git commit -m "feat(worker-vf): add stealth browser session with persistent profile, proxy injection, and real status capture"
```

---

### Task 8: Rewrite `runScan` with Session Lifecycle + Proxy Rotation + Throttle

**Files:**
- Modify: `cmd/worker-vesselfinder/main.go` — `runScan`, `runScanLoop`

- [ ] **Step 8.1: Rewrite `runScanLoop` to manage a session**

Replace `runScanLoop` (lines 136-164) with:

```go
func runScanLoop(cfg config, pool *proxypool.Pool, scanThrottle *throttle.Adaptive) {
	if cfg.ScanBatchLimit < cfg.Workers {
		cfg.ScanBatchLimit = cfg.Workers
	}

	var sess *browserSession
	ensureSession := func() error {
		if sess != nil && sess.reqCount < cfg.BrowserRecycleAfter {
			return nil
		}
		closeBrowserSession(sess)
		sess = nil
		proxyURL, ok := pool.Pick()
		if !ok {
			return fmt.Errorf("no active proxies in pool")
		}
		var err error
		sess, err = newBrowserSession(context.Background(), cfg, proxyURL)
		if err != nil {
			pool.Disable(proxyURL)
			return fmt.Errorf("browser session: %w", err)
		}
		observability.LogEvent("worker-vesselfinder", "browser_recycled", "", map[string]any{
			"proxy": proxyURL,
		})
		return nil
	}
	rotateProxy := func() {
		if sess != nil {
			pool.Disable(sess.proxyURL)
		}
		closeBrowserSession(sess)
		sess = nil
		scanThrottle.RecordBlock()
	}

	defer func() { closeBrowserSession(sess) }()

	for {
		if err := ensureSession(); err != nil {
			observability.LogEvent("worker-vesselfinder", "scan_loop_no_proxy", "", map[string]any{"error": err.Error()})
			time.Sleep(30 * time.Second)
			continue
		}

		stats, err := runScan(context.Background(), cfg, store{runner: migrate.NewHTTPRunner(cfg.ClickHouseHTTP)}, sess, scanThrottle)
		if err != nil {
			observability.LogEvent("worker-vesselfinder", "scan_loop_error", "", map[string]any{"error": err.Error()})
			time.Sleep(10 * time.Second)
			continue
		}

		if blocked, _ := stats["bot_block"].(bool); blocked {
			rotateProxy()
			continue
		}

		if claimed, _ := stats["claimed"].(int); claimed == 0 {
			time.Sleep(10 * time.Second)
			continue
		}
		observability.LogEvent("worker-vesselfinder", "scan_loop_batch", "", map[string]any{
			"source_id": cfg.SourceID,
			"claimed":   stats["claimed"],
			"scanned":   stats["scanned"],
			"failed":    stats["failed"],
		})
		time.Sleep(2 * time.Second)
	}
}
```

- [ ] **Step 8.2: Rewrite `runScan` to use `renderPage` and throttle**

Replace `runScan` (lines 260-292) with:

```go
func runScan(ctx context.Context, cfg config, st store, sess *browserSession, th *throttle.Adaptive) (map[string]any, error) {
	items, err := st.claimScanQueue(ctx, cfg.SourceID, cfg.ScanBatchLimit)
	if err != nil {
		return nil, err
	}
	scanned, failed := 0, 0
	for idx, item := range items {
		if idx > 0 {
			time.Sleep(th.Delay())
		}
		page, err := renderPage(sess, item.DetailURL, cfg.DetailTimeout)
		if err != nil {
			_ = st.updateScanFailure(ctx, cfg.SourceID, item, classifyRenderError(err), 0)
			failed++
			continue
		}

		// Detect block at fetch time
		if page.StatusCode == 403 || page.StatusCode == 429 || vf.IsBotPage(page.HTML) {
			_ = st.updateScanFailure(ctx, cfg.SourceID, item, "bot_block", page.StatusCode)
			failed++
			return map[string]any{
				"source_id": cfg.SourceID, "claimed": len(items),
				"scanned": scanned, "failed": failed, "bot_block": true,
			}, nil
		}

		if page.StatusCode == 200 {
			if err := st.insertRetainedHTML(ctx, cfg, page, item); err != nil {
				return nil, err
			}
			_ = st.updateScanSuccess(ctx, cfg.SourceID, item)
			th.RecordSuccess()
			scanned++
			continue
		}
		_ = st.updateScanFailure(ctx, cfg.SourceID, item, "http_status", page.StatusCode)
		failed++
	}
	return map[string]any{"source_id": cfg.SourceID, "claimed": len(items), "scanned": scanned, "failed": failed}, nil
}
```

- [ ] **Step 8.3: Build to verify**

```bash
cd /home/hal9000/docker/oida_backend
go build ./cmd/worker-vesselfinder/... 2>&1
```
Expected: compiles cleanly. (serve() still calls old runScanLoop/runScan — fix in Task 10.)

- [ ] **Step 8.4: Commit**

```bash
git add cmd/worker-vesselfinder/main.go
git commit -m "feat(worker-vf): rewrite scan loop with session lifecycle, proxy rotation on block, adaptive throttle"
```

---

### Task 9: Rewrite `runDiscovery` with Session + Throttle

**Files:**
- Modify: `cmd/worker-vesselfinder/main.go` — `runDiscovery`, `runDiscoveryLoop`

- [ ] **Step 9.1: Rewrite `runDiscoveryLoop`**

Replace `runDiscoveryLoop` (lines 117-134) with:

```go
func runDiscoveryLoop(cfg config, pool *proxypool.Pool, discThrottle *throttle.Adaptive) {
	var sess *browserSession
	backoff := 30 * time.Second

	defer func() { closeBrowserSession(sess) }()

	ensureSession := func() error {
		if sess != nil && sess.reqCount < cfg.BrowserRecycleAfter {
			return nil
		}
		closeBrowserSession(sess)
		sess = nil
		proxyURL, ok := pool.Pick()
		if !ok {
			return fmt.Errorf("no active proxies")
		}
		var err error
		sess, err = newBrowserSession(context.Background(), cfg, proxyURL)
		if err != nil {
			pool.Disable(proxyURL)
			return err
		}
		return nil
	}

	for {
		if err := ensureSession(); err != nil {
			observability.LogEvent("worker-vesselfinder", "discovery_no_proxy", "", map[string]any{"error": err.Error()})
			time.Sleep(30 * time.Second)
			continue
		}

		if _, err := runDiscovery(context.Background(), cfg, store{runner: migrate.NewHTTPRunner(cfg.ClickHouseHTTP)}, defaultDimensionSeed, sess, discThrottle); err != nil {
			observability.LogEvent("worker-vesselfinder", "discovery_loop_error", "", map[string]any{"error": err.Error(), "source_id": cfg.SourceID})
			// Rotate on bot block; otherwise back off
			if err.Error() == "bot_block" {
				pool.Disable(sess.proxyURL)
				closeBrowserSession(sess)
				sess = nil
				discThrottle.RecordBlock()
			} else {
				time.Sleep(backoff)
				if backoff < 15*time.Minute {
					backoff *= 2
				}
			}
			continue
		}
		backoff = 30 * time.Second
		time.Sleep(cfg.RediscoveryInterval)
	}
}
```

- [ ] **Step 9.2: Rewrite `runDiscovery` to use renderPage and throttle**

Replace `runDiscovery` (lines 198-258) with:

```go
func runDiscovery(ctx context.Context, cfg config, st store, seed int64, sess *browserSession, th *throttle.Adaptive) (map[string]any, error) {
	dimPage, err := renderPage(sess, vesselFinderBaseURL+"/vessels", cfg.ListTimeout)
	if err != nil {
		return nil, err
	}
	if dimPage.StatusCode == 403 || dimPage.StatusCode == 429 || vf.IsBotPage(dimPage.HTML) {
		return nil, fmt.Errorf("bot_block")
	}

	dims := vf.ExtractDimensions(dimPage.HTML)
	if err := st.upsertDimensions(ctx, cfg.SourceID, "country", dims.Countries); err != nil {
		return nil, err
	}
	if err := st.upsertDimensions(ctx, cfg.SourceID, "type", dims.Types); err != nil {
		return nil, err
	}
	countries := dimensionLabelMap(dims.Countries)
	types := dimensionLabelMap(dims.Types)
	jobs := vf.BuildPageJobs(dims.Countries, dims.Types, cfg.MaxPage, seed)
	insertedJobs, insertedLinks := 0, 0
	terminals, err := st.loadTerminal404(ctx, cfg.SourceID)
	if err != nil {
		return nil, err
	}
	for idx, job := range jobs {
		if vf.ShouldSkipPage(job, terminals) {
			continue
		}
		if idx > 0 {
			time.Sleep(th.Delay())
		}
		listPage, err := renderPage(sess, listURL(job), cfg.ListTimeout)
		if err != nil {
			if upsertErr := st.upsertPageJob(ctx, cfg.SourceID, job, 0, "failed", "render_error"); upsertErr != nil {
				return nil, upsertErr
			}
			continue
		}
		if listPage.StatusCode == 403 || listPage.StatusCode == 429 || vf.IsBotPage(listPage.HTML) {
			return nil, fmt.Errorf("bot_block")
		}
		th.RecordSuccess()
		links := vf.ExtractDetailLinks(listPage.HTML, listPage.URL)
		status, terminal := vf.ListPageOutcome(listPage.StatusCode, links)
		if err := st.upsertPageJob(ctx, cfg.SourceID, job, listPage.StatusCode, status, ""); err != nil {
			return nil, err
		}
		insertedJobs++
		for _, link := range links {
			item := vf.ScanQueueItem{
				DetailURL:    link,
				CountryCode:  job.CountryCode,
				CountryLabel: countries[job.CountryCode],
				TypeCode:     job.TypeCode,
				TypeLabel:    types[job.TypeCode],
				PlaceID:      flagPlaceID(job.CountryCode),
			}
			if err := st.upsertScanQueue(ctx, cfg.SourceID, item, time.Now().UTC()); err != nil {
				return nil, err
			}
			insertedLinks++
		}
		if terminal {
			terminals = append(terminals, vf.Terminal404{CountryCode: job.CountryCode, TypeCode: job.TypeCode, Page: job.Page})
		}
	}
	return map[string]any{"source_id": cfg.SourceID, "dimensions": len(dims.Countries) * len(dims.Types), "jobs": insertedJobs, "links": insertedLinks}, nil
}
```

- [ ] **Step 9.3: Build**

```bash
cd /home/hal9000/docker/oida_backend
go build ./cmd/worker-vesselfinder/... 2>&1
```

- [ ] **Step 9.4: Commit**

```bash
git add cmd/worker-vesselfinder/main.go
git commit -m "feat(worker-vf): rewrite discovery loop with session management and adaptive throttle"
```

---

### Task 10: Wire Everything in `serve()` + Remove Old `render()`

**Files:**
- Modify: `cmd/worker-vesselfinder/main.go` — `serve()`, remove `render()`, update `discoverOnce` / `scanOnce`

- [ ] **Step 10.1: Rewrite `serve()`**

Replace `serve` (lines 107-115) with:

```go
func serve(cfg config) {
	pool := proxypool.New()
	refresher := proxypool.NewRefresher(cfg.ProxySourcesFile, pool, cfg.ProxyRefreshInterval)
	validator := proxypool.NewValidator(pool, cfg.ProxyValidateInterval)

	detailFloor := time.Duration(float64(time.Minute) / cfg.RateFloorPerMin)
	detailCeil := time.Duration(float64(time.Minute) / cfg.RateCeilPerMin)
	discFloor := time.Duration(float64(time.Second) / cfg.DiscoveryFloorRPS)
	discCeil := time.Duration(float64(time.Second) / cfg.DiscoveryCeilRPS)

	scanThrottle := throttle.New(detailFloor, detailCeil, cfg.RateRampDuration)
	discThrottle := throttle.New(discFloor, discCeil, cfg.RateRampDuration)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go refresher.Run(ctx)
	go validator.Run(ctx)

	observability.LogEvent("worker-vesselfinder", "service_started", observability.NewCorrelationID("worker-vesselfinder"), map[string]any{
		"source_id":            cfg.SourceID,
		"proxy_sources_file":   cfg.ProxySourcesFile,
		"browser_recycle_after": cfg.BrowserRecycleAfter,
	})

	go runDiscoveryLoop(cfg, pool, discThrottle)
	runScanLoop(cfg, pool, scanThrottle)
}
```

- [ ] **Step 10.2: Update `discoverOnce` and `scanOnce`**

These one-shot commands need their own sessions. Replace `discoverOnce` (lines 166-180):

```go
func discoverOnce(cfg config, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("discover-once", flag.ContinueOnError)
	fs.SetOutput(stderr)
	seed := fs.Int64("seed", defaultDimensionSeed, "Deterministic shuffle seed.")
	proxyURL := fs.String("proxy", "", "Proxy URL (e.g. socks5://host:port). Optional.")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	sess, err := newBrowserSession(context.Background(), cfg, *proxyURL)
	if err != nil {
		fmt.Fprintf(stderr, "browser: %v\n", err)
		return 1
	}
	defer closeBrowserSession(sess)
	th := throttle.New(
		time.Duration(float64(time.Second)/cfg.DiscoveryFloorRPS),
		time.Duration(float64(time.Second)/cfg.DiscoveryCeilRPS),
		cfg.RateRampDuration,
	)
	stats, err := runDiscovery(context.Background(), cfg, store{runner: migrate.NewHTTPRunner(cfg.ClickHouseHTTP)}, *seed, sess, th)
	if err != nil {
		fmt.Fprintf(stderr, "discover: %v\n", err)
		return 1
	}
	return writeJSON(stdout, stats)
}
```

Replace `scanOnce` (lines 182-196):

```go
func scanOnce(cfg config, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("scan-once", flag.ContinueOnError)
	fs.SetOutput(stderr)
	limit := fs.Int("limit", cfg.ScanBatchLimit, "Maximum detail pages to scan.")
	proxyURL := fs.String("proxy", "", "Proxy URL (e.g. socks5://host:port). Optional.")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	cfg.ScanBatchLimit = *limit
	sess, err := newBrowserSession(context.Background(), cfg, *proxyURL)
	if err != nil {
		fmt.Fprintf(stderr, "browser: %v\n", err)
		return 1
	}
	defer closeBrowserSession(sess)
	th := throttle.New(
		time.Duration(float64(time.Minute)/cfg.RateFloorPerMin),
		time.Duration(float64(time.Minute)/cfg.RateCeilPerMin),
		cfg.RateRampDuration,
	)
	stats, err := runScan(context.Background(), cfg, store{runner: migrate.NewHTTPRunner(cfg.ClickHouseHTTP)}, sess, th)
	if err != nil {
		fmt.Fprintf(stderr, "scan: %v\n", err)
		return 1
	}
	return writeJSON(stdout, stats)
}
```

- [ ] **Step 10.3: Delete the old `render()` function**

Delete lines 294-329 (the `render()` function). Also delete `preflightTCP` calls from `runScan` (now replaced) — the preflight is kept only if still needed by `preflightTCP` itself. The function body (`preflightTCP`, `classifyRenderError`) can stay as they may be called by other code; verify with `grep`.

```bash
grep -n "preflightTCP\|render(" /home/hal9000/docker/oida_backend/cmd/worker-vesselfinder/main.go
```

Remove `render()` entirely and any remaining `preflightTCP(ctx, vesselFinderBaseURL, ...)` calls in `runScan` (which has been replaced).

- [ ] **Step 10.4: Build and run tests**

```bash
cd /home/hal9000/docker/oida_backend
go build ./cmd/worker-vesselfinder/... 2>&1
go test ./cmd/worker-vesselfinder/... 2>&1
```
Expected: builds cleanly; existing tests pass or are updated for new signatures.

- [ ] **Step 10.5: Commit**

```bash
git add cmd/worker-vesselfinder/main.go
git commit -m "feat(worker-vf): wire proxy pool + adaptive throttle into serve(); remove legacy render()"
```

---

### Task 11: `config/proxies/proxy_sources.txt` + Docker Compose

**Files:**
- Create: `config/proxies/proxy_sources.txt`
- Modify: `docker-compose.yml`

- [ ] **Step 11.1: Create `config/proxies/proxy_sources.txt`**

```
# VesselFinder proxy source list
# Format: [protocol] <url>
# - Lines starting with # are comments.
# - If a protocol hint (http, https, socks4, socks5) is given before the URL,
#   bare IP:PORT lines from that source are treated as that protocol.
# - If no hint, each line must contain protocol://IP:PORT.
# - This file is hot-reloaded every PROXY_REFRESH_INTERVAL (default: 1h).

# proxifly — HTTP/HTTPS/SOCKS4/SOCKS5 (~every 5 min)
http https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt
socks5 https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks5/data.txt

# TheSpeedX — large public lists
http https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt
socks4 https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt
socks5 https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt

# vakhov/fresh-proxy-list
https https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/https.txt
socks4 https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks4.txt
socks5 https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt

# r00tee/Proxy-List
socks5 https://raw.githubusercontent.com/r00tee/Proxy-List/main/Socks5.txt

# databay-labs
http https://raw.githubusercontent.com/databay-labs/free-proxy-list/main/proxies/http.txt

# dpangestuw/Free-Proxy
http https://raw.githubusercontent.com/dpangestuw/Free-Proxy/main/http.txt
socks5 https://raw.githubusercontent.com/dpangestuw/Free-Proxy/main/socks5.txt

# casa-ls
http https://raw.githubusercontent.com/casa-ls/proxy-list/main/http
socks5 https://raw.githubusercontent.com/casa-ls/proxy-list/main/socks5
```

- [ ] **Step 11.2: Update `docker-compose.yml` worker-vesselfinder service**

Find the `worker-vesselfinder:` service (lines ~183-210) and apply these changes:

**Add to environment block:**
```yaml
      PROXY_SOURCES_FILE: /config/proxies/proxy_sources.txt
      PROXY_REFRESH_INTERVAL: ${VESSELFINDER_PROXY_REFRESH_INTERVAL:-3600s}
      PROXY_VALIDATE_INTERVAL: ${VESSELFINDER_PROXY_VALIDATE_INTERVAL:-300s}
      BROWSER_RECYCLE_AFTER: ${VESSELFINDER_BROWSER_RECYCLE_AFTER:-50}
      CHROME_PROFILE_DIR: /chrome-profile
      RATE_FLOOR_PER_MIN: ${VESSELFINDER_RATE_FLOOR_PER_MIN:-2.0}
      RATE_CEIL_PER_MIN: ${VESSELFINDER_RATE_CEIL_PER_MIN:-6.0}
      RATE_RAMP_DURATION: ${VESSELFINDER_RATE_RAMP_DURATION:-1800s}
      DISCOVERY_FLOOR_RPS: ${VESSELFINDER_DISCOVERY_FLOOR_RPS:-0.1}
      DISCOVERY_CEIL_RPS: ${VESSELFINDER_DISCOVERY_CEIL_RPS:-0.3}
```

**Add volumes block to the service:**
```yaml
    volumes:
      - ./config/proxies:/config/proxies:ro
      - vessel_chrome_profile:/chrome-profile
```

**Add to top-level `volumes:` section** (find the `volumes:` key in docker-compose.yml):
```yaml
  vessel_chrome_profile:
```

- [ ] **Step 11.3: Remove old env vars that are now superseded**

The old `WORKER_RATE_PER_MINUTE` and `DISCOVERY_RPS` in the compose file are superseded by the new adaptive floor/ceiling vars. Remove them to avoid confusion. (The constants `defaultWorkerRate` and `defaultDiscoveryRPS` in main.go can be left as-is or removed if no longer referenced.)

- [ ] **Step 11.4: Build final check**

```bash
cd /home/hal9000/docker/oida_backend
go build ./... 2>&1
go test ./internal/proxypool/... ./internal/throttle/... ./internal/packs/maritime/vesselfinder/... 2>&1
```
Expected: everything builds and tests pass.

- [ ] **Step 11.5: Commit**

```bash
git add config/proxies/proxy_sources.txt docker-compose.yml
git commit -m "feat(worker-vf): add proxy sources config and docker volume mounts for proxy list and chrome profile"
```

---

## Verification Plan

After all tasks are complete:

**1. Local build:**
```bash
cd /home/hal9000/docker/oida_backend
go build ./... && go test ./internal/proxypool/... ./internal/throttle/... ./internal/packs/maritime/vesselfinder/...
```
Expected: zero errors, all tests green.

**2. Test proxy pool with a real source fetch:**
```bash
# Quick smoke test — needs internet access
cd /home/hal9000/docker/oida_backend
cat > /tmp/test_pool.go << 'EOF'
package main

import (
    "context"
    "fmt"
    "time"
    "global-osint-backend/internal/proxypool"
)

func main() {
    pool := proxypool.New()
    r := proxypool.NewRefresher("config/proxies/proxy_sources.txt", pool, time.Hour)
    if err := r.Run(context.Background()); err != nil {
        panic(err)
    }
    active, total := pool.Stats()
    fmt.Printf("Pool: %d active / %d total\n", active, total)
    proxy, ok := pool.Pick()
    fmt.Printf("Sample proxy: %s (ok=%v)\n", proxy, ok)
}
EOF
go run /tmp/test_pool.go
```
Expected: `Pool: N active / M total` with N > 0.

**3. Docker build:**
```bash
COMPOSE_PROFILES=live-crawl docker compose build worker-vesselfinder 2>&1 | tail -5
```
Expected: image builds without error.

**4. Smoke test with scan-once:**
```bash
COMPOSE_PROFILES=live-crawl docker compose run --rm worker-vesselfinder scan-once -limit 1 -proxy socks5://YOUR_KNOWN_PROXY:PORT
```
Watch logs for: `browser_recycled` event with proxy URL, `scan_loop_batch` with scanned=1, no `bot_block` events.

**5. Block detection test:**
```bash
# With a deliberately broken proxy, verify bot_block is surfaced and proxy gets disabled
COMPOSE_PROFILES=live-crawl docker compose run --rm \
  -e PROXY_SOURCES_FILE=/dev/null \
  worker-vesselfinder scan-once -limit 1 -proxy http://127.0.0.1:1
```
Expected: `bot_block` or render error, clean failure, worker exits cleanly.

**6. Profile persistence:**
```bash
# Run twice — second run should use the same chrome profile volume
COMPOSE_PROFILES=live-crawl docker compose run --rm worker-vesselfinder scan-once -limit 1
COMPOSE_PROFILES=live-crawl docker compose run --rm worker-vesselfinder scan-once -limit 1
```
Check that the `vessel_chrome_profile` volume persists between runs (`docker volume inspect oida_backend_vessel_chrome_profile`).
