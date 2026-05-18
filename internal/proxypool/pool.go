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
	return e.disabledUntil.IsZero() || time.Now().After(e.disabledUntil)
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
