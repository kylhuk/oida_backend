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

func (v *Validator) probe(ctx context.Context) {
	d := &net.Dialer{Timeout: validatorDialTimeout}
	for _, proxyURL := range v.pool.ProbeDue() {
		if v.reachable(ctx, d, proxyURL) {
			v.pool.Reactivate(proxyURL)
			observability.LogEvent("proxy-pool", "proxy_reactivated", "", map[string]any{"proxy": proxyURL})
		} else {
			v.pool.Disable(proxyURL) // extends cooldown another 24h
			observability.LogEvent("proxy-pool", "proxy_still_dead", "", map[string]any{"proxy": proxyURL})
		}
	}
}

func (v *Validator) reachable(ctx context.Context, d *net.Dialer, proxyURL string) bool {
	parsed, err := url.Parse(proxyURL)
	if err != nil || parsed.Host == "" {
		return false
	}
	conn, err := d.DialContext(ctx, "tcp", parsed.Host)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}
