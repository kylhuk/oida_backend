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

func TestValidator_RunStopsOnContextCancel(t *testing.T) {
	pool := New()
	v := NewValidator(pool, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { v.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Validator.Run did not stop after context cancel")
	}
}
