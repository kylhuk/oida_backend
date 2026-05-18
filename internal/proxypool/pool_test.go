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
