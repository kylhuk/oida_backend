package throttle

import (
	"testing"
	"time"
)

func TestAdaptive_StartsAtFloor(t *testing.T) {
	a := New(30*time.Second, 10*time.Second, time.Hour)
	a.mu.Lock()
	d := a.currentDelay()
	a.mu.Unlock()
	if d != 30*time.Second {
		t.Fatalf("expected floor 30s at start, got %v", d)
	}
}

func TestAdaptive_RampsTowardCeiling(t *testing.T) {
	a := New(30*time.Second, 10*time.Second, time.Hour)
	a.RecordSuccess()
	// Set cleanSince to half the ramp duration ago
	a.mu.Lock()
	a.cleanSince = time.Now().Add(-30 * time.Minute)
	a.mu.Unlock()
	a.mu.Lock()
	d := a.currentDelay()
	a.mu.Unlock()
	// At t=0.5: 30s + 0.5*(10s-30s) = 30s - 10s = 20s
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
	a.mu.Lock()
	d := a.currentDelay()
	a.mu.Unlock()
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
	a.mu.Lock()
	d := a.currentDelay()
	a.mu.Unlock()
	if d != 30*time.Second {
		t.Fatalf("expected floor after block, got %v", d)
	}
}

func TestAdaptive_DelayIncludesJitter(t *testing.T) {
	a := New(30*time.Second, 10*time.Second, time.Hour)
	// At floor (30s), jitter range is [30s*0.7, 30s*1.3] = [21s, 39s]
	for i := 0; i < 50; i++ {
		d := a.Delay()
		if d < 21*time.Second || d > 39*time.Second {
			t.Fatalf("delay %v out of jitter range [21s, 39s]", d)
		}
	}
}
