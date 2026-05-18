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
	factor := 0.7 + 0.6*a.rng.Float64() // uniform [0.7, 1.3) — Float64 upper bound is exclusive
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
