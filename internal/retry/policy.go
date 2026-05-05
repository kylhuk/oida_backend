package retry

import "time"

type Policy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

func (p Policy) Normalize() Policy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 3
	}
	if p.InitialBackoff <= 0 {
		p.InitialBackoff = 250 * time.Millisecond
	}
	if p.MaxBackoff <= 0 {
		p.MaxBackoff = 3 * time.Second
	}
	if p.MaxBackoff < p.InitialBackoff {
		p.MaxBackoff = p.InitialBackoff
	}
	return p
}

func (p Policy) Backoff(attempt int) time.Duration {
	policy := p.Normalize()
	if attempt <= 1 {
		return policy.InitialBackoff
	}
	backoff := policy.InitialBackoff
	for i := 1; i < attempt; i++ {
		if backoff >= policy.MaxBackoff {
			return policy.MaxBackoff
		}
		backoff *= 2
	}
	if backoff > policy.MaxBackoff {
		return policy.MaxBackoff
	}
	return backoff
}

func (p Policy) Exhausted(attempt int) bool {
	policy := p.Normalize()
	return attempt >= policy.MaxAttempts
}

func (p Policy) NextRetryAt(attempt int, now time.Time) (*time.Time, bool) {
	if p.Exhausted(attempt) {
		return nil, false
	}
	next := now.UTC().Add(p.Backoff(attempt))
	return &next, true
}
