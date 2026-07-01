package squawk

import (
	"sync"
	"sync/atomic"
	"time"
)

// rateLimiter is a simple token-bucket rate limiter.
// Tokens refill at 1-per-minInterval up to burst capacity.
type rateLimiter struct {
	mu      sync.Mutex
	min     time.Duration
	burst   int
	tokens  int
	last    time.Time
	Dropped atomic.Int64
}

func newRateLimiter(min time.Duration, burst int) *rateLimiter {
	if burst < 1 {
		burst = 1
	}
	return &rateLimiter{
		min:    min,
		burst:  burst,
		tokens: burst,
	}
}

// Allow returns true if a snapshot should proceed, false if it should be dropped.
func (r *rateLimiter) Allow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	if !r.last.IsZero() {
		elapsed := now.Sub(r.last)
		if refill := int(elapsed / r.min); refill > 0 {
			r.tokens += refill
			if r.tokens > r.burst {
				r.tokens = r.burst
			}
			r.last = r.last.Add(time.Duration(refill) * r.min)
		}
	} else {
		r.last = now
	}

	if r.tokens > 0 {
		r.tokens--
		return true
	}
	r.Dropped.Add(1)
	return false
}
