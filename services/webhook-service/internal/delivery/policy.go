// Package delivery sends signed webhooks and decides what to do when they fail.
package delivery

import (
	"math/rand/v2"
	"net/http"
	"time"
)

// RetryPolicy is exponential backoff with full jitter: the wait is a random
// value between 0 and base*2^attempt (capped). The randomness spreads out
// retries, so clients that failed together don't all retry at the same instant.
type RetryPolicy struct {
	MaxAttempts int
	Base        time.Duration
	Max         time.Duration
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 6, Base: 2 * time.Second, Max: 10 * time.Minute}
}

// NextDelay is the wait before attempt number `attempt+1`.
func (p RetryPolicy) NextDelay(attempt int) time.Duration {
	ceiling := p.Base << min(attempt, 30)
	if ceiling <= 0 || ceiling > p.Max {
		ceiling = p.Max
	}
	return time.Duration(rand.Int64N(int64(ceiling) + 1))
}

// Exhausted reports whether we've used every attempt.
func (p RetryPolicy) Exhausted(attempt int) bool {
	return attempt >= p.MaxAttempts
}

// Retryable decides if a failed delivery is worth trying again.
// Network errors, timeouts, 408, 429 and 5xx are temporary.
// Other 4xx mean the receiver rejects the request; retrying won't help.
func Retryable(statusCode int, err error) bool {
	if err != nil {
		return true
	}
	switch {
	case statusCode == http.StatusRequestTimeout, statusCode == http.StatusTooManyRequests:
		return true
	case statusCode >= 500:
		return true
	default:
		return false
	}
}
