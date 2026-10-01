// Package resilience holds patterns for calling unreliable dependencies.
package resilience

import (
	"errors"
	"sync"
	"time"
)

var ErrCircuitOpen = errors.New("circuit breaker is open")

type State int

const (
	StateClosed   State = iota // normal: calls go through
	StateOpen                  // tripped: calls fail fast without touching the dependency
	StateHalfOpen              // cooling off: one trial call decides the next state
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	}
	return "unknown"
}

// Breaker stops calling a dependency after repeated failures, then
// periodically lets one trial call through to see if it has recovered.
// It is safe for concurrent use.
type Breaker struct {
	mu               sync.Mutex
	name             string
	failureThreshold int           // consecutive failures before opening
	openTimeout      time.Duration // how long to stay open before a trial call
	state            State
	failures         int
	openedAt         time.Time
	trialInFlight    bool
	now              func() time.Time // replaceable in tests
}

func NewBreaker(name string, failureThreshold int, openTimeout time.Duration) *Breaker {
	return &Breaker{
		name:             name,
		failureThreshold: failureThreshold,
		openTimeout:      openTimeout,
		state:            StateClosed,
		now:              time.Now,
	}
}

// Execute runs fn if the breaker allows it and records the outcome.
func (b *Breaker) Execute(fn func() error) error {
	if err := b.allow(); err != nil {
		return err
	}
	err := fn()
	b.record(err)
	return err
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *Breaker) allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateOpen:
		if b.now().Sub(b.openedAt) < b.openTimeout {
			return ErrCircuitOpen
		}
		b.state = StateHalfOpen
		b.trialInFlight = true
		return nil
	case StateHalfOpen:
		// Only one trial call at a time; everyone else still fails fast.
		if b.trialInFlight {
			return ErrCircuitOpen
		}
		b.trialInFlight = true
		return nil
	}
	return nil
}

func (b *Breaker) record(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err == nil {
		b.state = StateClosed
		b.failures = 0
		b.trialInFlight = false
		return
	}

	if b.state == StateHalfOpen {
		b.trip() // trial failed, back to open
		return
	}

	b.failures++
	if b.failures >= b.failureThreshold {
		b.trip()
	}
}

func (b *Breaker) trip() {
	b.state = StateOpen
	b.openedAt = b.now()
	b.failures = 0
	b.trialInFlight = false
}
