package resilience

import (
	"errors"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

func fail() error { return errBoom }
func ok() error   { return nil }

func TestBreakerOpensAfterThreshold(t *testing.T) {
	b := NewBreaker("t", 3, time.Minute)
	for i := 0; i < 3; i++ {
		b.Execute(fail)
	}
	if b.State() != StateOpen {
		t.Fatalf("state = %v, want open", b.State())
	}

	called := false
	err := b.Execute(func() error { called = true; return nil })
	if !errors.Is(err, ErrCircuitOpen) || called {
		t.Error("open breaker must fail fast without calling fn")
	}
}

func TestBreakerSuccessResetsFailureCount(t *testing.T) {
	b := NewBreaker("t", 3, time.Minute)
	b.Execute(fail)
	b.Execute(fail)
	b.Execute(ok)
	b.Execute(fail)
	if b.State() != StateClosed {
		t.Error("failures are counted consecutively; a success should reset them")
	}
}

func TestBreakerHalfOpenRecovery(t *testing.T) {
	now := time.Now()
	b := NewBreaker("t", 1, 10*time.Second)
	b.now = func() time.Time { return now }

	b.Execute(fail)
	if b.State() != StateOpen {
		t.Fatal("expected open")
	}

	now = now.Add(11 * time.Second) // cool-off passed
	if err := b.Execute(ok); err != nil {
		t.Fatal(err)
	}
	if b.State() != StateClosed {
		t.Error("successful trial should close the breaker")
	}
}

func TestBreakerHalfOpenFailureReopens(t *testing.T) {
	now := time.Now()
	b := NewBreaker("t", 1, 10*time.Second)
	b.now = func() time.Time { return now }

	b.Execute(fail)
	now = now.Add(11 * time.Second)
	b.Execute(fail)
	if b.State() != StateOpen {
		t.Error("failed trial should reopen the breaker")
	}
}
