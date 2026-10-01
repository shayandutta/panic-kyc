package ratelimit

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestLimiter(t *testing.T) *Limiter {
	mr := miniredis.RunT(t) // in-memory Redis for tests
	return New(redis.NewClient(&redis.Options{Addr: mr.Addr()}))
}

func TestAllowsBurstThenBlocks(t *testing.T) {
	l := newTestLimiter(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		d, err := l.Allow(ctx, "bank-1", 1, 3)
		if err != nil {
			t.Fatal(err)
		}
		if !d.Allowed {
			t.Fatalf("request %d should be allowed within the burst", i+1)
		}
	}

	d, _ := l.Allow(ctx, "bank-1", 1, 3)
	if d.Allowed {
		t.Error("4th request should be rate limited")
	}
	if d.RetryAfter <= 0 {
		t.Error("blocked request should say when to retry")
	}
}

func TestClientsHaveSeparateBuckets(t *testing.T) {
	l := newTestLimiter(t)
	ctx := context.Background()

	l.Allow(ctx, "bank-1", 1, 1)
	if d, _ := l.Allow(ctx, "lender-2", 1, 1); !d.Allowed {
		t.Error("one client's usage must not affect another")
	}
}
