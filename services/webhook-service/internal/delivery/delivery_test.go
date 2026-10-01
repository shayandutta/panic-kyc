package delivery

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"kyc-platform/shared/webhooksig"
)

func TestDeliverSignsTheBody(t *testing.T) {
	var verifyErr error
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		verifyErr = webhooksig.Verify("whsec_test", r.Header.Get(webhooksig.Header), body, time.Now(), time.Minute)
		if r.Header.Get("X-KYC-Event-ID") != "evt-1" {
			verifyErr = errors.New("missing event id header")
		}
	}))
	defer srv.Close()

	res := NewSender(time.Second).Deliver(context.Background(), Endpoint{URL: srv.URL, Secret: "whsec_test"}, "evt-1", []byte(`{"a":1}`))
	if !res.OK() {
		t.Fatalf("delivery failed: %v", res)
	}
	if verifyErr != nil {
		t.Errorf("receiver could not verify: %v", verifyErr)
	}
}

func TestResultClassification(t *testing.T) {
	cases := []struct {
		status    int
		retryable bool
	}{
		{503, true}, {500, true}, {429, true}, {408, true},
		{400, false}, {401, false}, {404, false}, {410, false},
	}
	for _, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(c.status)
		}))
		res := NewSender(time.Second).Deliver(context.Background(), Endpoint{URL: srv.URL}, "e", nil)
		srv.Close()
		if res.Retryable() != c.retryable {
			t.Errorf("status %d: retryable = %v, want %v", c.status, res.Retryable(), c.retryable)
		}
	}
}

func TestSlowReceiverTimesOutAndIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer srv.Close()

	res := NewSender(50*time.Millisecond).Deliver(context.Background(), Endpoint{URL: srv.URL}, "e", nil)
	if res.OK() || !res.Retryable() {
		t.Errorf("timeout should be a retryable failure, got %v", res)
	}
}

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 5, Base: time.Second, Max: 8 * time.Second}
	for attempt := 0; attempt < 10; attempt++ {
		ceiling := min(time.Second<<attempt, 8*time.Second)
		for i := 0; i < 50; i++ {
			if d := p.NextDelay(attempt); d < 0 || d > ceiling {
				t.Fatalf("attempt %d: delay %v outside [0, %v]", attempt, d, ceiling)
			}
		}
	}
	if !p.Exhausted(5) || p.Exhausted(4) {
		t.Error("Exhausted should flip at MaxAttempts")
	}
}
