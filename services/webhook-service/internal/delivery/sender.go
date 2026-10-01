package delivery

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"kyc-platform/shared/webhooksig"
)

// Endpoint is where and how a client wants to receive webhooks.
type Endpoint struct {
	ClientID string `json:"client_id"`
	URL      string `json:"url"`
	Secret   string `json:"secret"`
}

// Result of one delivery attempt.
type Result struct {
	StatusCode int
	Err        error
	Duration   time.Duration
}

func (r Result) OK() bool { return r.Err == nil && r.StatusCode >= 200 && r.StatusCode < 300 }

func (r Result) Retryable() bool { return !r.OK() && Retryable(r.StatusCode, r.Err) }

func (r Result) String() string {
	if r.Err != nil {
		return r.Err.Error()
	}
	return fmt.Sprintf("status %d", r.StatusCode)
}

type Sender struct {
	client *http.Client
	now    func() time.Time
}

// NewSender uses a short timeout: receivers are told to reply 2xx quickly
// and process asynchronously. A slow receiver must not hold our workers.
func NewSender(timeout time.Duration) *Sender {
	return &Sender{client: &http.Client{Timeout: timeout}, now: time.Now}
}

func (s *Sender) Deliver(ctx context.Context, ep Endpoint, eventID string, body []byte) Result {
	start := s.now()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep.URL, bytes.NewReader(body))
	if err != nil {
		return Result{Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-KYC-Event-ID", eventID) // receivers deduplicate on this
	req.Header.Set(webhooksig.Header, webhooksig.Sign(ep.Secret, body, s.now()))

	resp, err := s.client.Do(req)
	if err != nil {
		return Result{Err: err, Duration: time.Since(start)}
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10)) // drain so the connection can be reused

	return Result{StatusCode: resp.StatusCode, Duration: time.Since(start)}
}
