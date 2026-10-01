package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"kyc-platform/services/webhook-service/internal/delivery"
	"kyc-platform/shared/contracts"

	"github.com/segmentio/kafka-go"
)

type published struct {
	topic   string
	attempt contracts.WebhookAttempt
}

type fakePublisher struct{ msgs []published }

func (p *fakePublisher) Publish(ctx context.Context, topic, key string, event any) error {
	p.msgs = append(p.msgs, published{topic: topic, attempt: event.(contracts.WebhookAttempt)})
	return nil
}

type fakeSender struct {
	res   delivery.Result
	calls int
}

func (s *fakeSender) Deliver(ctx context.Context, ep delivery.Endpoint, eventID string, body []byte) delivery.Result {
	s.calls++
	return s.res
}

func eventMsg(t *testing.T, clientID string) kafka.Message {
	b, err := json.Marshal(contracts.VerificationCompletedEvent{EventID: "e1", ClientID: clientID})
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Value: b}
}

func setup(res delivery.Result) (*Dispatcher, *fakeSender, *fakePublisher) {
	s := &fakeSender{res: res}
	p := &fakePublisher{}
	policy := delivery.RetryPolicy{MaxAttempts: 3, Base: time.Second, Max: time.Minute}
	d := New([]delivery.Endpoint{{ClientID: "bank-1", URL: "http://x"}}, s, policy, p)
	return d, s, p
}

func TestSuccessPublishesNothing(t *testing.T) {
	d, s, p := setup(delivery.Result{StatusCode: 200})
	d.HandleEvent(context.Background(), eventMsg(t, "bank-1"))
	if s.calls != 1 || len(p.msgs) != 0 {
		t.Errorf("calls=%d published=%d", s.calls, len(p.msgs))
	}
}

func TestRetryableFailureGoesToRetryTopic(t *testing.T) {
	d, _, p := setup(delivery.Result{StatusCode: 503})
	d.HandleEvent(context.Background(), eventMsg(t, "bank-1"))

	if len(p.msgs) != 1 || p.msgs[0].topic != contracts.TopicWebhookRetry {
		t.Fatalf("published %+v", p.msgs)
	}
	if p.msgs[0].attempt.Attempt != 2 || p.msgs[0].attempt.LastError != "status 503" {
		t.Errorf("unexpected retry %+v", p.msgs[0].attempt)
	}
}

func TestPermanentFailureGoesToDLQ(t *testing.T) {
	d, _, p := setup(delivery.Result{StatusCode: 400})
	d.HandleEvent(context.Background(), eventMsg(t, "bank-1"))
	if len(p.msgs) != 1 || p.msgs[0].topic != contracts.TopicWebhookDLQ {
		t.Fatalf("published %+v", p.msgs)
	}
}

func TestLastAttemptGoesToDLQ(t *testing.T) {
	d, _, p := setup(delivery.Result{Err: errors.New("connection refused")})
	b, _ := json.Marshal(contracts.WebhookAttempt{
		Event:   contracts.VerificationCompletedEvent{EventID: "e1", ClientID: "bank-1"},
		Attempt: 3, // MaxAttempts is 3
	})
	d.HandleRetry(context.Background(), kafka.Message{Value: b})
	if len(p.msgs) != 1 || p.msgs[0].topic != contracts.TopicWebhookDLQ {
		t.Fatalf("published %+v", p.msgs)
	}
}

func TestRetryWaitsUntilDue(t *testing.T) {
	d, s, _ := setup(delivery.Result{StatusCode: 200})
	now := time.Now()
	d.now = func() time.Time { return now }

	b, _ := json.Marshal(contracts.WebhookAttempt{
		Event:         contracts.VerificationCompletedEvent{EventID: "e1", ClientID: "bank-1"},
		Attempt:       2,
		NextAttemptAt: now.Add(time.Hour),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := d.HandleRetry(ctx, kafka.Message{Value: b}); err == nil {
		t.Error("expected the wait to be interrupted by the context")
	}
	if s.calls != 0 {
		t.Error("must not deliver before the attempt is due")
	}
}

func TestUnknownClientIsSkipped(t *testing.T) {
	d, s, p := setup(delivery.Result{StatusCode: 200})
	d.HandleEvent(context.Background(), eventMsg(t, "no-webhook-client"))
	if s.calls != 0 || len(p.msgs) != 0 {
		t.Error("clients without a webhook must be skipped")
	}
}
