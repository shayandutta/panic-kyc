// Package dispatcher turns events into webhook deliveries and routes failures
// to the retry topic or the dead letter topic.
package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"kyc-platform/services/webhook-service/internal/delivery"
	"kyc-platform/shared/contracts"

	"github.com/segmentio/kafka-go"
)

// Publisher puts messages on Kafka topics. events.Producer implements it.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, event any) error
}

type Sender interface {
	Deliver(ctx context.Context, ep delivery.Endpoint, eventID string, body []byte) delivery.Result
}

type Dispatcher struct {
	endpoints map[string]delivery.Endpoint
	sender    Sender
	policy    delivery.RetryPolicy
	publisher Publisher
	now       func() time.Time
}

func New(endpoints []delivery.Endpoint, sender Sender, policy delivery.RetryPolicy, publisher Publisher) *Dispatcher {
	byClient := make(map[string]delivery.Endpoint, len(endpoints))
	for _, ep := range endpoints {
		byClient[ep.ClientID] = ep
	}
	return &Dispatcher{endpoints: byClient, sender: sender, policy: policy, publisher: publisher, now: time.Now}
}

// HandleEvent processes a fresh verification.completed event: attempt 1.
func (d *Dispatcher) HandleEvent(ctx context.Context, msg kafka.Message) error {
	var event contracts.VerificationCompletedEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Printf("skipping malformed event at offset %d: %v", msg.Offset, err)
		return nil // a poison message must not block the partition
	}
	return d.attempt(ctx, contracts.WebhookAttempt{Event: event, Attempt: 1})
}

// HandleRetry processes a message from the retry topic. It waits until the
// attempt is due. Messages are written in due order per client, so waiting
// on the head of the partition is acceptable here; a large system would use
// several retry topics with fixed delays (1m, 10m, 1h) instead.
func (d *Dispatcher) HandleRetry(ctx context.Context, msg kafka.Message) error {
	var a contracts.WebhookAttempt
	if err := json.Unmarshal(msg.Value, &a); err != nil {
		log.Printf("skipping malformed retry at offset %d: %v", msg.Offset, err)
		return nil
	}

	if wait := a.NextAttemptAt.Sub(d.now()); wait > 0 {
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err() // not committed; picked up again after restart
		}
	}
	return d.attempt(ctx, a)
}

func (d *Dispatcher) attempt(ctx context.Context, a contracts.WebhookAttempt) error {
	ep, ok := d.endpoints[a.Event.ClientID]
	if !ok {
		return nil // client hasn't registered a webhook
	}

	body, err := json.Marshal(a.Event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	res := d.sender.Deliver(ctx, ep, a.Event.EventID, body)
	logLine := fmt.Sprintf("event_id=%s client_id=%s attempt=%d", a.Event.EventID, a.Event.ClientID, a.Attempt)

	if res.OK() {
		log.Printf("%s delivered status=%d duration=%s", logLine, res.StatusCode, res.Duration.Round(time.Millisecond))
		return nil
	}

	a.LastError = res.String()

	if !res.Retryable() || d.policy.Exhausted(a.Attempt) {
		log.Printf("%s dead-lettered: %s", logLine, a.LastError)
		return d.publisher.Publish(ctx, contracts.TopicWebhookDLQ, a.Event.ClientID, a)
	}

	delay := d.policy.NextDelay(a.Attempt)
	a.Attempt++
	a.NextAttemptAt = d.now().Add(delay)
	log.Printf("%s failed (%s), retrying in %s", logLine, a.LastError, delay.Round(time.Millisecond))
	// If this publish fails we return the error, so the original message is
	// not committed and will be processed again: nothing is lost.
	return d.publisher.Publish(ctx, contracts.TopicWebhookRetry, a.Event.ClientID, a)
}
