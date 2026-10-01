package contracts

import "time"

// Kafka topics.
const (
	// TopicVerificationCompleted gets one event per finished verification.
	// Consumers: webhook delivery (and later billing, analytics).
	TopicVerificationCompleted = "verification.completed"

	// TopicWebhookRetry holds webhook deliveries waiting for another attempt.
	TopicWebhookRetry = "webhook.retry"

	// TopicWebhookDLQ holds deliveries that ran out of attempts.
	TopicWebhookDLQ = "webhook.dlq"
)

// VerificationCompletedEvent is published after every verification.
// It carries only masked PII.
type VerificationCompletedEvent struct {
	EventID        string    `json:"event_id"`
	ClientID       string    `json:"client_id"`
	VerificationID string    `json:"verification_id"`
	ReferenceID    string    `json:"reference_id"`
	PANMasked      string    `json:"pan_masked"`
	Status         string    `json:"status"`
	NameMatch      bool      `json:"name_match"`
	Source         string    `json:"source"`
	OccurredAt     time.Time `json:"occurred_at"`
}

// WebhookAttempt wraps an event on the retry and dead letter topics.
type WebhookAttempt struct {
	Event         VerificationCompletedEvent `json:"event"`
	Attempt       int                        `json:"attempt"`
	NextAttemptAt time.Time                  `json:"next_attempt_at"`
	LastError     string                     `json:"last_error"`
}
