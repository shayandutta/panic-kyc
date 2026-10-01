// Package publisher announces finished verifications on Kafka.
package publisher

import (
	"context"

	"kyc-platform/services/verification-service/internal/domain"
	"kyc-platform/shared/contracts"
	"kyc-platform/shared/events"
)

type KafkaPublisher struct {
	producer *events.Producer
}

func NewKafkaPublisher(producer *events.Producer) *KafkaPublisher {
	return &KafkaPublisher{producer: producer}
}

// PublishVerificationCompleted sends only masked data. Kafka keeps messages
// for days and copies them to every consumer, so no raw PII goes in.
// The verification ID doubles as the event ID: one event per verification,
// so consumers can deduplicate on it.
func (p *KafkaPublisher) PublishVerificationCompleted(ctx context.Context, v *domain.Verification) error {
	return p.producer.Publish(ctx, contracts.TopicVerificationCompleted, v.ClientID, contracts.VerificationCompletedEvent{
		EventID:        v.ID,
		ClientID:       v.ClientID,
		VerificationID: v.ID,
		ReferenceID:    v.ReferenceID,
		PANMasked:      v.PANMasked,
		Status:         string(v.Status),
		NameMatch:      v.NameMatch,
		Source:         v.Source,
		OccurredAt:     v.CreatedAt,
	})
}
