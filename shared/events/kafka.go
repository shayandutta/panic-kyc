// Package events publishes and consumes JSON events on Kafka.
package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer publishes events. Messages with the same key go to the same
// partition, so we key by client ID to keep each client's events in order.
type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string) *Producer {
	return &Producer{writer: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.Hash{},    // same key, same partition
		RequiredAcks: kafka.RequireAll, // wait until all replicas have it
		BatchTimeout: 10 * time.Millisecond,
	}}
}

func (p *Producer) Publish(ctx context.Context, topic, key string, event any) error {
	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	return p.writer.WriteMessages(ctx, kafka.Message{Topic: topic, Key: []byte(key), Value: value})
}

func (p *Producer) Close() error { return p.writer.Close() }

// Handler processes one message. Return an error only for failures worth
// retrying in place; business failures should be routed elsewhere (e.g. a retry topic).
type Handler func(ctx context.Context, msg kafka.Message) error

// Consume reads topic as part of consumer group groupID until ctx is cancelled.
// The offset is committed only after the handler succeeds, which gives
// at-least-once delivery: after a crash, a message may be processed again,
// so handlers must be idempotent.
func Consume(ctx context.Context, brokers []string, topic, groupID string, handle Handler) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	})
	defer reader.Close()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("fetch from %s: %w", topic, err)
		}

		// Retry in place with backoff. This blocks only this partition, which
		// keeps ordering and never skips a message.
		for attempt := 1; ; attempt++ {
			if err := handle(ctx, msg); err == nil {
				break
			} else {
				log.Printf("topic=%s offset=%d attempt=%d handler failed: %v", topic, msg.Offset, attempt, err)
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff(attempt)):
			}
		}

		if err := reader.CommitMessages(ctx, msg); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("topic=%s offset=%d commit failed: %v", topic, msg.Offset, err)
		}
	}
}

func backoff(attempt int) time.Duration {
	d := time.Duration(1<<min(attempt, 6)) * 100 * time.Millisecond
	return min(d, 5*time.Second)
}

// EnsureTopics creates topics if they don't exist yet.
func EnsureTopics(brokers []string, partitions int, topics ...string) error {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("find controller: %w", err)
	}
	cconn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer cconn.Close()

	configs := make([]kafka.TopicConfig, len(topics))
	for i, t := range topics {
		configs[i] = kafka.TopicConfig{Topic: t, NumPartitions: partitions, ReplicationFactor: 1}
	}
	return cconn.CreateTopics(configs...)
}
