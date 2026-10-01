// Package outbox publishes events saved in the outbox table to Kafka.
package outbox

import (
	"context"
	"log"
	"time"

	"kyc-platform/shared/events"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Publisher sends events to Kafka. events.Producer implements it.
type Publisher interface {
	PublishRaw(ctx context.Context, msgs ...events.RawMessage) error
}

type Relay struct {
	pool      *pgxpool.Pool
	publisher Publisher
	batchSize int
	interval  time.Duration
	retention time.Duration
}

func NewRelay(pool *pgxpool.Pool, publisher Publisher, batchSize int, interval time.Duration) *Relay {
	return &Relay{pool: pool, publisher: publisher, batchSize: batchSize, interval: interval, retention: 7 * 24 * time.Hour}
}

// Run publishes batches until ctx is cancelled. When a batch is full it
// goes again immediately, so a backlog drains quickly; otherwise it waits.
func (r *Relay) Run(ctx context.Context) {
	cleanup := time.NewTicker(time.Hour)
	defer cleanup.Stop()

	for {
		n, err := r.PublishBatch(ctx)
		if err != nil && ctx.Err() == nil {
			log.Printf("outbox relay: %v (will retry)", err)
		}
		if n == r.batchSize {
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-cleanup.C:
			r.deletePublished(ctx)
		case <-time.After(r.interval):
		}
	}
}

// PublishBatch publishes up to batchSize unpublished events and returns how many.
//
// FOR UPDATE SKIP LOCKED lets several service replicas run relays at once:
// each locks a different set of rows and skips rows another relay holds.
//
// If Kafka fails, the transaction rolls back and the rows are retried next
// time. If we crash after Kafka accepted the batch but before COMMIT, the
// rows are published again. That's at-least-once delivery, which is why
// every event has an ID that consumers deduplicate on.
func (r *Relay) PublishBatch(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, topic, key, payload
		FROM outbox
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, r.batchSize)
	if err != nil {
		return 0, err
	}

	var ids []int64
	var msgs []events.RawMessage
	for rows.Next() {
		var id int64
		var m events.RawMessage
		if err := rows.Scan(&id, &m.Topic, &m.Key, &m.Value); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
		msgs = append(msgs, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(msgs) == 0 {
		return 0, nil
	}

	if err := r.publisher.PublishRaw(ctx, msgs...); err != nil {
		return 0, err
	}

	if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return 0, err
	}
	return len(msgs), tx.Commit(ctx)
}

// deletePublished keeps the table small. Published rows are only kept for a
// while in case someone needs to investigate.
func (r *Relay) deletePublished(ctx context.Context) {
	tag, err := r.pool.Exec(ctx, `DELETE FROM outbox WHERE published_at < now() - $1::interval`, r.retention.String())
	if err != nil {
		log.Printf("outbox cleanup: %v", err)
		return
	}
	if n := tag.RowsAffected(); n > 0 {
		log.Printf("outbox cleanup: deleted %d published events", n)
	}
}
