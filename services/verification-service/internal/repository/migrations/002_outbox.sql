-- Transactional outbox: events waiting to be published to Kafka. A row is
-- written in the same transaction as the business record, so either both
-- exist or neither does. The relay publishes rows and stamps published_at.
CREATE TABLE IF NOT EXISTS outbox (
    id           BIGSERIAL   PRIMARY KEY,
    topic        TEXT        NOT NULL,
    key          TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

-- The relay only ever looks for unpublished rows, oldest first.
CREATE INDEX IF NOT EXISTS outbox_unpublished
    ON outbox (id)
    WHERE published_at IS NULL;
