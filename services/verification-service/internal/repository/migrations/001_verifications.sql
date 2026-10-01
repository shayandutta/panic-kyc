-- One row per finished check. Append-only audit record: no raw PAN, only a
-- masked copy and a keyed fingerprint.
CREATE TABLE IF NOT EXISTS verifications (
    id              UUID PRIMARY KEY,
    client_id       TEXT        NOT NULL,
    reference_id    TEXT,
    pan_fingerprint TEXT        NOT NULL,
    pan_masked      TEXT        NOT NULL,
    status          TEXT        NOT NULL,
    name_match      BOOLEAN     NOT NULL,
    source          TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL
);

-- Idempotency guarantee: one record per (client, reference ID). Partial, so
-- requests without a reference ID don't collide with each other.
CREATE UNIQUE INDEX IF NOT EXISTS verifications_client_reference
    ON verifications (client_id, reference_id)
    WHERE reference_id IS NOT NULL;

-- Audit queries: "all checks for this PAN, newest first".
CREATE INDEX IF NOT EXISTS verifications_fingerprint_created
    ON verifications (pan_fingerprint, created_at DESC);
