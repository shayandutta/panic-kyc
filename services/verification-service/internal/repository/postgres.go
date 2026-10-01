package repository

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"sort"

	"kyc-platform/services/verification-service/internal/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrations embed.FS

// uniqueViolation is PostgreSQL's error code for a broken unique constraint.
const uniqueViolation = "23505"

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

// Migrate applies the SQL files in order. They use IF NOT EXISTS, so running
// them on every start is safe. A larger project would use a migration tool
// that records which versions have run.
func (r *PostgresRepository) Migrate(ctx context.Context) error {
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	for _, e := range entries {
		sql, err := migrations.ReadFile("migrations/" + e.Name())
		if err != nil {
			return err
		}
		if _, err := r.pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migration %s: %w", e.Name(), err)
		}
	}
	return nil
}

// Save writes the record and its event in one transaction. This is the
// heart of the outbox pattern: we never have a record without its event,
// or an event for a record that was rolled back.
func (r *PostgresRepository) Save(ctx context.Context, v *domain.Verification, event domain.OutboxMessage) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) // no-op after a successful Commit

	if _, err := tx.Exec(ctx, insertVerification, verificationArgs(v)...); err != nil {
		return translate(err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO outbox (topic, key, payload) VALUES ($1, $2, $3)`,
		event.Topic, event.Key, event.Payload,
	); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *PostgresRepository) GetByID(ctx context.Context, clientID, id string) (*domain.Verification, error) {
	// Filtering by client_id too means a client can't read another client's
	// record even if it guesses the ID.
	return r.queryOne(ctx, selectVerification+` WHERE id = $1 AND client_id = $2`, id, clientID)
}

func (r *PostgresRepository) GetByReference(ctx context.Context, clientID, referenceID string) (*domain.Verification, error) {
	return r.queryOne(ctx, selectVerification+` WHERE client_id = $1 AND reference_id = $2`, clientID, referenceID)
}

const insertVerification = `
INSERT INTO verifications
    (id, client_id, reference_id, pan_fingerprint, pan_masked, status, name_match, source, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

const selectVerification = `
SELECT id, client_id, COALESCE(reference_id, ''), pan_fingerprint, pan_masked,
       status, name_match, source, created_at
FROM verifications`

func verificationArgs(v *domain.Verification) []any {
	var ref *string // empty reference is stored as NULL, so the partial unique index ignores it
	if v.ReferenceID != "" {
		ref = &v.ReferenceID
	}
	return []any{v.ID, v.ClientID, ref, v.PANFingerprint, v.PANMasked, string(v.Status), v.NameMatch, v.Source, v.CreatedAt}
}

func (r *PostgresRepository) queryOne(ctx context.Context, sql string, args ...any) (*domain.Verification, error) {
	var v domain.Verification
	var status string
	err := r.pool.QueryRow(ctx, sql, args...).Scan(
		&v.ID, &v.ClientID, &v.ReferenceID, &v.PANFingerprint, &v.PANMasked,
		&status, &v.NameMatch, &v.Source, &v.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	v.Status = domain.Status(status)
	return &v, nil
}

// translate maps PostgreSQL errors to domain errors, so the service layer
// never depends on database-specific types.
func translate(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		return domain.ErrDuplicateReference
	}
	return err
}
