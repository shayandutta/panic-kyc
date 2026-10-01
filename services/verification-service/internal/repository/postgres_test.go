package repository

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"kyc-platform/services/verification-service/internal/domain"
	"kyc-platform/shared/db"

	"github.com/google/uuid"
)

// Integration test: runs only when TEST_POSTGRES_URL points at a database,
// e.g. the one from docker compose.
func newTestPostgres(t *testing.T) *PostgresRepository {
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	pool, err := db.ConnectPostgres(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	r := NewPostgresRepository(pool)
	if err := r.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return r
}

var testEvent = domain.OutboxMessage{Topic: "verification.completed", Key: "c", Payload: []byte(`{"test":true}`)}

func record(clientID, ref string) *domain.Verification {
	return &domain.Verification{
		ID: uuid.NewString(), ClientID: clientID, ReferenceID: ref,
		PANFingerprint: "fp", PANMasked: "AB******4F", Status: domain.StatusValid,
		Source: "source-a", CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
	}
}

func TestPostgresSaveAndRead(t *testing.T) {
	r := newTestPostgres(t)
	ctx := context.Background()
	v := record("c-"+uuid.NewString(), "ref-1")

	if err := r.Save(ctx, v, testEvent); err != nil {
		t.Fatal(err)
	}
	got, err := r.GetByReference(ctx, v.ClientID, "ref-1")
	if err != nil || got.ID != v.ID || !got.CreatedAt.Equal(v.CreatedAt) {
		t.Fatalf("got %+v err %v", got, err)
	}
	if _, err := r.GetByID(ctx, "someone-else", v.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Error("other clients must not read the record")
	}
}

func TestPostgresDuplicateReference(t *testing.T) {
	r := newTestPostgres(t)
	ctx := context.Background()
	client := "c-" + uuid.NewString()

	r.Save(ctx, record(client, "same"), testEvent)
	if err := r.Save(ctx, record(client, "same"), testEvent); !errors.Is(err, domain.ErrDuplicateReference) {
		t.Errorf("err = %v, want ErrDuplicateReference", err)
	}
	// No reference ID: many records allowed.
	if err := r.Save(ctx, record(client, ""), testEvent); err != nil {
		t.Fatal(err)
	}
	if err := r.Save(ctx, record(client, ""), testEvent); err != nil {
		t.Errorf("records without a reference must not collide: %v", err)
	}
}

func TestPostgresDuplicateWritesNoOutboxRow(t *testing.T) {
	r := newTestPostgres(t)
	ctx := context.Background()
	client := "c-" + uuid.NewString()

	count := func() int {
		var n int
		r.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE key = $1`, client).Scan(&n)
		return n
	}
	ev := domain.OutboxMessage{Topic: "verification.completed", Key: client, Payload: []byte(`{}`)}

	r.Save(ctx, record(client, "dup"), ev)
	r.Save(ctx, record(client, "dup"), ev) // rejected: duplicate reference
	if n := count(); n != 1 {
		t.Errorf("outbox rows = %d, want 1: the rolled-back save must not leave an event", n)
	}
}
