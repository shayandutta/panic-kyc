package outbox

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"kyc-platform/services/verification-service/internal/domain"
	"kyc-platform/services/verification-service/internal/repository"
	"kyc-platform/shared/db"
	"kyc-platform/shared/events"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type fakePublisher struct {
	mu   sync.Mutex
	err  error
	sent []events.RawMessage
}

func (f *fakePublisher) PublishRaw(ctx context.Context, msgs ...events.RawMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, msgs...)
	return nil
}

func (f *fakePublisher) sentFor(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, m := range f.sent {
		if m.Key == key {
			n++
		}
	}
	return n
}

// Integration test against the docker-compose postgres; skipped otherwise.
func setup(t *testing.T) (*pgxpool.Pool, *repository.PostgresRepository, string) {
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	pool, err := db.ConnectPostgres(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	repo := repository.NewPostgresRepository(pool)
	if err := repo.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}

	client := "relay-test-" + uuid.NewString()
	for i := 0; i < 3; i++ {
		v := &domain.Verification{ID: uuid.NewString(), ClientID: client, PANFingerprint: "fp", PANMasked: "AB******4F",
			Status: domain.StatusValid, Source: "a", CreatedAt: time.Now()}
		ev := domain.OutboxMessage{Topic: "verification.completed", Key: client, Payload: []byte(`{}`)}
		if err := repo.Save(context.Background(), v, ev); err != nil {
			t.Fatal(err)
		}
	}
	return pool, repo, client
}

func unpublished(t *testing.T, pool *pgxpool.Pool, key string) int {
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE key = $1 AND published_at IS NULL`, key).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func drain(relay *Relay) {
	for {
		n, err := relay.PublishBatch(context.Background())
		if err != nil || n == 0 {
			return
		}
	}
}

func TestRelayPublishesAndMarksRows(t *testing.T) {
	pool, _, client := setup(t)
	pub := &fakePublisher{}
	drain(NewRelay(pool, pub, 100, time.Second))

	if got := pub.sentFor(client); got != 3 {
		t.Errorf("published %d events, want 3", got)
	}
	if n := unpublished(t, pool, client); n != 0 {
		t.Errorf("%d rows still unpublished", n)
	}
}

func TestRelayKeepsRowsWhenKafkaFails(t *testing.T) {
	pool, _, client := setup(t)
	failing := &fakePublisher{err: errors.New("kafka unreachable")}

	if _, err := NewRelay(pool, failing, 100, time.Second).PublishBatch(context.Background()); err == nil {
		t.Fatal("expected an error")
	}
	if n := unpublished(t, pool, client); n != 3 {
		t.Errorf("unpublished = %d, want 3: nothing may be lost when Kafka is down", n)
	}

	// Kafka is back: the same rows go out.
	ok := &fakePublisher{}
	drain(NewRelay(pool, ok, 100, time.Second))
	if ok.sentFor(client) != 3 || unpublished(t, pool, client) != 0 {
		t.Error("rows were not published after recovery")
	}
}

func TestTwoRelaysNeverPublishTheSameRow(t *testing.T) {
	pool, _, client := setup(t)
	pub := &fakePublisher{}

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			drain(NewRelay(pool, pub, 1, time.Second))
		}()
	}
	wg.Wait()

	if got := pub.sentFor(client); got != 3 {
		t.Errorf("published %d, want exactly 3: SKIP LOCKED must prevent double publishing", got)
	}
}
