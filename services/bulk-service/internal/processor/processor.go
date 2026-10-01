// Package processor runs bulk jobs through a worker pool.
package processor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"kyc-platform/services/bulk-service/internal/domain"
	"kyc-platform/services/bulk-service/internal/workerpool"
	pb "kyc-platform/shared/proto/verification"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const MaxItems = 10_000

var ErrBadJob = fmt.Errorf("a job needs between 1 and %d items", MaxItems)

type Config struct {
	MinWorkers  int           // workers kept when idle
	MaxWorkers  int           // upper bound: the upstream providers' concurrency limit for bulk traffic
	QueueSize   int           // bounded queue: full queue = backpressure on job feeding
	ItemTimeout time.Duration // deadline for one verification
}

type Processor struct {
	repo     domain.Repository
	verifier pb.VerificationServiceClient
	pool     *workerpool.Pool[domain.Item]
	cfg      Config
	stop     chan struct{}
	feeders  sync.WaitGroup // goroutines pushing job items into the pool
	now      func() time.Time
}

func New(repo domain.Repository, verifier pb.VerificationServiceClient, cfg Config) *Processor {
	p := &Processor{repo: repo, verifier: verifier, cfg: cfg, stop: make(chan struct{}), now: time.Now}
	p.pool = workerpool.New(cfg.MinWorkers, cfg.QueueSize, p.process)
	go p.pool.Autoscale(cfg.MinWorkers, cfg.MaxWorkers, 200*time.Millisecond, p.stop)
	return p
}

// CreateJob saves the job and returns at once (the API answers 202 Accepted).
// Items are fed to the pool by a background goroutine; when the queue is
// full that goroutine waits, so a huge job can't flood memory.
func (p *Processor) CreateJob(ctx context.Context, clientID string, items []domain.Item) (*domain.Job, error) {
	if len(items) == 0 || len(items) > MaxItems {
		return nil, ErrBadJob
	}

	job := &domain.Job{
		ID:        uuid.NewString(),
		ClientID:  clientID,
		Status:    domain.StatusQueued,
		Total:     len(items),
		CreatedAt: p.now().UTC(),
	}
	if err := p.repo.Create(ctx, job); err != nil {
		return nil, err
	}

	p.feeders.Add(1)
	go func() {
		defer p.feeders.Done()
		for i, it := range items {
			it.JobID, it.ClientID, it.Row = job.ID, clientID, i
			if !p.pool.SubmitOrStop(it, p.stop) {
				// Shutting down. Known limitation: unfed items are dropped and the
				// job stays RUNNING. Persisting items (encrypted) would allow resuming.
				log.Printf("bulk job %s: shutdown after feeding %d of %d items", job.ID, i, len(items))
				return
			}
		}
	}()

	log.Printf("bulk job %s created for client %s with %d items", job.ID, clientID, job.Total)
	return job, nil
}

func (p *Processor) GetJob(ctx context.Context, clientID, id string) (*domain.Job, error) {
	return p.repo.Get(ctx, clientID, id)
}

func (p *Processor) process(it domain.Item) {
	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.ItemTimeout)
	defer cancel()

	outcome := p.verify(ctx, it)

	job, err := p.repo.Record(context.Background(), it.JobID, outcome)
	if err != nil {
		log.Printf("bulk job %s row %d: record outcome: %v", it.JobID, it.Row, err)
		return
	}
	if job.Processed == job.Total {
		if err := p.repo.MarkCompleted(context.Background(), job.ID, p.now().UTC()); err != nil {
			log.Printf("bulk job %s: mark completed: %v", job.ID, err)
		}
		log.Printf("bulk job %s completed: valid=%d invalid=%d failed=%d", job.ID, job.Valid, job.Invalid, job.Failed)
	}
}

func (p *Processor) verify(ctx context.Context, it domain.Item) domain.Outcome {
	// A deterministic reference per row makes every item idempotent: if the
	// job is ever re-run, finished rows return their stored result instead
	// of calling the provider (and billing) again.
	ref := it.ReferenceID
	if ref == "" {
		ref = fmt.Sprintf("bulk:%s:%d", it.JobID, it.Row)
	}

	v, err := p.verifier.VerifyPAN(ctx, &pb.VerifyPANRequest{
		ClientId:    it.ClientID,
		Pan:         it.PAN,
		Name:        it.Name,
		ReferenceId: ref,
	})
	switch {
	case err == nil && v.GetStatus() == pb.VerificationStatus_VERIFICATION_STATUS_VALID:
		return domain.OutcomeValid
	case err == nil:
		return domain.OutcomeInvalid
	case status.Code(err) == codes.InvalidArgument:
		return domain.OutcomeInvalid // malformed PAN
	case errors.Is(err, context.DeadlineExceeded), status.Code(err) == codes.Unavailable:
		return domain.OutcomeFailed
	default:
		log.Printf("bulk job %s row %d: %v", it.JobID, it.Row, err)
		return domain.OutcomeFailed
	}
}

func (p *Processor) Workers() int { return p.pool.Size() }

// Close stops autoscaling and feeding, then waits for queued items to finish.
// Order matters: feeders must stop before the pool's queue is closed.
func (p *Processor) Close() {
	close(p.stop)
	p.feeders.Wait()
	p.pool.Close()
}
