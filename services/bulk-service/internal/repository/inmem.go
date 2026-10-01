// Package repository stores bulk jobs.
package repository

import (
	"context"
	"sync"
	"time"

	"kyc-platform/services/bulk-service/internal/domain"
)

type InmemRepository struct {
	mu   sync.Mutex
	jobs map[string]*domain.Job
}

func NewInmemRepository() *InmemRepository {
	return &InmemRepository{jobs: map[string]*domain.Job{}}
}

func (r *InmemRepository) Create(ctx context.Context, j *domain.Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	copied := *j
	r.jobs[j.ID] = &copied
	return nil
}

func (r *InmemRepository) Get(ctx context.Context, clientID, id string) (*domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok || j.ClientID != clientID {
		return nil, domain.ErrNotFound
	}
	copied := *j
	return &copied, nil
}

func (r *InmemRepository) Record(ctx context.Context, jobID string, o domain.Outcome) (*domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[jobID]
	if !ok {
		return nil, domain.ErrNotFound
	}
	j.Processed++
	switch o {
	case domain.OutcomeValid:
		j.Valid++
	case domain.OutcomeInvalid:
		j.Invalid++
	default:
		j.Failed++
	}
	if j.Status == domain.StatusQueued {
		j.Status = domain.StatusRunning
	}
	copied := *j
	return &copied, nil
}

func (r *InmemRepository) MarkCompleted(ctx context.Context, jobID string, at time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if j, ok := r.jobs[jobID]; ok {
		j.Status = domain.StatusCompleted
		j.CompletedAt = &at
	}
	return nil
}
