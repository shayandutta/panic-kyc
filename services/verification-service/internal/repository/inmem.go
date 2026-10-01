package repository

import (
	"context"
	"sync"

	"kyc-platform/services/verification-service/internal/domain"
)

// InmemRepository is a Repository for tests and local experiments.
type InmemRepository struct {
	mu      sync.RWMutex
	records map[string]*domain.Verification
	outbox  []domain.OutboxMessage
}

func NewInmemRepository() *InmemRepository {
	return &InmemRepository{records: make(map[string]*domain.Verification)}
}

func (r *InmemRepository) Save(ctx context.Context, v *domain.Verification, event domain.OutboxMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if v.ReferenceID != "" {
		for _, existing := range r.records {
			if existing.ClientID == v.ClientID && existing.ReferenceID == v.ReferenceID {
				return domain.ErrDuplicateReference
			}
		}
	}
	copied := *v
	r.records[v.ID] = &copied
	r.outbox = append(r.outbox, event)
	return nil
}

// Outbox returns the events saved so far, for tests.
func (r *InmemRepository) Outbox() []domain.OutboxMessage {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]domain.OutboxMessage(nil), r.outbox...)
}

func (r *InmemRepository) GetByID(ctx context.Context, clientID, id string) (*domain.Verification, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	v, ok := r.records[id]
	if !ok || v.ClientID != clientID {
		return nil, domain.ErrNotFound
	}
	return v, nil
}

func (r *InmemRepository) GetByReference(ctx context.Context, clientID, referenceID string) (*domain.Verification, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, v := range r.records {
		if v.ClientID == clientID && v.ReferenceID == referenceID {
			return v, nil
		}
	}
	return nil, domain.ErrNotFound
}
