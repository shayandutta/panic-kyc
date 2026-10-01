// Package service holds the verification business logic.
package service

import (
	"context"
	"errors"
	"log"
	"time"

	"kyc-platform/services/verification-service/internal/domain"
	"kyc-platform/services/verification-service/internal/orchestrator"
	"kyc-platform/shared/pii"

	"github.com/google/uuid"
)

var (
	ErrInvalidPAN        = errors.New("invalid PAN format")
	ErrSourceUnavailable = errors.New("no upstream source could answer")
	// ErrReferenceConflict means the client reused a reference ID for a
	// different PAN. Returning the old result would silently answer the
	// wrong question, so we refuse instead.
	ErrReferenceConflict = errors.New("reference_id was already used for a different PAN")
)

// Lookup asks upstream sources about a PAN. The orchestrator implements it.
type Lookup interface {
	LookupPAN(ctx context.Context, pan string) (orchestrator.Result, error)
}

// EventPublisher announces finished verifications to other services.
type EventPublisher interface {
	PublishVerificationCompleted(ctx context.Context, v *domain.Verification) error
}

type VerifyRequest struct {
	ClientID    string
	PAN         string
	Name        string
	ReferenceID string
}

type Service struct {
	repo      domain.Repository
	cache     domain.Cache
	lookup    Lookup
	events    EventPublisher
	piiSecret string
	now       func() time.Time
}

func New(repo domain.Repository, cache domain.Cache, lookup Lookup, events EventPublisher, piiSecret string) *Service {
	return &Service{repo: repo, cache: cache, lookup: lookup, events: events, piiSecret: piiSecret, now: time.Now}
}

// VerifyPAN runs one verification:
//  1. validate input
//  2. idempotency: same client + reference ID returns the stored result
//  3. cache: recent answer for this PAN skips the upstream call
//  4. upstream lookup with fallback
//  5. save the audit record
//  6. publish a verification.completed event
func (s *Service) VerifyPAN(ctx context.Context, req VerifyRequest) (*domain.Verification, error) {
	pan := pii.NormalizePAN(req.PAN)
	if !pii.IsValidPAN(pan) {
		return nil, ErrInvalidPAN
	}

	panFingerprint := pii.Fingerprint(pan, s.piiSecret)

	if req.ReferenceID != "" {
		existing, err := s.repo.GetByReference(ctx, req.ClientID, req.ReferenceID)
		if err == nil {
			return sameRequestOrConflict(existing, panFingerprint)
		}
		if !errors.Is(err, domain.ErrNotFound) {
			return nil, err
		}
	}

	lookup, err := s.lookupPAN(ctx, pan, panFingerprint)
	if err != nil {
		return nil, err
	}

	v := &domain.Verification{
		ID:             uuid.NewString(),
		ClientID:       req.ClientID,
		ReferenceID:    req.ReferenceID,
		PANFingerprint: panFingerprint,
		PANMasked:      pii.MaskPAN(pan),
		Status:         domain.StatusInvalid,
		Source:         lookup.Source,
		CreatedAt:      s.now().UTC(),
	}
	if lookup.Exists {
		v.Status = domain.StatusValid
		v.NameMatch = req.Name != "" &&
			pii.Fingerprint(domain.NormalizeName(req.Name), s.piiSecret) == lookup.NameFingerprint
	}

	if err := s.repo.Save(ctx, v); err != nil {
		// Two requests with the same reference ID raced; the other one won.
		// Return its result so both callers see the same answer.
		if errors.Is(err, domain.ErrDuplicateReference) {
			existing, err := s.repo.GetByReference(ctx, req.ClientID, req.ReferenceID)
			if err != nil {
				return nil, err
			}
			return sameRequestOrConflict(existing, panFingerprint)
		}
		return nil, err
	}

	// Known gap: saving and publishing are two separate writes. If the
	// publish fails, the record exists without an event. The fix is the
	// transactional outbox pattern; for now we log loudly so it can be replayed.
	if err := s.events.PublishVerificationCompleted(ctx, v); err != nil {
		log.Printf("verification %s saved but event publish failed: %v", v.ID, err)
	}

	return v, nil
}

// sameRequestOrConflict returns the stored result for a retried request, or
// ErrReferenceConflict if the reference ID was used for another PAN.
func sameRequestOrConflict(existing *domain.Verification, panFingerprint string) (*domain.Verification, error) {
	if existing.PANFingerprint != panFingerprint {
		return nil, ErrReferenceConflict
	}
	return existing, nil
}

func (s *Service) GetVerification(ctx context.Context, clientID, id string) (*domain.Verification, error) {
	return s.repo.GetByID(ctx, clientID, id)
}

// lookupPAN checks the cache first, then the upstream sources.
// The cache is an optimization: if it fails, we log and carry on.
func (s *Service) lookupPAN(ctx context.Context, pan, panFingerprint string) (*domain.CachedLookup, error) {
	cached, found, err := s.cache.Get(ctx, panFingerprint)
	if err != nil {
		log.Printf("cache get failed, falling back to upstream: %v", err)
	}
	if found {
		return cached, nil
	}

	res, err := s.lookup.LookupPAN(ctx, pan)
	if err != nil {
		if errors.Is(err, orchestrator.ErrAllSourcesFailed) {
			return nil, ErrSourceUnavailable
		}
		return nil, err
	}

	lookup := &domain.CachedLookup{
		Exists: res.Exists,
		Source: res.Source,
	}
	if res.NameOnRecord != "" {
		lookup.NameFingerprint = pii.Fingerprint(domain.NormalizeName(res.NameOnRecord), s.piiSecret)
	}

	if err := s.cache.Set(ctx, panFingerprint, *lookup); err != nil {
		log.Printf("cache set failed: %v", err)
	}
	return lookup, nil
}
