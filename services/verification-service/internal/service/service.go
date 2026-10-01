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
)

// Lookup asks upstream sources about a PAN. The orchestrator implements it.
type Lookup interface {
	LookupPAN(ctx context.Context, pan string) (orchestrator.Result, error)
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
	piiSecret string
	now       func() time.Time
}

func New(repo domain.Repository, cache domain.Cache, lookup Lookup, piiSecret string) *Service {
	return &Service{repo: repo, cache: cache, lookup: lookup, piiSecret: piiSecret, now: time.Now}
}

// VerifyPAN runs one verification:
//  1. validate input
//  2. idempotency: same client + reference ID returns the stored result
//  3. cache: recent answer for this PAN skips the upstream call
//  4. upstream lookup with fallback
//  5. save the audit record
func (s *Service) VerifyPAN(ctx context.Context, req VerifyRequest) (*domain.Verification, error) {
	pan := pii.NormalizePAN(req.PAN)
	if !pii.IsValidPAN(pan) {
		return nil, ErrInvalidPAN
	}

	if req.ReferenceID != "" {
		existing, err := s.repo.GetByReference(ctx, req.ClientID, req.ReferenceID)
		if err == nil {
			return existing, nil
		}
		if !errors.Is(err, domain.ErrNotFound) {
			return nil, err
		}
	}

	panFingerprint := pii.Fingerprint(pan, s.piiSecret)

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
			return s.repo.GetByReference(ctx, req.ClientID, req.ReferenceID)
		}
		return nil, err
	}

	return v, nil
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
