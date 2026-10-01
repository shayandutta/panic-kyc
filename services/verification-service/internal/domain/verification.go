// Package domain holds the verification service's core types and the
// interfaces it needs from the outside world (storage, cache).
package domain

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"
)

type Status string

const (
	StatusValid   Status = "VALID"   // PAN exists
	StatusInvalid Status = "INVALID" // PAN does not exist
)

var (
	ErrNotFound           = errors.New("verification not found")
	ErrDuplicateReference = errors.New("reference id already used by this client")
)

// Verification is one finished check, stored as an audit record.
// It never holds the raw PAN: only a masked copy and a keyed fingerprint.
type Verification struct {
	ID             string    `bson:"_id"`
	ClientID       string    `bson:"client_id"`
	ReferenceID    string    `bson:"reference_id,omitempty"`
	PANFingerprint string    `bson:"pan_fingerprint"`
	PANMasked      string    `bson:"pan_masked"`
	Status         Status    `bson:"status"`
	NameMatch      bool      `bson:"name_match"`
	Source         string    `bson:"source"`
	CreatedAt      time.Time `bson:"created_at"`
}

// Repository stores verification records. Every read is scoped to a client
// so one client can never see another client's data.
type Repository interface {
	Save(ctx context.Context, v *Verification) error
	GetByID(ctx context.Context, clientID, id string) (*Verification, error)
	GetByReference(ctx context.Context, clientID, referenceID string) (*Verification, error)
}

// CachedLookup is what we remember about a PAN between requests.
// The name on record is stored as a fingerprint, not as text.
type CachedLookup struct {
	Exists          bool   `json:"exists"`
	NameFingerprint string `json:"name_fingerprint"`
	Source          string `json:"source"`
}

// Cache keeps recent upstream answers, keyed by PAN fingerprint.
type Cache interface {
	Get(ctx context.Context, panFingerprint string) (*CachedLookup, bool, error)
	Set(ctx context.Context, panFingerprint string, lookup CachedLookup) error
}

// NormalizeName lowercases, trims and sorts name parts, so
// "DUTTA  Shayan" and "shayan dutta" compare as equal.
func NormalizeName(name string) string {
	parts := strings.Fields(strings.ToLower(name))
	sort.Strings(parts)
	return strings.Join(parts, " ")
}
