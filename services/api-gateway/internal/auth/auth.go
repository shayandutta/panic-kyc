// Package auth authenticates clients by API key.
package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

var ErrUnauthorized = errors.New("missing or invalid API key")

// Client is an authenticated caller and its plan limits.
type Client struct {
	ID            string  `json:"client_id"`
	KeySHA256     string  `json:"key_sha256"`
	RatePerSecond float64 `json:"rate_per_second"`
	Burst         int     `json:"burst"`
}

// KeyStore finds clients by the hash of their API key.
// We never store API keys themselves: if this data leaks, the keys stay secret.
type KeyStore struct {
	byHash map[string]Client
}

func NewKeyStore(clients []Client) *KeyStore {
	byHash := make(map[string]Client, len(clients))
	for _, c := range clients {
		byHash[c.KeySHA256] = c
	}
	return &KeyStore{byHash: byHash}
}

// LoadKeyStore reads clients from a JSON file. In production this would be
// a database table managed by an admin API.
func LoadKeyStore(path string) (*KeyStore, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read clients file: %w", err)
	}
	var clients []Client
	if err := json.Unmarshal(raw, &clients); err != nil {
		return nil, fmt.Errorf("parse clients file: %w", err)
	}
	return NewKeyStore(clients), nil
}

// HashKey is SHA-256 of the key. A fast hash is fine here because API keys
// are long and random, unlike passwords.
func HashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func (s *KeyStore) Authenticate(key string) (Client, error) {
	if key == "" {
		return Client{}, ErrUnauthorized
	}
	c, ok := s.byHash[HashKey(key)]
	if !ok {
		return Client{}, ErrUnauthorized
	}
	return c, nil
}

// KeyFromHeader accepts "Authorization: Bearer <key>" or "X-API-Key: <key>".
func KeyFromHeader(authorization, xAPIKey string) string {
	if strings.HasPrefix(authorization, "Bearer ") {
		return strings.TrimPrefix(authorization, "Bearer ")
	}
	return xAPIKey
}

type ctxKey struct{}

func WithClient(ctx context.Context, c Client) context.Context {
	return context.WithValue(ctx, ctxKey{}, c)
}

// ClientFrom returns the authenticated client. Handlers must take the client ID
// from here, never from the request body, so clients can't act as each other.
func ClientFrom(ctx context.Context) (Client, bool) {
	c, ok := ctx.Value(ctxKey{}).(Client)
	return c, ok
}
