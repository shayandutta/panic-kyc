// Package cache stores recent upstream answers in Redis.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"kyc-platform/services/verification-service/internal/domain"

	"github.com/redis/go-redis/v9"
)

const keyPrefix = "pan-lookup:"

type RedisCache struct {
	client *redis.Client
	ttl    time.Duration
}

// NewRedisCache keeps entries for ttl. Keys are PAN fingerprints, never PANs.
func NewRedisCache(client *redis.Client, ttl time.Duration) *RedisCache {
	return &RedisCache{client: client, ttl: ttl}
}

func (c *RedisCache) Get(ctx context.Context, panFingerprint string) (*domain.CachedLookup, bool, error) {
	raw, err := c.client.Get(ctx, keyPrefix+panFingerprint).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil // cache miss is not an error
	}
	if err != nil {
		return nil, false, err
	}

	var lookup domain.CachedLookup
	if err := json.Unmarshal(raw, &lookup); err != nil {
		return nil, false, err
	}
	return &lookup, true, nil
}

func (c *RedisCache) Set(ctx context.Context, panFingerprint string, lookup domain.CachedLookup) error {
	raw, err := json.Marshal(lookup)
	if err != nil {
		return err
	}
	// The TTL makes Redis delete the entry by itself, so answers can't go stale forever.
	return c.client.Set(ctx, keyPrefix+panFingerprint, raw, c.ttl).Err()
}
