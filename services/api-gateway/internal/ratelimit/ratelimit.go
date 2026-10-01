// Package ratelimit implements a token bucket per client, stored in Redis
// so every gateway instance shares the same limits.
package ratelimit

import (
	"context"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// The whole check-and-update runs inside Redis as one script, so it is atomic:
// two gateway instances can't both read "1 token left" and both spend it.
// Time comes from Redis itself, so gateway clocks don't need to agree.
var tokenBucket = redis.NewScript(`
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local t     = redis.call('TIME')
local now   = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local state  = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])
if tokens == nil then
  tokens = burst
  ts = now
end

-- refill for the time that passed, never above the bucket size
tokens = math.min(burst, tokens + (math.max(0, now - ts) / 1000) * rate)

local allowed  = 0
local retry_ms = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry_ms = math.ceil((1 - tokens) / rate * 1000)
end

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) + 1000)
return {allowed, tostring(tokens), retry_ms}
`)

type Decision struct {
	Allowed    bool
	Remaining  int
	RetryAfter time.Duration
}

type Limiter struct {
	client *redis.Client
}

func New(client *redis.Client) *Limiter {
	return &Limiter{client: client}
}

// Allow spends one token from key's bucket. ratePerSecond is the refill
// speed; burst is the bucket size, i.e. how many requests can arrive at once.
func (l *Limiter) Allow(ctx context.Context, key string, ratePerSecond float64, burst int) (Decision, error) {
	res, err := tokenBucket.Run(ctx, l.client, []string{"ratelimit:" + key}, ratePerSecond, burst).Slice()
	if err != nil {
		return Decision{}, err
	}

	tokens, _ := strconv.ParseFloat(res[1].(string), 64)
	return Decision{
		Allowed:    res[0].(int64) == 1,
		Remaining:  int(math.Floor(tokens)),
		RetryAfter: time.Duration(res[2].(int64)) * time.Millisecond,
	}, nil
}
