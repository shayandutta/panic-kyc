package httpapi

import (
	"log"
	"math"
	"net/http"
	"strconv"

	"kyc-platform/services/api-gateway/internal/auth"
	"kyc-platform/services/api-gateway/internal/ratelimit"
	"kyc-platform/shared/contracts"
)

// RateLimit applies each client's token bucket. It must run after Authenticate.
// If Redis is down we fail open (allow the request): a broken limiter should
// not take the whole API down. That trade-off is deliberate.
func RateLimit(limiter *ratelimit.Limiter) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			client, ok := auth.ClientFrom(r.Context())
			if !ok {
				next.ServeHTTP(w, r)
				return
			}

			d, err := limiter.Allow(r.Context(), client.ID, client.RatePerSecond, client.Burst)
			if err != nil {
				log.Printf("request_id=%s rate limiter unavailable, allowing: %v", RequestIDFrom(r.Context()), err)
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(client.Burst))
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(d.Remaining))

			if !d.Allowed {
				seconds := int(math.Ceil(d.RetryAfter.Seconds()))
				w.Header().Set("Retry-After", strconv.Itoa(seconds))
				writeError(w, http.StatusTooManyRequests, contracts.ErrCodeRateLimited, "rate limit exceeded, retry later")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
