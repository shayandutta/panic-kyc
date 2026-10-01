package httpapi

import (
	"net/http"

	"kyc-platform/services/api-gateway/internal/auth"
	"kyc-platform/shared/contracts"
)

// Authenticate rejects requests without a valid API key and stores the
// client in the request context for later middleware and handlers.
func Authenticate(store *auth.KeyStore) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := auth.KeyFromHeader(r.Header.Get("Authorization"), r.Header.Get("X-API-Key"))
			client, err := store.Authenticate(key)
			if err != nil {
				writeError(w, http.StatusUnauthorized, contracts.ErrCodeUnauthorized, err.Error())
				return
			}
			next.ServeHTTP(w, r.WithContext(auth.WithClient(r.Context(), client)))
		})
	}
}
