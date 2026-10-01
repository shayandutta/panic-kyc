package httpapi

import (
	"net/http"

	"kyc-platform/services/api-gateway/internal/auth"
	"kyc-platform/services/api-gateway/internal/ratelimit"
	bulkpb "kyc-platform/shared/proto/bulk"
	pb "kyc-platform/shared/proto/verification"
)

type Deps struct {
	Keys          *auth.KeyStore
	Limiter       *ratelimit.Limiter
	Verifications pb.VerificationServiceClient
	Bulk          bulkpb.BulkServiceClient
}

// NewRouter builds the public API. /v1 routes require an API key and are rate limited.
func NewRouter(d Deps) http.Handler {
	v := &verificationHandlers{client: d.Verifications}
	b := &bulkHandlers{client: d.Bulk}

	api := http.NewServeMux()
	api.HandleFunc("POST /v1/verifications/pan", v.verifyPAN)
	api.HandleFunc("GET /v1/verifications/{id}", v.getVerification)
	api.HandleFunc("POST /v1/bulk-jobs", b.createJob)
	api.HandleFunc("GET /v1/bulk-jobs/{id}", b.getJob)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("/v1/", Chain(api, Authenticate(d.Keys), RateLimit(d.Limiter)))

	return Chain(mux, Recover, RequestID, Logging)
}
