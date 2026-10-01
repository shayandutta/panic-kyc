package contracts

// APIResponse is the envelope for every public API response.
type APIResponse struct {
	Data  any       `json:"data,omitempty"`
	Error *APIError `json:"error,omitempty"`
}

// APIError carries a stable, documented error code that clients can branch on.
type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Error codes are part of the public API. Never rename one; only add new ones.
const (
	ErrCodeBadRequest        = "bad_request"
	ErrCodeInvalidPAN        = "invalid_pan"
	ErrCodeUnauthorized      = "unauthorized"
	ErrCodeRateLimited       = "rate_limited"
	ErrCodeNotFound          = "not_found"
	ErrCodeSourceUnavailable = "source_unavailable"
	ErrCodeInternal          = "internal_error"
)
