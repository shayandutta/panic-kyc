package httpapi

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"

	"kyc-platform/shared/contracts"
)

type ctxKey int

const requestIDKey ctxKey = iota

const RequestIDHeader = "X-Request-ID"

// Middleware wraps a handler with extra behaviour, like Express middleware.
type Middleware func(http.Handler) http.Handler

// Chain applies middleware so the first one listed runs first.
func Chain(h http.Handler, mws ...Middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// RequestID reuses the caller's X-Request-ID or creates one, and echoes it back.
// The same ID is passed to every service so one request can be traced end to end.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if id == "" {
			id = uuid.NewString()
		}
		w.Header().Set(RequestIDHeader, id)
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// statusRecorder remembers the status code a handler wrote, for logging.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Logging writes one line per request. It never logs bodies, which may hold PII.
func Logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		log.Printf("request_id=%s method=%s path=%s status=%d duration=%s",
			RequestIDFrom(r.Context()), r.Method, r.URL.Path, rec.status, time.Since(start).Round(time.Millisecond))
	})
}

// Recover turns a panic in a handler into a 500 instead of crashing the gateway.
func Recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("request_id=%s panic: %v", RequestIDFrom(r.Context()), err)
				writeError(w, http.StatusInternalServerError, contracts.ErrCodeInternal, "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
