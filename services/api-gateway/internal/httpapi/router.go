package httpapi

import "net/http"

// NewRouter builds the public API.
func NewRouter() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	return Chain(mux, Recover, RequestID, Logging)
}
