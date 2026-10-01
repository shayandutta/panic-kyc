// Package httpapi is the public REST API: routing, middleware and handlers.
package httpapi

import (
	"encoding/json"
	"net/http"

	"kyc-platform/shared/contracts"
)

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(contracts.APIResponse{Data: data})
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(contracts.APIResponse{
		Error: &contracts.APIError{Code: code, Message: message},
	})
}
