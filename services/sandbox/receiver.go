package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"kyc-platform/shared/webhooksig"
)

// This file plays the part of a client's server receiving our webhooks.
// It does what we tell real clients to do: verify the signature, reply fast,
// and ignore duplicates by event ID.

var webhookSecrets = map[string]string{
	"bank-1":   "whsec_bank1_demo",
	"lender-2": "whsec_lender2_demo",
}

type receiver struct {
	mu         sync.Mutex
	seen       map[string]bool     // event IDs already processed
	received   map[string][]string // client -> event IDs, for the demo
	failStatus map[string]int      // client -> forced status code, to demo retries
}

var rcv = &receiver{
	seen:       map[string]bool{},
	received:   map[string][]string{},
	failStatus: map[string]int{},
}

func handleClientWebhook(w http.ResponseWriter, r *http.Request) {
	client := r.PathValue("client")
	secret, ok := webhookSecrets[client]
	if !ok {
		http.Error(w, "unknown client", http.StatusNotFound)
		return
	}

	rcv.mu.Lock()
	forced := rcv.failStatus[client]
	rcv.mu.Unlock()
	if forced != 0 {
		http.Error(w, "simulated outage", forced)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	if err := webhooksig.Verify(secret, r.Header.Get(webhooksig.Header), body, time.Now(), 5*time.Minute); err != nil {
		log.Printf("client %s rejected webhook: %v", client, err)
		http.Error(w, "bad signature", http.StatusUnauthorized)
		return
	}

	eventID := r.Header.Get("X-KYC-Event-ID")
	rcv.mu.Lock()
	duplicate := rcv.seen[eventID]
	if !duplicate {
		rcv.seen[eventID] = true
		rcv.received[client] = append(rcv.received[client], eventID)
	}
	rcv.mu.Unlock()

	if duplicate {
		log.Printf("client %s ignored duplicate event %s", client, eventID)
	} else {
		log.Printf("client %s received event %s: %s", client, eventID, body)
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleClientWebhookConfig makes a client's endpoint fail on purpose,
// e.g. {"fail_status": 503}, or recover with {"fail_status": 0}.
func handleClientWebhookConfig(w http.ResponseWriter, r *http.Request) {
	client := r.PathValue("client")
	var req struct {
		FailStatus int `json:"fail_status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	rcv.mu.Lock()
	rcv.failStatus[client] = req.FailStatus
	rcv.mu.Unlock()
	log.Printf("client %s webhook endpoint forced status: %d", client, req.FailStatus)
	writeJSON(w, http.StatusOK, req)
}

func handleListReceived(w http.ResponseWriter, r *http.Request) {
	rcv.mu.Lock()
	ids := append([]string(nil), rcv.received[r.PathValue("client")]...)
	rcv.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"received": ids, "count": len(ids)})
}
