// Sandbox fakes the outside world for local development:
// upstream PAN data sources (stand-ins for providers like NSDL or DigiLocker)
// whose latency and failure rate can be changed at runtime.
package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"kyc-platform/shared/env"
)

type sourceConfig struct {
	mu       sync.RWMutex
	failRate float64       // 0.0 to 1.0, share of requests answered with 503
	latency  time.Duration // added delay per request
}

func (c *sourceConfig) get() (float64, time.Duration) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.failRate, c.latency
}

func (c *sourceConfig) set(failRate float64, latency time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failRate = failRate
	c.latency = latency
}

var sources = map[string]*sourceConfig{
	"source-a": {latency: 80 * time.Millisecond},
	"source-b": {latency: 150 * time.Millisecond},
}

// knownPANs gives a few PANs fixed names so name matching can be demoed.
var knownPANs = map[string]string{
	"ABCDE1234F": "Shayan Dutta",
	"PQRST6789K": "Asha Verma",
}

type lookupRequest struct {
	PAN string `json:"pan"`
}

type lookupResponse struct {
	Exists bool   `json:"exists"`
	Name   string `json:"name,omitempty"`
}

type configRequest struct {
	FailRate  float64 `json:"fail_rate"`
	LatencyMS int     `json:"latency_ms"`
}

func handleLookup(w http.ResponseWriter, r *http.Request) {
	cfg, ok := sources[r.PathValue("name")]
	if !ok {
		http.Error(w, "unknown source", http.StatusNotFound)
		return
	}

	failRate, latency := cfg.get()

	select {
	case <-time.After(latency):
	case <-r.Context().Done():
		return // caller gave up
	}

	if rand.Float64() < failRate {
		http.Error(w, "source temporarily unavailable", http.StatusServiceUnavailable)
		return
	}

	var req lookupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}

	resp := lookupResponse{}
	if name, ok := knownPANs[req.PAN]; ok {
		resp = lookupResponse{Exists: true, Name: name}
	} else if !strings.HasSuffix(req.PAN, "X") {
		// Any other PAN exists unless it ends in X, so "not found" is easy to demo.
		resp = lookupResponse{Exists: true, Name: "Test User"}
	}

	writeJSON(w, http.StatusOK, resp)
}

// handleConfigure lets you break a source on purpose, e.g. to watch fallback.
func handleConfigure(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cfg, ok := sources[name]
	if !ok {
		http.Error(w, "unknown source", http.StatusNotFound)
		return
	}

	var req configRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}

	cfg.set(req.FailRate, time.Duration(req.LatencyMS)*time.Millisecond)
	log.Printf("source %s configured: fail_rate=%.2f latency=%dms", name, req.FailRate, req.LatencyMS)
	writeJSON(w, http.StatusOK, req)
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func main() {
	addr := env.GetString("HTTP_ADDR", ":8090")

	mux := http.NewServeMux()
	mux.HandleFunc("POST /sources/{name}/pan", handleLookup)
	mux.HandleFunc("PUT /admin/sources/{name}", handleConfigure)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	server := &http.Server{Addr: addr, Handler: mux}

	go func() {
		log.Printf("sandbox listening on %s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("sandbox server failed: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)
}
