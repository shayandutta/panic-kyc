// Package source talks to upstream PAN data providers.
package source

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Result is what an upstream source knows about a PAN.
// "PAN not found" is a valid answer (Exists=false), not an error.
// An error means the source itself failed and we should try another one.
type Result struct {
	Exists bool
	Name   string
}

// Source is any upstream provider we can ask about a PAN.
type Source interface {
	Name() string
	LookupPAN(ctx context.Context, pan string) (Result, error)
}

// HTTPSource calls a provider over HTTP with a hard timeout.
type HTTPSource struct {
	name   string
	url    string
	client *http.Client
}

func NewHTTPSource(name, url string, timeout time.Duration) *HTTPSource {
	return &HTTPSource{
		name: name,
		url:  url,
		// Never call an upstream without a timeout. A hung provider would
		// otherwise hold our goroutines and connections forever.
		client: &http.Client{Timeout: timeout},
	}
}

func (s *HTTPSource) Name() string { return s.name }

func (s *HTTPSource) LookupPAN(ctx context.Context, pan string) (Result, error) {
	body, err := json.Marshal(map[string]string{"pan": pan})
	if err != nil {
		return Result{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return Result{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return Result{}, fmt.Errorf("%s request failed: %w", s.name, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return Result{}, fmt.Errorf("%s returned status %d", s.name, resp.StatusCode)
	}

	var out struct {
		Exists bool   `json:"exists"`
		Name   string `json:"name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return Result{}, fmt.Errorf("%s returned invalid body: %w", s.name, err)
	}

	return Result{Exists: out.Exists, Name: out.Name}, nil
}
