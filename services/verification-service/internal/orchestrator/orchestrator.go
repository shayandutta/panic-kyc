// Package orchestrator asks upstream sources about a PAN, in priority order,
// falling back to the next source when one fails.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"kyc-platform/services/verification-service/internal/source"
	"kyc-platform/shared/resilience"
)

var ErrAllSourcesFailed = errors.New("all upstream sources failed")

// Result is the answer plus which source gave it (kept for the audit trail).
type Result struct {
	Exists       bool
	NameOnRecord string
	Source       string
}

type guardedSource struct {
	source  source.Source
	breaker *resilience.Breaker
}

type Orchestrator struct {
	sources []guardedSource
}

// New wraps every source in its own circuit breaker.
// Sources are tried in the order given: first is the primary.
func New(sources []source.Source, failureThreshold int, openTimeout time.Duration) *Orchestrator {
	guarded := make([]guardedSource, len(sources))
	for i, s := range sources {
		guarded[i] = guardedSource{
			source:  s,
			breaker: resilience.NewBreaker(s.Name(), failureThreshold, openTimeout),
		}
	}
	return &Orchestrator{sources: guarded}
}

func (o *Orchestrator) LookupPAN(ctx context.Context, pan string) (Result, error) {
	var errs []error

	for _, gs := range o.sources {
		// The caller's deadline covers the whole fallback chain.
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}

		var res source.Result
		err := gs.breaker.Execute(func() error {
			var callErr error
			res, callErr = gs.source.LookupPAN(ctx, pan)
			return callErr
		})
		if err == nil {
			return Result{Exists: res.Exists, NameOnRecord: res.Name, Source: gs.source.Name()}, nil
		}

		// Log the source and error only. Never the PAN.
		log.Printf("source %s failed (breaker %s): %v", gs.source.Name(), gs.breaker.State(), err)
		errs = append(errs, fmt.Errorf("%s: %w", gs.source.Name(), err))
	}

	return Result{}, fmt.Errorf("%w: %w", ErrAllSourcesFailed, errors.Join(errs...))
}

// BreakerStates reports each source's breaker, for health endpoints and logs.
func (o *Orchestrator) BreakerStates() map[string]string {
	states := make(map[string]string, len(o.sources))
	for _, gs := range o.sources {
		states[gs.source.Name()] = gs.breaker.State().String()
	}
	return states
}
