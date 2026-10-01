package service

import (
	"context"
	"errors"
	"sync"
	"testing"

	"kyc-platform/services/verification-service/internal/domain"
	"kyc-platform/services/verification-service/internal/orchestrator"
	"kyc-platform/services/verification-service/internal/repository"
)

type fakeLookup struct {
	mu    sync.Mutex
	res   orchestrator.Result
	err   error
	calls int
}

func (f *fakeLookup) LookupPAN(ctx context.Context, pan string) (orchestrator.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.res, f.err
}

type mapCache struct {
	mu sync.Mutex
	m  map[string]domain.CachedLookup
}

func newMapCache() *mapCache { return &mapCache{m: map[string]domain.CachedLookup{}} }

func (c *mapCache) Get(ctx context.Context, key string) (*domain.CachedLookup, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.m[key]
	return &v, ok, nil
}

func (c *mapCache) Set(ctx context.Context, key string, v domain.CachedLookup) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[key] = v
	return nil
}

func newTestService(l *fakeLookup) *Service {
	return New(repository.NewInmemRepository(), newMapCache(), l, "test-secret")
}

var ctx = context.Background()

func TestValidPANWithMatchingName(t *testing.T) {
	l := &fakeLookup{res: orchestrator.Result{Exists: true, NameOnRecord: "Shayan Dutta", Source: "source-a"}}
	v, err := newTestService(l).VerifyPAN(ctx, VerifyRequest{ClientID: "c1", PAN: "abcde1234f", Name: "DUTTA shayan"})
	if err != nil {
		t.Fatal(err)
	}
	if v.Status != domain.StatusValid || !v.NameMatch || v.PANMasked != "AB******4F" {
		t.Errorf("unexpected verification %+v", v)
	}
}

func TestInvalidFormatNeverCallsUpstream(t *testing.T) {
	l := &fakeLookup{}
	_, err := newTestService(l).VerifyPAN(ctx, VerifyRequest{ClientID: "c1", PAN: "123"})
	if !errors.Is(err, ErrInvalidPAN) || l.calls != 0 {
		t.Errorf("err=%v calls=%d", err, l.calls)
	}
}

func TestSameReferenceReturnsSameResult(t *testing.T) {
	l := &fakeLookup{res: orchestrator.Result{Exists: true, Source: "source-a"}}
	s := newTestService(l)
	req := VerifyRequest{ClientID: "c1", PAN: "ABCDE1234F", ReferenceID: "ref-1"}

	first, _ := s.VerifyPAN(ctx, req)
	second, _ := s.VerifyPAN(ctx, req)
	if first.ID != second.ID {
		t.Error("retry with the same reference ID must return the original result")
	}
}

func TestCacheSkipsUpstreamOnSecondCall(t *testing.T) {
	l := &fakeLookup{res: orchestrator.Result{Exists: true, Source: "source-a"}}
	s := newTestService(l)
	s.VerifyPAN(ctx, VerifyRequest{ClientID: "c1", PAN: "ABCDE1234F"})
	s.VerifyPAN(ctx, VerifyRequest{ClientID: "c2", PAN: "ABCDE1234F"})
	if l.calls != 1 {
		t.Errorf("upstream called %d times, want 1", l.calls)
	}
}

func TestAllSourcesDown(t *testing.T) {
	l := &fakeLookup{err: orchestrator.ErrAllSourcesFailed}
	_, err := newTestService(l).VerifyPAN(ctx, VerifyRequest{ClientID: "c1", PAN: "ABCDE1234F"})
	if !errors.Is(err, ErrSourceUnavailable) {
		t.Errorf("err = %v", err)
	}
}

func TestClientsCannotReadEachOthersRecords(t *testing.T) {
	l := &fakeLookup{res: orchestrator.Result{Exists: true}}
	s := newTestService(l)
	v, _ := s.VerifyPAN(ctx, VerifyRequest{ClientID: "c1", PAN: "ABCDE1234F"})
	if _, err := s.GetVerification(ctx, "c2", v.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Error("client c2 must not see client c1's verification")
	}
}
