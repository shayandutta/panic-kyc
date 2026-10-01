package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"kyc-platform/services/verification-service/internal/source"
)

type fakeSource struct {
	name  string
	res   source.Result
	err   error
	calls int
}

func (f *fakeSource) Name() string { return f.name }

func (f *fakeSource) LookupPAN(ctx context.Context, pan string) (source.Result, error) {
	f.calls++
	return f.res, f.err
}

func TestUsesPrimaryWhenHealthy(t *testing.T) {
	a := &fakeSource{name: "a", res: source.Result{Exists: true, Name: "A"}}
	b := &fakeSource{name: "b"}
	o := New([]source.Source{a, b}, 3, time.Minute)

	res, err := o.LookupPAN(context.Background(), "ABCDE1234F")
	if err != nil || res.Source != "a" {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if b.calls != 0 {
		t.Error("secondary should not be called when primary works")
	}
}

func TestFallsBackWhenPrimaryFails(t *testing.T) {
	a := &fakeSource{name: "a", err: errors.New("503")}
	b := &fakeSource{name: "b", res: source.Result{Exists: true, Name: "B"}}
	o := New([]source.Source{a, b}, 3, time.Minute)

	res, err := o.LookupPAN(context.Background(), "ABCDE1234F")
	if err != nil || res.Source != "b" {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

func TestOpenBreakerSkipsBrokenSource(t *testing.T) {
	a := &fakeSource{name: "a", err: errors.New("503")}
	b := &fakeSource{name: "b", res: source.Result{Exists: true}}
	o := New([]source.Source{a, b}, 2, time.Minute)

	for i := 0; i < 5; i++ {
		o.LookupPAN(context.Background(), "ABCDE1234F")
	}
	if a.calls != 2 {
		t.Errorf("primary called %d times; breaker should stop calls after 2 failures", a.calls)
	}
	if o.BreakerStates()["a"] != "open" {
		t.Error("expected breaker a to be open")
	}
}

func TestAllSourcesFailed(t *testing.T) {
	a := &fakeSource{name: "a", err: errors.New("503")}
	b := &fakeSource{name: "b", err: errors.New("timeout")}
	o := New([]source.Source{a, b}, 3, time.Minute)

	_, err := o.LookupPAN(context.Background(), "ABCDE1234F")
	if !errors.Is(err, ErrAllSourcesFailed) {
		t.Errorf("err = %v, want ErrAllSourcesFailed", err)
	}
}

func TestStopsWhenContextCancelled(t *testing.T) {
	a := &fakeSource{name: "a", res: source.Result{Exists: true}}
	o := New([]source.Source{a}, 3, time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := o.LookupPAN(ctx, "ABCDE1234F"); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if a.calls != 0 {
		t.Error("no source should be called after cancellation")
	}
}
