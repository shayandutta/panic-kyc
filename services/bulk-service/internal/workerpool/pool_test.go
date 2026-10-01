package workerpool

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestProcessesEveryItem(t *testing.T) {
	var done atomic.Int64
	p := New(4, 10, func(int) { done.Add(1) })
	for i := 0; i < 1000; i++ {
		p.Submit(i)
	}
	p.Close()
	if got := done.Load(); got != 1000 {
		t.Fatalf("processed %d, want 1000", got)
	}
}

func TestResize(t *testing.T) {
	p := New(2, 10, func(int) {})
	defer p.Close()
	p.Resize(5)
	if p.Size() != 5 {
		t.Fatalf("size = %d, want 5", p.Size())
	}
	p.Resize(1)
	if p.Size() != 1 {
		t.Fatalf("size = %d, want 1", p.Size())
	}
}

func TestShrinkingNeverInterruptsAnItem(t *testing.T) {
	var started, finished atomic.Int64
	p := New(4, 10, func(int) {
		started.Add(1)
		time.Sleep(30 * time.Millisecond)
		finished.Add(1)
	})
	for i := 0; i < 4; i++ {
		p.Submit(i)
	}
	time.Sleep(5 * time.Millisecond) // let all four start
	p.Resize(1)                      // blocks until three workers stop
	p.Close()
	if started.Load() != finished.Load() {
		t.Errorf("started %d but finished %d", started.Load(), finished.Load())
	}
}

func TestAutoscaleGrowsUnderLoadAndShrinksWhenIdle(t *testing.T) {
	p := New(1, 200, func(int) { time.Sleep(10 * time.Millisecond) })
	stop := make(chan struct{})
	go p.Autoscale(1, 6, 5*time.Millisecond, stop)

	for i := 0; i < 200; i++ {
		p.Submit(i)
	}
	time.Sleep(60 * time.Millisecond)
	if p.Size() < 3 {
		t.Errorf("did not grow under load: %d workers", p.Size())
	}

	deadline := time.Now().Add(2 * time.Second)
	for p.Size() > 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if p.Size() != 1 {
		t.Errorf("did not shrink when idle: %d workers", p.Size())
	}
	close(stop)
	p.Close()
}
