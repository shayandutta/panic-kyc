// Package workerpool runs tasks on a number of goroutines that can grow and
// shrink with the backlog.
package workerpool

import (
	"sync"
	"time"
)

// Pool processes items of type T with handle. The queue is bounded, so when
// it's full Submit blocks: that's backpressure on whoever is producing work.
type Pool[T any] struct {
	jobs    chan T
	quit    chan struct{}
	handle  func(T)
	mu      sync.Mutex
	workers int
	wg      sync.WaitGroup
}

func New[T any](size, queueSize int, handle func(T)) *Pool[T] {
	p := &Pool[T]{
		jobs:   make(chan T, queueSize),
		quit:   make(chan struct{}),
		handle: handle,
	}
	p.Resize(size)
	return p
}

func (p *Pool[T]) Submit(item T) { p.jobs <- item }

// SubmitOrStop is Submit that gives up when stop is closed, so a producer
// blocked on a full queue can exit during shutdown. It reports whether the
// item was queued.
func (p *Pool[T]) SubmitOrStop(item T, stop <-chan struct{}) bool {
	select {
	case p.jobs <- item:
		return true
	case <-stop:
		return false
	}
}

func (p *Pool[T]) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.workers
}

func (p *Pool[T]) Backlog() int { return len(p.jobs) }

// Resize starts or stops workers until there are exactly n.
// A stopping worker always finishes its current item first.
func (p *Pool[T]) Resize(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.workers < n {
		p.workers++
		p.wg.Add(1)
		go p.worker()
	}
	for p.workers > n {
		p.workers--
		p.quit <- struct{}{}
	}
}

func (p *Pool[T]) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.quit:
			return
		case item, ok := <-p.jobs:
			if !ok {
				return
			}
			p.handle(item)
		}
	}
}

// Autoscale adds a worker when the backlog is bigger than the pool, and
// removes one when the queue is empty, one step per tick, between min and max.
// Small steps avoid flapping up and down.
func (p *Pool[T]) Autoscale(min, max int, tick time.Duration, stop <-chan struct{}) {
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			backlog, size := p.Backlog(), p.Size()
			switch {
			case backlog > size && size < max:
				p.Resize(size + 1)
			case backlog == 0 && size > min:
				p.Resize(size - 1)
			}
		}
	}
}

// Close stops accepting work and waits for queued items to finish.
// Every producer must have stopped calling Submit first: sending on a
// closed channel panics.
func (p *Pool[T]) Close() {
	close(p.jobs)
	p.wg.Wait()
}
