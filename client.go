package pubsub

import (
	"iter"
	"sync"
)

// Client: a caller's own line to an Op — it submits, and reads back only the
// results of its own submissions (no one else's traffic in it, nothing of its own
// lost). It counts its room when it submits: at most window results in flight or
// unread; Submit waits for room, TrySubmit says there is none. A result is read
// through Results, which frees its room — so the executor's delivery never waits.
type Client[A, R any] struct {
	submit  func(arg A, reply chan<- Result[R]) ID
	results chan Result[R]
	room    chan struct{} // a token per result in flight or unread
	closed  chan struct{}
	mu      sync.RWMutex // a Submit in progress holds it shared, Close whole
}

func newClient[A, R any](window int, submit func(A, chan<- Result[R]) ID) *Client[A, R] {
	window = max(window, 1)
	return &Client[A, R]{
		submit:  submit,
		results: make(chan Result[R], window),
		room:    make(chan struct{}, window),
		closed:  make(chan struct{}),
	}
}

// Submit: queued (waiting for room while window results are in flight or unread),
// its ID — its result comes through Results. Submit on a closed client panics.
func (c *Client[A, R]) Submit(arg A) ID {
	c.mu.RLock()
	defer c.mu.RUnlock()
	c.mustOpen()
	c.room <- struct{}{}
	return c.submit(arg, c.results)
}

// TrySubmit: as Submit, or false right away when there is no room
func (c *Client[A, R]) TrySubmit(arg A) (ID, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	c.mustOpen()
	select {
	case c.room <- struct{}{}:
		return c.submit(arg, c.results), true
	default:
		return 0, false
	}
}

// Results: this client's results, in the order the executor finished them; each
// read frees its room. Once the client is closed, it ends after the last result in
// flight.
func (c *Client[A, R]) Results() iter.Seq[Result[R]] {
	return func(yield func(Result[R]) bool) {
		for {
			var r Result[R]
			if c.isClosed() {
				if len(c.room) == 0 {
					return
				}
				r = <-c.results
			} else {
				select {
				case r = <-c.results:
				case <-c.closed:
					continue
				}
			}
			<-c.room
			if !yield(r) {
				return
			}
		}
	}
}

// Close: no more submissions from this client (one after it panics); Results ends
// once the results in flight are read. It waits for the Submits in progress (one
// waiting for room waits for a read): call it from the submitting side, not from
// the loop over Results.
func (c *Client[A, R]) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.isClosed() {
		close(c.closed)
	}
}

func (c *Client[A, R]) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

func (c *Client[A, R]) mustOpen() {
	if c.isClosed() {
		panic("pubsub: Submit on a closed Client")
	}
}
