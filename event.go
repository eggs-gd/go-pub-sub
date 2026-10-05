package pubsub

import (
	"sync"
	"time"
)

// SlowListener: a listener that takes longer is logged (log/slog) — set it before
// anything publishes
var SlowListener = 10 * time.Millisecond

// Topic: events of one kind. The zero value is ready to use.
//
// Publish calls each listener, in the order they subscribed, right there on the
// publisher's goroutine — the dispatch of every UI engine. So a listener is thin:
// it hands the work off (to its channel, its queue, its workers) and returns. A
// listener that panics is recovered and logged; one that takes longer than
// SlowListener is logged — a broken contract is seen, not guessed.
type Topic[E any] struct {
	mu        sync.RWMutex
	listeners []*Subscription[E]
}

// Publish: the event to every listener; nobody listening — nothing happens
func (t *Topic[E]) Publish(e E) {
	t.mu.RLock()
	listeners := t.listeners
	t.mu.RUnlock()
	for _, s := range listeners {
		s.call(e)
	}
}

// Subscribe: fn is called with every event published from now on
func (t *Topic[E]) Subscribe(fn func(E)) *Subscription[E] {
	s := &Subscription[E]{topic: t, fn: fn}
	t.mu.Lock()
	t.listeners = append(t.listeners[:len(t.listeners):len(t.listeners)], s) // a new slice: a Publish in progress keeps its own
	t.mu.Unlock()
	return s
}

// Subscription: a listener on a topic
type Subscription[E any] struct {
	topic *Topic[E]
	fn    func(E)
}

// Close: the listener is called no more (an event published while Close runs may
// still reach it)
func (s *Subscription[E]) Close() {
	t := s.topic
	t.mu.Lock()
	defer t.mu.Unlock()
	kept := make([]*Subscription[E], 0, len(t.listeners))
	for _, l := range t.listeners {
		if l != s {
			kept = append(kept, l)
		}
	}
	t.listeners = kept
}

func (s *Subscription[E]) call(e E) { listen(s.fn, e) }
