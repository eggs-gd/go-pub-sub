package pubsub

import (
	"slices"
	"testing"
)

// A client gets its own results only — not another client's, not Do's — and ends
// once closed and drained
func TestClientOwn(t *testing.T) {
	w := newWriter()
	defer w.stop()
	op := New(w, Frame, double)
	mine, other := op.Client(4), op.Client(4)
	var ids []ID
	for n := range 3 {
		ids = append(ids, mine.Submit(n))
		other.Submit(100 + n)
	}
	op.Do(50)
	mine.Close()
	var got []ID
	for r := range mine.Results() {
		got = append(got, r.ID)
	}
	if !slices.Equal(got, ids) {
		t.Errorf("got %v, want %v", got, ids)
	}
}

// Room is counted at submit: window results in flight or unread, no more; reading
// one frees its room
func TestClientRoom(t *testing.T) {
	op := New(Inline[string]{}, Frame, func(_ string, n int) (int, error) { return n, nil })
	c := op.Client(2)
	c.Submit(1)
	c.Submit(2)
	if _, ok := c.TrySubmit(3); ok {
		t.Fatal("TrySubmit past the window")
	}
	for r := range c.Results() {
		if r.Value != 1 {
			t.Errorf("first result %d", r.Value)
		}
		break
	}
	if _, ok := c.TrySubmit(3); !ok {
		t.Error("TrySubmit with room")
	}
	c.Close()
	var rest []int
	for r := range c.Results() {
		rest = append(rest, r.Value)
	}
	if !slices.Equal(rest, []int{2, 3}) {
		t.Errorf("rest %v", rest)
	}
}

// A client of many goroutines' worth: a blocked Submit waits for a read, and the
// executor never waits for the client
func TestClientFlow(t *testing.T) {
	w := newWriter()
	defer w.stop()
	op := New(w, Frame, double)
	c := op.Client(3)
	const n = 100
	go func() {
		for i := range n {
			c.Submit(i)
		}
		c.Close()
	}()
	sum := 0
	for r := range c.Results() {
		sum += r.Value
	}
	if want := n * (n - 1); sum != want {
		t.Errorf("sum %d, want %d", sum, want)
	}
}

// Submit on a closed client panics
func TestClientClosed(t *testing.T) {
	c := New(Inline[string]{}, Frame, double).Client(1)
	c.Close()
	defer func() {
		if recover() == nil {
			t.Error("no panic")
		}
	}()
	c.Submit(1)
}
