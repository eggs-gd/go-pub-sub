package pubsub

import (
	"errors"
	"slices"
	"sync"
	"testing"
)

// writer: a test executor as a database writer would be — one goroutine runs the
// jobs of a batch in a "transaction", then ends them; commit fails the whole batch
// when set
type writer struct {
	jobs   chan Job[string]
	commit error
	done   sync.WaitGroup
}

func newWriter() *writer {
	w := &writer{jobs: make(chan Job[string], 64)}
	w.done.Go(func() {
		for j := range w.jobs {
			batch := []Job[string]{j}
			for len(w.jobs) > 0 { // what is already queued joins the batch
				batch = append(batch, <-w.jobs)
			}
			for _, b := range batch {
				b.Run("tx")
			}
			for _, b := range batch {
				b.Done(w.commit)
			}
		}
	})
	return w
}

func (w *writer) Enqueue(j Job[string]) { w.jobs <- j }
func (w *writer) stop()                 { close(w.jobs); w.done.Wait() }

func double(tx string, n int) (int, error) {
	if tx != "tx" {
		return 0, errors.New("not in the transaction")
	}
	if n < 0 {
		return 0, errors.New("negative")
	}
	return n * 2, nil
}

// Submit gives an ID at once; the result reaches the subscriber with it, in order
func TestSubmitSubscribe(t *testing.T) {
	w := newWriter()
	op := New(w, Frame, double)
	sub := op.Subscribe(16)
	var ids []ID
	for n := range 3 {
		ids = append(ids, op.Submit(n))
	}
	w.stop()
	sub.Close()
	var got []Result[int]
	for r := range sub.C {
		got = append(got, r)
	}
	want := []Result[int]{{ids[0], 0, nil}, {ids[1], 2, nil}, {ids[2], 4, nil}}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// Two callers on one topic: each sees every result and picks its own by ID
func TestOwnByID(t *testing.T) {
	op := New(Inline[string]{}, Now, func(_ string, n int) (int, error) { return n, nil })
	a, b := op.Subscribe(8), op.Subscribe(8)
	mine := op.Submit(1)
	op.Submit(2)
	a.Close()
	b.Close()
	var own []int
	for r := range a.C {
		if r.ID == mine {
			own = append(own, r.Value)
		}
	}
	if !slices.Equal(own, []int{1}) {
		t.Errorf("a's own: %v", own)
	}
	if n := len(b.C); n != 2 {
		t.Errorf("b got %d results, want both", n)
	}
}

// A rule's error is its result's; a failed commit replaces a good result
func TestErrors(t *testing.T) {
	w := newWriter()
	op := New(w, Frame, double)
	if _, err := op.Do(-1); err == nil || err.Error() != "negative" {
		t.Errorf("rule error: %v", err)
	}
	w.commit = errors.New("disk full")
	if v, err := op.Do(1); err == nil || v != 0 {
		t.Errorf("after a failed commit: %v, %v", v, err)
	}
	w.stop()
}

// Do waits for its own result; the subscribers get it too
func TestDo(t *testing.T) {
	w := newWriter()
	defer w.stop()
	op := New(w, Now, double)
	sub := op.Subscribe(4)
	if v, err := op.Do(21); v != 42 || err != nil {
		t.Errorf("Do: %v, %v", v, err)
	}
	if r := <-sub.C; r.Value != 42 {
		t.Errorf("the subscriber got %v", r)
	}
}

// A subscriber that does not read misses results; the executor never waits for it
func TestFullBufferDrops(t *testing.T) {
	op := New(Inline[string]{}, Idle, func(_ string, n int) (int, error) { return n, nil })
	sub := op.Subscribe(1)
	for n := range 5 {
		op.Submit(n) // Inline runs on this goroutine: a block here would hang the test
	}
	if sub.Dropped() != 4 || len(sub.C) != 1 {
		t.Errorf("dropped %d, buffered %d", sub.Dropped(), len(sub.C))
	}
}

// Closed: no more results, C closed; closing twice is harmless
func TestClose(t *testing.T) {
	op := New(Inline[string]{}, Now, func(_ string, n int) (int, error) { return n, nil })
	sub := op.Subscribe(4)
	sub.Close()
	sub.Close()
	op.Submit(1)
	if _, open := <-sub.C; open {
		t.Error("a result after Close")
	}
}

// Many callers at once (run with -race): every result delivered exactly once
func TestConcurrent(t *testing.T) {
	w := newWriter()
	op := New(w, Frame, double)
	sub := op.Subscribe(1000)
	var callers sync.WaitGroup
	for range 10 {
		callers.Go(func() {
			for n := range 50 {
				op.Submit(n)
			}
		})
	}
	callers.Wait()
	w.stop()
	sub.Close()
	seen := map[ID]bool{}
	for r := range sub.C {
		if seen[r.ID] {
			t.Fatalf("result %d twice", r.ID)
		}
		seen[r.ID] = true
	}
	if len(seen) != 500 || sub.Dropped() != 0 {
		t.Errorf("delivered %d, dropped %d", len(seen), sub.Dropped())
	}
}
