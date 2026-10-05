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

// Submit gives an ID at once; Done's listeners get every result, in order
func TestSubmitDone(t *testing.T) {
	w := newWriter()
	op := New(w, Frame, double)
	var got []Result[int]
	op.Done().Subscribe(func(r Result[int]) { got = append(got, r) })
	var ids []ID
	for n := range 3 {
		ids = append(ids, op.Submit(n))
	}
	w.stop()
	want := []Result[int]{{ids[0], 0, nil}, {ids[1], 2, nil}, {ids[2], 4, nil}}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// Do waits for its own result, its error too; Done's listeners get it as well
func TestDo(t *testing.T) {
	w := newWriter()
	defer w.stop()
	op := New(w, Now, double)
	var heard int
	op.Done().Subscribe(func(Result[int]) { heard++ })
	if n, err := op.Do(21); n != 42 || err != nil {
		t.Errorf("Do(21) = %d, %v", n, err)
	}
	if _, err := op.Do(-1); err == nil {
		t.Error("Do(-1): no error")
	}
	if heard != 2 {
		t.Errorf("listeners heard %d results", heard)
	}
}

// The executor failing the job (a commit) replaces a good result with its error
func TestCommitFails(t *testing.T) {
	w := newWriter()
	w.commit = errors.New("disk full")
	defer w.stop()
	if n, err := New(w, Frame, double).Do(1); n != 0 || err == nil {
		t.Errorf("Do = %d, %v; want the commit's error", n, err)
	}
}

// Inline runs the job right there: Submit returns after the work
func TestInline(t *testing.T) {
	var ran bool
	op := New(Inline[int]{}, Now, func(int, None) (None, error) { ran = true; return None{}, nil })
	op.Submit(None{})
	if !ran {
		t.Error("Inline did not run the job in Submit")
	}
}
