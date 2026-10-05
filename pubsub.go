// Package pubsub: operations as topics. An operation kind is an Op: a caller
// submits an argument and gets an ID at once; an Executor runs the operation (in
// its own goroutine, its own transaction, batched as it likes) and, once it is
// done, the result goes to every subscriber of the Op — each picks its own by ID.
// Do submits and waits for its own result, for callers that may block.
//
// The package does not know what runs the operations: T is the executor's env
// (a database transaction, a connection, a client), seen only by whoever made the
// Op and by its Executor; a caller sees a Topic — an argument in, a result out.
//
// Who may do what is decided by who holds what: an Op is made with the executor,
// so whoever keeps the executor to itself decides which operations exist; others
// get Topics — no env, no code of their own (Job is sealed: only an Op makes
// one).
//
// Not obvious:
//   - Delivery never blocks the executor: a subscriber whose buffer is full misses
//     the result (Dropped counts it). A lost subscription is its owner's loss.
//   - A result is delivered only when the executor is done with its job (for a
//     database: after the commit), so what its receiver does next sees it. If the
//     executor fails the job (a commit that failed), a good result is replaced by
//     that error.
//   - Results reach a subscriber in the order the executor finished them; across
//     subscribers nothing is ordered.
package pubsub

import (
	"sync"
	"sync/atomic"
)

// ID: an operation of an Op, unique within it
type ID uint64

// Class: how long an operation tolerates waiting while its executor gathers a
// batch — a property of the Op, not of its caller; a hint the executor may use
type Class int

const (
	Now   Class = iota // never waits; taken before the others
	Frame              // waits a frame (~33 ms) or a full batch
	Idle               // waits longer, only while nothing else does
)

// Job: one submitted operation as an executor sees it — its class, the operation to
// run in the executor's env, and the end once the executor is done with it.
// Sealed: only an Op makes jobs, so an executor runs nothing but the operations
// made with it
type Job[T any] interface {
	Class() Class
	// Run: the operation, in the executor's env (its result stays in the job)
	Run(env T)
	// Done: the executor is done with the job (err: it failed it, e.g. a commit);
	// delivers the result
	Done(err error)
	sealed()
}

// Executor: who runs the jobs — a goroutine with a database transaction, a worker
// pool, anything; Inline runs them at once
type Executor[T any] interface {
	Enqueue(job Job[T])
}

// Topic: an operation as a caller sees it, whatever runs it
type Topic[A, R any] interface {
	Submit(arg A) ID
	Subscribe(buffer int) *Sub[R]
	Do(arg A) (R, error)
}

// Result: what an operation gave — its ID, its value or its error
type Result[R any] struct {
	ID    ID
	Value R
	Err   error
}

// Op: one kind of operation, a topic of its results
type Op[T, A, R any] struct {
	exec  Executor[T]
	class Class
	fn    func(env T, arg A) (R, error)
	last  atomic.Uint64

	mu   sync.Mutex
	subs map[*Sub[R]]struct{}
}

var _ Topic[int, int] = (*Op[any, int, int])(nil)

// New: an operation of this class, run by exec
func New[T, A, R any](exec Executor[T], class Class, fn func(env T, arg A) (R, error)) *Op[T, A, R] {
	return &Op[T, A, R]{exec: exec, class: class, fn: fn, subs: map[*Sub[R]]struct{}{}}
}

// Submit: queued, returns its ID at once; the result goes to the subscribers
func (o *Op[T, A, R]) Submit(arg A) ID {
	id := ID(o.last.Add(1))
	o.exec.Enqueue(&job[T, A, R]{op: o, id: id, arg: arg})
	return id
}

// Do: submitted, and waited for — its own result (the subscribers get it too)
func (o *Op[T, A, R]) Do(arg A) (R, error) {
	reply := make(chan Result[R], 1)
	o.exec.Enqueue(&job[T, A, R]{op: o, id: ID(o.last.Add(1)), arg: arg, reply: reply})
	r := <-reply
	return r.Value, r.Err
}

// Subscribe: every result of this Op from now on, buffered; filter yours by ID
func (o *Op[T, A, R]) Subscribe(buffer int) *Sub[R] {
	s := &Sub[R]{c: make(chan Result[R], buffer), op: o}
	s.C = s.c
	o.mu.Lock()
	o.subs[s] = struct{}{}
	o.mu.Unlock()
	return s
}

// publish: a result to every subscriber, never waiting for one
func (o *Op[T, A, R]) publish(r Result[R]) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for s := range o.subs {
		select {
		case s.c <- r:
		default:
			s.dropped.Add(1)
		}
	}
}

func (o *Op[T, A, R]) unsubscribe(s *Sub[R]) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.subs[s]; ok {
		delete(o.subs, s)
		close(s.c) // under the lock: no publish sends after it
	}
}

// Sub: a subscription to an Op's results
type Sub[R any] struct {
	C       <-chan Result[R]
	c       chan Result[R]
	op      interface{ unsubscribe(*Sub[R]) }
	dropped atomic.Uint64
}

// Close: no more results; C closes (what it buffered can still be read)
func (s *Sub[R]) Close() { s.op.unsubscribe(s) }

// Dropped: the results this subscription missed, its buffer full
func (s *Sub[R]) Dropped() uint64 { return s.dropped.Load() }

// job: an operation of an Op — its argument and, once run, its result
type job[T, A, R any] struct {
	op    *Op[T, A, R]
	id    ID
	arg   A
	value R
	err   error
	reply chan Result[R] // Do's own; nil for Submit
}

func (j *job[T, A, R]) Class() Class { return j.op.class }

func (*job[T, A, R]) sealed() {}

func (j *job[T, A, R]) Run(env T) { j.value, j.err = j.op.fn(env, j.arg) }

func (j *job[T, A, R]) Done(err error) {
	r := Result[R]{ID: j.id, Value: j.value, Err: j.err}
	if err != nil && j.err == nil {
		var zero R
		r.Value, r.Err = zero, err
	}
	j.op.publish(r) // Do's too: a topic is every result of its Op
	if j.reply != nil {
		j.reply <- r
	}
}

// Inline: an executor that runs a job at once on the caller's goroutine, with the
// zero env — for tests, and for operations with nothing to batch
type Inline[T any] struct{}

func (Inline[T]) Enqueue(j Job[T]) {
	var env T
	j.Run(env)
	j.Done(nil)
}
