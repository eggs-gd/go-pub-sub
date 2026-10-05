package pubsub

import "sync/atomic"

// ID: an operation of an Op, unique within it
type ID uint64

// Result: what an operation gave — its ID, its value or its error
type Result[R any] struct {
	ID    ID
	Value R
	Err   error
}

// Command: an operation as a caller sees it, whatever runs it
type Command[A, R any] interface {
	// Submit: fire and forget — queued, its ID at once; its result goes only to
	// the listeners of Done
	Submit(arg A) ID
	// Do: submit and wait for its own result
	Do(arg A) (R, error)
	// Client: submit and read its own results, at most window of them in flight
	Client(window int) *Client[A, R]
	// Done: every result of this operation, as an event — for anyone to listen to
	Done() *Topic[Result[R]]
}

// Op: one kind of operation — its function, its class, its executor
type Op[T, A, R any] struct {
	exec  Executor[T]
	class Class
	fn    func(env T, arg A) (R, error)
	last  atomic.Uint64
	done  Topic[Result[R]]
}

var _ Command[int, int] = (*Op[any, int, int])(nil)

// New: an operation of this class, run by exec
func New[T, A, R any](exec Executor[T], class Class, fn func(env T, arg A) (R, error)) *Op[T, A, R] {
	return &Op[T, A, R]{exec: exec, class: class, fn: fn}
}

func (o *Op[T, A, R]) Submit(arg A) ID { return o.submit(arg, nil) }

func (o *Op[T, A, R]) Do(arg A) (R, error) {
	reply := make(chan Result[R], 1)
	o.submit(arg, reply)
	r := <-reply
	return r.Value, r.Err
}

func (o *Op[T, A, R]) Client(window int) *Client[A, R] { return newClient(window, o.submit) }

func (o *Op[T, A, R]) Done() *Topic[Result[R]] { return &o.done }

// submit: a job of this Op, its result to reply (nil: to Done's listeners only)
func (o *Op[T, A, R]) submit(arg A, reply chan<- Result[R]) ID {
	id := ID(o.last.Add(1))
	o.exec.Enqueue(&job[T, A, R]{op: o, id: id, arg: arg, reply: reply})
	return id
}
