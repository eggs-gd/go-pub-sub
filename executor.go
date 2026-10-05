package pubsub

// Class: how long an operation tolerates waiting while its executor gathers a
// batch — a property of the Op, not of its caller; a hint the executor may use.
// A class only delays a result: throughput comes from many operations in flight (a
// Client), not from Do per item in a loop.
type Class int

const (
	Now   Class = iota // never waits; taken before the others
	Frame              // may wait a frame (~33 ms) or a full batch
	Idle               // may wait longer, only while nothing else does
)

// Job: one submitted operation as an executor sees it — its class, the operation to
// run in the executor's env, and the end once the executor is done with it. Sealed:
// only an Op makes jobs, so an executor runs nothing but the operations made with
// it (whoever keeps the executor decides which operations exist).
type Job[T any] interface {
	Class() Class
	// Run: the operation, in the executor's env (its result stays in the job)
	Run(env T)
	// Done: the executor is done with the job — delivers the result. err: the
	// executor failed it (a commit): a good result becomes err, an operation's own
	// error stays its own.
	Done(err error)
	sealed()
}

// Executor: who runs the jobs — yours. Enqueue queues the job and returns: it must
// not run the work (Submit returns when Enqueue does). It may wait for room when its
// queue is full — backpressure, the executor's choice. Inline is the one exception,
// by design.
type Executor[T any] interface {
	Enqueue(job Job[T])
}

// Inline: an executor that runs a job right there, on the caller's goroutine, with
// the zero env — Submit returns after the work: for tests, and for operations with
// nothing to batch
type Inline[T any] struct{}

func (Inline[T]) Enqueue(j Job[T]) {
	var env T
	j.Run(env)
	j.Done(nil)
}
