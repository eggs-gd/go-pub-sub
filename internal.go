package pubsub

import (
	"fmt"
	"log/slog"
	"time"
)

// The private side of the layers: the job an Op hands its executor, the shapes'
// views, a listener's call.

// job: an operation of an Op — its argument and, once run, its result
type job[T, A, R any] struct {
	op    *Op[T, A, R]
	id    ID
	arg   A
	value R
	err   error
	reply chan<- Result[R] // its caller's: Do's, a Client's; nil for Submit
}

func (j *job[T, A, R]) Class() Class { return j.op.class }

func (*job[T, A, R]) sealed() {}

func (j *job[T, A, R]) Run(env T) { j.value, j.err = j.op.fn(env, j.arg) }

// Done: the result to the Op's listeners, then to its caller. Never waits: Do's
// reply holds one, a Client's holds its window and the Client counted its room.
func (j *job[T, A, R]) Done(err error) {
	r := Result[R]{ID: j.id, Value: j.value, Err: j.err}
	if err != nil && j.err == nil { // the executor failed it (a commit): a good result is not
		var zero R
		r.Value, r.Err = zero, err
	}
	j.op.done.Publish(r)
	if j.reply != nil {
		j.reply <- r
	}
}

// listen: one listener's call — a panic recovered and logged, a slow one logged
func listen[E any](fn func(E), e E) {
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			slog.Error("pubsub: a listener panicked", "event", fmt.Sprintf("%T", e), "panic", r)
		}
		if took := time.Since(start); took > SlowListener {
			slog.Warn("pubsub: a slow listener", "event", fmt.Sprintf("%T", e), "took", took)
		}
	}()
	fn(e)
}

// The shapes' views: an Op with None hidden

type message[A any] struct{ c Command[A, None] }

func (m message[A]) Submit(arg A) ID                    { return m.c.Submit(arg) }
func (m message[A]) Client(window int) *Client[A, None] { return m.c.Client(window) }
func (m message[A]) Done() *Topic[Result[None]]         { return m.c.Done() }
func (m message[A]) Do(arg A) error {
	_, err := m.c.Do(arg)
	return err
}

type signal[R any] struct{ c Command[None, R] }

func (s signal[R]) Submit() ID                         { return s.c.Submit(None{}) }
func (s signal[R]) Do() (R, error)                     { return s.c.Do(None{}) }
func (s signal[R]) Client(window int) *SignalClient[R] { return &SignalClient[R]{s.c.Client(window)} }
func (s signal[R]) Done() *Topic[Result[R]]            { return s.c.Done() }

type trigger struct{ signal[None] }

func (t trigger) Do() error {
	_, err := t.signal.Do()
	return err
}
