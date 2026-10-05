package pubsub

// The four shapes of an operation. Every Op takes one argument and gives one
// result; when an operation has no argument, or no result, None stands in — inside,
// never in a caller's hands. These views give each shape the API it deserves:
//
//	argument  result   view
//	yes       yes      Topic[A, R]   Submit(A)  Do(A) (R, error)
//	yes       no       Message[A]    Submit(A)  Do(A) error
//	no        yes      Signal[R]     Submit()   Do() (R, error)
//	no        no       Trigger       Submit()   Do() error
//
// Without a result, a subscriber still gets one Result per operation — its ID and
// its Err (nil: done): the end of the work is the news.

// None: no argument, or no result
type None = struct{}

// Message: an operation with an argument and no result
type Message[A any] interface {
	Submit(arg A) ID
	Subscribe(buffer int) *Sub[None]
	Do(arg A) error
}

// Signal: an operation with no argument and a result
type Signal[R any] interface {
	Submit() ID
	Subscribe(buffer int) *Sub[R]
	Do() (R, error)
}

// Trigger: an operation with neither
type Trigger interface {
	Submit() ID
	Subscribe(buffer int) *Sub[None]
	Do() error
}

// MessageOf: an operation without a result, seen as a Message
func MessageOf[T, A any](op *Op[T, A, None]) Message[A] { return message[A]{op} }

// SignalOf: an operation without an argument, seen as a Signal
func SignalOf[T, R any](op *Op[T, None, R]) Signal[R] { return signal[R]{op} }

// TriggerOf: an operation with neither, seen as a Trigger
func TriggerOf[T any](op *Op[T, None, None]) Trigger { return trigger{op} }

type message[A any] struct{ t Topic[A, None] }

func (m message[A]) Submit(arg A) ID                 { return m.t.Submit(arg) }
func (m message[A]) Subscribe(buffer int) *Sub[None] { return m.t.Subscribe(buffer) }
func (m message[A]) Do(arg A) error {
	_, err := m.t.Do(arg)
	return err
}

type signal[R any] struct{ t Topic[None, R] }

func (s signal[R]) Submit() ID                   { return s.t.Submit(None{}) }
func (s signal[R]) Subscribe(buffer int) *Sub[R] { return s.t.Subscribe(buffer) }
func (s signal[R]) Do() (R, error)               { return s.t.Do(None{}) }

type trigger struct{ t Topic[None, None] }

func (t trigger) Submit() ID                      { return t.t.Submit(None{}) }
func (t trigger) Subscribe(buffer int) *Sub[None] { return t.t.Subscribe(buffer) }
func (t trigger) Do() error {
	_, err := t.t.Do(None{})
	return err
}
