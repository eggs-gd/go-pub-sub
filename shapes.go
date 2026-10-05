package pubsub

import "iter"

// The four shapes of an operation. Every Op takes one argument and gives one
// result; when an operation has no argument, or no result, None stands in — inside,
// never in a caller's hands. These views give each shape the API it deserves:
//
//	argument  result   view           Submit     Do
//	yes       yes      Command[A, R]  Submit(A)  Do(A) (R, error)
//	yes       no       Message[A]     Submit(A)  Do(A) error
//	no        yes      Signal[R]      Submit()   Do() (R, error)
//	no        no       Trigger        Submit()   Do() error
//
// Without a result, a listener or a client still gets one Result per operation —
// its ID and its Err (nil: done): the end of the work is the news.

// None: no argument, or no result
type None = struct{}

// Message: an operation with an argument and no result
type Message[A any] interface {
	Submit(arg A) ID
	Do(arg A) error
	Client(window int) *Client[A, None]
	Done() *Topic[Result[None]]
}

// Signal: an operation with no argument and a result
type Signal[R any] interface {
	Submit() ID
	Do() (R, error)
	Client(window int) *SignalClient[R]
	Done() *Topic[Result[R]]
}

// Trigger: an operation with neither
type Trigger interface {
	Submit() ID
	Do() error
	Client(window int) *SignalClient[None]
	Done() *Topic[Result[None]]
}

// MessageOf: an operation without a result, seen as a Message
func MessageOf[A any](c Command[A, None]) Message[A] { return message[A]{c} }

// SignalOf: an operation without an argument, seen as a Signal
func SignalOf[R any](c Command[None, R]) Signal[R] { return signal[R]{c} }

// TriggerOf: an operation with neither, seen as a Trigger
func TriggerOf(c Command[None, None]) Trigger { return trigger{signal[None]{c}} }

// SignalClient: a Client of an operation without an argument (a Signal, a Trigger)
type SignalClient[R any] struct{ c *Client[None, R] }

func (s *SignalClient[R]) Submit() ID                   { return s.c.Submit(None{}) }
func (s *SignalClient[R]) TrySubmit() (ID, bool)        { return s.c.TrySubmit(None{}) }
func (s *SignalClient[R]) Results() iter.Seq[Result[R]] { return s.c.Results() }
func (s *SignalClient[R]) Close()                       { s.c.Close() }
