package pubsub

import (
	"slices"
	"testing"
	"time"
)

// Nobody listening: nothing happens; listening: each listener, in order, right there
func TestTopic(t *testing.T) {
	var topic Topic[int]
	topic.Publish(0)
	var got []string
	a := topic.Subscribe(func(n int) { got = append(got, "a", string(rune('0'+n))) })
	topic.Subscribe(func(n int) { got = append(got, "b", string(rune('0'+n))) })
	topic.Publish(1)
	a.Close()
	topic.Publish(2)
	if want := []string{"a", "1", "b", "1", "b", "2"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// A listener that panics is recovered: the publisher and the other listeners go on
func TestTopicPanic(t *testing.T) {
	var topic Topic[int]
	topic.Subscribe(func(int) { panic("boom") })
	var got int
	topic.Subscribe(func(n int) { got = n })
	topic.Publish(7)
	if got != 7 {
		t.Errorf("the next listener got %d", got)
	}
}

// A listener may subscribe or close while it is called: the publish in progress
// keeps its own list
func TestTopicReentrant(t *testing.T) {
	var topic Topic[int]
	var calls int
	var self *Subscription[int]
	self = topic.Subscribe(func(int) {
		calls++
		self.Close()
		topic.Subscribe(func(int) { calls += 10 })
	})
	topic.Publish(1)
	topic.Publish(2)
	if calls != 11 {
		t.Errorf("calls %d, want 11", calls)
	}
}

// A slow listener is only logged: it still gets the event
func TestTopicSlow(t *testing.T) {
	var topic Topic[int]
	var got int
	topic.Subscribe(func(n int) { time.Sleep(SlowListener + time.Millisecond); got = n })
	topic.Publish(3)
	if got != 3 {
		t.Errorf("got %d", got)
	}
}
