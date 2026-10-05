package pubsub

import (
	"errors"
	"testing"
)

// Each shape: Do gives what the operation gives; a subscriber gets one result per
// operation, its ID and its error even when there is no value
func TestShapes(t *testing.T) {
	var log []string
	save := MessageOf(New(Inline[string]{}, Frame, func(_ string, s string) (None, error) {
		if s == "" {
			return None{}, errors.New("empty")
		}
		log = append(log, s)
		return None{}, nil
	}))
	count := SignalOf(New(Inline[string]{}, Frame, func(string, None) (int, error) { return len(log), nil }))
	reset := TriggerOf(New(Inline[string]{}, Idle, func(string, None) (None, error) { log = nil; return None{}, nil }))

	saved := save.Subscribe(4)
	if err := save.Do("a"); err != nil {
		t.Fatal(err)
	}
	id := save.Submit("")
	if r := <-saved.C; r.Err != nil { // Do's
		t.Errorf("Do's result: %v", r.Err)
	}
	if r := <-saved.C; r.ID != id || r.Err == nil {
		t.Errorf("a failed message: %+v", r)
	}
	if n, err := count.Do(); n != 1 || err != nil {
		t.Errorf("count: %d, %v", n, err)
	}
	resets := reset.Subscribe(1)
	rid := reset.Submit()
	if r := <-resets.C; r.ID != rid || r.Err != nil || len(log) != 0 {
		t.Errorf("trigger: %+v, log %v", r, log)
	}
}
