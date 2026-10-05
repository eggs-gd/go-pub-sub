package pubsub

import (
	"errors"
	"testing"
)

// Each shape: Do gives what the operation gives; a listener and a client get one
// result per operation, its ID and its error even when there is no value
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

	var saved []Result[None]
	save.Done().Subscribe(func(r Result[None]) { saved = append(saved, r) })
	if err := save.Do("a"); err != nil {
		t.Fatal(err)
	}
	id := save.Submit("")
	if len(saved) != 2 || saved[0].Err != nil || saved[1].ID != id || saved[1].Err == nil {
		t.Errorf("message results: %+v", saved)
	}
	counts := count.Client(1)
	counts.Submit()
	counts.Close()
	for r := range counts.Results() {
		if r.Value != 1 || r.Err != nil {
			t.Errorf("count: %+v", r)
		}
	}
	resets := reset.Client(1)
	rid := resets.Submit()
	resets.Close()
	for r := range resets.Results() {
		if r.ID != rid || r.Err != nil || len(log) != 0 {
			t.Errorf("trigger: %+v, log %v", r, log)
		}
	}
}
