package pubsub_test

import (
	"fmt"
	"strings"

	pubsub "github.com/eggs-gd/go-pub-sub"
)

// store: an executor — one goroutine owns the data (its env); it takes whatever is
// queued as one batch, runs it, then ends every job of the batch
type store struct {
	jobs chan pubsub.Job[map[string]int]
	data map[string]int
	done chan struct{}
}

func newStore() *store {
	s := &store{jobs: make(chan pubsub.Job[map[string]int], 64), data: map[string]int{}, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		for j := range s.jobs {
			batch := []pubsub.Job[map[string]int]{j}
			for len(s.jobs) > 0 {
				batch = append(batch, <-s.jobs)
			}
			for _, b := range batch {
				b.Run(s.data)
			}
			for _, b := range batch { // "committed": the results go out
				b.Done(nil)
			}
		}
	}()
	return s
}

func (s *store) Enqueue(j pubsub.Job[map[string]int]) { s.jobs <- j }
func (s *store) close()                               { close(s.jobs); <-s.done }

// Operations are made with the executor; callers get them as Topics: submit and
// pick your own results by ID, or Do and wait.
func Example() {
	s := newStore()
	count := pubsub.New(s, pubsub.Frame, func(data map[string]int, word string) (int, error) {
		data[strings.ToLower(word)]++
		return data[strings.ToLower(word)], nil
	})
	var counter pubsub.Topic[string, int] = count // what a caller sees

	results := counter.Subscribe(16)
	mine := map[pubsub.ID]string{}
	for _, w := range []string{"Go", "chain", "go"} {
		mine[counter.Submit(w)] = w // an ID at once; the work happens elsewhere
	}
	n, _ := counter.Do("GO") // or wait for one result
	s.close()
	results.Close()

	for r := range results.C {
		if w, ok := mine[r.ID]; ok {
			fmt.Println(w, r.Value)
		}
	}
	fmt.Println("GO", n)
	// Output:
	// Go 1
	// chain 1
	// go 2
	// GO 3
}
