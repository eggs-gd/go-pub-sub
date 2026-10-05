package pubsub_test

import (
	"fmt"
	"slices"
	"strings"
	"sync"

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

// Operations are made with the executor; callers get them as Commands: Do and
// wait, or a Client — submit as many as its window, read back your own results.
func Example() {
	s := newStore()
	defer s.close()
	count := pubsub.New(s, pubsub.Frame, func(data map[string]int, word string) (int, error) {
		data[strings.ToLower(word)]++
		return data[strings.ToLower(word)], nil
	})
	var counter pubsub.Command[string, int] = count // what a caller sees

	words := counter.Client(2)
	go func() {
		for _, w := range []string{"Go", "chain", "go"} {
			words.Submit(w) // waits only while two results are unread
		}
		words.Close()
	}()
	for r := range words.Results() {
		fmt.Println(r.ID, r.Value)
	}
	n, _ := counter.Do("GO")
	fmt.Println("GO", n)
	// Output:
	// 1 1
	// 2 1
	// 3 2
	// GO 3
}

// The bridge: a finished command is an event. A listener hears every result of the
// Op — whoever submitted it — without submitting anything.
func Example_done() {
	s := newStore()
	count := pubsub.New(s, pubsub.Frame, func(data map[string]int, word string) (int, error) {
		data[word]++
		return data[word], nil
	})
	var heard []int
	count.Done().Subscribe(func(r pubsub.Result[int]) { heard = append(heard, r.Value) }) // thin: on the executor's goroutine
	count.Submit("a")                                                                    // fire and forget
	count.Do("a")
	s.close()
	fmt.Println(heard)
	// Output: [1 2]
}

// A wake-up: the event is a hint, the data is the truth. The listener only marks
// "there is news" (a channel of one: many events, one wake-up); the worker wakes,
// reads what there is to do now, and does it — an event lost or merged costs
// nothing, the next read sees the state.
func Example_wakeUp() {
	var published pubsub.Topic[string]
	var mu sync.Mutex
	todo := []string{} // the truth (a database, in a program)

	wake := make(chan struct{}, 1)
	published.Subscribe(func(string) {
		select {
		case wake <- struct{}{}:
		default: // already woken: this news is in the read to come
		}
	})

	done := make(chan []string)
	go func() {
		var rendered []string
		for range wake {
			mu.Lock()
			batch := todo // what there is to do now, not what the events said
			todo = nil
			mu.Unlock()
			rendered = append(rendered, batch...)
			if len(rendered) == 3 {
				done <- rendered
				return
			}
		}
	}()
	for _, item := range []string{"a", "b", "c"} {
		mu.Lock()
		todo = append(todo, item) // the write first
		mu.Unlock()
		published.Publish(item) // then the hint
	}
	fmt.Println(<-done)
	// Output: [a b c]
}

// pool: an executor of n workers — the work bounded by them, not by its callers
type pool struct {
	jobs chan pubsub.Job[int]
	wg   sync.WaitGroup
}

func newPool(n int) *pool {
	p := &pool{jobs: make(chan pubsub.Job[int])} // no queue: Enqueue waits for a free worker
	for worker := range n {
		p.wg.Go(func() {
			for j := range p.jobs {
				j.Run(worker)
				j.Done(nil)
			}
		})
	}
	return p
}

func (p *pool) Enqueue(j pubsub.Job[int]) { p.jobs <- j }
func (p *pool) close()                    { close(p.jobs); p.wg.Wait() }

// Workers: heavy work (a render, a transcode) on a pool of its own; the producer
// is held back by the pool's room and its client's window, not by a queue that
// grows.
func Example_workers() {
	p := newPool(3)
	defer p.close()
	render := pubsub.New(p, pubsub.Idle, func(worker int, name string) (string, error) {
		return strings.ToUpper(name), nil
	})
	renders := render.Client(4)
	go func() {
		for _, name := range []string{"a", "b", "c", "d", "e"} {
			renders.Submit(name)
		}
		renders.Close()
	}()
	var out []string
	for r := range renders.Results() {
		out = append(out, r.Value) // in the order the workers finished
	}
	slices.Sort(out)
	fmt.Println(out)
	// Output: [A B C D E]
}
