# go-pub-sub

[![Go Reference](https://pkg.go.dev/badge/github.com/eggs-gd/go-pub-sub.svg)](https://pkg.go.dev/github.com/eggs-gd/go-pub-sub)

**Events and commands** for Go: a topic of events with listeners, and commands —
work run by an executor of yours (one goroutine with a transaction, a worker pool),
with results routed back to whoever asked. Typed with generics, standard library
only.

```
go get github.com/eggs-gd/go-pub-sub
```

```go
count := pubsub.New(store, pubsub.Frame, func(data map[string]int, word string) (int, error) {
	data[word]++
	return data[word], nil
})

n, err := count.Do("go")         // submit, wait for your own result
words := count.Client(8)         // your own results, at most 8 in flight or unread
words.Submit("chain")
words.Close()
for r := range words.Results() { // r.ID, r.Value, r.Err
	…
}
count.Done().Subscribe(func(r pubsub.Result[int]) { … }) // every result, as an event
```

The API, its rules and the layers: [the package documentation](https://pkg.go.dev/github.com/eggs-gd/go-pub-sub).

## Why

Some work has to happen in one place: a database with one writer (SQLite), a
device, a rate-limited client. Funnelled through one goroutine it never conflicts,
and that goroutine can batch whatever is queued — one transaction for many writes.
Callers should not care: they submit and go on, or wait when they need the answer.
A plain channel of requests gives the first half; this package adds the second —
typed results routed back to their caller, and the end of the work as an event for
anyone else.

## What it is not

The package is a mechanism. How a program handles an event — wakes a worker,
queues by key, fans out to a pool — is the program's decision. Left out on purpose:

- **No queue or goroutine per subscription**: a listener is a function called
  right there; a listener that needs a queue makes its own (it knows its keys,
  its limits).
- **No delivery policies, no buffers in the API**: for the publisher an event is a
  black hole; for a command, room is counted by the client that submits.
- **No workqueue**: rate limits, retries, dedup by key are a few lines in the
  program — see the patterns.

## Patterns

Each one an example, run as a test:

- **Wake-up** ([`Example_wakeUp`](example_test.go)): the event is a hint, the data
  is the truth. The listener only marks "there is news" (a channel of one: many
  events, one wake-up); the worker wakes, reads what there is to do now, and does
  it. An event lost or merged costs nothing — the next read sees the state.
- **Workers** ([`Example_workers`](example_test.go)): heavy work on an executor
  that is a pool of n goroutines; the producer is held back by the pool and its
  client's window, not by a queue that grows.
- **Transit** ([`Example`](example_test.go)): a stream through a client — submit
  as the window allows, read results as they come.
- **The bridge** ([`Example_done`](example_test.go)): reacting to every result of
  an Op without submitting any.
- **Keyed queue**: a listener puts a key into a map under a lock and wakes a
  worker; the latest entry per key wins.

## Tests

`go test -race ./...`: a test file per layer, next to its code, and the examples.

## License

MIT
