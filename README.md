# go-pub-sub

[![Go Reference](https://pkg.go.dev/badge/github.com/eggs-gd/go-pub-sub.svg)](https://pkg.go.dev/github.com/eggs-gd/go-pub-sub)

**Events and commands** for Go, in layers: a topic of events with listeners, and
commands — work run by an executor of yours (one goroutine with a transaction, a
worker pool), with results routed back to whoever asked. Typed with generics,
standard library only.

```
go get github.com/eggs-gd/go-pub-sub
```

The package is a mechanism. How a program handles an event — wakes a worker,
queues by key, fans out to a pool — is the program's decision; the
[examples](example_test.go) show the usual ones.

## The layers

| layer | file | what |
|---|---|---|
| event | [`event.go`](event.go) | `Topic[E]`: `Publish(e)`, `Subscribe(fn) *Subscription` |
| command | [`command.go`](command.go), [`client.go`](client.go) | `Op[T, A, R]` run by an `Executor[T]`; a caller sees a `Command[A, R]`: `Submit`, `Do`, `Client`, `Done` |
| the bridge | [`command.go`](command.go) | `Op.Done()`: a `Topic` of the Op's results |
| shapes | [`shapes.go`](shapes.go) | `Message`, `Signal`, `Trigger`: an Op without an argument or a result |
| executor | [`executor.go`](executor.go) | `Executor[T]`, the sealed `Job[T]`, `Class`, `Inline` |

The private side — the job, the shapes' views, a listener's call — is in
[`internal.go`](internal.go).

## Events

```go
var published pubsub.Topic[ItemPublished]
sub := published.Subscribe(func(e ItemPublished) { … })
published.Publish(ItemPublished{GUID: guid})
sub.Close()
```

- **For the publisher a black hole**: nobody listening — nothing happens; no
  buffer, no capacity, nothing to tune.
- **Listening, each listener is called**, in the order they subscribed, right there
  on the publisher's goroutine — the dispatch of every UI engine. So **a listener is
  thin**: it hands the work off — to its channel, its queue, its workers — and
  returns.
- A listener that panics is recovered and logged (`log/slog`); one that takes
  longer than `SlowListener` is logged: a broken contract is seen, not guessed.
- Subscribing or closing inside a listener is fine: a publish in progress keeps its
  own list.

## Commands

```go
count := pubsub.New(store, pubsub.Frame, func(data map[string]int, word string) (int, error) {
	data[word]++
	return data[word], nil
})
var counter pubsub.Command[string, int] = count // what a caller sees

n, err := counter.Do("go")   // submit, wait for your own result
id := counter.Submit("go")   // fire and forget: an ID once queued

words := counter.Client(8)   // your own results, at most 8 in flight or unread
go func() {
	for _, w := range input {
		words.Submit(w)      // waits while 8 are in flight or unread
	}
	words.Close()
}()
for r := range words.Results() { … } // r.ID, r.Value, r.Err
```

| | |
|---|---|
| `Op[T, A, R]` | one kind of operation: its function `fn(env T, arg A) (R, error)`, its class, its executor |
| `Command[A, R]` | an operation as a caller sees it — no env, no executor |
| `Client[A, R]` | a caller's own line to an Op: `Submit`, `TrySubmit`, `Results()`, `Close` |
| `Executor[T]` | yours: `Enqueue(Job[T])` queues the job and returns; later it runs `job.Run(env)` where it likes, then `job.Done(err)` |
| `Job[T]` | a submitted operation as the executor sees it: `Class()`, `Run(env)`, `Done(err)`; sealed |
| `Result[R]` | `ID`, `Value`, `Err` |
| `Inline[T]` | an executor that runs a job right there, Submit returning after it — tests, nothing to batch |

- **Submit never waits for the work**: the ID comes back once the job is queued.
  It may wait for **room** — the executor's (a bounded queue: backpressure) or a
  client's (its window). `Enqueue` must not run the work; `Inline` is the one
  exception, by design.
- **Three ways to get a result**, chosen by the caller: `Do` waits for its own;
  a `Client` reads back only its own — no one else's traffic, nothing of its own
  lost; `Submit` forgets it.
- **A client counts its room when it submits**, not when the result arrives: a
  result in flight or unread holds one place of the window, reading it frees it. So
  delivery to a client never waits, and **the executor never waits for a caller**.
  `TrySubmit` says there is no room instead of waiting. After `Close`, `Results`
  ends once the results in flight are read.
- **A result is delivered when the executor is done with the job** — for a
  database, after the commit — so whatever its receiver does next sees it. If the
  executor fails the job (`Done(err)`: a failed commit), a good result becomes that
  error; an operation's own error stays its own.
- **Order**: a client gets its results in the order the executor finished them; one
  FIFO executor means submit order.

### The bridge

A finished command is an event: `op.Done()` is a `Topic[Result[R]]` of every
result of the Op — whoever submitted it, `Do`'s and clients' too. Listen to it to
react to the work without submitting any; it is called on the executor's goroutine,
so the listener is as thin as any.

## Shapes

Every `Op` takes one argument and gives one result — Go's generics have no variadic
type parameters. Several arguments or results travel as one message (a struct); when
there is none, `None` (`struct{}`) stands in. So that `None` never reaches a caller,
an `Op` is handed out in the view of its shape:

| argument | result | view | calls |
|---|---|---|---|
| yes | yes | `Command[A, R]` (the `Op` itself) | `Submit(a)`, `Do(a) (R, error)` |
| yes | no | `Message[A]` — `MessageOf(op)` | `Submit(a)`, `Do(a) error` |
| no | yes | `Signal[R]` — `SignalOf(op)` | `Submit()`, `Do() (R, error)` |
| no | no | `Trigger` — `TriggerOf(op)` | `Submit()`, `Do() error` |

Each view has its `Client` (a `SignalClient` for the shapes without an argument:
`Submit()`) and its `Done`. Without a result a listener or a client still gets one
`Result` per operation — its `ID` and its `Err` (nil: done): the end of the work is
the news.

## Who may do what

An `Op` is made with the executor. Keep the executor to yourself and you decide
which operations exist; give others `Command`s. `Job` is sealed — only an `Op`
makes one — so an executor runs nothing but the operations made with it: no caller
can slip it "drop every table". Within one process this is design discipline, not a
security boundary: code in the process can reach the resource directly.

## Classes

A class is the `Op`'s, not the caller's: how long the operation may wait while the
executor gathers a batch.

| class | meant for |
|---|---|
| `Now` | someone is waiting to see it: never held back, taken first |
| `Frame` | regular work: may wait a frame (~33 ms) or until a batch is full |
| `Idle` | housekeeping: waits longer, only while nothing else does |

The package only carries the class; the executor decides what it means. A deadline
only delays a result — throughput comes from many operations in flight (a
`Client`), so do not wait on a deadline in a synchronous loop (`Do` per item).

## Patterns

Not in the API — a program picks its own. The [examples](example_test.go):

- **Wake-up** (`Example_wakeUp`): the event is a hint, the data is the truth. The
  listener only marks "there is news" (a channel of one: many events, one wake-up);
  the worker wakes, reads what there is to do now, and does it. An event lost or
  merged costs nothing — the next read sees the state.
- **Workers** (`Example_workers`): heavy work on an executor that is a pool of n
  goroutines; the producer is held back by the pool and its client's window, not by
  a queue that grows.
- **Transit** (`Example`): a stream through a client — submit as the window allows,
  read results as they come.
- **The bridge** (`Example_done`): reacting to every result of an Op.

A keyed queue (one pending entry per key, the latest wins) is a listener putting a
key into a map under a lock and waking a worker — a few lines, and the program
knows its keys better than a library would.

## Tests

`go test -race ./...`, a file per layer next to its code, plus the examples.

## License

MIT
