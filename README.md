# go-pub-sub

[![Go Reference](https://pkg.go.dev/badge/github.com/eggs-gd/go-pub-sub.svg)](https://pkg.go.dev/github.com/eggs-gd/go-pub-sub)

**Operations as topics** for Go: a dispatcher where a caller submits work and gets an
ID as soon as it is queued, an executor of your own runs the work where and how it likes (one
goroutine, a transaction, a batch), and the result is published to every subscriber
of that kind of operation — each picks its own by ID. Typed with generics, standard
library only.

```
go get github.com/eggs-gd/go-pub-sub
```

```go
count := pubsub.New(store, pubsub.Frame, func(data map[string]int, word string) (int, error) {
	data[word]++
	return data[word], nil
})

results := count.Subscribe(16)    // every result of this operation
id := count.Submit("go")          // queued: an ID, not waiting for the work
n, err := count.Do("chain")       // or submit and wait for your own

for r := range results.C {        // r.ID, r.Value, r.Err
	if r.ID == id { … }
}
```

The whole example, with an executor that batches: [`example_test.go`](example_test.go).

## Why

Some work has to happen in one place: a database with one writer (SQLite), a
device, a rate-limited client. Funnelled through one goroutine it never conflicts,
and that goroutine can batch whatever is queued — one transaction for many writes.
Callers should not care: they submit and go on, or wait when they need the answer.
A plain channel of requests gives the first half; this package adds the second —
typed results routed back, many callers on one kind of operation, never blocking
the executor.

## The pieces

| | |
|---|---|
| `Op[T, A, R]` | one kind of operation: its function `fn(env T, arg A) (R, error)`, its class, its executor, its subscribers |
| `Topic[A, R]` | an operation as a caller sees it: `Submit(A) ID`, `Subscribe(n) *Sub[R]`, `Do(A) (R, error)` — no env, no executor |
| `Executor[T]` | yours: `Enqueue(Job[T])` queues the job and returns (it may wait for room: backpressure; it must not run the work); later it runs `job.Run(env)` where it likes, then `job.Done(err)` |
| `Job[T]` | a submitted operation as the executor sees it: `Class()`, `Run(env)`, `Done(err)`; sealed |
| `Result[R]` | `ID`, `Value`, `Err` |
| `Class` | `Now`, `Frame`, `Idle`: how long an operation tolerates waiting while the executor gathers a batch |
| `None`, `Message[A]`, `Signal[R]`, `Trigger` | the shapes without an argument or a result — see "Shapes" |
| `Inline[T]` | an executor that runs a job right there, Submit returning after it — tests, nothing to batch |

`T` — the executor's env (a `*sql.Tx`, a connection, a client) — is seen only by
whoever makes the `Op` and by the executor. Hand callers a `Topic[A, R]`: they can
submit an argument for an operation that exists, nothing more.

## Shapes

Every `Op` takes one argument and gives one result — Go's generics have no variadic
type parameters. Several arguments or results travel as one message (a struct); when
there is none, `None` (`struct{}`) stands in. So that `None` never reaches a caller,
an `Op` is handed out in the view of its shape:

| argument | result | view | calls |
|---|---|---|---|
| yes | yes | `Topic[A, R]` (the `Op` itself) | `Submit(a)`, `Do(a) (R, error)` |
| yes | no | `Message[A]` — `MessageOf(op)` | `Submit(a)`, `Do(a) error` |
| no | yes | `Signal[R]` — `SignalOf(op)` | `Submit()`, `Do() (R, error)` |
| no | no | `Trigger` — `TriggerOf(op)` | `Submit()`, `Do() error` |

Without a result a subscriber still gets one `Result` per operation — its `ID` and
its `Err` (nil: done): the end of the work is the news.

```go
del := pubsub.New(store, pubsub.Frame, func(db *sql.Tx, id int) (pubsub.None, error) { … })
deleter := pubsub.MessageOf(del) // Message[int]: Submit(id), Do(id) error
```

## Rules

- **Submit never waits for the work**: the ID comes back once the job is queued.
  It may wait for **room**: an executor with a bounded queue makes a fast caller
  wait while it is full (backpressure — an unbounded queue would only pile up
  memory). The executor's `Enqueue` must not run the work itself; `Inline` is the
  one exception, by design.
- **Every result goes to every subscriber** of its `Op` — `Do`'s too; a subscriber
  keeps the IDs it submitted and picks its own.
- **Delivery never blocks the executor.** A subscriber whose buffer is full misses
  the result; `Dropped()` counts what it missed. A subscription nobody reads is its
  owner's loss, not everyone's stall.
- **A result is delivered when the executor is done with the job** — for a database,
  after the commit — so whatever its receiver does next sees it. If the executor
  fails the job (`Done(err)`: a failed commit), a good result becomes that error; an
  operation's own error stays its own.
- **Order**: a subscriber gets results in the order the executor finished them; one
  FIFO executor means submit order. Across subscribers nothing is ordered.
- **`Close`** ends a subscription: no more results, `C` closes (what it buffered can
  still be read).

## Who may do what

An `Op` is made with the executor. Keep the executor to yourself and you decide
which operations exist; give others `Topic`s. `Job` is sealed — only an `Op` makes
one — so an executor runs nothing but the operations made with it: no caller can
slip it "drop every table". Within one process this is design discipline, not a
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
only delays a result — throughput comes from many operations in flight, so do not
wait on a deadline in a synchronous loop (`Do` per item, one by one).

## Tests

`go test -race ./...`: results in submit order, two callers picking their own by
ID, an operation's error and a failed `Done`, `Do`, a full buffer dropping without
blocking, `Close`, ten callers at once (each result exactly once); the example.

## License

MIT
