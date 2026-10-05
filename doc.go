// Package pubsub: events and commands, in layers.
//
//   - Event (event.go): a Topic[E] — Publish an event, Subscribe a listener. For
//     the publisher a black hole: nobody listening, nothing happens; listening,
//     each listener is called, in order, right there. Listeners are thin: they
//     hand work off and return.
//   - Command (command.go, client.go): an Op — work run by an Executor of yours
//     (one goroutine with a database transaction, a pool, anything). Do waits for
//     its own result; a Client gets only its own results and counts its room when
//     it submits; Submit without a client — fire and forget.
//   - The bridge: a command that finished is an event — Op.Done is a Topic of its
//     results, for anyone to listen to without submitting.
//   - Shapes (shapes.go): an Op takes one argument and gives one result; Message,
//     Signal and Trigger hand out an Op without an argument or a result, so None
//     never reaches a caller.
//
// The package is a mechanism; how to handle an event (a wake-up, a keyed queue,
// workers) is the program's decision — see the examples.
package pubsub
