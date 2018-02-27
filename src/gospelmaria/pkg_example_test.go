// +build !without_mariadb,!without_examples

package gospelmaria_test

import (
	"context"
	"fmt"
	"time"

	"github.com/jmalloc/gospel/src/gospel"
	"github.com/jmalloc/gospel/src/gospelmaria"
)

// This example illustrates how to append and read events to/from an event store.
func Example_appendAndRead() {
	//
	// Begin MariaDB specific bootstrapping code.
	//

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Open a MariaDB client.
	c, err := gospelmaria.OpenEnv()
	if err != nil {
		panic(err)
	}
	defer c.Close()

	// Use the "examples" store for code examples.
	es, err := c.OpenStore(ctx, "examples")
	if err != nil {
		panic(err)
	}

	//
	// End MariaDB specific bootstrapping code.
	//

	// Append some new events at the beginning of a new stream.
	_, err = es.Append(
		ctx,
		gospel.Address{
			Stream: "append-and-read-example",
			Offset: 0,
		},
		gospel.Event{EventType: "event-1"},
		gospel.Event{EventType: "event-2"},
		gospel.Event{EventType: "event-3"},
	)
	if err != nil {
		panic(err)
	}

	// Open a new reader that starts reading from the beginning of
	// the "append-and-read-example" stream.
	r, err := es.Open(ctx, gospel.Address{
		Stream: "append-and-read-example",
		Offset: 0,
	})
	if err != nil {
		panic(err)
	}
	defer r.Close()

	// Loop until we don't receive any events in a timely fashion.
	// Note that there is no "end of the stream".
	for {
		_, err := r.Next(ctx)
		if err != nil {
			panic(err)
		}

		fact := r.Get()
		fmt.Println(fact.Event.EventType)

		// Bail once we know we've reached the end of our test events.
		if fact.Event.EventType == "event-3" {
			return
		}
	}

	// Output:
	// event-1
	// event-2
	// event-3
}
