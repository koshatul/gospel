// +build !without_mariadb,!without_examples

package mariadb_test

import (
	"context"
	"fmt"
	"time"

	"github.com/jmalloc/gospel/src/driver/mariadb"
	"github.com/jmalloc/gospel/src/gospel"
)

// This example illustrates how to append and read events to/from an event store.
func Example_appendAndRead() {
	//
	// Begin MariaDB specific bootstrapping code.
	//

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Open a MariaDB client.
	c, err := mariadb.OpenEnv()
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
			Stream: "my-stream",
			Offset: 0,
		},
		gospel.Event{EventType: "event-1"},
		gospel.Event{EventType: "event-2"},
		gospel.Event{EventType: "event-3"},
	)
	if err != nil {
		panic(err)
	}

	// Open a new reader that starts reading from the beginning of "my-stream".
	r, err := es.Open(ctx, gospel.Address{
		Stream: "my-stream",
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
		if err == context.DeadlineExceeded {
			return
		} else if err != nil {
			panic(err)
		}

		fact := r.Get()
		fmt.Println(fact.Event.EventType)
	}

	// Output:
	// event-1
	// event-2
	// event-3
}
