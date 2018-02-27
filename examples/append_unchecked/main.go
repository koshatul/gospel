package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jmalloc/gospel/examples"
	"github.com/jmalloc/gospel/src/gospel"
	"github.com/jmalloc/gospel/src/gospelmaria"
)

// This example shows how to use EventStore.AppendUnchecked() to append events
// without using optimistic concurrency control.
//
// The example also mimics load spikes using a rate limiter with a randomized
// limit.

func main() {
	// Open a connection to MariaDB using the GOSPEL_MARIADB_DSN environment
	// variable for the DSN.
	c, err := gospelmaria.OpenEnv()
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// Create a context and cancel it if we receive an interrupt (CTRL-C) signal.
	ctx, cancel := examples.WithCancelOnInterrupt(context.Background())
	defer cancel()

	// Open the "example-store" event store. All of the examples in this
	// directory use the same store and stream so that they can be used together.
	es, err := c.OpenStore(ctx, "example-store")
	if err != nil {
		log.Fatal(err)
	}

	var counter uint64

	for {
		examples.RateLimit(ctx)

		_, err := es.AppendUnchecked(
			ctx,
			"example-stream",
			gospel.Event{
				EventType:   "append-unchecked-example",
				ContentType: "text/plain",
				Body: []byte(fmt.Sprintf(
					"pid %d, event #%d",
					os.Getpid(),
					counter,
				)),
			},
		)
		if err != nil {
			log.Fatal(err)
		}
		counter++
	}
}
