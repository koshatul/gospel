package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jmalloc/gospel/examples"
	"github.com/jmalloc/gospel/src/gospel"
	"github.com/jmalloc/gospel/src/gospelmaria"
	"github.com/sirupsen/logrus"
)

// This example shows how to use EventStore.Append() to append events using
// optimistic concurrency control.
//
// It first performs an "unchecked append" (EventStore.AppendUchecked()) to
// append to the end of the stream and discover the offset, then repeatedly
// appends events at what it thinks is the next unused offset.
//
// If a conflict is encountered (that is, some other process wrote to the
// stream), it starts the entire process again. Run the example in multiple
// processes to see conflicts occur.
//
// The example also mimics load spikes using a rate limiter with a randomized
// limit. Rate limiting can be disabled by setting the GOSPEL_EXAMPLES_RATELIMIT
// environment variable to "false".

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
	next := gospel.Address{
		Stream: "example-stream",
		Offset: 0,
	}

	// Open a reader for the example stream
	r, err := es.Open(ctx, next)
	if err != nil {
		logrus.Fatal(err)
	}

	// Read from the stream until reaching the end (catching up on what's happened)
	for {
		// if os.Getenv("GOSPEL_EXAMPLES_RATELIMIT") != "false" {
		// 	examples.RateLimit(ctx)
		// }

		last, ok, err := r.TryNext(ctx)
		if err != nil {
			log.Fatal(err)
		}
		counter++
		if !ok {
			break
		}

		next = last
	}

AppendEvent:
	for {
		// Then add a single new event append with optimistic concurrency control via Append().
		next, err = es.Append(
			ctx,
			next,
			gospel.Event{
				EventType:   "append-example",
				ContentType: "text/plain",
				Body: []byte(fmt.Sprintf(
					"pid %d, event #%d",
					os.Getpid(),
					counter,
				)),
			},
		)

		if gospel.IsConflict(err) {
			continue AppendEvent
		} else if err != nil {
			log.Fatal(err)
		}

		// Leave the retry loop
		break
	}
}
