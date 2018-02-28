package main

import (
	"context"
	"log"
	"os"

	"github.com/jmalloc/gospel/examples"
	"github.com/jmalloc/gospel/src/gospel"
	"github.com/jmalloc/gospel/src/gospelmaria"
	"github.com/jmalloc/twelf/src/twelf"
)

// This example shows how to use a Reader to consume facts from a stream.
//
// The reader used in this example (MariaDB) uses adaptive rate limiting to
// minimise database polls. It can be seen adapting to rate changes from the
// 'append' and 'append_unchecked' examples.
//
// If the GOSPEL_EXAMPLES_RATELIMIT variable is set to "true", the read rate is
// also randimized to simulate a slow consumer.

func main() {
	// Open a connection to MariaDB using the GOSPEL_MARIADB_DSN environment
	// variable for the DSN.
	c, err := gospelmaria.OpenEnv(
		gospel.Logger(
			&twelf.StandardLogger{
				// Enable debug logging so we can see the adaptive rate limiting
				// in effect.
				CaptureDebug: true,
			},
		),
	)
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

	// Open a reader at the beginning of "example-stream".
	r, err := es.Open(ctx, gospel.Address{
		Stream: "example-stream",
		Offset: 0,
	})
	if err != nil {
		log.Fatal(err)
	}

	for {
		if os.Getenv("GOSPEL_EXAMPLES_RATELIMIT") == "true" {
			examples.RateLimit(ctx)
		}

		_, err := r.Next(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}
}
