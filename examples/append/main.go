package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/jmalloc/gospel/src/gospel"
	"github.com/jmalloc/gospel/src/mariadb"
	"github.com/jmalloc/twelf/src/twelf"
)

func main() {
	c, err := mariadb.OpenEnv(
		gospel.Logger(
			&twelf.StandardLogger{
				CaptureDebug: true,
			},
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()

	es, err := c.OpenStore(ctx, "example")
	if err != nil {
		log.Fatal(err)
	}

	var counter uint64
	var delay time.Duration

FindOffset:
	for {
		// Use AppendUnchecked once just to find the next offset of the stream.
		addr, err := es.AppendUnchecked(
			ctx,
			"my-stream",
			gospel.Event{
				EventType: "example-event",
				Body:      []byte(strconv.FormatUint(counter, 10)),
			},
		)
		if err != nil {
			log.Fatal(err)
		}

		// Then proceed to use a checked append.
		for {
			counter++

			if rand.Intn(100) == 0 {
				delay = time.Duration(rand.Intn(150)) * time.Millisecond

				d := time.Duration(rand.Intn(30)) * time.Second
				fmt.Println("SLEEP", d, "THEN EVERY", delay)
				time.Sleep(d)
			}

			time.Sleep(delay)

			addr, err = es.Append(
				ctx,
				addr,
				gospel.Event{
					EventType: "example-event",
					Body:      []byte(strconv.FormatUint(counter, 10)),
				},
			)

			if gospel.IsConflict(err) {
				continue FindOffset
			} else if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
