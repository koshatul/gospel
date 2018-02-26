package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/jmalloc/gospel/src/gospel"
	"github.com/jmalloc/gospel/src/mariadb"
	"github.com/jmalloc/twelf/src/twelf"
	"golang.org/x/time/rate"
)

func main() {
	c, err := mariadb.OpenEnv(
		gospel.Logger(
			&twelf.StandardLogger{
				Target: log.New(ioutil.Discard, "", 0),
				// CaptureDebug: true,
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
	limiter := rate.NewLimiter(
		rate.Limit(90),
		// rate.Inf,
		1,
	)

	lim := rate.Limit(5 + rand.Intn(95))
	limiter.SetLimit(lim)
	fmt.Printf("new rate is %0.02f/s\n", lim)

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

			if err = limiter.Wait(ctx); err != nil {
				log.Fatal(err)
			}

			if rand.Intn(300) == 0 {
				lim := rate.Limit(5 + rand.Intn(1995))
				limiter.SetLimit(lim)
				fmt.Printf("new rate is %0.02f/s\n", (lim))
			}

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
