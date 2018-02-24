package main

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/jmalloc/gospel/src/gospel"
	"github.com/jmalloc/gospel/src/mariadb"
)

func main() {
	c, err := mariadb.OpenEnv()
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
	for {
		_, err := es.AppendUnchecked(
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

		counter++
		time.Sleep(500 * time.Millisecond)
	}
}
