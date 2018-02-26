package main

import (
	"context"
	"log"
	"math/rand"
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

	r, err := es.Open(ctx, gospel.Address{
		Stream: "my-stream",
		Offset: 0,
	})
	if err != nil {
		log.Fatal(err)
	}

	for {
		_, err := r.Next(ctx)
		if err != nil {
			log.Fatal(err)
		}

		s := time.Duration(rand.Intn(5)) * time.Millisecond
		time.Sleep(s)
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
