package examples

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"golang.org/x/time/rate"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const (
	minRate          = rate.Limit(1)
	maxRate          = rate.Limit(50)
	rateChangeChance = 1.0 / 150.0
)

var rateLimiter = rate.NewLimiter(0, 1)

// RateLimit blocks until the rate-limit would not be exceeded, it also has a
// random chance to change the current rate limit.
func RateLimit(ctx context.Context) {
	if rateLimiter.Limit() == 0 || rand.Float64() <= rateChangeChance {
		rnd := rate.Limit(rand.Float64())
		limit := minRate + (rnd * (maxRate - minRate))
		rateLimiter.SetLimit(limit)
		fmt.Printf("-- RATE LIMIT CHANGED TO %0.02f/s\n", limit)
	}

	if err := rateLimiter.Wait(ctx); err != nil {
		log.Fatal(err)
	}
}
