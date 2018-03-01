package metrics

import (
	"math"
	"sync"
	"time"

	"github.com/VividCortex/ewma"
)

// RateCounter keeps track of the the average rate of some event based on the
// average of the interval between calls to c.Tick().
type RateCounter struct {
	m       sync.Mutex
	avg     ewma.MovingAverage
	prev    time.Time
	samples uint8
}

// NewRateCounter returns a new rate counter.
func NewRateCounter() *RateCounter {
	return &RateCounter{
		avg:  ewma.NewMovingAverage(30), // TODO: whut?
		prev: time.Now(),
	}
}

// Tick records the occurrence of an event.
func (c *RateCounter) Tick() {
	c.m.Lock()
	defer c.m.Unlock()

	now := time.Now()
	delta := now.Sub(c.prev)
	c.prev = now

	c.avg.Add(delta.Seconds())

	if c.samples < ewma.WARMUP_SAMPLES {
		c.samples++
	}
}

// Rate returns the average number of calls to Tick() per second.
func (c *RateCounter) Rate() float64 {
	c.m.Lock()
	defer c.m.Unlock()

	if c.samples < ewma.WARMUP_SAMPLES {
		return 0
	}

	seconds := c.avg.Value()
	if seconds == 0 {
		return math.MaxFloat64
	}

	return 1.0 / seconds
}
