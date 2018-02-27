package metrics_test

import (
	"time"

	"github.com/VividCortex/ewma"
	. "github.com/jmalloc/gospel/src/internal/metrics"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RateCounter", func() {
	var rateCounter *RateCounter

	BeforeEach(func() {
		rateCounter = NewRateCounter()
	})

	Describe("Rate", func() {
		It("returns zero before the warmup samples have been received", func() {
			for i := uint8(0); i < ewma.WARMUP_SAMPLES-1; i++ {
				rateCounter.Tick()
				Expect(rateCounter.Rate()).To(Equal(0.0))
			}
		})

		It("returns the average number of calls to Tick() per second", func() {
			n := 100
			sleep := 10 * time.Millisecond

			for i := 0; i < n; i++ {
				rateCounter.Tick()
				time.Sleep(sleep)
			}

			Expect(1 / rateCounter.Rate()).To(
				BeNumerically("~", sleep.Seconds(), 0.005),
			)
		})
	})
})
