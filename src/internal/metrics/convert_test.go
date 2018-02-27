package metrics_test

import (
	"time"

	. "github.com/jmalloc/gospel/src/internal/metrics"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RateToDuration", func() {
	It("returns the inverse of the rate", func() {
		d := RateToDuration(125.0)

		Expect(d).To(Equal(8 * time.Millisecond))
	})
})
