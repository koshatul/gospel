package gospelmaria

import (
	"time"

	"github.com/jmalloc/gospel/src/internal/driver"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("read-buffer size option", func() {
	Describe("ReadBufferSize", func() {
		It("sets the buffer size", func() {
			opts := &driver.ReaderOptions{}

			ReadBufferSize(10)(opts)

			Expect(getReadBufferSize(opts)).To(
				BeNumerically("==", 10),
			)
		})

		It("caps the minimum size at 2", func() {
			opts := &driver.ReaderOptions{}
			ReadBufferSize(1)(opts)

			Expect(getReadBufferSize(opts)).To(
				BeNumerically("==", 2),
			)
		})
	})

	Describe("getReadBufferSize", func() {
		It("returns the default buffer size if none is set", func() {
			opts := &driver.ReaderOptions{}

			Expect(getReadBufferSize(opts)).To(
				BeNumerically("==", DefaultReadBufferSize),
			)
		})
	})
})

var _ = Describe("acceptable latency option", func() {
	Describe("AcceptableLatency", func() {
		It("sets the acceptable latency", func() {
			opts := &driver.ReaderOptions{}

			AcceptableLatency(10 * time.Second)(opts)

			Expect(getAcceptableLatency(opts)).To(
				Equal(10 * time.Second),
			)
		})

		It("caps the minimum at zero", func() {
			opts := &driver.ReaderOptions{}

			AcceptableLatency(-time.Second)(opts)

			Expect(getAcceptableLatency(opts)).To(
				Equal(0 * time.Second),
			)
		})
	})

	Describe("getAcceptableLatency", func() {
		It("returns the default latency if none is set", func() {
			opts := &driver.ReaderOptions{}

			Expect(getAcceptableLatency(opts)).To(
				Equal(DefaultAcceptableLatency),
			)
		})
	})
})

var _ = Describe("starvation latency option", func() {
	Describe("StarvationLatency", func() {
		It("sets the acceptable latency", func() {
			opts := &driver.ReaderOptions{}

			StarvationLatency(10 * time.Second)(opts)

			Expect(getStarvationLatency(opts)).To(
				Equal(10 * time.Second),
			)
		})

		It("caps the minimum at zero", func() {
			opts := &driver.ReaderOptions{}
			AcceptableLatency(0)(opts) // this is necessary for getStarvationLatency to allow 0

			StarvationLatency(-time.Second)(opts)

			Expect(getStarvationLatency(opts)).To(
				Equal(0 * time.Second),
			)
		})
	})

	Describe("getStarvationLatency", func() {
		It("returns the default latency if none is set", func() {
			opts := &driver.ReaderOptions{}

			Expect(getStarvationLatency(opts)).To(
				Equal(DefaultAcceptableLatency * StarvationLatencyFactor),
			)
		})

		It("computes the latency from the acceptable latency if it is set", func() {
			opts := &driver.ReaderOptions{}
			AcceptableLatency(10 * time.Second)(opts)

			Expect(getStarvationLatency(opts)).To(
				Equal(10 * time.Second * StarvationLatencyFactor),
			)
		})
	})
})
