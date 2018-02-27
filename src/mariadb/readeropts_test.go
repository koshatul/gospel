package mariadb

import (
	"time"

	"github.com/jmalloc/gospel/src/internal/driver"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ReadBufferSize", func() {
	It("sets the buffer size", func() {
		opts := &driver.ReaderOptions{}

		ReadBufferSize(10)(opts)

		Expect(getReadBufferSize(opts)).To(BeNumerically("==", 10))
	})

	It("caps the minimum size at 2", func() {
		opts := &driver.ReaderOptions{}

		ReadBufferSize(1)(opts)

		Expect(getReadBufferSize(opts)).To(BeNumerically("==", 2))
	})
})

var _ = Describe("getReadBufferSize", func() {
	It("returns the default buffer size if none is set", func() {
		opts := &driver.ReaderOptions{}

		Expect(getReadBufferSize(opts)).To(BeNumerically("==", DefaultReadBuffer))
	})
})

var _ = Describe("AcceptableLatency", func() {
	It("sets the acceptable latency", func() {
		opts := &driver.ReaderOptions{}

		AcceptableLatency(10 * time.Second)(opts)

		Expect(getAcceptableLatency(opts)).To(Equal(10 * time.Second))
	})

	It("caps the minimum at zero", func() {
		opts := &driver.ReaderOptions{}

		AcceptableLatency(-time.Second)(opts)

		Expect(getAcceptableLatency(opts)).To(Equal(0 * time.Second))
	})
})

var _ = Describe("AcceptableLatency", func() {
	It("returns the default latency if none is set", func() {
		opts := &driver.ReaderOptions{}

		Expect(getAcceptableLatency(opts)).To(Equal(DefaultAcceptableLatency))
	})
})
