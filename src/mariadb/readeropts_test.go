package mariadb_test

import (
	"github.com/jmalloc/gospel/src/internal/driver"
	. "github.com/jmalloc/gospel/src/mariadb"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ReadBufferSize", func() {
	It("sets the buffer size", func() {
		opts := &driver.ReaderOptions{}

		ReadBufferSize(10)(opts)

		Expect(GetReadBufferSize(opts)).To(BeNumerically("==", 10))
	})

	It("caps the minimum size at 2", func() {
		opts := &driver.ReaderOptions{}

		ReadBufferSize(1)(opts)

		Expect(GetReadBufferSize(opts)).To(BeNumerically("==", 2))
	})
})

var _ = Describe("GetReadBufferSize", func() {
	It("returns the default buffer size if none is set", func() {
		opts := &driver.ReaderOptions{}

		Expect(GetReadBufferSize(opts)).To(BeNumerically("==", DefaultReadBuffer))
	})
})
