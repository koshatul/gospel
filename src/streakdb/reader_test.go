package streakdb_test

import (
	"github.com/jmalloc/streakdb/src/driver"
	. "github.com/jmalloc/streakdb/src/streakdb"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("FilterByEventType", func() {
	It("returns an option that sets the event types", func() {
		opts := &driver.ReaderOptions{}

		FilterByEventType("foo", "bar")(opts)

		Expect(opts.EventTypes).To(Equal(
			map[string]struct{}{
				"foo": {},
				"bar": {},
			},
		))
	})

	It("is additive", func() {
		opts := &driver.ReaderOptions{}

		FilterByEventType("foo", "bar")(opts)
		FilterByEventType("baz", "qux")(opts)

		Expect(opts.EventTypes).To(Equal(
			map[string]struct{}{
				"foo": {},
				"bar": {},
				"baz": {},
				"qux": {},
			},
		))
	})
})
