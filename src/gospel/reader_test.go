package gospel_test

import (
	. "github.com/jmalloc/gospel/src/gospel"
	"github.com/jmalloc/gospel/src/internal/options"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("FilterByEventType", func() {
	It("enables the event type filter", func() {
		opts := &options.ReaderOptions{}

		FilterByEventType("foo", "bar")(opts)

		Expect(opts.FilterByEventType).To(BeTrue())
	})

	It("sets the event types", func() {
		opts := &options.ReaderOptions{}

		FilterByEventType("foo", "bar")(opts)

		Expect(opts.EventTypes).To(Equal(
			[]string{
				"foo",
				"bar",
			},
		))
	})

	It("is additive", func() {
		opts := &options.ReaderOptions{}

		FilterByEventType("foo", "bar")(opts)
		FilterByEventType("baz", "qux")(opts)

		Expect(opts.EventTypes).To(Equal(
			[]string{
				"foo",
				"bar",
				"baz",
				"qux",
			},
		))
	})
})
