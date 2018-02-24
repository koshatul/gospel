package driver_test

import (
	. "github.com/jmalloc/streakdb/src/driver"
	"github.com/jmalloc/streakdb/src/streakdb"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("NewReaderOptions", func() {
	It("applies the provided options", func() {
		opts := NewReaderOptions(
			[]ReaderOption{
				streakdb.FilterByEventType("foo"),
			},
		)

		Expect(opts.FilterByEventType).To(BeTrue())
	})
})

var _ = Describe("ReaderOptions", func() {
	Describe("Get", func() {
		It("fetches a non-standard option by its key", func() {
			opts := &ReaderOptions{}
			opts.Set("foo", "bar")

			v, ok := opts.Get("foo")
			Expect(v).To(Equal("bar"))
			Expect(ok).To(BeTrue())
		})

		It("includes type when matching key values", func() {
			type keyType1 string
			type keyType2 string

			opts := &ReaderOptions{}
			opts.Set(keyType1("foo"), "bar")
			opts.Set(keyType2("foo"), "qux")

			v, ok := opts.Get(keyType1("foo"))
			Expect(v).To(Equal("bar"))
			Expect(ok).To(BeTrue())
		})
	})

	Describe("Set", func() {
		It("associates a non-standard option with a value", func() {
			opts := &ReaderOptions{}
			opts.Set("foo", "bar")

			v, ok := opts.Get("foo")
			Expect(v).To(Equal("bar"))
			Expect(ok).To(BeTrue())
		})
	})
})
