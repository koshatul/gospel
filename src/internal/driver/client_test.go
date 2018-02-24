package driver_test

import (
	. "github.com/jmalloc/gospel/src/internal/driver"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("NewClientOptions", func() {
	XIt("applies the provided options", func() {
		// opts := NewClientOptions(
		// 	[]ClientOption{
		// 		gospel.FilterByEventType("foo"),
		// 	},
		// )
		//
		// Expect(opts.FilterByEventType).To(BeTrue())
	})
})

var _ = Describe("ClientOptions", func() {
	Describe("Get", func() {
		It("fetches a non-standard option by its key", func() {
			opts := &ClientOptions{}
			opts.Set("foo", "bar")

			v, ok := opts.Get("foo")
			Expect(v).To(Equal("bar"))
			Expect(ok).To(BeTrue())
		})

		It("includes type when matching key values", func() {
			type keyType1 string
			type keyType2 string

			opts := &ClientOptions{}
			opts.Set(keyType1("foo"), "bar")
			opts.Set(keyType2("foo"), "qux")

			v, ok := opts.Get(keyType1("foo"))
			Expect(v).To(Equal("bar"))
			Expect(ok).To(BeTrue())
		})
	})

	Describe("Set", func() {
		It("associates a non-standard option with a value", func() {
			opts := &ClientOptions{}
			opts.Set("foo", "bar")

			v, ok := opts.Get("foo")
			Expect(v).To(Equal("bar"))
			Expect(ok).To(BeTrue())
		})
	})
})
