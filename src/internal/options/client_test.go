package options_test

import (
	"github.com/jmalloc/gospel/src/gospel"
	. "github.com/jmalloc/gospel/src/internal/options"
	"github.com/jmalloc/twelf/src/twelf"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("NewClientOptions", func() {
	It("applies the provided options", func() {
		l := &twelf.StandardLogger{}

		opts := NewClientOptions(
			[]ClientOption{
				gospel.Logger(l),
			},
		)

		Expect(opts.Logger).To(BeIdenticalTo(l))
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
