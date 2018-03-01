package apierror_test

import (
	"github.com/jmalloc/gospel/src/gospel"
	. "github.com/jmalloc/gospel/src/internal/apierror"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ConflictError", func() {
	It("is considered a conflict by gospel.IsConflict", func() {
		Expect(gospel.IsConflict(ConflictError{})).To(BeTrue())
	})

	Describe("Error", func() {
		It("returns a meaningful error message", func() {
			err := NewConflict(
				gospel.Address{
					Stream: "test-stream",
					Offset: 123,
				},
				gospel.Event{
					EventType:   "event-type",
					ContentType: "text/plain",
					Body:        []byte("Hello, world!"),
				},
			)

			Expect(err.Error()).To(Equal(
				"conflict occurred appending event-type! event at test-stream+123",
			))
		})
	})
})
