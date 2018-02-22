package streakdb_test

import (
	"errors"

	. "github.com/jmalloc/streakdb/src/streakdb"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ConflictError", func() {
	Describe("Error", func() {
		It("returns a meaningful error message", func() {
			err := ConflictError{
				Addr: Address{
					Stream: "test-stream",
					Offset: 123,
				},
				Event: Event{
					EventType:   "event-type",
					ContentType: "text/plain",
					Body:        []byte("Hello, world!"),
				},
			}

			Expect(err.Error()).To(Equal(
				"conflict occurred appending event-type! event at test-stream+123",
			))
		})
	})
})

var _ = Describe("IsConflict", func() {
	It("returns true if the error is a ConflictError", func() {
		Expect(IsConflict(ConflictError{})).To(BeTrue())
	})

	It("returns false if the error is nil", func() {
		Expect(IsConflict(nil)).To(BeFalse())
	})

	It("returns false if the error any other kind of error", func() {
		Expect(IsConflict(errors.New("<error>"))).To(BeFalse())
	})
})
