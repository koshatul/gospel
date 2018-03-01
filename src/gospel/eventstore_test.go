package gospel_test

import (
	"errors"

	. "github.com/jmalloc/gospel/src/gospel"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("IsConflict", func() {
	It("returns true if the error implements the ConflictError interface", func() {
		type e struct {
			ConflictError
		}

		Expect(IsConflict(e{})).To(BeTrue())
	})

	It("returns false if the error is nil", func() {
		Expect(IsConflict(nil)).To(BeFalse())
	})

	It("returns false if the error any other kind of error", func() {
		Expect(IsConflict(errors.New("<error>"))).To(BeFalse())
	})
})
