package streakdb_test

import (
	. "github.com/jmalloc/streakdb/src/streakdb"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Address", func() {
	Describe("Next", func() {
		It("returns the address at the next offset", func() {
			addr := Address{
				Stream: "test-stream",
				Offset: 123,
			}

			Expect(addr.Next()).To(Equal(
				Address{
					Stream: "test-stream",
					Offset: 124,
				},
			))
		})
	})

	Describe("String", func() {
		It("describes stream name and the offset", func() {
			addr := Address{
				Stream: "test-stream",
				Offset: 123,
			}

			Expect(addr.String()).To(Equal("test-stream+123"))
		})

		It("does not return an empty name for the ε-stream", func() {
			addr := Address{
				Stream: "",
				Offset: 123,
			}

			Expect(addr.String()).To(Equal("ε+123"))
		})
	})
})
