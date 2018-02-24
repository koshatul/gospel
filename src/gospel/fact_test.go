package gospel_test

import (
	"time"

	. "github.com/jmalloc/gospel/src/gospel"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Fact", func() {
	Describe("String", func() {
		It("describes the event by its type", func() {
			f := Fact{
				Addr: Address{
					Stream: "test-stream",
					Offset: 123,
				},
				Time: time.Now(),
				Event: Event{
					EventType:   "event-type",
					ContentType: "text/plain",
					Body:        []byte("Hello, world!"),
				},
			}

			Expect(f.String()).To(Equal(
				"event-type!test-stream+123",
			))
		})
	})
})
