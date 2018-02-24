package gospel_test

import (
	. "github.com/jmalloc/gospel/src/gospel"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Event", func() {
	Describe("String", func() {
		It("describes the event by its type", func() {
			ev := Event{
				EventType:   "event-type",
				ContentType: "text/plain",
				Body:        []byte("Hello, world!"),
			}

			Expect(ev.String()).To(Equal(
				"event-type!",
			))
		})
	})
})
