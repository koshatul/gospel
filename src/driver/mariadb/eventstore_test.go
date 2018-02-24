// +build !without_mariadb

package mariadb_test

import (
	"context"
	"time"

	. "github.com/jmalloc/gospel/src/driver/mariadb"
	"github.com/jmalloc/gospel/src/gospel"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("EventStore", func() {
	var (
		ctx    context.Context
		cancel func()

		client *Client
		store  *EventStore
	)

	BeforeEach(func() {
		var fn func()
		ctx, fn = context.WithTimeout(context.Background(), 250*time.Millisecond)
		cancel = fn // defeat go vet warning about unused cancel func

		client, store = getTestStore()
	})

	AfterEach(func() {
		cancel()
		client.Close()
		destroyTestSchema()
	})

	Describe("Append", func() {
		Context("when the stream is empty", func() {
			next := gospel.Address{
				Stream: "test-stream",
				Offset: 0,
			}

			It("returns the next address", func() {
				nx, err := store.Append(
					ctx,
					next,
					gospel.Event{},
				)

				Expect(err).ShouldNot(HaveOccurred())
				Expect(nx).To(Equal(
					next.Next(),
				))
			})

			It("returns the next address when appending multiple events", func() {
				nx, err := store.Append(
					ctx,
					next,
					gospel.Event{},
					gospel.Event{},
				)

				Expect(err).ShouldNot(HaveOccurred())
				Expect(nx).To(Equal(
					next.Next().Next(),
				))
			})

			It("returns a conflict error when the offset is too high", func() {
				_, err := store.Append(
					ctx,
					next.Next(),
					gospel.Event{},
				)

				Expect(err).Should(HaveOccurred())
				Expect(gospel.IsConflict(err)).To(BeTrue())
			})
		})

		Context("when the stream is not empty", func() {
			var next gospel.Address

			BeforeEach(func() {
				nx, err := store.Append(
					ctx,
					gospel.Address{
						Stream: "test-stream",
						Offset: 0,
					},
					gospel.Event{},
					gospel.Event{},
				)
				Expect(err).ShouldNot(HaveOccurred())
				next = nx
			})

			It("returns the next address", func() {
				nx, err := store.Append(
					ctx,
					next,
					gospel.Event{},
				)

				Expect(err).ShouldNot(HaveOccurred())
				Expect(nx).To(Equal(
					next.Next(),
				))
			})

			It("returns the next address when appending multiple events", func() {
				nx, err := store.Append(
					ctx,
					next,
					gospel.Event{},
					gospel.Event{},
				)

				Expect(err).ShouldNot(HaveOccurred())
				Expect(nx).To(Equal(
					next.Next().Next(),
				))
			})

			It("returns a conflict error when the offset is too high", func() {
				_, err := store.Append(
					ctx,
					next.Next(),
					gospel.Event{},
				)

				Expect(err).Should(HaveOccurred())
				Expect(gospel.IsConflict(err)).To(BeTrue())
			})

			It("returns a conflict error when the offset is too low", func() {
				addr := gospel.Address{
					Stream: "test-stream",
					Offset: 0,
				}

				_, err := store.Append(
					ctx,
					addr,
					gospel.Event{},
				)

				Expect(err).Should(HaveOccurred())
				Expect(gospel.IsConflict(err)).To(BeTrue())
			})
		})

		It("panics if called with no events", func() {
			Expect(func() {
				store.Append(
					ctx,
					gospel.Address{Stream: "test-stream"},
				)
			}).To(Panic())
		})

		It("panics if called with an ε-stream address", func() {
			Expect(func() {
				store.Append(
					ctx,
					gospel.Address{Stream: ""},
					gospel.Event{},
				)
			}).To(Panic())
		})

		It("does not produce any facts when there is a conflict", func() {
			// append event at +0
			_, err := store.Append(
				ctx,
				gospel.Address{
					Stream: "test-stream",
					Offset: 0,
				},
				gospel.Event{},
			)
			Expect(err).ShouldNot(HaveOccurred())

			// append a second event at +0, expected to conflict
			_, err = store.Append(
				ctx,
				gospel.Address{
					Stream: "test-stream",
					Offset: 0,
				},
				gospel.Event{},
			)
			Expect(gospel.IsConflict(err)).To(BeTrue())

			// append a second event at +1, expected to still be unused
			_, err = store.Append(
				ctx,
				gospel.Address{
					Stream: "test-stream",
					Offset: 1,
				},
				gospel.Event{},
			)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Describe("AppendUnchecked", func() {
		Context("when the stream is empty", func() {
			next := gospel.Address{
				Stream: "test-stream",
				Offset: 0,
			}

			It("returns the next address", func() {
				nx, err := store.AppendUnchecked(
					ctx,
					"test-stream",
					gospel.Event{},
				)

				Expect(err).ShouldNot(HaveOccurred())
				Expect(nx).To(Equal(
					next.Next(),
				))
			})

			It("returns the next address when appending multiple events", func() {
				nx, err := store.AppendUnchecked(
					ctx,
					"test-stream",
					gospel.Event{},
					gospel.Event{},
				)

				Expect(err).ShouldNot(HaveOccurred())
				Expect(nx).To(Equal(
					next.Next().Next(),
				))
			})
		})

		Context("when the stream is not empty", func() {
			var next gospel.Address

			BeforeEach(func() {
				nx, err := store.Append(
					ctx,
					gospel.Address{
						Stream: "test-stream",
						Offset: 0,
					},
					gospel.Event{},
					gospel.Event{},
				)
				Expect(err).ShouldNot(HaveOccurred())
				next = nx
			})

			It("returns the next address", func() {
				nx, err := store.AppendUnchecked(
					ctx,
					"test-stream",
					gospel.Event{},
				)

				Expect(err).ShouldNot(HaveOccurred())
				Expect(nx).To(Equal(
					next.Next(),
				))
			})

			It("returns the next address when appending multiple events", func() {
				nx, err := store.AppendUnchecked(
					ctx,
					"test-stream",
					gospel.Event{},
					gospel.Event{},
				)

				Expect(err).ShouldNot(HaveOccurred())
				Expect(nx).To(Equal(
					next.Next().Next(),
				))
			})
		})

		It("panics if called with no events", func() {
			Expect(func() {
				store.AppendUnchecked(
					ctx,
					"test-stream",
				)
			}).To(Panic())
		})

		It("panics if called with the ε-stream", func() {
			Expect(func() {
				store.AppendUnchecked(
					ctx,
					"",
					gospel.Event{},
				)
			}).To(Panic())
		})
	})
})
