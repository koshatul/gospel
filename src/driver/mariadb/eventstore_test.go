// +build !without_mariadb

package mariadb_test

import (
	"context"
	"time"

	. "github.com/jmalloc/streakdb/src/driver/mariadb"
	"github.com/jmalloc/streakdb/src/streakdb"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("EventStore", func() {
	var (
		client *Client
		store  *EventStore
		ctx    context.Context
		cancel func()
	)

	BeforeEach(func() {
		client, store = getTestStore()
		var fn func()
		ctx, fn = context.WithTimeout(context.Background(), 10*time.Second)
		cancel = fn // defeat go vet warning about unused cancel func
	})

	AfterEach(func() {
		cancel()
		client.Close()
		destroyTestSchema()
	})

	Describe("Append", func() {
		Context("when the stream is empty", func() {
			next := streakdb.Address{
				Stream: "test-stream",
				Offset: 0,
			}

			It("returns the next address", func() {
				nx, err := store.Append(
					ctx,
					next,
					streakdb.Event{},
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
					streakdb.Event{},
					streakdb.Event{},
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
					streakdb.Event{},
				)

				Expect(err).Should(HaveOccurred())
				Expect(streakdb.IsConflict(err)).To(BeTrue())
			})
		})

		Context("when the stream is not empty", func() {
			var next streakdb.Address

			BeforeEach(func() {
				nx, err := store.Append(
					ctx,
					streakdb.Address{
						Stream: "test-stream",
						Offset: 0,
					},
					streakdb.Event{},
					streakdb.Event{},
				)
				Expect(err).ShouldNot(HaveOccurred())
				next = nx
			})

			It("returns the next address", func() {
				nx, err := store.Append(
					ctx,
					next,
					streakdb.Event{},
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
					streakdb.Event{},
					streakdb.Event{},
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
					streakdb.Event{},
				)

				Expect(err).Should(HaveOccurred())
				Expect(streakdb.IsConflict(err)).To(BeTrue())
			})

			It("returns a conflict error when the offset is too low", func() {
				addr := streakdb.Address{
					Stream: "test-stream",
					Offset: 0,
				}

				_, err := store.Append(
					ctx,
					addr,
					streakdb.Event{},
				)

				Expect(err).Should(HaveOccurred())
				Expect(streakdb.IsConflict(err)).To(BeTrue())
			})
		})

		It("panics if called with no events", func() {
			Expect(func() {
				store.Append(
					ctx,
					streakdb.Address{Stream: "test-stream"},
				)
			}).To(Panic())
		})

		It("panics if called with an ε-stream address", func() {
			Expect(func() {
				store.Append(
					ctx,
					streakdb.Address{Stream: ""},
					streakdb.Event{},
				)
			}).To(Panic())
		})

		It("does not produce any facts when there is a conflict", func() {
			// append event at +0
			_, err := store.Append(
				ctx,
				streakdb.Address{
					Stream: "test-stream",
					Offset: 0,
				},
				streakdb.Event{},
			)
			Expect(err).ShouldNot(HaveOccurred())

			// append a second event at +0, expected to conflict
			_, err = store.Append(
				ctx,
				streakdb.Address{
					Stream: "test-stream",
					Offset: 0,
				},
				streakdb.Event{},
			)
			Expect(streakdb.IsConflict(err)).To(BeTrue())

			// append a second event at +1, expected to still be unused
			_, err = store.Append(
				ctx,
				streakdb.Address{
					Stream: "test-stream",
					Offset: 1,
				},
				streakdb.Event{},
			)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Describe("AppendUnchecked", func() {
		Context("when the stream is empty", func() {
			next := streakdb.Address{
				Stream: "test-stream",
				Offset: 0,
			}

			It("returns the next address", func() {
				nx, err := store.AppendUnchecked(
					ctx,
					"test-stream",
					streakdb.Event{},
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
					streakdb.Event{},
					streakdb.Event{},
				)

				Expect(err).ShouldNot(HaveOccurred())
				Expect(nx).To(Equal(
					next.Next().Next(),
				))
			})
		})

		Context("when the stream is not empty", func() {
			var next streakdb.Address

			BeforeEach(func() {
				nx, err := store.Append(
					ctx,
					streakdb.Address{
						Stream: "test-stream",
						Offset: 0,
					},
					streakdb.Event{},
					streakdb.Event{},
				)
				Expect(err).ShouldNot(HaveOccurred())
				next = nx
			})

			It("returns the next address", func() {
				nx, err := store.AppendUnchecked(
					ctx,
					"test-stream",
					streakdb.Event{},
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
					streakdb.Event{},
					streakdb.Event{},
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
					streakdb.Event{},
				)
			}).To(Panic())
		})
	})
})
