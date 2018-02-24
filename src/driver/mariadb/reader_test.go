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

var _ = Describe("Reader", func() {
	var (
		ctx    context.Context
		cancel func()

		client *Client
		store  *EventStore
		reader gospel.Reader

		addr gospel.Address
		opts []gospel.ReaderOption
	)

	BeforeEach(func() {
		var fn func()
		ctx, fn = context.WithTimeout(context.Background(), 1*time.Second)
		cancel = fn // defeat go vet warning about unused cancel func

		client, store = getTestStore()

		_, err := store.AppendUnchecked(
			ctx,
			"test-stream",
			gospel.Event{EventType: "event-type-1", Body: []byte("event-1")},
			gospel.Event{EventType: "event-type-2", Body: []byte("event-2")},
			gospel.Event{EventType: "event-type-3", Body: []byte("event-3")},
		)
		Expect(err).ShouldNot(HaveOccurred())

		addr = gospel.Address{
			Stream: "test-stream",
			Offset: 0,
		}
		opts = nil
	})

	JustBeforeEach(func() {
		var err error
		reader, err = store.Open(ctx, addr, opts...)
		if err != nil {
			panic(err)
		}
	})

	AfterEach(func() {
		cancel()
		reader.Close()
		client.Close()
		destroyTestSchema()
	})

	Describe("Next", func() {
		It("returns the address of the next fact", func() {
			nx, err := reader.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(nx).To(Equal(addr.Next()))
		})

		It("blocks until the deadline if there are no more facts to read", func() {
			_, err := reader.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			_, err = reader.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			_, err = reader.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			nextCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
			defer cancel()

			_, err = reader.Next(nextCtx)
			Expect(err).To(Equal(context.DeadlineExceeded))
		})

		It("returns an error if the reader is closed", func() {
			reader.Close()

			_, err := reader.Next(ctx)
			Expect(err).To(MatchError("reader is closed"))
		})
	})

	Describe("Get", func() {
		It("returns the current fact", func() {
			_, err := reader.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			f := reader.Get()

			Expect(f).To(Equal(gospel.Fact{
				Addr: addr,
				Event: gospel.Event{
					EventType: "event-type-1",
					Body:      []byte("event-1"),
				},
				Time: f.Time, // perform fuzzy check for time below
			}))

			// Use a loose comparison for time, as MariaDB is typically going
			// to be running in a VM (ie, a separate clock) during testing.
			Expect(f.Time).To(
				BeTemporally(
					"~",
					time.Now(),
					1*time.Minute,
				),
			)
		})

		It("returns the expected facts", func() {
			var bodies [][]byte

			for {
				_, err := reader.Next(ctx)
				Expect(err).ShouldNot(HaveOccurred())

				bodies = append(
					bodies,
					reader.Get().Event.Body,
				)

				if len(bodies) == 3 {
					break
				}
			}

			Expect(bodies).To(Equal([][]byte{
				[]byte("event-1"),
				[]byte("event-2"),
				[]byte("event-3"),
			}))
		})

		It("returns the same fact until next is called", func() {
			_, err := reader.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			f1 := reader.Get()
			f2 := reader.Get()
			Expect(f2).To(Equal(f1))

			_, err = reader.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			f3 := reader.Get()
			Expect(f3).NotTo(Equal(f1))
		})
	})

	Context("when using an event-type filter", func() {
		BeforeEach(func() {
			opts = append(opts, gospel.FilterByEventType(
				"event-type-1",
				"event-type-3",
			))
		})

		Describe("Next", func() {
			It("returns the address of the next unfiltered fact", func() {
				nx, err := reader.Next(ctx)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(nx).To(SatisfyAny(
					Equal(addr.Next().Next()), // second fact is filtered out
					Equal(addr.Next()),        // write to facts channel wasn't fast enough to provide the next event
				))
			})

			It("skips over filtered facts", func() {
				var bodies [][]byte

				for {
					_, err := reader.Next(ctx)
					Expect(err).ShouldNot(HaveOccurred())

					bodies = append(
						bodies,
						reader.Get().Event.Body,
					)

					if len(bodies) == 2 {
						break
					}
				}

				Expect(bodies).To(Equal([][]byte{
					[]byte("event-1"),
					[]byte("event-3"),
				}))
			})
		})
	})
})
