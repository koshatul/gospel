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

var _ = Describe("Reader", func() {
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

		_, err := store.AppendUnchecked(
			ctx,
			"test-stream",
			streakdb.Event{EventType: "event-1"},
			streakdb.Event{EventType: "event-2"},
			streakdb.Event{EventType: "event-3"},
		)
		Expect(err).ShouldNot(HaveOccurred())
	})

	AfterEach(func() {
		cancel()
		client.Close()
		destroyTestSchema()
	})

	Describe("Next", func() {
		It("returns the address of the next fact", func() {
			addr := streakdb.Address{
				Stream: "test-stream",
				Offset: 0,
			}

			r, err := store.Open(addr)
			Expect(err).ShouldNot(HaveOccurred())
			defer r.Close()

			nx, err := r.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(nx).To(Equal(addr.Next()))
		})

		It("blocks until the deadline if there are no more facts to read", func() {
			r, err := store.Open(
				streakdb.Address{
					Stream: "test-stream",
					Offset: 3,
				},
			)
			Expect(err).ShouldNot(HaveOccurred())
			defer r.Close()

			_, err = r.Next(ctx)
			Expect(err).To(Equal(context.DeadlineExceeded))
		})

		It("returns an error if the reader is closed", func() {
			r, err := store.Open(streakdb.Address{
				Stream: "test-stream",
				Offset: 0,
			})
			Expect(err).ShouldNot(HaveOccurred())
			r.Close()

			_, err = r.Next(ctx)
			Expect(err).To(MatchError("reader is closed"))
		})
	})

	Describe("Get", func() {
		It("returns the current fact", func() {
			addr := streakdb.Address{
				Stream: "test-stream",
				Offset: 0,
			}

			r, err := store.Open(addr)
			Expect(err).ShouldNot(HaveOccurred())
			defer r.Close()

			_, err = r.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			f := r.Get()

			Expect(f).To(Equal(streakdb.Fact{
				Addr:  addr,
				Event: streakdb.Event{EventType: "event-1"},
				Time:  f.Time,
			}))

			// Use a loose comparison for time, as MariaDB is typically going
			// to be running in a VM (ie, a separate clock) during testing.
			Expect(f.Time).To(BeTemporally("~", time.Now(), 5*time.Second))
		})
	})
})
