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

var _ = Describe("Open", func() {
	AfterEach(destroyTestSchema)

	It("returns a client", func() {
		c, err := Open(getTestDSN())

		Expect(err).ShouldNot(HaveOccurred())
		Expect(c).NotTo(BeNil())

		c.Close()
	})
})

var _ = Describe("Client", func() {
	var (
		ctx    context.Context
		cancel func()
		client *Client
	)

	BeforeEach(func() {
		var fn func()
		ctx, fn = context.WithTimeout(context.Background(), 250*time.Millisecond)
		cancel = fn // defeat go vet warning about unused cancel func

		client = getTestClient()
	})

	AfterEach(func() {
		cancel()
		client.Close()
		destroyTestSchema()
	})

	Describe("OpenStore", func() {
		It("returns a streakdb.EventStore", func() {
			var es streakdb.EventStore // static interface check
			es, err := client.OpenStore(ctx, "test")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(es).NotTo(BeNil())
		})

		It("returns an error if the client is closed", func() {
			client.Close()
			_, err := client.OpenStore(ctx, "test")
			Expect(err).Should(MatchError("sql: database is closed"))
		})
	})
})
