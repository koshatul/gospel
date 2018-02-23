// +build !without_mariadb

package mariadb_test

import (
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
	var client *Client

	BeforeEach(func() {
		client = getTestClient()
	})

	AfterEach(func() {
		client.Close()
		destroyTestSchema()
	})

	Describe("GetStore", func() {
		It("returns a streakdb.EventStore", func() {
			var es streakdb.EventStore // static interface check
			es, err := client.GetStore("test")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(es).NotTo(BeNil())
		})

		It("returns an error if the client is closed", func() {
			client.Close()
			_, err := client.GetStore("test")
			Expect(err).Should(MatchError("sql: database is closed"))
		})
	})
})
