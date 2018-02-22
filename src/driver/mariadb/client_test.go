// +build !without_mariadb,!without_functests

package mariadb_test

import (
	"database/sql"
	"os"

	"github.com/go-sql-driver/mysql"
	. "github.com/jmalloc/streakdb/src/driver/mariadb"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func testDSN() string {
	dsn := os.Getenv("STREAKDB_MARIADB_TEST_DSN")
	if dsn != "" {
		return dsn
	}

	return "streakdb:streakdb@tcp(127.0.0.1:3306)/streakdb"
}

func tearDown() {
	cfg, err := mysql.ParseDSN(testDSN())
	if err != nil {
		panic(err)
	}

	db, err := sql.Open("mysql", testDSN())
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(`DROP SCHEMA ` + cfg.DBName)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(`CREATE SCHEMA IF NOT EXISTS ` + cfg.DBName)
	if err != nil {
		panic(err)
	}
}

var _ = Describe("Open", func() {
	AfterEach(tearDown)

	It("returns a client", func() {
		c, err := Open(testDSN())

		Expect(err).ShouldNot(HaveOccurred())
		Expect(c).NotTo(BeNil())

		c.Close()
	})

	It("creates the schema", func() {
		c, err := Open(testDSN())
		Expect(err).ShouldNot(HaveOccurred())
		defer c.Close()

		db, err := sql.Open("mysql", testDSN())
		Expect(err).ShouldNot(HaveOccurred())

		rows, err := db.Query("SHOW TABLES")
		Expect(err).ShouldNot(HaveOccurred())

		var tables []string
		for rows.Next() {
			var t string
			err := rows.Scan(&t)
			Expect(err).ShouldNot(HaveOccurred())

			tables = append(tables, t)
		}

		Expect(tables).To(ConsistOf("event", "fact"))
	})
})
