// +build !without_mariadb

package mariadb_test

import (
	"context"
	"database/sql"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmalloc/gospel/src/driver/mariadb"
)

// getTestClient returns a Client that uses the test DSN.
func getTestClient() *mariadb.Client {
	c, err := mariadb.OpenEnv()
	if err != nil {
		panic(err)
	}

	return c
}

// getTestStore returns an EventStore that uses the test DSN.
func getTestStore() (*mariadb.Client, *mariadb.EventStore) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	c := getTestClient()
	es, err := c.OpenStore(ctx, "test")
	if err != nil {
		c.Close()
		panic(err)
	}

	return c, es
}

// destroyTestSchema removes all tables and procedures from the the database
// schema specified by getTestDSN().
func destroyTestSchema() {
	dsn := os.Getenv("GOSPEL_MARIADB_DSN")

	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		panic(err)
	}

	db, err := sql.Open("mysql", dsn)
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
