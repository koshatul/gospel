// +build !without_mariadb

package gospelmaria_test

import (
	"context"
	"database/sql"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmalloc/gospel/src/gospel"
	"github.com/jmalloc/gospel/src/gospelmaria"
	"github.com/jmalloc/twelf/src/twelf"
)

// getTestClient returns a Client that uses the test DSN.
func getTestClient() *gospelmaria.Client {
	c, err := gospelmaria.OpenEnv(
		gospel.Logger(
			&twelf.StandardLogger{
				CaptureDebug: true,
			},
		),
	)

	if err != nil {
		panic(err)
	}

	return c
}

// getTestStore returns an EventStore that uses the test DSN.
func getTestStore() (*gospelmaria.Client, *gospelmaria.EventStore) {
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
	if dsn == "" {
		dsn = gospelmaria.DefaultDSN
	}

	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		panic(err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	_, err = db.Exec(`DROP SCHEMA ` + cfg.DBName)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(`CREATE SCHEMA IF NOT EXISTS ` + cfg.DBName)
	if err != nil {
		panic(err)
	}
}
