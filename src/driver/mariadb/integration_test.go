// +build !without_mariadb

package mariadb_test

import (
	"database/sql"
	"os"

	"github.com/go-sql-driver/mysql"
)

// testDSN returns the MariaDB DSN used for integration tests.
func testDSN() string {
	dsn := os.Getenv("STREAKDB_MARIADB_TEST_DSN")
	if dsn != "" {
		return dsn
	}

	return "streakdb:streakdb@tcp(127.0.0.1:3306)/streakdb"
}

// destroyTestSchema removes all tables and procedures from the the database
// schema specified by testDSN().
func destroyTestSchema() {
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
