package mariadb

import (
	"database/sql"

	"github.com/go-sql-driver/mysql"
	schema "github.com/jmalloc/streakdb/artifacts/mariadb"
	"github.com/uber-go/multierr"
)

// Client is a connection to a MariaDB server.
//
// Each server supports an arbitrary number of named event stores.
type Client struct {
	db *sql.DB
}

// Open returns a new Client instance for the given MariaDB DSN.
func Open(dsn string) (*Client, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	cfg.MultiStatements = true   // required to init schema in single query
	cfg.ParseTime = true         // allow row.Scan into time.Time
	cfg.InterpolateParams = true // inject query values client-side (reduces roundtrips, no prepared statements)

	dsn = cfg.FormatDSN()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(schema.Statements)
	if err != nil {
		return nil, multierr.Append(
			err,
			db.Close(),
		)
	}

	return &Client{db}, nil
}

// Close closes the database connection.
func (c *Client) Close() error {
	return c.db.Close()
}
