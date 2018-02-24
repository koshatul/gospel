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
	// db is the pool of MariaDB connections used by the event stores accessed
	// through this client.
	db *sql.DB
}

// Open returns a new Client instance for the given MariaDB DSN.
func Open(dsn string) (*Client, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}

	cfg.Collation = "binary"
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

// GetStore returns an event store by name.
func (c *Client) GetStore(name string) (*EventStore, error) {
	if err := c.db.Ping(); err != nil {
		return nil, err
	}

	return &EventStore{c.db, name}, nil
}

// Close closes the database connection.
func (c *Client) Close() error {
	return c.db.Close()
}
