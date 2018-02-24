package mariadb

import (
	"context"
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

// OpenStore returns an event store by name.
//
// ctx applies to the opening of the store, and not to the store itself.
func (c *Client) OpenStore(ctx context.Context, name string) (es *EventStore, err error) {
	var id int64

	res, err := c.db.ExecContext(ctx, `INSERT INTO store SET name = ?`, name)

	if isDuplicateEntry(err) {
		row := c.db.QueryRowContext(ctx, `SELECT id FROM store WHERE name = ?`, name)
		err = row.Scan(&id)
	} else if err == nil {
		id, err = res.LastInsertId()
	}

	es = &EventStore{c.db, uint64(id)}
	return
}

// Close closes the database connection.
func (c *Client) Close() error {
	return c.db.Close()
}
