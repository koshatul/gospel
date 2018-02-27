package gospelmaria

import (
	"context"
	"database/sql"
	"errors"
	"os"

	"github.com/go-sql-driver/mysql"
	schema "github.com/jmalloc/gospel/artifacts/mariadb"
	"github.com/jmalloc/gospel/src/gospel"
	"github.com/jmalloc/gospel/src/internal/driver"
	"github.com/jmalloc/twelf/src/twelf"
	"github.com/uber-go/multierr"
	"golang.org/x/time/rate"
)

// Client is a connection to a MariaDB server.
//
// Each server supports an arbitrary number of named event stores.
type Client struct {
	// db is the pool of MariaDB connections used by the event stores accessed
	// through this client.
	db *sql.DB

	// rlimit is a rate-limiter that limits the number of polling queries that
	// can be performed each second. It is shared by all readers, and hence
	// provides a global cap of the number of read queries per second.
	rlimit *rate.Limiter

	// logger is the logger to use for activity and debug logging. It is
	// inherited by all event stores and their readers.
	logger twelf.Logger
}

// Open returns a new Client instance for the given MariaDB DSN.
func Open(dsn string, opts ...gospel.Option) (*Client, error) {
	o := driver.NewClientOptions(opts)

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

	o.Logger.Log(
		"connected to MariaDB event store at %s@%s/%s",
		cfg.User,
		cfg.Addr,
		cfg.DBName,
	)

	return &Client{
		db,
		rate.NewLimiter(500, 1), // TODO, allow configuration
		o.Logger,
	}, nil
}

// OpenEnv returns a new Client instance for the MariaDB DSN described by
// the GOSPEL_MARIADB_DSN environment variable.
//
// If the environment variable is not set,
func OpenEnv(opts ...gospel.Option) (*Client, error) {
	dsn := os.Getenv("GOSPEL_MARIADB_DSN")
	if dsn == "" {
		return nil, errors.New("the GOSPEL_MARIADB_DSN environment variable is not set")
	}

	return Open(dsn, opts...)
}

// OpenStore returns an event store by name.
//
// ctx applies to the opening of the store, and not to the store itself.
func (c *Client) OpenStore(ctx context.Context, name string) (*EventStore, error) {
	var id int64

	res, err := c.db.ExecContext(ctx, `INSERT INTO store SET name = ?`, name)

	if isDuplicateEntry(err) {
		row := c.db.QueryRowContext(ctx, `SELECT id FROM store WHERE name = ?`, name)
		err = row.Scan(&id)
		if err != nil {
			return nil, err
		}

		c.logger.Debug("opened existing event store '%s'", name)
	} else if err == nil {
		id, err = res.LastInsertId()
		if err != nil {
			return nil, err
		}

		c.logger.Debug("created new event store '%s'", name)
	} else {
		return nil, err
	}

	return &EventStore{
		c.db,
		uint64(id),
		name,
		c.rlimit,
		c.logger,
	}, nil
}

// Close closes the database connection.
func (c *Client) Close() error {
	return c.db.Close()
}
