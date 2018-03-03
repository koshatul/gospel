package gospelmaria

import (
	"context"
	"database/sql"
	"os"

	"github.com/go-sql-driver/mysql"
	"github.com/jmalloc/gospel/src/gospel"
	"github.com/jmalloc/gospel/src/gospelmaria/schema"
	"github.com/jmalloc/gospel/src/internal/options"
	"github.com/jmalloc/twelf/src/twelf"
	"go.uber.org/multierr"
	"golang.org/x/time/rate"
)

// DefaultDSN is the default MariaDB DSN to use if none is specified.
const DefaultDSN = "gospel:gospel@tcp(127.0.0.1:3306)/gospel"

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
// If DSN is empty, DefaultDSN is used instead.
func Open(dsn string, opts ...gospel.Option) (*Client, error) {
	if dsn == "" {
		dsn = DefaultDSN
	}

	o := options.NewClientOptions(opts)

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

	err = schema.Create(db)
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
// If GOSPEL_MARIADB_DSN is empty, DefaultDSN is used instead.
func OpenEnv(opts ...gospel.Option) (*Client, error) {
	return Open(
		os.Getenv("GOSPEL_MARIADB_DSN"),
		opts...,
	)
}

// OpenStore returns an event store by name.
//
// ctx applies to the opening of the store, and not to the store itself.
func (c *Client) OpenStore(ctx context.Context, name string) (*EventStore, error) {
	var id int64

	tx, err := c.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx, `SELECT open_store(?)`, name)

	if err := row.Scan(&id); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	c.logger.Debug("opened '%s' event store", name)

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
