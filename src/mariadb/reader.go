package mariadb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jmalloc/gospel/src/gospel"
	"github.com/jmalloc/gospel/src/internal/driver"
	"github.com/jmalloc/twelf/src/twelf"
	"github.com/jpillora/backoff"
	"golang.org/x/time/rate"
)

// Reader is an interface for reading facts from a stream stored in MariaDB.
type Reader struct {
	// stmt is a prepared statement used to query for facts.
	// It accepts two parameters, stream offset and limit.
	stmt *sql.Stmt

	// backoff describes the exponential-backoff policy to be used when a poll
	// query produces no results, i.e. when the reader has reached the end of
	// a stream.
	backoff *backoff.Backoff

	// limit is a rate-limiter that limits the number of polling queries that
	// can be performed each second. It is shared by all readers, and hence
	// provides a global cap of the number of read queries per second.
	limit *rate.Limiter

	// logger is the target for debug logging. readers do not perform activity
	// logging.
	logger twelf.Logger

	// opts is the options specified when opening the reader.
	opts *driver.ReaderOptions

	// facts is a channel on which facts are delivered to the caller of Next().
	// A worker goroutine polls the database and delivers the facts to this
	// channel.
	facts chan gospel.Fact

	// current is the "current" fact which is returned by Get() until Next() is
	// called again.
	current *gospel.Fact

	// next is the "next" fact, after "current". It is read from the facts
	// channel when a call to Next() to improve the accuracy of Next()'s 'nx'
	// output parameter (rather than just returning r.current.Addr().Next()).
	next *gospel.Fact

	// done is a signaling channel which is closed when the worker goroutine
	// returns. The error that caused the closure, if any, is sent to the
	// channel before it closed. This means a pending call to Next() will return
	// the error when it first occurs, but subsequent calls will return a more
	// generic "reader is closed" error.
	done chan error

	// ctx is a context that is canceled when Close() is called, or when the
	// worker goroutine returns. It is used to abort any in-progress database
	// queries when the reader is closed.
	//
	// Context cancellation errors are not sent to the 'done' channel, so any
	// pending Next() call will receive a generic "reader is closed" error.
	ctx    context.Context
	cancel func()

	// addr is the starting address for the next database poll.
	addr gospel.Address
}

// errReaderClosed is an error returned by Next() when it is called on a closed
// reader, or when the reader is closed while a call to Next() is pending.
var errReaderClosed = errors.New("reader is closed")

// openReader returns a new reader that begins at addr.
func openReader(
	ctx context.Context,
	db *sql.DB,
	storeID uint64,
	addr gospel.Address,
	limit *rate.Limiter,
	logger twelf.Logger,
	opts *driver.ReaderOptions,
) (*Reader, error) {
	// Note that runCtx is NOT derived from ctx, which is only used for the
	// opening of the reader itself.
	runCtx, cancel := context.WithCancel(context.Background())

	// Create a read-buffer smaller than the lookahead amount so that
	// the worker tends to block when queuing the last fact from each poll.
	size := getLookahead(opts)
	if size != 0 {
		size--
	}

	r := &Reader{
		limit:  limit,
		logger: logger,
		opts:   opts,
		facts:  make(chan gospel.Fact, size),
		done:   make(chan error, 1),
		ctx:    runCtx,
		cancel: cancel,
		addr:   addr,
	}

	if err := r.init(ctx, db, storeID); err != nil {
		return nil, err
	}

	go r.run()

	return r, nil
}

// Next blocks until a fact is available for reading or ctx is canceled.
//
// If err is nil, the "current" fact is ready to be returned by Get().
//
// nx is the offset within the stream that the reader has reached. It can be
// used to efficiently resume reading in a future call to EventStore.Open().
//
// Note that nx is not always the address immediately following the fact
// returned by Get() - it may be "further ahead" in the stream, this skipping
// over any facts that the reader is not interested in.
func (r *Reader) Next(ctx context.Context) (nx gospel.Address, err error) {
	if r.next == nil {
		select {
		case f := <-r.facts:
			r.current = &f
		case <-ctx.Done():
			err = ctx.Err()
			return
		case err = <-r.done:
			if err == nil {
				err = errReaderClosed
			}
			return
		}
	} else {
		r.current = r.next
		r.next = nil
	}

	// Perform a non-blocking lookahead to see if we have the next fact already.
	select {
	case f := <-r.facts:
		r.next = &f
		nx = r.next.Addr

		r.logger.Debug(
			"[reader %p] advanced to '%s', next is '%s'",
			r,
			r.current,
			r.next,
		)
	default:
		// assume next is literally the next fact on the stream
		nx = r.current.Addr.Next()

		r.logger.Debug(
			"[reader %p] advanced to '%s', next is not yet available",
			r,
			r.current,
		)
	}

	return
}

// Get returns the "current" fact.
//
// It panics if Next() has not been called.
// Get() returns the same Fact until Next() is called again.
func (r *Reader) Get() gospel.Fact {
	if r.current == nil {
		panic("Next() must be called before calling Get()")
	}

	return *r.current
}

// Close closes the reader.
func (r *Reader) Close() error {
	select {
	case err := <-r.done:
		return err
	default:
		r.cancel()
		return <-r.done
	}
}

// init intialises the reader based on r.opts.
func (r *Reader) init(ctx context.Context, db *sql.DB, storeID uint64) error {
	if err := r.prepareStatement(ctx, db, storeID); err != nil {
		return err
	}

	r.backoff = &backoff.Backoff{
		Jitter: true,
		Max:    1 * time.Second, // TODO: allow configuration via options
	}

	return nil
}

// prepareStatement creates r.stmt, an SQL prepared statement used to poll
// for new facts.
func (r *Reader) prepareStatement(ctx context.Context, db *sql.DB, storeID uint64) error {
	filter := ""
	if r.opts.FilterByEventType {
		types := strings.Join(escapeStrings(r.opts.EventTypes), `, `)
		filter = `AND e.event_type IN (` + types + `)`
	}

	query := fmt.Sprintf(
		`SELECT
			f.offset,
			f.time,
			e.event_type,
			e.content_type,
			e.body
		FROM fact AS f
		INNER JOIN event AS e
		ON e.id = f.event_id
		%s
		WHERE f.store_id = %d
			AND f.stream = %s
			AND f.offset >= ?
		ORDER BY offset
		LIMIT ?`,
		filter,
		storeID,
		escapeString(r.addr.Stream),
	)

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return err
	}

	r.stmt = stmt
	return nil
}

// run polls the database for facts and sends them to r.facts until r.ctx is
// canceled or an error occurs.
func (r *Reader) run() {
	defer r.cancel()
	defer close(r.done)
	defer r.stmt.Close()

	if r.logger.IsDebug() {
		filter := "*"
		if r.opts.FilterByEventType {
			filter = strings.Join(r.opts.EventTypes, ", ")
		}

		r.logger.Debug(
			"[reader %p] opened at %s (poll limit: %.02f/s, filter: %s)",
			r,
			r.addr,
			float64(r.limit.Limit()),
			filter,
		)
	}

	var err error

	for err == nil {
		err = r.tick()
	}

	if err != context.Canceled {
		r.done <- err
	}
}

// tick executes one pass of the worker goroutine.
func (r *Reader) tick() error {
	if err := r.limit.Wait(r.ctx); err != nil {
		return err
	}

	limit := getLookahead(r.opts) - len(r.facts)

	count, err := r.poll(limit)
	if err != nil {
		return err
	}

	if count > 0 || r.backoff.Attempt() == 0 {
		r.logger.Debug(
			"[reader %p] worker found %d of %d requested fact(s)",
			r,
			count,
			limit,
		)
	}

	if count == limit {
		r.backoff.Reset()

		return nil
	}

	d := r.backoff.Duration()

	r.logger.Debug(
		"[reader %p] worker is backing off for %s",
		r,
		d,
	)

	select {
	case <-time.After(d):
		return nil
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
}

// fetch queries the database for facts beginning at r.addr.
func (r *Reader) poll(limit int) (int, error) {
	rows, err := r.stmt.QueryContext(
		r.ctx,
		r.addr.Offset,
		limit,
	)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	f := gospel.Fact{
		Addr: r.addr,
	}

	count := 0

	for rows.Next() {
		if err := rows.Scan(
			&f.Addr.Offset,
			&f.Time,
			&f.Event.EventType,
			&f.Event.ContentType,
			&f.Event.Body,
		); err != nil {
			return count, err
		}

		select {
		case r.facts <- f:
			r.addr = f.Addr.Next()
			count++
		case <-r.ctx.Done():
			return count, r.ctx.Err()
		}
	}

	return count, nil
}
