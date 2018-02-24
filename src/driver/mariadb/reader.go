package mariadb

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jmalloc/streakdb/src/streakdb"
	"golang.org/x/time/rate"
)

// Reader is an interface for reading facts from a stream stored in MariaDB.
type Reader struct {
	db    *sql.DB
	limit *rate.Limiter
	store string

	current *streakdb.Fact
	next    *streakdb.Fact
	facts   chan streakdb.Fact
	done    chan error

	addr   streakdb.Address
	count  int
	ctx    context.Context
	cancel func()
}

var errReaderClosed = errors.New("reader is closed")

// newReader returns a new reader that begins at addr.
func newReader(
	db *sql.DB,
	limit *rate.Limiter,
	store string,
	lookahead int,
	addr streakdb.Address,
) *Reader {
	ctx, cancel := context.WithCancel(context.Background())

	r := &Reader{
		db:    db,
		limit: limit,
		store: store,

		facts: make(chan streakdb.Fact, lookahead),
		done:  make(chan error, 1),

		addr:   addr,
		ctx:    ctx,
		cancel: cancel,
	}

	go r.run()

	return r
}

// Next blocks until a fact is available for reading or ctx is canceled.
//
// If err is nil, the "current" fact is ready to be returned by Get().
//
// addr is the offset within the stream that the reader has reached. It
// can be used to efficiently resume reading in a future call to
// EventStore.Open().
//
// Note that addr is not always the address immediately following the fact
// returned by Get() - it may be "further ahead" in the stream, skipping
// over any facts that the reader is not interested in.
func (r *Reader) Next(ctx context.Context) (addr streakdb.Address, err error) {
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
		addr = r.next.Addr
	default:
		addr = r.current.Addr.Next()
	}

	return
}

// Get returns the "current" fact.
//
// It panics if Next() has not been called.
// Get() returns the same Fact until Next() is called again.
func (r *Reader) Get() streakdb.Fact {
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

func (r *Reader) run() {
	defer r.cancel()
	defer close(r.done)

	var err error

	for {
		err = r.limit.Wait(r.ctx)
		if err != nil {
			break
		}

		err = r.poll()
		if err != nil {
			break
		}

	}

	if err != context.Canceled {
		r.done <- err
	}
}

func (r *Reader) poll() error {
	rows, err := r.fetch()
	if err != nil {
		return err
	}

	f := streakdb.Fact{
		Addr: r.addr,
	}

	for rows.Next() {
		if err := rows.Scan(
			&f.Addr.Offset,
			&f.Time,
			&f.Event.EventType,
			&f.Event.ContentType,
			&f.Event.Body,
		); err != nil {
			return err
		}

		select {
		case r.facts <- f:
			r.addr = f.Addr.Next()
		case <-r.ctx.Done():
			return r.ctx.Err()
		}
	}

	return nil
}

func (r *Reader) fetch() (*sql.Rows, error) {
	// always select enough facts to fill the lookahead buffer, plus 1 to
	// make the send to f.facts block until more facts are needed.
	limit := cap(r.facts) - len(r.facts) + 1

	return r.db.QueryContext(
		r.ctx,
		`SELECT
            f.offset,
            f.time,
            e.event_type,
            e.content_type,
            e.body
        FROM fact AS f
        INNER JOIN event AS e
        ON e.id = f.event_id
        WHERE f.store = ?
            AND f.stream = ?
            AND f.offset >= ?
        ORDER BY offset
        LIMIT ?`,
		r.store,
		r.addr.Stream,
		r.addr.Offset,
		limit,
	)
}
