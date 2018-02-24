package mariadb

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jmalloc/streakdb/src/driver"
	"github.com/jmalloc/streakdb/src/streakdb"
	"golang.org/x/time/rate"
)

// Reader is an interface for reading facts from a stream stored in MariaDB.
type Reader struct {
	stmt  *sql.Stmt
	limit *rate.Limiter

	current *streakdb.Fact
	next    *streakdb.Fact
	facts   chan streakdb.Fact
	done    chan error

	addr   streakdb.Address
	ctx    context.Context
	cancel func()
}

var errReaderClosed = errors.New("reader is closed")

// openReader returns a new reader that begins at addr.
func openReader(
	ctx context.Context,
	db *sql.DB,
	storeID uint64,
	addr streakdb.Address,
	opts *driver.ReaderOptions,
) (*Reader, error) {
	stmt, err := prepareReaderStatement(ctx, db, opts, storeID, addr.Stream)
	if err != nil {
		return nil, err
	}

	// Note that runCtx is NOT derived from ctx, which is only used for the
	// opening of the reader itself.
	runCtx, cancel := context.WithCancel(context.Background())

	r := &Reader{
		stmt:  stmt,
		limit: rate.NewLimiter(rate.Inf, 1), // TODO

		facts: make(chan streakdb.Fact, 10), // TODO
		done:  make(chan error, 1),

		addr:   addr,
		ctx:    runCtx,
		cancel: cancel,
	}

	go r.run()

	return r, nil
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
	defer r.stmt.Close()

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
	// always select enough facts to fill the lookahead buffer, plus 1 to
	// make the send to f.facts block until more facts are needed.
	limit := cap(r.facts) - len(r.facts) + 1

	rows, err := r.stmt.QueryContext(
		r.ctx,
		r.addr.Offset,
		limit,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

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

func prepareReaderStatement(
	ctx context.Context,
	db *sql.DB,
	opts *driver.ReaderOptions,
	storeID uint64,
	stream string,
) (*sql.Stmt, error) {
	filter := ""
	if opts.FilterByEventType {
		types := strings.Join(escapeStrings(opts.EventTypes), `, `)
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
		escapeString(stream),
	)

	return db.PrepareContext(ctx, query)
}

// escapeString returns a quoted and escaped representation of s.
// Neither the Go sql package, nor the mysql driver currently expose string
// escaping functions. See https://github.com/golang/go/issues/18478.
func escapeString(s string) string {
	var buf bytes.Buffer

	buf.WriteRune('\'')

	for _, r := range s {
		switch r {
		case '\x00':
			buf.WriteString(`\0`)
		case '\x1a':
			buf.WriteString(`\Z`)
		case '\r':
			buf.WriteString(`\r`)
		case '\n':
			buf.WriteString(`\n`)
		case '\b':
			buf.WriteString(`\b`)
		case '\t':
			buf.WriteString(`\t`)
		case '\\':
			buf.WriteString(`\\`)
		case '\'':
			buf.WriteString(`\'`)
		default:
			buf.WriteRune(r)
		}
	}

	buf.WriteRune('\'')

	return buf.String()
}

// escapeStrings escapes a slice of strings.
func escapeStrings(s []string) []string {
	escaped := make([]string, len(s))

	for i, v := range s {
		escaped[i] = escapeString(v)
	}

	return escaped
}
