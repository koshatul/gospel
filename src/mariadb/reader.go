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
	"golang.org/x/time/rate"
)

// Reader is an interface for reading facts from a stream stored in MariaDB.
type Reader struct {
	// stmt is a prepared statement used to query for facts.
	// It accepts two parameters, stream offset and limit.
	stmt *sql.Stmt

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

	// globalLimit is a rate-limiter that limits the number of polling queries that
	// can be performed each second. It is shared by all readers, and hence
	// provides a global cap of the number of read queries per second.
	globalLimit *rate.Limiter

	// adaptiveLimit is a rate-limiter that is adjusted on-the-fly in an attempt
	// to perform polling queries just often enough to keep the lookahead
	// buffer full.
	adaptiveLimit *rate.Limiter

	// minAdaptiveLimit is the absolute minimum polling frequency, and therefore
	// sets the maximum possible latency before a reader is delivered new facts.
	minAdaptiveLimit rate.Limit

	bias int

	// prevPollEmpty is true if the previous database poll did not return any
	// facts. It is only used to mute consecutive debug messages if there are no
	// facts to report.
	prevPollEmpty bool

	prevFPS  time.Time
	fps      float64
	fpsCount int
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

	lookahead := getLookahead(opts)

	r := &Reader{
		logger:           logger,
		opts:             opts,
		facts:            make(chan gospel.Fact, lookahead),
		done:             make(chan error, 1),
		ctx:              runCtx,
		cancel:           cancel,
		addr:             addr,
		globalLimit:      limit,
		adaptiveLimit:    rate.NewLimiter(limit.Limit(), 1),
		minAdaptiveLimit: rate.Every(3 * time.Second), // TODO: make into configuration option
		prevFPS:          time.Now(),
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

		// r.logger.Debug(
		// 	"[reader %p] advanced to '%s', next is '%s'",
		// 	r,
		// 	r.current,
		// 	r.next,
		// )
	default:
		// assume next is literally the next fact on the stream
		nx = r.current.Addr.Next()

		// r.logger.Debug(
		// 	"[reader %p] advanced to '%s', next is not yet available",
		// 	r,
		// 	r.current,
		// )
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

// init creates r.stmt, an SQL prepared statement used to poll
// for new facts.
func (r *Reader) init(ctx context.Context, db *sql.DB, storeID uint64) error {
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
			"[reader %p] opened at %s, global poll limit: %.02f/s, min adaptive poll limit: %.02f/s, lookahead: %d, event-type filter: %s",
			r,
			r.addr,
			float64(r.globalLimit.Limit()),
			float64(r.minAdaptiveLimit),
			getLookahead(r.opts),
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
	if err := r.globalLimit.Wait(r.ctx); err != nil {
		return err
	}

	if err := r.adaptiveLimit.Wait(r.ctx); err != nil {
		return err
	}

	limit := cap(r.facts) - len(r.facts) + 1
	// if limit == 0 {
	// 	limit = 1
	// }
	// limit := getLookahead(r.opts)

	count, err := r.poll(limit)
	if err != nil {
		return err
	}

	r.fpsCount++

	_, diff := r.adjustRate(count, limit)

	if r.logger.IsDebug() {
		if (count != 0 || !r.prevPollEmpty) || // only log first empty poll if nothing else changed
			diff != 0 { // log poll rate changes

			flag := ""
			if count == 0 {
				flag = "***************"
			}

			elapsed := time.Since(r.prevFPS)
			if elapsed >= time.Second {
				r.fps = float64(r.fpsCount) / elapsed.Seconds()
				r.fpsCount = 0
				r.prevFPS = time.Now()
			}

			message := fmt.Sprintf(
				"[reader %p] next: %s, fetched: %d/%d, enqueued: %d/%d, adaptive poll rate: %.02f/s, actual poll rate: %.02f/s %s",
				r,
				r.addr,
				count,
				limit,
				len(r.facts),
				cap(r.facts),
				r.adaptiveLimit.Limit(),
				r.fps,
				flag,
			)

			// if diff != 0 {
			// 	polarity := "increased"
			// 	if diff < 0 {
			// 		polarity = "decreased"
			// 	}
			//
			// 	message += fmt.Sprintf(
			// 		", %s adaptive poll rate to %.02f/s",
			// 		polarity,
			// 		lim,
			// 	)
			// }

			r.logger.DebugString(message)
		}

		// if count == 0 {
		// 	r.prevPollEmpty = true
		// }
	}

	return nil
}

// adjustRate updates the adaptive poll rate in an attempt to balance database
// poll frequency with latency.
func (r *Reader) adjustRate(count, limit int) (lim rate.Limit, diff rate.Limit) {
	var delta int

	if count < 1 {
		delta = -1
		if r.bias > 0 {
			r.bias = 0
		}
	} else if count == limit {
		delta = +count
		if r.bias < 0 {
			r.bias = 0
		}
	} else if count > 1 {
		delta = +1
		if r.bias < 0 {
			r.bias = 0
		}
	} else {
		return
	}

	bias := r.bias + delta
	step := rate.Limit(0.01)
	rateDelta := rate.Limit(bias) * step

	lim, diff = r.setPollRate(
		r.adaptiveLimit.Limit() + rateDelta,
	)

	if diff != 0 {
		r.bias = bias
	}

	return
}

func (r *Reader) setPollRate(lim rate.Limit) (rate.Limit, rate.Limit) {
	min := r.minAdaptiveLimit
	max := r.globalLimit.Limit()

	if lim < min {
		lim = min
	} else if lim > max {
		lim = max
	}

	prev := r.adaptiveLimit.Limit()

	if lim != prev {
		r.adaptiveLimit.SetLimit(lim)
	}
	return lim, lim - prev
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
