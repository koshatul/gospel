package gospelmaria

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/jmalloc/gospel/src/gospel"
	"github.com/jmalloc/gospel/src/internal/metrics"
	"github.com/jmalloc/gospel/src/internal/options"
	"github.com/jmalloc/twelf/src/twelf"
	"golang.org/x/time/rate"
)

const (
	// averageLatencyAge is average age of samples to keep when computing the
	// average latency. A sample is taken after each poll.
	//
	// Averages are computed using an exponentially-weighted moving average.
	// See https://github.com/VividCortex/ewma for more information.
	averageLatencyAge = 20.0
)

// Reader is an interface for reading facts from a stream stored in MariaDB.
type Reader struct {
	// stmt is a prepared statement used to query for facts.
	// It accepts the stream offset as a parameter.
	stmt *sql.Stmt

	// logger is the target for debug logging. Readers do not perform general
	// activity logging.
	logger twelf.Logger

	// facts is a channel on which facts are delivered to the caller of Next().
	// A worker goroutine polls the database and delivers the facts to this
	// channel.
	facts chan gospel.Fact

	// current is the fact returned by Get() until Next() is called again.
	current *gospel.Fact

	// next is the fact that will become "current" when Next() is called.
	// If it is nil, no additional facts were available in the buffer on the
	// previous call to Next().
	next *gospel.Fact

	// end is a signaling channel that is closed when the database polling
	// goroutine fetches 0 facts.
	end chan struct{}

	// done is a signaling channel which is closed when the database polling
	// goroutine returns. The error that caused the closure, if any, is sent to
	// the channel before it closed. This means a pending call to Next() will
	// return the error when it first occurs, but subsequent calls will return
	// a more generic "reader is closed" error.
	done chan error

	// ctx is a context that is canceled when Close() is called, or when the
	// database polling goroutine returns. It is used to abort any in-progress
	// database queries or rate-limit pauses when the reader is closed.
	//
	// Context cancellation errors are not sent to the 'done' channel, so any
	// pending Next() call will receive a generic "reader is closed" error.
	ctx    context.Context
	cancel func()

	// addr is the starting address for the next database poll.
	addr gospel.Address

	// globalLimit is a rate-limiter that limits the number of polling queries
	// that can be performed each second. It is shared by all readers, and hence
	// provides a global cap of the number of read queries per second.
	globalLimit *rate.Limiter

	// adaptiveLimit is a rate-limiter that is adjusted on-the-fly in an attempt
	// to balance the number of database polls against the latency of facts.
	// It is not shared by other readers.
	adaptiveLimit *rate.Limiter

	// acceptableLatency is the amount of latency that is generally acceptable
	// for the purposes of this reader. The reader will attempt to maintain this
	// latency by adjusting its polling rate.
	acceptableLatency time.Duration

	// starvationLatency is the amount of latency that is acceptable once the
	// reader has reached the end of the stream and is "starving" for facts.
	// This setting informs the minimum poll rate.
	starvationLatency time.Duration

	// instantaneousLatency is the latency computed from the facts returend by
	// the most recent database poll. If there are no facts the latency is 0.
	instantaneousLatency time.Duration

	// averageLatency tracks the average latency of the last 10 database polls.
	// The average latency is weighed against the acceptableLatency and
	// starvationLatency values to decide how the poll rate is adjusted.
	averageLatency ewma.MovingAverage

	// debug contains several properties that are only relevant when the reader
	// is using a debug logger.
	debug *readerDebug
}

// readerDebug contains several properties that are only relevant when the
// reader is using a debug logger.
type readerDebug struct {
	// opts is the options specified when opening the reader.
	opts *options.ReaderOptions

	// averagePollRate keeps track of the average polling rate, which can be
	// substantially lower than the adaptive limit for slow readers.
	averagePollRate *metrics.RateCounter

	// averageFactRate keeps track of the average rate of delivery of facts.
	averageFactRate *metrics.RateCounter

	// previousPollRate is compared to the poll rate after each poll to
	// determine whether a log message should be displayed.
	previousPollRate rate.Limit

	// muteEmptyPolls is true if the previous database poll did not return any
	// facts. It is only used to mute repeated debug messages if there is no new
	// information to report.
	muteEmptyPolls bool
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
	opts *options.ReaderOptions,
) (*Reader, error) {
	// Note that runCtx is NOT derived from ctx, which is only used for the
	// opening of the reader itself.
	runCtx, cancel := context.WithCancel(context.Background())

	accetableLatency := getAcceptableLatency(opts)

	r := &Reader{
		logger:            logger,
		facts:             make(chan gospel.Fact, getReadBufferSize(opts)),
		end:               make(chan struct{}),
		done:              make(chan error, 1),
		ctx:               runCtx,
		cancel:            cancel,
		addr:              addr,
		globalLimit:       limit,
		adaptiveLimit:     rate.NewLimiter(rate.Every(accetableLatency), 1),
		acceptableLatency: accetableLatency,
		starvationLatency: getStarvationLatency(opts),
		averageLatency:    ewma.NewMovingAverage(averageLatencyAge),
	}

	if logger.IsDebug() {
		r.debug = &readerDebug{
			opts:            opts,
			averagePollRate: metrics.NewRateCounter(),
			averageFactRate: metrics.NewRateCounter(),
		}
	}

	if err := r.prepareStatement(ctx, db, storeID, opts); err != nil {
		return nil, err
	}

	r.logInitialization()

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
	nx, _, err = r.tryNext(ctx, nil)
	return nx, err
}

// TryNext blocks until the next fact is available for reading, the end of
// stream is reached, or ctx is canceled.
//
// If ok is true, a new fact is available and is ready to be returned by
// Get(). ok is false if the current fact is the last known fact in the
// stream.
//
// nx is the offset within the stream that the reader has reached. It can be
// used to efficiently resume reading in a future call to EventStore.Open().
// nx is invalid if ok is false.
func (r *Reader) TryNext(ctx context.Context) (nx gospel.Address, ok bool, err error) {
	return r.tryNext(ctx, r.end)
}

func (r *Reader) tryNext(ctx context.Context, end <-chan struct{}) (nx gospel.Address, ok bool, err error) {
	if r.next == nil {
		select {
		case f := <-r.facts:
			r.current = &f
			ok = true
		case <-end:
			// no fact is available, return with ok == false
			return
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
		ok = true
	}

	// Perform a non-blocking lookahead to see if we have the next fact already.
	select {
	case f := <-r.facts:
		r.next = &f
		nx = r.next.Addr
	default:
		// assume next is literally the next fact on the stream
		nx = r.current.Addr.Next()
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

// prepareStatement creates r.stmt, an SQL prepared statement used to poll
// for new facts.
func (r *Reader) prepareStatement(
	ctx context.Context,
	db *sql.DB,
	storeID uint64,
	opts *options.ReaderOptions,
) error {
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
			e.body,
			CURRENT_TIMESTAMP(6)
		FROM fact AS f
		INNER JOIN event AS e
		ON e.id = f.event_id
		%s
		WHERE f.store_id = %d
			AND f.stream = %s
			AND f.offset >= ?
		ORDER BY offset
		LIMIT %d`,
		filter,
		storeID,
		escapeString(r.addr.Stream),
		cap(r.facts),
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

	count, err := r.poll()
	if err != nil {
		return err
	}

	r.adjustRate()
	r.logPoll(count)

	return nil
}

// fetch queries the database for facts beginning at r.addr.
func (r *Reader) poll() (int, error) {
	rows, err := r.stmt.QueryContext(
		r.ctx,
		r.addr.Offset,
	)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	f := gospel.Fact{
		Addr: r.addr,
	}

	count := 0

	var first, now time.Time

	for rows.Next() {
		if err := rows.Scan(
			&f.Addr.Offset,
			&f.Time,
			&f.Event.EventType,
			&f.Event.ContentType,
			&f.Event.Body,
			&now,
		); err != nil {
			return count, err
		}

		select {
		case r.facts <- f:
		case <-r.ctx.Done():
			return count, r.ctx.Err()
		}

		r.addr = f.Addr.Next()

		// keep the time of the first fact in the result  to compute the maximum
		// instantaneous latency for this poll.
		if count == 0 {
			first = f.Time
		}

		count++

		if r.debug != nil {
			r.debug.averageFactRate.Tick()
		}
	}

	// TODO: this doesn't account for the time spent waiting to write to r.facts.
	r.instantaneousLatency = now.Sub(first)
	r.averageLatency.Add(r.instantaneousLatency.Seconds())

	if count == 0 {
		select {
		case r.end <- struct{}{}:
		default:
		}
	}

	return count, nil
}

// setRate sets the adaptive polling rate, capped between the mininum (set by
// r.starvationLatency) and the maximum (set by the global rate limit).
func (r *Reader) setRate(lim rate.Limit) bool {
	min := rate.Every(r.starvationLatency)
	max := r.globalLimit.Limit()

	if lim < min {
		lim = min
	} else if lim > max {
		lim = max
	}

	prev := r.adaptiveLimit.Limit()

	if lim != prev {
		r.adaptiveLimit.SetLimit(lim)
		return true
	}

	return false
}

// adjustRate updates the adaptive poll rate in an attempt to balance database
// poll frequency with latency.
func (r *Reader) adjustRate() bool {
	latency := r.effectiveLatency()

	// headroom is the difference between the acceptable latency and the
	// effective latency. If the headroom is positive, we're doing 'better' than
	// the acceptable latency and can backoff the poll rate.
	headroom := r.acceptableLatency - latency

	// don't back off if our headroom is less than 25%
	// if headroom > 0 && headroom < r.acceptableLatency/25 {
	// 	return false
	// }

	// Get the current rate in terms of an interval.
	currentInterval := metrics.RateToDuration(
		r.adaptiveLimit.Limit(),
	)

	return r.setRate(
		rate.Every(currentInterval + headroom),
	)
}

// effectiveLatency returns the latency used to adjust the poll rate.
//
// The rolling average needs to be primed with several samples before the
// average is available, until then it reports zero, in which case the
// instantaneousLatency value is used instead.
func (r *Reader) effectiveLatency() time.Duration {
	latency := r.averageLatency.Value()

	if latency == 0 {
		return r.instantaneousLatency
	}

	return time.Duration(
		latency * float64(time.Second),
	)
}

// logInitialization logs a debug message describing the reader settings.
func (r *Reader) logInitialization() {
	if !r.logger.IsDebug() {
		return
	}

	filter := "*"
	if r.debug.opts.FilterByEventType {
		filter = strings.Join(r.debug.opts.EventTypes, ", ")
	}

	r.logger.Debug(
		"[reader %p] %s | global poll limit: %s | acceptable latency: %s | starvation latency: %s | read-buffer: %d | filter: %s",
		r,
		r.addr,
		formatRate(r.globalLimit.Limit()),
		formatDuration(r.acceptableLatency),
		formatDuration(r.starvationLatency),
		getReadBufferSize(r.debug.opts),
		filter,
	)
}

// logPoll logs a debug message containing metrics for the previous poll and
// adjustments to the adaptive poll rate.
func (r *Reader) logPoll(count int) {
	if r.debug == nil {
		return
	}

	r.debug.averagePollRate.Tick()

	pollRate := r.adaptiveLimit.Limit()

	if pollRate == r.debug.previousPollRate &&
		count == 0 && r.debug.muteEmptyPolls {
		return
	}

	r.debug.muteEmptyPolls = count == 0

	r.logger.Debug(
		"[reader %p] %s | fetch: %3d %s | queue: %3d/%3d | adaptive poll: %s | avg poll: %s | latency: %s",
		r,
		r.addr,
		count,
		formatRate(rate.Limit(r.debug.averageFactRate.Rate())),
		len(r.facts),
		cap(r.facts),
		formatRate(r.adaptiveLimit.Limit()),
		formatRate(rate.Limit(r.debug.averagePollRate.Rate())),
		formatDuration(r.effectiveLatency()),
	)

	r.debug.previousPollRate = pollRate
}

// formatRate formats a rate limit for display in reader debug logs.
func formatRate(r rate.Limit) string {
	if r == 0 {
		//     "500.00/s   2.00ms"
		return "  ?.??/s   ?.??µs"
	}

	d := metrics.RateToDuration(r)

	return fmt.Sprintf(
		"%6.02f/s %s",
		r,
		formatDuration(d),
	)
}

// formatDuration formats a duration for display in reader debug logs.
func formatDuration(d time.Duration) string {
	if d >= time.Hour {
		return fmt.Sprintf("%6.02fh ", d.Seconds()/3600)
	} else if d >= time.Minute {
		return fmt.Sprintf("%6.02fm ", d.Seconds()/60)
	} else if d >= time.Second {
		return fmt.Sprintf("%6.02fs ", d.Seconds())
	} else if d >= time.Millisecond {
		return fmt.Sprintf("%6.02fms", d.Seconds()/time.Millisecond.Seconds())
	}

	return fmt.Sprintf("%6.02fµs", d.Seconds()/time.Microsecond.Seconds())
}
