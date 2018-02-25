package mariadb

import (
	"context"
	"database/sql"

	"github.com/jmalloc/gospel/src/gospel"
	"github.com/jmalloc/gospel/src/internal/driver"
	"github.com/jmalloc/gospel/src/internal/logging"
	"github.com/jmalloc/twelf/src/twelf"
	"golang.org/x/time/rate"
)

// EventStore an interface for reading and writing streams of events stored in
// a MariaDB database.
type EventStore struct {
	// db is the pool of MariaDB connections used by the event stores and the
	// readers it creates.
	db *sql.DB

	// id and store are the auto-increment ID and name of the store, respectively.
	id    uint64
	store string

	// rlimit is a rate-limiter that limits the number of polling queries that
	// can be performed each second. It is shared by all readers, and hence
	// provides a global cap of the number of read queries per second.
	rlimit *rate.Limiter

	// logger is the logger to use for activity and debug logging.
	logger twelf.Logger
}

// Append atomically writes one or more events to the end of a stream,
// producing a contiguous block of facts.
//
// addr.Offset must refer to the next unused offset within the stream,
// otherwise the append fails, and IsConflict(err) returns true.
//
// nx is the address of the next unused offset after the facts have been
// appended.
//
// Append panics if ev is empty.
func (es *EventStore) Append(
	ctx context.Context,
	addr gospel.Address,
	ev ...gospel.Event,
) (gospel.Address, error) {
	err := es.append(ctx, &addr, ev, appendChecked)

	if err == nil {
		logging.AppendChecked(
			es.logger,
			gospel.Address{
				Stream: es.store + "::" + addr.Stream,
				Offset: addr.Offset,
			},
			ev,
		)
	} else if e, ok := err.(gospel.ConflictError); ok {
		logging.Conflict(es.logger, e)
	}

	return addr, err
}

// AppendUnchecked atomically writes one or more events to the end of a
// stream, producing a contiguous block of facts.
//
// Unlike Append(), the caller is not required to know the next unused
// offset of the stream, hence the offset is said to be "unchecked".
//
// nx is the address of the next unused offset after the facts have been
// appended.
//
// AppendUnchecked panics if ev is empty.
func (es *EventStore) AppendUnchecked(
	ctx context.Context,
	stream string,
	ev ...gospel.Event,
) (gospel.Address, error) {
	addr := gospel.Address{Stream: stream}
	err := es.append(ctx, &addr, ev, appendUnchecked)

	if err == nil {
		logging.AppendUnchecked(
			es.logger,
			gospel.Address{
				Stream: es.store + "::" + addr.Stream,
				Offset: addr.Offset,
			},
			ev,
		)
	}

	return addr, err
}

// Open returns a reader that begins reading facts at addr.
//
// ctx applies to the opening of the reader, and not to the reader itself.
func (es *EventStore) Open(
	ctx context.Context,
	addr gospel.Address,
	opts ...gospel.ReaderOption,
) (gospel.Reader, error) {
	return openReader(
		ctx,
		es.db,
		es.id,
		addr,
		es.rlimit,
		es.logger,
		driver.NewReaderOptions(opts),
	)
}

// append writes events to a stream using the given append strategy.
//
// If a deadlock occurs (which can occur for a single statement when using
// InnoDB!) the append is retried. There is no limit on the retries other than
// the context deadline.
func (es *EventStore) append(
	ctx context.Context,
	addr *gospel.Address,
	events []gospel.Event,
	strategy appendStrategy,
) error {
	if addr.Stream == "" {
		panic("can not append to the ε-stream")
	}

	count := len(events)

	if count == 0 {
		panic("no events provided")
	}

	for {
		err := atomicAppend(
			ctx,
			es.db,
			es.id,
			addr,
			events,
			strategy,
		)

		if !isDeadlock(err) {
			return err
		}
	}
}
