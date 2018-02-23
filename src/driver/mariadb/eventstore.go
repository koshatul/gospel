package mariadb

import (
	"context"
	"database/sql"

	"github.com/jmalloc/streakdb/src/driver"
	"github.com/jmalloc/streakdb/src/streakdb"
	"golang.org/x/time/rate"
)

// EventStore an interface for reading and writing streams of events stored in
// a MariaDB database.
type EventStore struct {
	// db is the pool of MariaDB connections used by the event stores and the
	// readers it creates.
	db *sql.DB

	// name is the name of the store.
	name string
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
	addr streakdb.Address,
	ev ...streakdb.Event,
) (streakdb.Address, error) {
	err := es.append(ctx, &addr, ev, appendChecked)
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
	ev ...streakdb.Event,
) (streakdb.Address, error) {
	addr := streakdb.Address{Stream: stream}
	err := es.append(ctx, &addr, ev, appendUnchecked)
	return addr, err
}

// Open returns a reader that begins reading facts at addr.
func (es *EventStore) Open(
	addr streakdb.Address,
	opts ...streakdb.ReaderOption,
) (streakdb.Reader, error) {
	var o driver.ReaderOptions

	for _, fn := range opts {
		fn(&o)
	}

	if o.EventTypes != nil {
		panic("event type filtering is not supported")
	}

	return newReader(
		es.db,
		rate.NewLimiter(rate.Inf, 1), // TODO: use reader option
		es.name,
		10, // lookahead -- TODO: use reader option
		addr,
	), nil
}

// append writes events to a stream using the given append strategy.
//
// If a deadlock occurs (which can occur for a single statement when using
// InnoDB!) the append is retried. There is no limit on the retries other than
// the context deadline.
func (es *EventStore) append(
	ctx context.Context,
	addr *streakdb.Address,
	events []streakdb.Event,
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
			es.name,
			addr,
			events,
			strategy,
		)

		if !isDeadlock(err) {
			return err
		}
	}
}
