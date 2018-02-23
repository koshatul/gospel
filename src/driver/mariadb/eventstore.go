package mariadb

import (
	"context"
	"database/sql"

	"github.com/jmalloc/streakdb/src/streakdb"
)

// EventStore an interface for reading and writing streams of events stored in
// a MariaDB database.
type EventStore struct {
	db   *sql.DB
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
	panic("not implemented")
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
	panic("not implemented")
}

// Open returns a reader that begins reading facts at addr.
func (es *EventStore) Open(
	addr streakdb.Address,
	opts ...streakdb.ReaderOption,
) (streakdb.Reader, error) {
	panic("not implemented")
}
