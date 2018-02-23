package streakdb

import "context"

// EventStore is an interface for reading and writing streams of events.
type EventStore interface {
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
	Append(ctx context.Context, addr Address, ev ...Event) (nx Address, err error)

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
	AppendUnchecked(ctx context.Context, stream string, ev ...Event) (nx Address, err error)

	// Open returns a reader that begins reading facts at addr.
	Open(addr Address, opts ...ReaderOption) (Reader, error)
}
