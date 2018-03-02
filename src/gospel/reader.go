package gospel

import (
	"context"

	"github.com/jmalloc/gospel/src/internal/options"
)

// Reader is an interface for reading facts from a stream in order.
type Reader interface {
	// Next blocks until the next fact is available for reading or ctx is
	// canceled.
	//
	// If err is nil, the "current" fact is ready to be returned by Get().
	//
	// nx is the offset within the stream that the reader has reached. It can be
	// used to efficiently resume reading in a future call to EventStore.Open().
	//
	// Note that nx is not always the address immediately following the fact
	// returned by Get() - it may be "further ahead" in the stream, thus
	// skipping over any facts that the reader is not interested in.
	Next(ctx context.Context) (nx Address, err error)

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
	TryNext(ctx context.Context) (nx Address, ok bool, err error)

	// Get returns the "current" fact.
	//
	// It panics if Next() or TryNext() has not returned successfully at least
	// once. Get() returns the same Fact until Next() or TryNext() succeeds again.
	Get() Fact

	// Close closes the reader.
	Close() error
}

// ReaderOption is a function that applies a reader option to a ReaderOptions
// struct.
type ReaderOption = options.ReaderOption

// FilterByEventType is a reader option that limits the reader to facts with
// events of a specific type.
//
// Multiple FilterByEventType options can be combined to expand the list of
// allowed types.
func FilterByEventType(types ...string) ReaderOption {
	return func(o *options.ReaderOptions) {
		o.FilterByEventType = true
		o.EventTypes = append(o.EventTypes, types...)
	}
}
