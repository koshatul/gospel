package streakdb

import (
	"context"

	"github.com/jmalloc/streakdb/src/driver"
)

// Reader is an interface for reading facts from a stream in order.
type Reader interface {
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
	Next(ctx context.Context) (addr Address, err error)

	// Get returns the "current" fact.
	//
	// It panics if Next() has not been called.
	// Get() returns the same Fact until Next() is called again.
	Get() Fact

	// Close closes the reader.
	Close() error
}

// ReaderOption is a function that applies a reader option to a ReaderOptions
// struct.
type ReaderOption func(o *driver.ReaderOptions)

// FilterByEventType is a reader option that limits the reader to facts with
// events of a specific type.
//
// Multiple FilterByEventType options can be combined to expand the list of
// allowed types.
func FilterByEventType(types ...string) ReaderOption {
	return func(o *driver.ReaderOptions) {
		if o.EventTypes == nil {
			o.EventTypes = map[string]struct{}{}
		}

		for _, t := range types {
			o.EventTypes[t] = struct{}{}
		}
	}
}
