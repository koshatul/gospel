package streakdb

import "context"

// Reader is an interface for reading facts from a stream.
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
