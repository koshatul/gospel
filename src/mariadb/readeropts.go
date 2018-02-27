package mariadb

import (
	"time"

	"github.com/jmalloc/gospel/src/internal/driver"
)

const (
	// DefaultReadBuffer is the default read-buffer size for each reader.
	// It is used if no specific size is set via ReadBufferSize().
	DefaultReadBuffer = 100

	// DefaultAcceptableLatency is the default value "acceptable latency" value.
	// It is used if no specific value is set via AcceptableLatency().
	DefaultAcceptableLatency = 200 * time.Millisecond
)

// readerOptionKey is a custom type used to ensure that MariaDB-specific keys
// can not clash with custom options from other systems.
type readerOptionKey int

const (
	readBufferKey readerOptionKey = iota
	acceptableLatencyKey
)

// ReadBufferSize is a reader option that sets the number of facts to buffer
// in memory before a call to Next().
//
// The minimum read-buffer size is 2.
func ReadBufferSize(n uint) driver.ReaderOption {
	if n < 2 {
		n = 2
	}

	return func(o *driver.ReaderOptions) {
		o.Set(readBufferKey, n)
	}
}

// getReadBufferSize returns the read-buffer size to use for the given reader
// options, falling back to the default if necessary.
func getReadBufferSize(o *driver.ReaderOptions) uint {
	if v, ok := o.Get(readBufferKey); ok {
		return v.(uint)
	}

	return DefaultReadBuffer
}

// AcceptableLatency is a reader option that set the amount of latency that is
// generally acceptable for the purposes of the reader. The reader will attempt
// to maintain this latency by adjusting its polling rate against the average
// latency of the delivered facts.
func AcceptableLatency(latency time.Duration) driver.ReaderOption {
	if latency < 0 {
		latency = 0
	}

	return func(o *driver.ReaderOptions) {
		o.Set(acceptableLatencyKey, latency)
	}
}

// getAcceptableLatency returns the acceptable latency to use for the given
// reader options, falling back to the default if necessary.
func getAcceptableLatency(o *driver.ReaderOptions) time.Duration {
	if v, ok := o.Get(acceptableLatencyKey); ok {
		return v.(time.Duration)
	}

	return DefaultAcceptableLatency
}
