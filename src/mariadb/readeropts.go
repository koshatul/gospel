package mariadb

import (
	"time"

	"github.com/jmalloc/gospel/src/internal/driver"
)

const (
	// DefaultReadBufferSize is the default read-buffer size for each reader.
	// It is used if no specific size is set via ReadBufferSize().
	DefaultReadBufferSize = 100

	// DefaultAcceptableLatency is the default value "acceptable latency" value
	// for each reader. It is used if no specific value is set via
	// AcceptableLatency().
	DefaultAcceptableLatency = 200 * time.Millisecond

	// StarvationLatencyFactor is used to compute the "starvation latency"
	// setting for a reader if no specific value is set via StarvationLatency().
	//
	//     <starvation latency> = <acceptable latency> * StarvationLatencyFactor
	StarvationLatencyFactor = 10
)

// readerOptionKey is a custom type used to ensure that MariaDB-specific keys
// can not clash with custom options from other systems.
type readerOptionKey int

const (
	readBufferKey readerOptionKey = iota
	acceptableLatencyKey
	starvationLatencyKey
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

	return DefaultReadBufferSize
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

// StarvationLatency is a reader option that set the amount of latency that is
// acceptable once the reader has reached the end of the stream and is "starving"
// for facts.
//
// The setting is ignored if latency is less than the acceptable latency value.
func StarvationLatency(latency time.Duration) driver.ReaderOption {
	if latency < 0 {
		latency = 0
	}

	return func(o *driver.ReaderOptions) {
		o.Set(starvationLatencyKey, latency)
	}
}

// getStarvationLatency returns the acceptable latency to use for the given
// reader options, falling back to the default if necessary.
func getStarvationLatency(o *driver.ReaderOptions) time.Duration {
	acceptable := getAcceptableLatency(o)

	if v, ok := o.Get(starvationLatencyKey); ok {
		latency := v.(time.Duration)

		if latency >= acceptable {
			return latency
		}
	}

	return acceptable * StarvationLatencyFactor
}
