package mariadb

import "github.com/jmalloc/gospel/src/internal/driver"

const (
	// DefaultReadBuffer is the default read-buffer size used if no reader-specific
	// value is set via ReadBufferSize().
	DefaultReadBuffer = 100
)

// readerOptionKey is a custom type used to ensure that MariaDB-specific keys
// can not clash with custom options from other systems.
type readerOptionKey int

const (
	readBufferKey readerOptionKey = iota
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

// GetReadBufferSize returns the read-buffer size to use given the options o,
// falling back to the default if necessary.
func GetReadBufferSize(o *driver.ReaderOptions) uint {
	if v, ok := o.Get(readBufferKey); ok {
		return v.(uint)
	}

	return DefaultReadBuffer
}
