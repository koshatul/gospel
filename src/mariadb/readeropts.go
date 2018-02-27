package mariadb

import "github.com/jmalloc/gospel/src/internal/driver"

// readerOptionKey is a custom string used to ensure that MariaDB-specific keys
// can not clash with custom options from other systems.
type readerOptionKey string

const readBufferKey readerOptionKey = "read-buffer"

// DefaultReadBuffer is the default read-buffer size used if no reader-specific
// value is set via ReadBufferSize().
const DefaultReadBuffer = 100

// ReadBufferSize is a reader option that sets the number of facts to buffer
// in memory before a call to Next().
func ReadBufferSize(n int) driver.ReaderOption {
	if n < 0 {
		panic("read buffer size can not be negative")
	}

	return func(o *driver.ReaderOptions) {
		o.Set(readBufferKey, n)
	}
}

// getReadBufferSize returns the read-buffer size to use given the options o,
// falling back to the default if necessary.
func getReadBufferSize(o *driver.ReaderOptions) int {
	if v, ok := o.Get(readBufferKey); ok {
		n := v.(int)

		// lookahead at least until the 'next' fact.
		if n < 2 {
			return 2
		}

		return n
	}

	return DefaultReadBuffer
}
