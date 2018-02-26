package mariadb

import "github.com/jmalloc/gospel/src/internal/driver"

type lookaheadOptionKey struct{}

// DefaultLookahead is the lookahead hint.
// See the Lookahead() reader option.
const DefaultLookahead = 20

// Lookahead is a reader option that specifies a hint as to how many facts
// should be buffered before they are required by a call to Reader.Next().
//
// A higher lookahead will reduce read latency at the expensse of memory.
func Lookahead(n int) driver.ReaderOption {
	if n < 0 {
		panic("lookahead can not be negative")
	}

	return func(o *driver.ReaderOptions) {
		o.Set(lookaheadOptionKey{}, n)
	}
}

// getLookahead returns the lookahead to use given the options o, falling back
// to the default if necessary.
func getLookahead(o *driver.ReaderOptions) int {
	v, ok := o.Get(lookaheadOptionKey{})
	if ok {
		n := v.(int)

		// lookahead at least until the 'next' fact.
		if n < 2 {
			return 2
		}

		return n
	}

	return DefaultLookahead
}
