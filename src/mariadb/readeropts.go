package mariadb

import "github.com/jmalloc/gospel/src/internal/driver"

type lookaheadOptionKey struct{}

// DefaultLookahead is the default number of facts to 'pre-fetch' before they
// are required by a call to Reader.Next(). The Lookahead() reader option can be
// used to override this default on a per-reader basis.
const DefaultLookahead = 20

// Lookahead is a reader option specifies how many facts should be 'pre-fetched'
// before they are used by Next().
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
	la, ok := o.Get(lookaheadOptionKey{})
	if ok {
		return la.(int)
	}

	return DefaultLookahead
}
