package gospel

import (
	"github.com/jmalloc/gospel/src/internal/driver"
	"github.com/jmalloc/twelf/src/twelf"
)

// Option is a function that applies a common options to a ClientOptions
// struct.
type Option = driver.ClientOption

// Logger is an option that sets the logger to use.
func Logger(l twelf.Logger) Option {
	return func(o *driver.ClientOptions) {
		o.Logger = l
	}
}
