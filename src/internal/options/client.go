package options

import "github.com/jmalloc/twelf/src/twelf"

// ClientOptions is a struct that contains the options applied by ClientOption
// functions.
type ClientOptions struct {
	Logger twelf.Logger
	extra  map[interface{}]interface{}
}

// ClientOption is a function that applies a reader option to a ClientOptions
// struct.
type ClientOption func(o *ClientOptions)

// NewClientOptions returns a new ClientOptions struct with opts applied.
func NewClientOptions(opts []ClientOption) *ClientOptions {
	o := &ClientOptions{}

	for _, fn := range opts {
		fn(o)
	}

	if o.Logger == nil {
		o.Logger = &twelf.StandardLogger{}
	}

	return o
}

// Get returns the non-standard option associated with k.
func (o *ClientOptions) Get(k interface{}) (interface{}, bool) {
	v, ok := o.extra[k]
	return v, ok
}

// Set associates the non-standard option v with k.
func (o *ClientOptions) Set(k, v interface{}) {
	if o.extra == nil {
		o.extra = map[interface{}]interface{}{}
	}

	o.extra[k] = v
}
