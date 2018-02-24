package driver

// ReaderOptions is a struct that contains the options applied by
// streakdb.ReaderOption functions.
type ReaderOptions struct {
	FilterByEventType bool
	EventTypes        []string
	extra             map[interface{}]interface{}
}

// ReaderOption is a function that applies a reader option to a ReaderOptions
// struct.
type ReaderOption func(o *ReaderOptions)

// NewReaderOptions returns a new ReaderOptions struct with opts applied.
func NewReaderOptions(opts []ReaderOption) *ReaderOptions {
	o := &ReaderOptions{}

	for _, fn := range opts {
		fn(o)
	}

	return o
}

// Get returns the non-standard option associated with k.
func (o *ReaderOptions) Get(k interface{}) (interface{}, bool) {
	v, ok := o.extra[k]
	return v, ok
}

// Set associates the non-standard option v with k.
func (o *ReaderOptions) Set(k, v interface{}) {
	if o.extra == nil {
		o.extra = map[interface{}]interface{}{}
	}

	o.extra[k] = v
}
