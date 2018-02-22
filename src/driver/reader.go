package driver

// ReaderOptions is a struct that contains the options applied by
// streakdb.ReaderOption functions.
type ReaderOptions struct {
	EventTypes map[string]struct{} // nil == do not filter
	extra      map[interface{}]interface{}
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
