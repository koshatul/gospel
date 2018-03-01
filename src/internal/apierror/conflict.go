package apierror

import (
	"fmt"

	"github.com/jmalloc/gospel/src/gospel"
)

// ConflictError indicates than an EventStore.Append() call failed because the
// addr argument did not refer to the next unused offset.
//
// gospel.IsConflict() is a convenience method for checking if an err is a
// ConflictError.
type ConflictError struct {
	addr  gospel.Address
	event gospel.Event
}

// NewConflict returns a new ConflictError, which implements gospel.ConflictError.
func NewConflict(addr gospel.Address, ev gospel.Event) ConflictError {
	return ConflictError{addr, ev}
}

// ConflictDetails returns the address at which the conflict occurred and the
// event that failed to append.
func (e ConflictError) ConflictDetails() (gospel.Address, gospel.Event) {
	return e.addr, e.event
}

func (e ConflictError) Error() string {
	return fmt.Sprintf(
		"conflict occurred appending %s event at %s",
		e.event,
		e.addr,
	)
}
