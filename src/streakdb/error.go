package streakdb

import "fmt"

// ConflictError indicates than an EventStore.Append() call failed because the
// addr argument did not refer to the next unused offset.
//
// IsConflict() is a convenience method for checking if an err is a
// ConflictError.
type ConflictError struct {
	Addr  Address
	Event Event
}

func (e ConflictError) Error() string {
	return fmt.Sprintf(
		"conflict occurred appending %s event at %s",
		e.Event,
		e.Addr,
	)
}

// IsConflict returns true if err indicates that an EventStore.Append() call
// failed because the addr argument did not refer to the next unused offset.
func IsConflict(err error) bool {
	_, ok := err.(ConflictError)
	return ok
}
