package streakdb

import "time"

// Fact is an event that has been appended to a stream.
type Fact struct {
	// Addr identifies the fact by its stream and position within that stream.
	Addr Address

	// Time is the time at which the fact was created.
	//
	// This does not necessarily correlate with the time at which the event
	// occurred within the application.
	//
	// This field is informational only. Its exact value is implementation
	// specific, and may not increase monotonically. For example, if the system
	// time is changed.
	Time time.Time

	// Event is the application-defined event data.
	Event Event
}

func (f Fact) String() string {
	return f.Event.String() + f.Addr.String()
}
