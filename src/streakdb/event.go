package streakdb

// Event is an application-defined event.
//
// Events are appended to a named stream to produce facts.
type Event struct {
	// EventType is the kind of event that occurred.
	//
	// It is typically expressed as a human-readable verb in the past-tense,
	// but may be any non-empty string.
	EventType string

	// ContentType is the format of the data in the event body.
	//
	// It is typically a standard MIME type such as "application/json", or a
	// custom "vendor" MIME type which includes event schema and/or versioning
	// information such as "application/vnd.mycompany.some-event.v1+json".
	ContentType string

	// Body is application-defined binary data containing the specifics of the
	// event.
	Body []byte
}

func (f Event) String() string {
	return f.EventType + `!`
}
