package logging

import (
	"github.com/jmalloc/gospel/src/gospel"
	"github.com/jmalloc/twelf/src/twelf"
)

// AppendChecked logs new events being appended to a stream using
// EventStore.Append().
func AppendChecked(
	logger twelf.Logger,
	next gospel.Address,
	events []gospel.Event,
) {
	addr := next
	addr.Offset -= uint64(len(events))

	switch len(events) {
	case 1:
		logger.Log(
			"appended %s at %s (checked)",
			events[0],
			addr,
		)
	case 2:
		logger.Log(
			"appended %s and 1 more event at %s (checked)",
			events[0],
			addr,
		)
	default:
		logger.Log(
			"appended %s and %d more events at %s (checked)",
			events[0],
			len(events)-1,
			addr,
		)
	}
}

// AppendUnchecked logs new events being appended to a stream using
// EventStore.AppendUnchecked().
func AppendUnchecked(
	logger twelf.Logger,
	next gospel.Address,
	events []gospel.Event,
) {
	addr := next
	addr.Offset -= uint64(len(events))

	switch len(events) {
	case 1:
		logger.Log(
			"appended %s at %s (unchecked)",
			events[0],
			addr,
		)
	case 2:
		logger.Log(
			"appended %s and 1 more event at %s (unchecked)",
			events[0],
			addr,
		)
	default:
		logger.Log(
			"appended %s and %d more events at %s (unchecked)",
			events[0],
			len(events)-1,
			addr,
		)
	}
}

// Conflict logs an append that failed due to a conflict.
func Conflict(
	logger twelf.Logger,
	err gospel.ConflictError,
) {
	logger.Log(
		"conflict appending %s at %s",
		err.Event,
		err.Addr,
	)
}
