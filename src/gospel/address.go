package gospel

import "strconv"

// Address identifies a fact within a stream.
type Address struct {
	// Stream is the name of the stream that contains the fact.
	//
	// Each stream is an ordered set of facts. Facts are produced by appending
	// events to a named stream.
	//
	// A special read-only stream with a zero-length name called the ε-stream
	// (pronounced epsilon) contains facts for every event appended to the store,
	// along with facts describing events within the store itself, such as the
	// creation of new streams or the archiving of facts.
	Stream string

	// Offset is the zero-based position of the fact within the stream.
	Offset uint64
}

// Next returns the address immediately following a.
func (a Address) Next() Address {
	a.Offset++
	return a
}

func (a Address) String() string {
	s := a.Stream
	if s == "" {
		s = epsilon
	}

	return s + `+` + strconv.FormatUint(a.Offset, 10)
}

// epsilon is the lowercase epsilon character, which is often used to denote
// the empty string. It is used in string representations of addresses that
// refer to the epsilon stream.
const epsilon = `ε`
