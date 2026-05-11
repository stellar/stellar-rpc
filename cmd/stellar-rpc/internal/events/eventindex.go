package events

import (
	"iter"

	"github.com/RoaringBitmap/roaring/v2"
)

// EventIndex maps (field, value) terms to roaring bitmaps of event IDs
// for a single chunk. Each (field, value) pair is identified internally
// by a 16-byte TermKey; callers do not interact with TermKey directly
// except when iterating via All.
//
// Implementations must be safe for one writer and multiple readers.
type EventIndex interface {
	// Add indexes one or more event IDs for the given (value, field) pair.
	Add(value []byte, field Field, eventIDs ...uint32) error

	// Lookup returns a roaring bitmap of event IDs matching (value, field).
	// Returns nil, nil if not found. Callers must not mutate the returned bitmap.
	Lookup(value []byte, field Field) (*roaring.Bitmap, error)

	// All returns an iterator over all terms.
	// Callers must not mutate the yielded bitmaps.
	All() iter.Seq2[TermKey, *roaring.Bitmap]

	// Len returns the number of distinct terms.
	Len() int64

	// Close marks the index as immutable. Subsequent Add calls return an error.
	// Reads (Lookup, All, Len) remain valid. Idempotent.
	Close() error
}

// NewEventIndex returns an in-memory EventIndex.
func NewEventIndex() EventIndex {
	return newMemBitmaps()
}
