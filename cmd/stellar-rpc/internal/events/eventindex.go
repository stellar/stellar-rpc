package events

import (
	"iter"

	"github.com/RoaringBitmap/roaring/v2"
)

// EventIndex manages a chunk's term-to-bitmap index.
type EventIndex interface {
	// Add indexes one or more event IDs for the given (value, field) pair.
	Add(value []byte, field Field, eventIDs ...uint32) error

	// Lookup returns a roaring bitmap of event IDs matching (value, field).
	// Returns nil, nil if not found.
	// The caller must not mutate the returned bitmap.
	Lookup(value []byte, field Field) (*roaring.Bitmap, error)

	// All returns an iterator over all terms.
	// The caller must not mutate the yielded bitmaps.
	All() iter.Seq2[TermKey, *roaring.Bitmap]

	// Len returns the number of distinct terms.
	Len() int64

	// Close marks the index as immutable. Subsequent Add calls return an error.
	// Reads (Lookup, All, Len) remain valid. Idempotent.
	Close() error
}

// eventIndex is the default EventIndex implementation.
type eventIndex struct {
	store BitmapStore
}

// NewEventIndex creates an EventIndex backed by an in-memory BitmapStore.
func NewEventIndex() EventIndex {
	return &eventIndex{store: newMemBitmaps()}
}

// NewEventIndexWithStore creates an EventIndex with the given BitmapStore.
func NewEventIndexWithStore(store BitmapStore) EventIndex {
	return &eventIndex{store: store}
}

func (s *eventIndex) Add(value []byte, field Field, eventIDs ...uint32) error {
	key := ComputeTermKey(value, field)
	return s.store.AddTo(key, eventIDs...)
}

func (s *eventIndex) Lookup(value []byte, field Field) (*roaring.Bitmap, error) {
	key := ComputeTermKey(value, field)
	return s.store.Get(key)
}

func (s *eventIndex) All() iter.Seq2[TermKey, *roaring.Bitmap] {
	return s.store.All()
}

func (s *eventIndex) Len() int64 {
	return s.store.Len()
}

func (s *eventIndex) Close() error {
	return s.store.Close()
}
