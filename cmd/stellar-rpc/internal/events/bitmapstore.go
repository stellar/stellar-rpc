package events

import (
	"iter"

	"github.com/RoaringBitmap/roaring/v2"
)

// BitmapStore is a key-value store for term bitmaps.
// A term is a unique (field, value) pair identifying a contract ID or
// topic value. Each term maps to a bitmap of event IDs that match it.
//
// Implementations must be safe for concurrent use by a single writer
// and multiple readers.
type BitmapStore interface {
	// Get returns the bitmap for a term key. Returns nil, nil if not found.
	Get(key TermKey) (*roaring.Bitmap, error)

	// Put stores a bitmap for a term key.
	Put(key TermKey, bm *roaring.Bitmap) error

	// AddTo adds one or more event IDs to the bitmap for a term key.
	// Creates the bitmap if it does not exist.
	AddTo(key TermKey, eventIDs ...uint32) error

	// Delete removes a term key from the store.
	Delete(key TermKey) error

	// All returns an iterator over all terms.
	All() iter.Seq2[TermKey, *roaring.Bitmap]

	// Len returns the number of entries.
	Len() int64

	// Close releases resources.
	Close() error
}
