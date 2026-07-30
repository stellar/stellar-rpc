package events

import (
	"github.com/RoaringBitmap/roaring/v2"
)

// Bitmaps is the in-memory event index for single-threaded
// build-then-freeze paths (cold backfill ingest feeding
// WriteColdIndex).
//
// As a named map type, it supports the natural map operations
// (`for term, bm := range b`, `len(b)`, `b[term]`) directly — no
// All/Len/Get methods exist or are needed. The only behavior the
// type adds is AddTo, which handles the bitmap allocation +
// AddMany sequence callers would otherwise duplicate.
//
// NOT safe for concurrent access. The caller guarantees serial
// access (build then hand off, or a single goroutine throughout).
// For the concurrent-reader-vs-single-writer case (live HotStore
// ingest) use ConcurrentBitmaps.
type Bitmaps map[TermKey]*roaring.Bitmap

// NewBitmaps returns an empty Bitmaps. Equivalent to make(Bitmaps).
func NewBitmaps() Bitmaps {
	return make(Bitmaps)
}

// AddTo records each eventID under key. Idempotent — roaring's
// AddMany dedupes against the existing set, so retried ingests of
// the same (key, eventID) pair are silently skipped.
//
// The eventIDs slice may be in any order; AddMany handles
// insertion. For monotonically-ordered batches (the cold backfill
// ingest pattern), AddMany takes its fast path.
func (b Bitmaps) AddTo(key TermKey, eventIDs ...uint32) {
	bm, ok := b[key]
	if !ok {
		bm = roaring.New()
		b[key] = bm
	}
	bm.AddMany(eventIDs)
}
