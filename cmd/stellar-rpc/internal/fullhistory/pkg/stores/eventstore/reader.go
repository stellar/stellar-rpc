package eventstore

import (
	"context"
	"errors"
	"iter"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ErrTermNotFound is the canonical "this term has no matching events
// in this Chunk" sentinel returned by all Reader implementations.
// Callers use errors.Is(err, ErrTermNotFound) to distinguish a clean
// no-match from real errors (closed store, decode failure, etc.).
//
// HotStore returns this when the in-memory mirror has no entry for
// the key. ColdReader returns this when streamhash fast-no-matches
// the key OR when the index.pack fingerprint mismatches the key
// (a residual MPHF collision).
var ErrTermNotFound = errors.New("events: term not found in chunk")

// ErrClosed is the canonical "this reader/store has been closed"
// sentinel for the package. Returned by HotStore and ColdReader
// read methods (Lookup, FetchEvents, All) after Close.
//
// Metadata accessors (ChunkID, EventCount, Offsets) survive Close
// and never return ErrClosed — they read values cached at Open.
var ErrClosed = errors.New("events: store is closed")

// Reader is the unified read surface for one Chunk's events,
// implemented by both HotStore (RocksDB + in-memory caches) and
// ColdReader (mmap'd events.pack + index.pack + index.hash).
// Consumers like the query coordinator (PR-3c) work against this
// interface so they don't need to branch on hot-vs-cold beyond
// reader construction.
//
// All implementations return events in chunk-relative eventID
// order. EventIDs are dense in `[0, EventCount())`.
type Reader interface {
	// ChunkID identifies which Chunk this Reader serves.
	ChunkID() chunk.ID

	// EventCount is the total number of events in this Chunk.
	// Equal to the last events.LedgerOffsets cumulative count.
	EventCount() uint32

	// Offsets exposes the ledger→eventID range cache. The coordinator
	// uses this to stitch a multi-ledger query range into chunk-relative
	// event-id ranges: call LedgerOffsets.EventIDs(ledger) per ledger
	// in the query, then union the per-ledger [start, end) ranges
	// before fetching events.
	//
	// Survives Close — populated at Open and never mutated. Callers
	// must not mutate the returned value.
	Offsets() *events.LedgerOffsets

	// Lookup returns the bitmap of event IDs in this Chunk that
	// match the given term. Implementations clone (or freshly
	// construct) before returning so callers can intersect/union
	// freely.
	//
	// Returns (nil, ErrTermNotFound) when the term has no matching
	// events. Other errors signal corruption or internal failure.
	Lookup(key events.TermKey) (*roaring.Bitmap, error)

	// FetchEvents decodes events for the supplied chunk-relative
	// eventIDs and returns them positionally aligned with the input
	// slice (result[i] corresponds to eventIDs[i]).
	//
	// eventIDs MUST be sorted ascending with no duplicates. The
	// coordinator iterating a bitmap intersection
	// (roaring.Bitmap.Iterator yields ascending) satisfies this for
	// free. Behavior is undefined otherwise — the cold path will
	// return a wrapped packfile.ErrPositionsUnsorted, the hot path
	// reads in the given order, but callers must not rely on either.
	//
	// ctx cancels in-flight I/O; the cold path checks ctx between
	// scattered-read batches, the hot path checks between Gets.
	//
	// A missing row is an error: eventIDs only reach this path
	// through Lookup, so a miss signals corruption or a
	// writer/reader mismatch, not a normal not-found case.
	FetchEvents(ctx context.Context, eventIDs []uint32) ([]events.Payload, error)

	// All streams every event in this Chunk in chunk-relative
	// eventID order. The freeze loop uses this to dump a hot Chunk
	// into a Writer without intermediate buffering.
	// Each events.Payload carries its LedgerSequence, so consumers can
	// track ledger boundaries without separate signaling.
	All() iter.Seq2[events.Payload, error]

	// Close releases any resources the Reader holds. Idempotent.
	// After Close, Lookup / FetchEvents / All return ErrClosed.
	// Metadata accessors (ChunkID, EventCount, Offsets) survive
	// Close — see each impl's docstring for details.
	Close() error
}
