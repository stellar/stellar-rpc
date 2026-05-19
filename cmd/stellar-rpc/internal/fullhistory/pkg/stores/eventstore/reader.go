package eventstore

import (
	"context"
	"errors"
	"fmt"
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
// read methods (Lookup, LookupKeys, FetchEvents, All, EventCount,
// Offsets) after Close. ChunkID is the one exception — it returns
// its constructor-supplied value unchanged after Close.
var ErrClosed = errors.New("events: store is closed")

// ErrUnsortedEventIDs is returned by FetchEvents when the supplied
// eventIDs slice violates the sorted-ascending-no-duplicates
// precondition. Mirror of packfile.ErrPositionsUnsorted on the
// cold side; both surface the same shape so callers can errors.Is
// against this sentinel regardless of hot-vs-cold.
var ErrUnsortedEventIDs = errors.New("events: eventIDs must be sorted ascending with no duplicates")

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
	// Returns (0, ErrClosed) after Close. On ColdReader, the value
	// is read lazily from events.pack's trailer on first call.
	EventCount() (uint32, error)

	// Offsets exposes the ledger→eventID range cache. The coordinator
	// uses this to stitch a multi-ledger query range into chunk-relative
	// event-id ranges: call LedgerOffsets.EventIDs(ledger) per ledger
	// in the query, then union the per-ledger [start, end) ranges
	// before fetching events.
	//
	// Returns (nil, ErrClosed) after Close. Callers must not mutate
	// the returned value — implementations share a cached snapshot
	// across readers.
	Offsets() (*events.LedgerOffsets, error)

	// Lookup returns the bitmap of event IDs in this Chunk that
	// match the given term. Implementations clone (or freshly
	// construct) before returning so callers can intersect/union
	// freely.
	//
	// Returns (nil, ErrTermNotFound) when the term has no matching
	// events. Other errors signal corruption or internal failure.
	Lookup(key events.TermKey) (*roaring.Bitmap, error)

	// LookupKeys returns bitmaps for each key, aligned positionally
	// with the input slice (result[i] corresponds to keys[i]).
	// result[i] is nil if keys[i] has no matching events in this
	// chunk — the function does not return ErrTermNotFound for
	// individual misses.
	//
	// Equivalent to calling Lookup for each key (and treating
	// ErrTermNotFound as a nil result), but ColdReader coalesces
	// the underlying packfile reads into a single ReadItems pass,
	// fanning out across the worker count configured via
	// ColdReaderOptions.Concurrency. HotStore performs N in-memory
	// mirror clones — no I/O to batch.
	//
	// ctx cancels in-flight I/O on the cold path.
	LookupKeys(ctx context.Context, keys []events.TermKey) ([]*roaring.Bitmap, error)

	// FetchEvents decodes events for the supplied chunk-relative
	// eventIDs and returns them positionally aligned with the input
	// slice (result[i] corresponds to eventIDs[i]).
	//
	// eventIDs MUST be sorted ascending with no duplicates. The
	// coordinator iterating a bitmap intersection
	// (roaring.Bitmap.Iterator yields ascending) satisfies this for
	// free. Both implementations validate the precondition up front
	// and return a wrapped ErrUnsortedEventIDs on violation.
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

// validateSortedEventIDs returns a wrapped ErrUnsortedEventIDs if
// eventIDs contains a non-strictly-ascending pair. O(N), no
// allocation. Empty input is valid (caller short-circuits).
func validateSortedEventIDs(eventIDs []uint32) error {
	for i := 1; i < len(eventIDs); i++ {
		if eventIDs[i] <= eventIDs[i-1] {
			return fmt.Errorf("%w: position %d (%d) not greater than position %d (%d)",
				ErrUnsortedEventIDs, i, eventIDs[i], i-1, eventIDs[i-1])
		}
	}
	return nil
}
