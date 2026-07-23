package eventstore

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
)

// Closed-store lifecycle: HotStore and ColdReader read methods
// (LookupKeys, FetchEvents, All, EventCount, Offsets) return
// stores.ErrStoreClosed after Close, per the storage/stores
// translation contract. ChunkID is the one exception — it returns
// its constructor-supplied value unchanged after Close.

// ErrUnsortedEventIDs is returned by FetchEvents when the supplied
// eventIDs slice violates the sorted-ascending-no-duplicates
// precondition. Mirror of packfile.ErrPositionsUnsorted on the
// cold side; both surface the same shape so callers can errors.Is
// against this sentinel regardless of hot-vs-cold.
var ErrUnsortedEventIDs = errors.New("events: eventIDs must be sorted ascending with no duplicates")

// ErrFetchRangeOutOfBounds is the canonical sentinel for "the
// requested [start, start+count) range falls outside [0, EventCount)
// for this chunk." Returned (wrapped) by validateFetchRange — the
// shared check both HotStore.FetchRange and ColdReader.FetchRange
// drive on entry. Mirrors the ErrUnsortedEventIDs shape so callers
// can errors.Is against a single sentinel regardless of hot/cold.
var ErrFetchRangeOutOfBounds = errors.New("events: FetchRange out of bounds")

// Reader is the unified read surface for one Chunk's events,
// implemented by both HotStore (RocksDB + in-memory caches) and
// ColdReader (mmap'd events.pack + index.pack + index.hash).
// Consumers like the query coordinator (PR-3c) work against this
// interface so they don't need to branch on hot-vs-cold beyond
// reader construction.
//
// All implementations return events in chunk-relative eventID
// order. EventIDs are dense in `[0, EventCount())`.
//
// Payload shape: both implementations yield Payloads carrying the raw
// ContractEvent XDR in ContractEventBytes (no struct decode on the read
// path); consumers read fields off it, e.g. via xdr.ContractEventView.
//
// Ownership differs by method. FetchEvents returns owned Payloads whose
// ContractEventBytes are safe to retain. FetchRange and All yield borrowed
// Payloads — ContractEventBytes aliases the reader's iteration buffer and
// is valid only until the next step; a consumer that retains one past the
// step must clone its ContractEventBytes. See events.Payload.Unmarshal for
// the alias contract.
type Reader interface {
	// ChunkID identifies which Chunk this Reader serves.
	ChunkID() chunk.ID

	// EventCount is the total number of events in this Chunk.
	// Equal to the last events.LedgerOffsets cumulative count.
	// Returns (0, stores.ErrStoreClosed) after Close. On ColdReader, the value
	// is read lazily from events.pack's trailer on first call.
	EventCount() (uint32, error)

	// Offsets returns a point-in-time *LedgerOffsets covering the
	// chunk. The coordinator uses this to stitch a multi-ledger query
	// range into chunk-relative event-id ranges: call EventIDs(ledger)
	// per ledger in the query, then union the per-ledger [start, end)
	// ranges before fetching events.
	//
	// Implementations:
	//   - HotStore returns a View sharing the live
	//     ConcurrentLedgerOffsets backing array, capped to the count
	//     visible at call time. A concurrent ingest may extend the
	//     underlying state after Offsets returns, but the returned
	//     view reflects what was visible at call time. Callers
	//     (Query) take the view once at entry and pass it through
	//     their helpers.
	//   - ColdReader returns the lazily-decoded LedgerOffsets cached
	//     on the reader; the same pointer is returned to every
	//     caller. Both paths must treat the returned value as
	//     read-only — mutation would corrupt either the live mirror
	//     (hot, indirectly via the view's backing slice) or every
	//     other reader holding the cached pointer (cold).
	//
	// Returns (nil, stores.ErrStoreClosed) after Close.
	Offsets() (*events.LedgerOffsets, error)

	// LookupKeys returns bitmaps for each key, aligned positionally
	// with the input slice (result[i] corresponds to keys[i]).
	// result[i] is nil if keys[i] has no matching events in this
	// chunk — a per-key miss is not an error.
	//
	// ColdReader coalesces the underlying packfile reads into a
	// single ReadItems pass, fanning out across the worker count
	// configured via ColdReaderOptions.Concurrency. HotStore returns
	// borrowed mirror references with no per-key Clone.
	//
	// Bitmap ownership: callers MUST treat returned bitmaps as
	// read-only. The hot path returns immutable snapshots of the
	// live mirror — ConcurrentBitmaps stores bitmap pointers via
	// atomic.Pointer COW, so a returned pointer will never be
	// mutated by anyone. The cold path returns freshly-unmarshaled
	// bitmaps logically owned by the caller. Either way callers
	// must not mutate; eventstore.Query is the only consumer today
	// and never mutates, and downstream roaring.FastAnd/FastOr never
	// mutate inputs.
	//
	// ctx cancels in-flight I/O on the cold path (MPHF load,
	// index.pack ReadAt); hot side checks ctx as a fast guard before
	// touching the in-memory mirror.
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
	// through LookupKeys, so a miss signals corruption or a
	// writer/reader mismatch, not a normal not-found case.
	FetchEvents(ctx context.Context, eventIDs []uint32) ([]events.Payload, error)

	// FetchRange streams count events starting at chunk-relative
	// event ID start, in ascending event-ID order. Equivalent to
	// FetchEvents over the dense ID range [start, start+count) but
	// without forcing the caller to materialize an []uint32 — and on
	// the cold path it dispatches to packfile.ReadRange directly
	// instead of going through the position-coalescing logic.
	//
	// Use this when the caller knows it wants a contiguous range
	// (match-all query, ledger-range query, full-chunk streaming).
	// Use FetchEvents when the IDs come from a bitmap intersection
	// and may be sparse.
	//
	// ctx cancels in-flight I/O on both paths. Yielding
	// (events.Payload{}, stores.ErrStoreClosed) and stopping is the after-Close
	// behavior, mirroring All.
	//
	// count == 0 is a no-op regardless of start (both implementations
	// short-circuit before bounds-checking). A non-zero count whose
	// range escapes [0, EventCount) yields a wrapped
	// ErrFetchRangeOutOfBounds and stops — callers cap count against
	// EventCount themselves.
	FetchRange(ctx context.Context, start, count uint32) iter.Seq2[events.Payload, error]

	// All streams every event in this Chunk in chunk-relative
	// eventID order without intermediate buffering. Equivalent to
	// FetchRange(ctx, 0, EventCount). (The freeze path does NOT use
	// this — it re-derives cold artifacts from raw LCMs.)
	// Each events.Payload carries its LedgerSequence, so consumers can
	// track ledger boundaries without separate signaling.
	All(ctx context.Context) iter.Seq2[events.Payload, error]
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

// validateFetchRange returns a wrapped ErrFetchRangeOutOfBounds if
// [start, start+count) falls outside [0, total). Uses uint64
// arithmetic to catch overflow on the upper bound. Shared between
// HotStore.FetchRange and ColdReader.FetchRange so the error
// message format and sentinel are identical.
func validateFetchRange(start, count, total uint32, chunkID chunk.ID) error {
	if uint64(start)+uint64(count) > uint64(total) {
		return fmt.Errorf("%w: chunk %s [%d, %d) exceeds count=%d",
			ErrFetchRangeOutOfBounds, chunkID,
			start, uint64(start)+uint64(count), total)
	}
	return nil
}
