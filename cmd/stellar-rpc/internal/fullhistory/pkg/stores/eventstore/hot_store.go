package eventstore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"math"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/linxGnu/grocksdb"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
)

// Column-family names used inside one chunk's hot RocksDB DB. The
// per-Chunk DB directory encodes the chunk ID, so the CF names
// themselves carry no chunk suffix.
const (
	DataCF    = "events_data"
	IndexCF   = "events_index"
	OffsetsCF = "events_offsets"
)

// Per-CF tuning for the hot store, passed via rocksdb.Config.PerCFOptions:
//
//   - DataCF holds XDR-encoded event payloads: compressible (zstd
//     typically 2-3× on XDR) and read in batches via
//     BatchedMultiGetCF. Larger blocks give zstd more context per
//     compression unit and align with batch-fetch shapes.
//   - IndexCF stores 20-byte (term_hash || event_id) keys with
//     empty values — nothing in the values to compress, and small
//     blocks reduce wasted I/O per random Lookup miss (each Lookup
//     reads one block to find one key).
//   - OffsetsCF stores 8-byte (ledger_seq -> event_count) rows in
//     the tens-of-thousands per chunk — same shape as IndexCF.
const (
	dataCFBlockSize    = 32 * 1024
	indexCFBlockSize   = 4 * 1024
	offsetsCFBlockSize = 4 * 1024
)

func hotStoreCFOptions() map[string]rocksdb.CFOptions {
	return map[string]rocksdb.CFOptions{
		DataCF: {
			Compression: grocksdb.ZSTDCompression,
			BlockSize:   dataCFBlockSize,
		},
		IndexCF:   {BlockSize: indexCFBlockSize},
		OffsetsCF: {BlockSize: offsetsCFBlockSize},
	}
}

// CFNames returns the three CFs this facade owns. Exported so the hotchunk
// shared-DB opener can register them alongside the other CFs (decision (a)).
func CFNames() []string { return []string{DataCF, IndexCF, OffsetsCF} }

// CFOptions returns this facade's per-CF options. Exported so the hotchunk
// opener merges them into the shared per-chunk DB's PerCFOptions.
func CFOptions() map[string]rocksdb.CFOptions { return hotStoreCFOptions() }

const (
	dataKeyLen   = 4      // event_id (chunk encoded by per-Chunk DB directory)
	indexKeyLen  = 16 + 4 // term hash || event_id
	offsetKeyLen = 4      // ledger_seq
	offsetValLen = 4      // per-ledger event count (uint32 BE)
)

// ErrLedgerOutOfRange is returned by IngestLedgerToBatch when the
// supplied ledger sequence falls outside the chunk's [FirstLedger,
// LastLedger] window.
var ErrLedgerOutOfRange = errors.New("events: ledger outside chunk range")

// ErrLedgerOutOfOrder is returned by IngestLedgerToBatch when the
// supplied ledger sequence is not the next-expected one. Catches
// duplicate ingest of an already-committed ledger as well as gaps
// (skipping ahead). Both would silently corrupt the per-ledger
// offset chain if not rejected up front.
var ErrLedgerOutOfOrder = errors.New("events: ledger out of order")

// HotStore wraps one chunk's hot RocksDB DB plus the in-memory term mirror and
// ledger-offset cache that feed the query path.
//
// Atomicity: the per-Chunk DB is the source of truth. IngestLedgerToBatch queues
// data + index + offsets into one atomic batch, then (post-commit) the apply
// hook updates the in-memory mirrors; warmup reconstructs them from the on-disk
// CFs on next startup.
//
// Concurrency:
//
//   - Writes (IngestLedgerToBatch) are single-writer (one goroutine per chunk).
//   - Reads (Lookup, FetchEvents, All) take NO HotStore-level lock — they guard
//     via chunkStore.IsClosed() and rely on the mirror's internal locks and
//     RocksDB's thread-safety.
//   - Metadata split after the caller-owned store is closed: ChunkID is
//     infallible (cached, usable post-close); EventCount and
//     Offsets return ErrClosed after close (Reader-interface contract).
type HotStore struct {
	chunkStore *rocksdb.Store
	chunkID    chunk.ID
	mirror     *events.ConcurrentBitmaps
	offsets    *events.ConcurrentLedgerOffsets
}

// Compile-time guard: *HotStore satisfies Reader.
var _ Reader = (*HotStore)(nil)

// NewWithStore wraps an ALREADY-OPEN rocksdb.Store as an events HotStore on the
// three events CFs (CFNames()), running the mandatory warmup to rebuild the
// in-memory mirror + offsets. The store is owned by the caller — in production,
// hotchunk.DB composes this facade over the shared per-chunk DB and closes that
// DB once. The store must have CFNames() registered + CFOptions() applied.
// A warmup failure returns the error WITHOUT closing the caller-owned store.
func NewWithStore(store *rocksdb.Store, chunkID chunk.ID) (*HotStore, error) {
	mirror, offsets, err := warmup(store, chunkID)
	if err != nil {
		return nil, fmt.Errorf("events: warmup chunk %s: %w", chunkID, err)
	}
	return &HotStore{
		chunkStore: store,
		chunkID:    chunkID,
		mirror:     mirror,
		offsets:    offsets,
	}, nil
}

// ChunkID returns the chunk this store serves.
func (h *HotStore) ChunkID() chunk.ID { return h.chunkID }

// EventCount is the total number of events committed to this Chunk
// so far. Equal to the next event-id IngestLedgerToBatch would assign.
// Returns (0, ErrClosed) after the caller-owned store is closed. The Reader interface signature
// is fallible to accommodate ColdReader's lazy metadata load; on the
// hot side the value is always live and the error is only ErrClosed.
func (h *HotStore) EventCount() (uint32, error) {
	if h.chunkStore.IsClosed() {
		return 0, ErrClosed
	}
	return h.offsets.TotalEvents(), nil
}

// Offsets returns a point-in-time view of the ledger-offset cache.
// The coordinator uses this to stitch a multi-ledger query range
// into chunk-relative event-id ranges (see Reader.Offsets).
//
// Implementation: returns a *LedgerOffsets sharing the live
// backing array, capped at the count visible at call time
// (~24-byte allocation per Query). A concurrent IngestLedgerToBatch
// may extend the backing past the cap, but the returned view's
// slice stays bounded to what was visible when Offsets returned.
// Callers (Query) take the view once at entry and pass it through
// their helpers.
//
// Read-only: the returned view's underlying slice shares memory
// with the live backing array. Calling Append on the view would
// silently fork it from the live data; the contract is read-only.
//
// Returns (nil, ErrClosed) after the caller-owned store is closed.
func (h *HotStore) Offsets() (*events.LedgerOffsets, error) {
	if h.chunkStore.IsClosed() {
		return nil, ErrClosed
	}
	return h.offsets.View(), nil
}

// index returns the in-memory term mirror. Test-only write hook: no
// production path reads it. The live-chunk freeze re-derives the cold
// event index from raw LCMs (see backfill), so it never snapshots this
// mirror. Kept unexported until #772 decides whether the v2 read path
// hooks a snapshot here.
func (h *HotStore) index() *events.ConcurrentBitmaps { return h.mirror }

// Lookup returns the bitmap of event IDs in this Chunk that match
// the given term. The returned bitmap is an immutable snapshot of
// the live mirror — writers publish new pointers via atomic.Store
// (see ConcurrentBitmaps), so the caller never observes a mutating
// bitmap. Callers MUST NOT mutate it themselves. See Reader.Lookup
// and ConcurrentBitmaps.Get for the full contract. Returns
// (nil, ErrTermNotFound) when the term has no matching events.
// Returns (nil, ErrClosed) after the caller-owned store is closed.
//
// ctx is checked as a fast guard but the hot path does no blocking
// I/O — the bitmap comes from the in-memory mirror.
func (h *HotStore) Lookup(ctx context.Context, key events.TermKey) (*roaring.Bitmap, error) {
	if h.chunkStore.IsClosed() {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	bm, err := h.mirror.Get(key)
	if err != nil {
		return nil, err
	}
	if bm == nil {
		return nil, ErrTermNotFound
	}
	return bm, nil
}

// LookupKeys returns bitmaps for each key, aligned positionally with
// the input slice. result[i] is nil if keys[i] has no matching
// events. See Reader.LookupKeys for the semantics — in particular
// the borrowed-bitmap contract (callers must not mutate).
//
// Hot-side implementation is N in-memory mirror lookups — no I/O
// to batch — but exposing this method satisfies the Reader
// interface so callers can program against batched lookups
// uniformly.
func (h *HotStore) LookupKeys(ctx context.Context, keys []events.TermKey) ([]*roaring.Bitmap, error) {
	if h.chunkStore.IsClosed() {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}
	results := make([]*roaring.Bitmap, len(keys))
	for i, key := range keys {
		bm, err := h.mirror.Get(key)
		if err != nil {
			return nil, fmt.Errorf("events: LookupKeys for chunk %s: %w", h.chunkID, err)
		}
		results[i] = bm // nil for misses — Get already returns nil bitmap for not-found
	}
	return results, nil
}

// FetchEvents decodes the events_data row for each provided eventID
// and returns them positionally aligned with the input slice. See
// Reader.FetchEvents for the sorted-input precondition.
//
// Implementation: validates eventIDs are sorted ascending with no
// duplicates (returns wrapped ErrUnsortedEventIDs otherwise — same
// shape as the cold side), encodes them to BE-uint32 keys, then
// calls rocksdb.Store.BatchMultiGet once with sortedInput=true.
// The batched API crosses CGO a single time regardless of key count
// and enables async_io so the kernel can overlap SST page reads —
// a meaningful win on EBS / high-random-latency storage. ctx is
// honored at the top of the call; the underlying CGO call is not
// cancellable mid-flight.
//
// A missing row is an error: eventIDs only reach this path through
// Lookup, which only returns IDs the mirror knows about — implying
// RocksDB also has them. A miss indicates corruption or a
// writer/reader mismatch, not a normal not-found case.
//
// After the caller-owned store is closed, returns ErrClosed.
func (h *HotStore) FetchEvents(ctx context.Context, eventIDs []uint32) ([]events.Payload, error) {
	if h.chunkStore.IsClosed() {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(eventIDs) == 0 {
		return nil, nil
	}
	if err := validateSortedEventIDs(eventIDs); err != nil {
		return nil, err
	}

	keys := make([][]byte, len(eventIDs))
	for i, id := range eventIDs {
		keys[i] = encodeDataKey(id)
	}
	values, err := h.chunkStore.BatchMultiGet(DataCF, keys)
	if err != nil {
		return nil, fmt.Errorf("events: batch fetch from chunk %s: %w", h.chunkID, err)
	}
	// BatchMultiGet guarantees len(values) == len(keys); the assertion
	// keeps gosec quiet on the index reads below and surfaces any future
	// wrapper-contract regression loudly rather than as a slice panic.
	if len(values) != len(eventIDs) {
		return nil, fmt.Errorf("events: BatchMultiGet returned %d values for %d keys in chunk %s",
			len(values), len(eventIDs), h.chunkID)
	}

	results := make([]events.Payload, len(eventIDs))
	for i, id := range eventIDs {
		v := values[i]
		if v == nil {
			return nil, fmt.Errorf("events: event %d missing from chunk %s", id, h.chunkID)
		}
		// BatchMultiGet already copies out of rocksdb's pinned pages
		// (see rocksdb.Store.BatchMultiGet); v is Go-owned and outlives
		// the returned Payload, so Unmarshal's alias is safe without
		// an extra clone.
		if err := results[i].Unmarshal(v); err != nil {
			return nil, fmt.Errorf("events: decode event %d from chunk %s: %w", id, h.chunkID, err)
		}
	}
	return results, nil
}

// FetchRange streams count events starting at chunk-relative event
// ID start, in ascending eventID order. See Reader.FetchRange for
// semantics; the hot path drives rocksdb.Store.IterateRange over
// DataCF with start and end keys derived from encodeDataKey.
//
// Yielded Payloads are borrowed: ContractEventBytes aliases the iteration
// buffer and is valid only until the next step — clone to retain.
//
// After the caller-owned store is closed, yields (zero Payload, ErrClosed) and stops.
// ctx is checked at entry and between iterator steps —
// rocksdb.Store.IterateRange does not itself accept a ctx, so a
// very slow Next() can block past a cancellation until the next
// yielded entry observes the cancel.
//
// Out-of-range arguments yield an error and stop:
//   - count == 0 is a natural no-op (no yields).
//   - start+count > the committed event count (overflow-safe via uint64)
//     yields a wrapped out-of-bounds error.
//   - A short scan (fewer DataCF rows than count) yields a wrapped
//     error after the partial stream — the CF should be dense in
//     [0, committed count), so a hole indicates corruption.
func (h *HotStore) FetchRange(ctx context.Context, start, count uint32) iter.Seq2[events.Payload, error] {
	return func(yield func(events.Payload, error) bool) {
		if h.chunkStore.IsClosed() {
			yield(events.Payload{}, ErrClosed)
			return
		}
		if err := ctx.Err(); err != nil {
			yield(events.Payload{}, err)
			return
		}
		if count == 0 {
			return
		}
		if err := validateFetchRange(start, count, h.offsets.TotalEvents(), h.chunkID); err != nil {
			yield(events.Payload{}, err)
			return
		}

		startKey := encodeDataKey(start)
		endKey := encodeDataKey(start + count - 1) // inclusive
		yielded := uint32(0)
		for entry, err := range h.chunkStore.IterateRange(DataCF, startKey, endKey) {
			if err != nil {
				yield(events.Payload{}, fmt.Errorf("events: scan chunk %s: %w", h.chunkID, err))
				return
			}
			if err := ctx.Err(); err != nil {
				yield(events.Payload{}, err)
				return
			}
			var p events.Payload
			// entry.Value is a zero-copy ref into the IterateRange
			// iterator buffer, valid only for this step; Unmarshal aliases
			// it into p.ContractEventBytes, so the yielded Payload is
			// borrowed (see the FetchRange doc). A retaining consumer clones.
			if err := p.Unmarshal(entry.Value); err != nil {
				yield(events.Payload{}, fmt.Errorf("events: decode event from chunk %s: %w",
					h.chunkID, err))
				return
			}
			if !yield(p, nil) {
				return
			}
			yielded++
		}
		if yielded != count {
			yield(events.Payload{}, fmt.Errorf(
				"events: FetchRange short scan for chunk %s: got %d of %d events at [%d, %d)",
				h.chunkID, yielded, count, start, start+count))
		}
	}
}

// All streams every event in this Chunk in chunk-relative eventID
// order. Used by the freeze loop to dump a hot Chunk into a
// ColdWriter without buffering. Thin wrapper over FetchRange; its
// yielded Payloads are likewise borrowed (valid only for the step).
//
// The committed event count is read inside the returned closure body, so a
// concurrent ingest between r.All(ctx) returning the Seq2 and the
// consumer's first range step is included in the snapshot.
//
// After the caller-owned store is closed, yields (zero Payload, ErrClosed) and stops.
func (h *HotStore) All(ctx context.Context) iter.Seq2[events.Payload, error] {
	return func(yield func(events.Payload, error) bool) {
		// FetchRange stops iterating after yielding an error; we
		// just forward whatever it yields and exit on the same step.
		for p, err := range h.FetchRange(ctx, 0, h.offsets.TotalEvents()) {
			if !yield(p, err) {
				return
			}
		}
	}
}

// IngestLedgerToBatch validates one ledger's events, marshals them, and queues
// their CF Puts into the SHARED batch b, returning the post-commit apply hook the
// caller runs AFTER b commits (decision (a)). Validation + term derivation happen
// before any Put; on any error Store.Batch discards the whole WriteBatch, so a
// rejected ledger never leaves committed rows behind.
//
// payloads is produced by events.LCMViewToPayloads, which emits each ledger's
// events in ascending getEvents cursor order — write order here IS the cursor
// contract (event IDs are assigned by arrival position). Terms are derived via
// events.TermsForBytes on each payload's ContractEventBytes.
//
// Sequence validation, before any Put or mirror mutation:
//
//   - ledgerSeq must lie within [chunkID.FirstLedger(), chunkID.LastLedger()] —
//     out-of-range returns ErrLedgerOutOfRange.
//   - ledgerSeq must equal the next expected ledger (StartLedger + LedgerCount).
//     Under decision (a) resume is always MaxCommittedSeq+1, so a non-expected
//     ledger is a mis-sequencing source (the ingestion loop's seq guard should
//     have caught it) — an error (ErrLedgerOutOfOrder), never silent tolerance.
//
// Post-batch atomicity: once the batch commits, the apply hook's in-memory
// mirror + offsets updates are infallible by construction. Any failure there
// panics rather than returning an error, because a returned error would leave
// on-disk state ahead of in-memory state with no clean recovery short of
// close + reopen.
func (h *HotStore) IngestLedgerToBatch(
	b *rocksdb.BatchWriter, ledgerSeq uint32, payloads []events.Payload,
) (func(), error) {
	// Validate BEFORE any Put. On error Store.Batch discards the whole WriteBatch,
	// so a mid-loop failure never orphans rows — no separate staging buffer needed.
	if ledgerSeq < h.chunkID.FirstLedger() || ledgerSeq > h.chunkID.LastLedger() {
		return nil, fmt.Errorf("%w: ledger %d not in chunk %s [%d, %d]",
			ErrLedgerOutOfRange, ledgerSeq, h.chunkID,
			h.chunkID.FirstLedger(), h.chunkID.LastLedger())
	}
	expected := h.offsets.StartLedger() + uint32(h.offsets.LedgerCount()) //nolint:gosec
	if ledgerSeq != expected {
		return nil, fmt.Errorf("%w: expected ledger %d, got %d",
			ErrLedgerOutOfOrder, expected, ledgerSeq)
	}

	// Derive term keys per payload up front (a TermsForBytes error rejects the
	// ledger without any Put) and retain them for the post-commit mirror update.
	termKeys := make([][]events.TermKey, len(payloads))
	for i := range payloads {
		keys, err := events.TermsForBytes(payloads[i].ContractEventBytes)
		if err != nil {
			return nil, fmt.Errorf("derive terms for payload %d in ledger %d: %w", i, ledgerSeq, err)
		}
		termKeys[i] = keys
	}

	startID := h.offsets.TotalEvents()
	if uint64(startID)+uint64(len(payloads)) > math.MaxUint32 {
		return nil, fmt.Errorf("chunk %s would overflow uint32 event-id space at ledger %d",
			h.chunkID, ledgerSeq)
	}

	// Marshal + queue each event directly into b. BatchWriter.Put copies
	// synchronously, so ONE reused scratch buffer serves every event — the caller
	// opens exactly one batch per ledger, so no row must outlive this call.
	var scratch []byte
	for i := range payloads {
		blob, err := payloads[i].MarshalInto(scratch[:0])
		if err != nil {
			return nil, fmt.Errorf("marshal payload %d for ledger %d: %w", i, ledgerSeq, err)
		}
		scratch = blob
		eventID := startID + uint32(i) //nolint:gosec // i < len(payloads), overflow-guarded above
		b.Put(DataCF, encodeDataKey(eventID), blob)
		for _, key := range termKeys[i] {
			b.Put(IndexCF, encodeIndexKey(key, eventID), nil)
		}
	}
	//nolint:gosec // len bounded by the overflow guard above
	b.Put(OffsetsCF, encodeOffsetKey(ledgerSeq), encodeLedgerEventCount(uint32(len(payloads))))

	return func() { h.applyLedger(startID, termKeys) }, nil
}

// applyLedger updates the mirror + offsets for a ledger whose rows are durable.
// Infallible by construction (IngestLedgerToBatch validated seq under the
// single-writer contract); the only non-completion is a crash, after which warmup
// rebuilds.
//
// Ordering invariant: mirror BEFORE offsets. A concurrent Query that snapshots
// offsets then reads the mirror must see either the prior state or a consistent
// later one. Reversing it would let a reader see an offsets count including IDs
// the mirror hasn't published — FetchEvents would then miss them, silently.
func (h *HotStore) applyLedger(startID uint32, termKeys [][]events.TermKey) {
	// Batch by key so each AddTo clones at most once per (key, ledger), not per
	// (key, event) — turns N COW clones into 1 for popular terms. Cap 64 ≈ a few
	// × unique-terms per ledger; the map grows past that.
	perKeyIDs := make(map[events.TermKey][]uint32, 64)
	for i, keys := range termKeys {
		eventID := startID + uint32(i) //nolint:gosec // i < len(termKeys), overflow-guarded in IngestLedgerToBatch
		for _, key := range keys {
			perKeyIDs[key] = append(perKeyIDs[key], eventID)
		}
	}
	for key, ids := range perKeyIDs {
		h.mirror.AddTo(key, ids...)
	}
	//nolint:gosec // len bounded by IngestLedgerToBatch's overflow guard
	h.offsets.Append(uint32(len(termKeys)))
}

// ──────────────────────────────────────────────────────────────────
// Warmup — reconstructs the in-memory mirror + offsets from the
// per-Chunk DB's on-disk CFs. Called by NewWithStore.
// ──────────────────────────────────────────────────────────────────

// warmup rebuilds the in-memory mirrors for chunkID by prefix-scanning
// the chunk's two on-disk caches once each:
//
//   - events_index  → *events.ConcurrentBitmaps — every
//     (events.TermKey, eventID) row replayed into a fresh in-memory
//     bitmap mirror.
//   - events_offsets → *events.ConcurrentLedgerOffsets — every
//     (ledger_seq, per_ledger_count) row replayed into a fresh
//     offset cache.
//
// chunkID seeds events.ConcurrentLedgerOffsets.StartLedger for empty
// chunks; on-disk rows carry the full ledger sequence themselves.
// Both mirrors are empty for fresh chunks.
func warmup(
	chunkStore *rocksdb.Store, chunkID chunk.ID,
) (*events.ConcurrentBitmaps, *events.ConcurrentLedgerOffsets, error) {
	mirror, indexUpperBound, err := warmupIndex(chunkStore)
	if err != nil {
		return nil, nil, err
	}
	offsets, err := warmupOffsets(chunkStore, chunkID)
	if err != nil {
		return nil, nil, err
	}
	if err := verifyChunkConsistency(chunkStore, offsets.TotalEvents(), indexUpperBound); err != nil {
		return nil, nil, err
	}
	return mirror, offsets, nil
}

// verifyChunkConsistency cross-checks the three on-disk CFs after warmup,
// turning a torn or tampered chunk into a loud open failure instead of a
// silently inconsistent in-memory cache. The CFs are written in one
// atomic batch, so under normal operation these invariants always hold;
// a violation means a bug or external corruption.
//
//   - the index may not reference an event the offsets don't account for:
//     indexUpperBound (max indexed event ID + 1, 0 if none) <= total.
//   - the data tail matches total: event total-1 present (when total > 0)
//     and no data row at any id >= total. Together those pin the max data
//     id to exactly total-1 — one Get plus one bounded seek.
//
// Not detected here: interior data holes (a missing id within 0..total-2,
// masked by a higher present id), under-indexed terms, and wrong
// per-ledger boundaries — each would need a full scan. The atomic batch
// makes all of them impossible for the writer; an interior hole that did
// appear (corruption/tamper) is caught lazily by FetchRange's short-scan
// check on first read. This is a cheap open-time tripwire on denormalized
// state, not load-bearing correctness.
func verifyChunkConsistency(chunkStore *rocksdb.Store, total, indexUpperBound uint32) error {
	if indexUpperBound > total {
		return fmt.Errorf("events: corrupt chunk: index references event %d but only %d committed",
			indexUpperBound-1, total)
	}
	if total > 0 {
		_, ok, err := chunkStore.Get(DataCF, encodeDataKey(total-1))
		if err != nil {
			return fmt.Errorf("events: verify data tail: %w", err)
		}
		if !ok {
			return fmt.Errorf("events: corrupt chunk: offsets count %d but event %d missing from data",
				total, total-1)
		}
	}
	// Nothing may live at or beyond total. The bounded seek lands on the
	// first such row if one exists; reaching the loop body at all (with no
	// iteration error) means an orphan is present — at total or far past it.
	for _, err := range chunkStore.IterateRange(DataCF, encodeDataKey(total), nil) {
		if err != nil {
			return fmt.Errorf("events: verify data tail: %w", err)
		}
		return fmt.Errorf("events: corrupt chunk: data present at id >= committed count %d", total)
	}
	return nil
}

// warmupIndex scans the events_index CF and replays every
// (events.TermKey, eventID) row into a fresh events.ConcurrentBitmaps.
// Design doc §12 step 3.
//
// Implementation: build into a single-threaded events.Bitmaps via
// per-term batching (rocksdb's byte-sorted iteration delivers all
// rows for term K consecutively, so a small buffer flushes when the
// term changes), then convert to ConcurrentBitmaps at the end. This
// avoids paying the per-row Clone cost the concurrent ConcurrentBitmaps.AddTo
// would do for popular terms — without batching, warmup of a
// 10M-event chunk does ~50M Clones (one per index row) and saturates
// GC for many minutes.
//
// Also returns the exclusive upper bound of indexed event IDs (max + 1,
// or 0 if the index is empty) so warmup can cross-check it against the
// committed event count.
func warmupIndex(chunkStore *rocksdb.Store) (*events.ConcurrentBitmaps, uint32, error) {
	builder := events.NewBitmaps()
	var (
		hasPrev         bool
		prevTerm        events.TermKey
		buf             []uint32
		indexUpperBound uint32 // max indexed event ID + 1; 0 if no rows
	)
	flush := func() {
		if !hasPrev || len(buf) == 0 {
			return
		}
		builder.AddTo(prevTerm, buf...)
		buf = buf[:0]
	}

	for entry, err := range chunkStore.Iterate(IndexCF, nil) {
		if err != nil {
			return nil, 0, fmt.Errorf("events: warmup scan %s: %w", IndexCF, err)
		}
		if len(entry.Key) != indexKeyLen {
			return nil, 0, fmt.Errorf("events: warmup unexpected %s key length %d (want %d)",
				IndexCF, len(entry.Key), indexKeyLen)
		}
		var term events.TermKey
		copy(term[:], entry.Key[0:16])
		eventID := binary.BigEndian.Uint32(entry.Key[16:20])
		if eventID+1 > indexUpperBound {
			indexUpperBound = eventID + 1
		}
		if hasPrev && term != prevTerm {
			flush()
		}
		prevTerm = term
		hasPrev = true
		buf = append(buf, eventID)
	}
	flush()

	return events.NewConcurrentBitmapsFromBitmaps(builder), indexUpperBound, nil
}

// warmupOffsets scans events_offsets and replays every (ledger_seq,
// event_count) row into a fresh *events.ConcurrentLedgerOffsets. The
// on-disk shape matches the in-memory Append input directly
// (per-ledger counts, not cumulative), so no delta arithmetic is
// needed.
//
// Iteration order is byte-sorted == numeric-sorted under the big-endian
// uint32 key encoding, so rows arrive in ledger order. On-disk rows are
// untrusted, so each is validated as the next in-chunk ledger before the
// positional Append — a gap or stray row is rejected here rather than
// silently mis-attributing counts (ConcurrentLedgerOffsets.Append no
// longer checks the sequence; the trust boundary is here).
func warmupOffsets(chunkStore *rocksdb.Store, chunkID chunk.ID) (*events.ConcurrentLedgerOffsets, error) {
	offsets := events.NewConcurrentLedgerOffsets(chunkID.FirstLedger())

	for entry, err := range chunkStore.Iterate(OffsetsCF, nil) {
		if err != nil {
			return nil, fmt.Errorf("events: warmup scan %s: %w", OffsetsCF, err)
		}
		if len(entry.Key) != offsetKeyLen {
			return nil, fmt.Errorf("events: warmup unexpected %s key length %d (want %d)",
				OffsetsCF, len(entry.Key), offsetKeyLen)
		}
		if len(entry.Value) != offsetValLen {
			return nil, fmt.Errorf("events: warmup unexpected %s value length %d (want %d)",
				OffsetsCF, len(entry.Value), offsetValLen)
		}
		ledger := binary.BigEndian.Uint32(entry.Key)
		eventCount := binary.BigEndian.Uint32(entry.Value)
		// Each row must be the next sequential ledger and within the
		// chunk. The first test catches a gap, an out-of-order row, or a
		// wrong start; the second catches an excess row past the chunk
		// (which would otherwise append past capacity and panic).
		if expected := offsets.EndLedger(); ledger != expected || ledger > chunkID.LastLedger() {
			return nil, fmt.Errorf("events: warmup offsets: chunk %s expected ledger %d, got %d",
				chunkID, expected, ledger)
		}
		// On-disk counts are untrusted: guard the cumulative against uint32
		// overflow, the same check the ingest path makes up front.
		if uint64(offsets.TotalEvents())+uint64(eventCount) > math.MaxUint32 {
			return nil, fmt.Errorf("events: warmup offsets: chunk %s cumulative event count overflow at ledger %d",
				chunkID, ledger)
		}
		offsets.Append(eventCount)
	}
	return offsets, nil
}

// ──────────────────────────────────────────────────────────────────
// Key encoding helpers — RocksDB key layouts for the per-Chunk DB.
// ──────────────────────────────────────────────────────────────────

func encodeDataKey(eventID uint32) []byte {
	var key [dataKeyLen]byte
	binary.BigEndian.PutUint32(key[:], eventID)
	return key[:]
}

func encodeIndexKey(term events.TermKey, eventID uint32) []byte {
	var key [indexKeyLen]byte
	copy(key[:16], term[:])
	binary.BigEndian.PutUint32(key[16:], eventID)
	return key[:]
}

func encodeOffsetKey(ledgerSeq uint32) []byte {
	var key [offsetKeyLen]byte
	binary.BigEndian.PutUint32(key[:], ledgerSeq)
	return key[:]
}

func encodeLedgerEventCount(eventCount uint32) []byte {
	var val [offsetValLen]byte
	binary.BigEndian.PutUint32(val[:], eventCount)
	return val[:]
}
