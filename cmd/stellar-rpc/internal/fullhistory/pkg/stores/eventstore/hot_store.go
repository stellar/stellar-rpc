package eventstore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"math"
	"os"
	"path/filepath"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/linxGnu/grocksdb"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
)

// HotDirName is the subdirectory under EventsFullHistoryDataDir that
// contains one DB per active hot chunk (the current_hot_chunk plus
// any chunk currently being frozen).
const HotDirName = "hot"

// Column-family names used inside one chunk's hot RocksDB DB. The
// per-Chunk DB directory encodes the chunk ID, so the CF names
// themselves carry no chunk suffix.
const (
	DataCF    = "events_data"
	IndexCF   = "events_index"
	OffsetsCF = "events_offsets"
)

// HotChunkDir returns the on-disk path of chunkID's per-Chunk hot DB
// rooted at dataDir.
func HotChunkDir(dataDir string, chunkID chunk.ID) string {
	return filepath.Join(dataDir, HotDirName, chunkID.String())
}

// RemoveHotChunkDir deletes chunkID's hot DB directory. Idempotent —
// returns nil when the directory is already absent.
//
// The caller MUST close chunkID's HotStore before calling this;
// otherwise RocksDB's LOCK file is still held and the on-disk state
// will be inconsistent.
func RemoveHotChunkDir(dataDir string, chunkID chunk.ID) error {
	return os.RemoveAll(HotChunkDir(dataDir, chunkID))
}

// Per-CF tuning passed via rocksdb.Config.PerCFOptions:
//
//   - DataCF holds XDR payloads (compressible, batch-read) — larger blocks give
//     zstd more context and align with batch-fetch shapes.
//   - IndexCF holds 20-byte keys / empty values — small blocks cut wasted I/O per
//     random Lookup (one block per key).
//   - OffsetsCF holds 8-byte rows — same shape as IndexCF.
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

// openHotChunk opens (or creates) chunkID's per-Chunk hot DB. The three CFs
// auto-create on a fresh DB and rediscover on reopen. Unexported — OpenHotStore
// is the only caller (warmup is mandatory before the store is usable).
func openHotChunk(dataDir string, chunkID chunk.ID, logger *supportlog.Entry) (*rocksdb.Store, error) {
	store, err := rocksdb.New(rocksdb.Config{
		Path:           HotChunkDir(dataDir, chunkID),
		ColumnFamilies: []string{DataCF, IndexCF, OffsetsCF},
		Logger:         logger,
		PerCFOptions:   hotStoreCFOptions(),
	})
	if err != nil {
		return nil, fmt.Errorf("events: open hot chunk %s: %w", chunkID, err)
	}
	return store, nil
}

const (
	dataKeyLen   = 4      // event_id (chunk encoded by per-Chunk DB directory)
	indexKeyLen  = 16 + 4 // term hash || event_id
	offsetKeyLen = 4      // ledger_seq
	offsetValLen = 4      // per-ledger event count (uint32 BE)
)

// ErrLedgerOutOfRange is returned by IngestLedgerEvents when the
// supplied ledger sequence falls outside the chunk's [FirstLedger,
// LastLedger] window.
var ErrLedgerOutOfRange = errors.New("events: ledger outside chunk range")

// ErrLedgerOutOfOrder is returned by IngestLedgerEvents when the
// supplied ledger sequence is not the next-expected one. Catches
// duplicate ingest of an already-committed ledger as well as gaps
// (skipping ahead). Both would silently corrupt the per-ledger
// offset chain if not rejected up front.
var ErrLedgerOutOfOrder = errors.New("events: ledger out of order")

// HotStore wraps one chunk's hot RocksDB DB plus the in-memory term mirror and
// ledger-offset cache that feed the query path.
//
// Atomicity: the per-Chunk DB is the source of truth. IngestLedgerEvents commits
// data + index + offsets in one atomic batch, then updates the in-memory
// mirrors; warmup reconstructs them from the on-disk CFs on next startup.
//
// Concurrency:
//
//   - Writes (IngestLedgerEvents) are single-writer (one goroutine per chunk).
//   - Reads (Lookup, FetchEvents, All) take NO HotStore-level lock — they guard
//     via chunkStore.IsClosed() and rely on the mirror's internal locks and
//     RocksDB's thread-safety.
//   - Metadata split by Close: ChunkID, NextEventID, Index are infallible
//     (cached, usable post-Close); EventCount, Offsets return ErrClosed after
//     Close (Reader-interface contract).
type HotStore struct {
	chunkStore *rocksdb.Store
	chunkID    chunk.ID
	mirror     *events.ConcurrentBitmaps
	offsets    *events.ConcurrentLedgerOffsets
	// ownsStore is true on the standalone OpenHotStore path; false when wrapping
	// the SHARED per-chunk DB via NewWithStore (decision (a)), which hotchunk.DB
	// owns and closes once.
	ownsStore bool
}

// Compile-time guard: *HotStore satisfies Reader.
var _ Reader = (*HotStore)(nil)

// OpenHotStore opens (or creates) chunkID's hot DB, warms up the mirror +
// offsets from disk, and returns a ready HotStore that owns its chunkStore.
func OpenHotStore(
	dataDir string,
	chunkID chunk.ID,
	logger *supportlog.Entry,
) (*HotStore, error) {
	if dataDir == "" {
		return nil, errors.New("events: OpenHotStore requires a data dir")
	}
	if logger == nil {
		return nil, errors.New("events: OpenHotStore requires a logger")
	}

	chunkStore, err := openHotChunk(dataDir, chunkID, logger)
	if err != nil {
		return nil, err
	}
	h, err := NewWithStore(chunkStore, chunkID)
	if err != nil {
		_ = chunkStore.Close()
		return nil, err
	}
	h.ownsStore = true
	return h, nil
}

// NewWithStore wraps an ALREADY-OPEN rocksdb.Store as an events HotStore on the
// three events CFs (CFNames()), running the mandatory warmup to rebuild the
// in-memory mirror + offsets. The store is NOT owned (Close is a no-op) — the
// constructor hotchunk uses to compose this facade over the shared per-chunk DB
// (decision (a)). The store must have CFNames() registered + CFOptions() applied.
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

// Close releases the chunk store IF this HotStore owns it (standalone
// OpenHotStore); a no-op when wrapping the shared per-chunk DB (NewWithStore),
// which hotchunk.DB closes once. Idempotent; not safe to call alongside in-flight
// reads/writes on this HotStore.
func (h *HotStore) Close() error {
	if !h.ownsStore {
		return nil
	}
	return h.chunkStore.Close()
}

// ChunkID returns the chunk this store serves.
func (h *HotStore) ChunkID() chunk.ID { return h.chunkID }

// EventCount is the total events committed to this Chunk so far (== the next
// event-id). Returns (0, ErrClosed) after Close. The fallible signature is for
// the Reader interface (ColdReader's lazy load); the hot value is always live.
func (h *HotStore) EventCount() (uint32, error) {
	if h.chunkStore.IsClosed() {
		return 0, ErrClosed
	}
	return h.offsets.TotalEvents(), nil
}

// NextEventID is the next chunk-relative event ID IngestLedgerEvents will
// assign. Same value as EventCount; exposed under both names for the ingest- and
// reader-side mental models. Infallible (hot-only, not on Reader).
func (h *HotStore) NextEventID() uint32 { return h.offsets.TotalEvents() }

// Offsets returns a point-in-time, read-only view of the ledger-offset cache
// (see Reader.Offsets), capped at the count visible at call time. A concurrent
// ingest may extend the backing past the cap; the view's slice stays bounded.
// The view shares the live backing array — Append on it would silently fork it,
// so the contract is read-only. Returns (nil, ErrClosed) after Close.
func (h *HotStore) Offsets() (*events.LedgerOffsets, error) {
	if h.chunkStore.IsClosed() {
		return nil, ErrClosed
	}
	return h.offsets.View(), nil
}

// Index returns the in-memory term mirror, used by the freezer to snapshot every
// (TermKey, bitmap) pair without rebuilding from RocksDB. Callers typically use
// h.Index().Snapshot() for a uniquely owned Bitmaps.
func (h *HotStore) Index() *events.ConcurrentBitmaps { return h.mirror }

// Lookup returns the bitmap of event IDs in this Chunk matching key. The bitmap
// is an immutable snapshot of the live mirror (writers publish via atomic.Store)
// — callers MUST NOT mutate it. Returns (nil, ErrTermNotFound) on no match,
// (nil, ErrClosed) after Close. ctx is a fast guard; the hot path does no I/O.
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

// LookupKeys returns bitmaps positionally aligned with keys; result[i] is nil on
// no match. See Reader.LookupKeys (borrowed-bitmap contract: callers must not
// mutate). Hot-side is just N mirror lookups, exposed only to satisfy Reader.
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

// FetchEvents decodes the events_data row for each eventID, positionally aligned
// with the input. See Reader.FetchEvents for the sorted-input precondition
// (violations return wrapped ErrUnsortedEventIDs). One BatchMultiGet crosses CGO
// once regardless of count and enables async_io (a win on high-latency storage);
// ctx is honored at entry but the CGO call is not cancellable mid-flight.
//
// A missing row is an error, not a normal miss: IDs only reach here via Lookup,
// which only returns IDs the mirror (hence RocksDB) has — a miss means
// corruption. Returns ErrClosed after Close.
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
		// BatchMultiGet already copies out of rocksdb's pinned pages; v is
		// Go-owned, so Unmarshal's alias is safe without an extra clone.
		if err := results[i].Unmarshal(v); err != nil {
			return nil, fmt.Errorf("events: decode event %d from chunk %s: %w", id, h.chunkID, err)
		}
	}
	return results, nil
}

// FetchRange streams count events from chunk-relative event ID start, ascending
// (see Reader.FetchRange). Yielded Payloads are borrowed: ContractEventBytes
// aliases the iteration buffer, valid only until the next step — clone to retain.
// After Close yields (zero, ErrClosed). ctx is checked at entry and between
// steps; IterateRange takes no ctx, so a slow Next can block past a cancel.
//
// Out-of-range args yield an error and stop: count==0 is a no-op; start+count >
// NextEventID is out-of-bounds; a short scan (fewer rows than count) signals
// corruption (the CF should be dense in [0, NextEventID)).
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
		if err := validateFetchRange(start, count, h.NextEventID(), h.chunkID); err != nil {
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
			// entry.Value is a zero-copy ref valid only this step; Unmarshal
			// aliases it into p, so the yielded Payload is borrowed (clone to retain).
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

// All streams every event in this Chunk in eventID order — used by the freeze
// loop to dump a hot Chunk without buffering. Thin wrapper over FetchRange (same
// borrowed-Payload contract). NextEventID is read inside the closure, so an
// ingest between All returning and the first range step is included. After Close
// yields (zero, ErrClosed).
func (h *HotStore) All(ctx context.Context) iter.Seq2[events.Payload, error] {
	return func(yield func(events.Payload, error) bool) {
		// FetchRange stops iterating after yielding an error; we
		// just forward whatever it yields and exit on the same step.
		for p, err := range h.FetchRange(ctx, 0, h.NextEventID()) {
			if !yield(p, err) {
				return
			}
		}
	}
}

// IngestLedgerEvents commits one ledger's events to the chunk store atomically,
// then updates the in-memory mirrors. payloads (from LCMViewToPayloads) arrive in
// getEvents cursor order; write order here IS the cursor contract (event IDs are
// assigned by arrival position). Terms are derived internally via TermsForBytes.
//
// Sequence validation runs up front (before any write or mirror mutation), so a
// rejected call leaves all state untouched:
//   - out of [chunkID.FirstLedger(), LastLedger()] → ErrLedgerOutOfRange.
//   - == expected (StartLedger + LedgerCount) → appended.
//   - < expected (already ingested) → idempotent no-op nil (a restarted ingester
//     can blindly re-deliver; the re-delivered events are not re-verified).
//   - > expected (a gap) → ErrLedgerOutOfOrder.
//
// Post-batch atomicity: once the batch commits, the mirror + offsets updates are
// infallible — a failure there panics rather than leaving on-disk state ahead of
// in-memory with no clean recovery short of close + reopen.
func (h *HotStore) IngestLedgerEvents(ledgerSeq uint32, payloads []events.Payload) error {
	if h.chunkStore.IsClosed() {
		return ErrClosed
	}

	// Same prepare → queue → commit → apply pipeline hotchunk drives across the
	// shared DB; here the batch holds only the events CFs.
	apply, err := h.IngestLedgerToBatchCommit(ledgerSeq, payloads)
	if err != nil {
		return err
	}
	if apply != nil {
		apply()
	}
	return nil
}

// IngestLedgerToBatchCommit is IngestLedgerEvents over a batch this facade owns
// end-to-end (validate → marshal → one synced batch). Returns the post-commit
// apply hook (mirror+offsets) to run after the batch is durable, or (nil, nil)
// for an idempotent duplicate. Split out so IngestLedgerToBatch can share the
// prepare step while committing into a SHARED cross-CF batch instead.
func (h *HotStore) IngestLedgerToBatchCommit(ledgerSeq uint32, payloads []events.Payload) (func(), error) {
	prep, err := h.prepareLedger(ledgerSeq, payloads)
	if err != nil {
		return nil, err
	}
	if prep == nil {
		return nil, nil // idempotent duplicate no-op
	}
	if cerr := h.chunkStore.Batch(func(b *rocksdb.BatchWriter) error {
		return prep.queue(b)
	}); cerr != nil {
		return nil, fmt.Errorf("events: commit ledger %d to chunk %s: %w", ledgerSeq, h.chunkID, cerr)
	}
	return prep.apply, nil
}

// IngestLedgerToBatch validates+marshals one ledger's events and queues their CF
// Puts into the SHARED batch b, returning the post-commit apply hook the caller
// runs AFTER b commits (decision (a)). Returns (nil, nil) for an idempotent
// duplicate. All validation + term derivation happen up front, so a rejected
// ledger leaves b untouched.
func (h *HotStore) IngestLedgerToBatch(b *rocksdb.BatchWriter, ledgerSeq uint32, payloads []events.Payload) (func(), error) {
	if h.chunkStore.IsClosed() {
		return nil, ErrClosed
	}
	prep, err := h.prepareLedger(ledgerSeq, payloads)
	if err != nil {
		return nil, err
	}
	if prep == nil {
		return nil, nil
	}
	if qerr := prep.queue(b); qerr != nil {
		return nil, qerr
	}
	return prep.apply, nil
}

// preparedLedger is one validated, marshaled ledger ready to queue into
// a write batch (queue) and, once that batch is durable, apply to the
// in-memory mirror + offsets (apply).
type preparedLedger struct {
	ledgerSeq uint32
	startID   uint32
	blobs     [][]byte           // marshaled payload XDR, positional with payloads
	termKeys  [][]events.TermKey // per-payload term keys
	apply     func()             // post-commit mirror + offsets update (infallible)
}

// queue writes the prepared ledger's rows into b: one DataCF row per
// event, one IndexCF row per (term, event), and one OffsetsCF row for
// the ledger's per-ledger event count.
func (p *preparedLedger) queue(b *rocksdb.BatchWriter) error {
	for i := range p.blobs {
		eventID := p.startID + uint32(i)
		b.Put(DataCF, encodeDataKey(eventID), p.blobs[i])
		for _, key := range p.termKeys[i] {
			b.Put(IndexCF, encodeIndexKey(key, eventID), nil)
		}
	}
	//nolint:gosec // bounds-checked in prepareLedger's overflow guard
	eventCount := uint32(len(p.blobs))
	b.Put(OffsetsCF, encodeOffsetKey(p.ledgerSeq), encodeLedgerEventCount(eventCount))
	return nil
}

// prepareLedger runs the pre-commit pipeline for one ledger (validate → derive
// terms → marshal into fresh per-event buffers), returning a *preparedLedger
// ready to queue + apply, or (nil, nil) for an idempotent duplicate. It does NO
// disk write and NO mirror mutation, so it is safe to call before touching a
// shared batch.
//
//nolint:cyclop // sequential pipeline: validate -> derive terms -> marshal -> build apply hook
func (h *HotStore) prepareLedger(ledgerSeq uint32, payloads []events.Payload) (*preparedLedger, error) {
	// Validate BEFORE marshaling: failing after a shared batch holds this
	// ledger's rows would orphan them.
	if ledgerSeq < h.chunkID.FirstLedger() || ledgerSeq > h.chunkID.LastLedger() {
		return nil, fmt.Errorf("%w: ledger %d not in chunk %s [%d, %d]",
			ErrLedgerOutOfRange, ledgerSeq, h.chunkID,
			h.chunkID.FirstLedger(), h.chunkID.LastLedger())
	}
	expected := h.offsets.StartLedger() + uint32(h.offsets.LedgerCount()) //nolint:gosec
	if ledgerSeq < expected {
		// Already ingested: idempotent no-op (a restarted ingester may
		// re-deliver). Re-delivered events are not re-verified.
		return nil, nil
	}
	if ledgerSeq > expected {
		return nil, fmt.Errorf("%w: expected ledger %d, got %d",
			ErrLedgerOutOfOrder, expected, ledgerSeq)
	}

	// Pre-derive term keys so the post-commit mirror update needn't re-hash. A
	// TermsForBytes error rejects the ledger without touching the batch (a decode
	// failure on core-validated XDR is a corruption signal worth aborting on).
	termKeys := make([][]events.TermKey, len(payloads))
	for i := range payloads {
		keys, err := events.TermsForBytes(payloads[i].ContractEventBytes)
		if err != nil {
			return nil, fmt.Errorf("events: derive terms for payload %d in ledger %d: %w", i, ledgerSeq, err)
		}
		termKeys[i] = keys
	}

	startID := h.offsets.TotalEvents()
	if uint64(startID)+uint64(len(payloads)) > math.MaxUint32 {
		return nil, fmt.Errorf("events: chunk %s would overflow uint32 event-id space at ledger %d",
			h.chunkID, ledgerSeq)
	}

	// Marshal each payload into its OWN fresh buffer (not reused scratch): a
	// shared batch may hold many ledgers' rows before commit, so each blob must
	// outlive prepare until the Write copies it. BatchWriter.Put copies
	// synchronously, so the buffers are free after queue returns.
	blobs := make([][]byte, len(payloads))
	for i := range payloads {
		blob, err := payloads[i].MarshalInto(nil)
		if err != nil {
			return nil, fmt.Errorf("events: marshal payload %d for ledger %d: %w", i, ledgerSeq, err)
		}
		blobs[i] = blob
	}

	prep := &preparedLedger{
		ledgerSeq: ledgerSeq,
		startID:   startID,
		blobs:     blobs,
		termKeys:  termKeys,
	}
	prep.apply = func() { h.applyLedger(prep) }
	return prep, nil
}

// applyLedger updates the mirror + offsets for a ledger whose rows are durable.
// Infallible by construction (prepare validated seq under the single-writer
// contract); the only non-completion is a crash, after which warmup rebuilds.
//
// Ordering invariant: mirror BEFORE offsets. A concurrent Query that snapshots
// offsets then reads the mirror must see either the prior state or a consistent
// later one. Reversing it would let a reader see an offsets count including IDs
// the mirror hasn't published — FetchEvents would then miss them, silently.
func (h *HotStore) applyLedger(p *preparedLedger) {
	// Batch by key so each AddTo clones at most once per (key, ledger), not per
	// (key, event) — turns N COW clones into 1 for popular terms. Cap 64 ≈ a few
	// × unique-terms per ledger; the map grows past that.
	perKeyIDs := make(map[events.TermKey][]uint32, 64)
	for i, keys := range p.termKeys {
		eventID := p.startID + uint32(i)
		for _, key := range keys {
			perKeyIDs[key] = append(perKeyIDs[key], eventID)
		}
	}
	for key, ids := range perKeyIDs {
		h.mirror.AddTo(key, ids...)
	}
	//nolint:gosec // len bounded by prepareLedger's overflow guard
	h.offsets.Append(uint32(len(p.blobs)))
}

// Warmup — reconstructs the in-memory mirror + offsets from the on-disk CFs.

// warmup rebuilds chunkID's in-memory mirrors by scanning the two on-disk caches
// once each: events_index → ConcurrentBitmaps, events_offsets →
// ConcurrentLedgerOffsets. chunkID seeds StartLedger for empty chunks; both
// mirrors are empty for fresh chunks.
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

// verifyChunkConsistency cross-checks the three on-disk CFs after warmup, turning
// a torn/tampered chunk into a loud open failure. A cheap open-time tripwire, not
// load-bearing correctness (the atomic batch makes violations impossible for the
// writer; only a bug/corruption trips it):
//
//   - index may not reference an event offsets don't account for:
//     indexUpperBound (max indexed id + 1, 0 if none) <= total.
//   - data tail matches total: id total-1 present (when total > 0) and nothing at
//     id >= total — together pinning the max data id to total-1.
//
// Not detected (would need a full scan): interior data holes, under-indexed
// terms, wrong per-ledger boundaries. An interior hole that did appear is caught
// lazily by FetchRange's short-scan check.
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
	// Nothing may live at or beyond total. Reaching the loop body (no iteration
	// error) means an orphan row is present.
	for _, err := range chunkStore.IterateRange(DataCF, encodeDataKey(total), nil) {
		if err != nil {
			return fmt.Errorf("events: verify data tail: %w", err)
		}
		return fmt.Errorf("events: corrupt chunk: data present at id >= committed count %d", total)
	}
	return nil
}

// warmupIndex replays every (TermKey, eventID) row of events_index into a fresh
// ConcurrentBitmaps. Builds into a single-threaded Bitmaps via per-term batching
// (byte-sorted iteration groups a term's rows, flushed on term change), then
// converts at the end — avoiding the per-row Clone the concurrent AddTo would do
// for popular terms (a 10M-event chunk would otherwise do ~50M Clones, saturating
// GC). Also returns the exclusive upper bound of indexed IDs (max + 1, 0 if
// empty) for warmup's cross-check against the committed count.
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

// warmupOffsets replays every (ledger_seq, event_count) row of events_offsets
// into a fresh ConcurrentLedgerOffsets. The on-disk shape (per-ledger counts)
// matches Append's input directly. BE-uint32 keys sort in ledger order; on-disk
// rows are untrusted, so each is validated as the next in-chunk ledger before the
// positional Append (the trust boundary moved here — Append no longer checks).
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
		// Each row must be the next sequential in-chunk ledger: the first test
		// catches a gap/out-of-order/wrong-start, the second an excess row past
		// the chunk (which would append past capacity and panic).
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

// Key encoding helpers — RocksDB key layouts for the per-Chunk DB.

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
