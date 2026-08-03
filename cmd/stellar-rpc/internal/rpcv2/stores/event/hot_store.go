package event

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"math"
	"path/filepath"
	"strings"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/linxGnu/grocksdb"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
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
//   - IndexCF stores ONE packed row per ledger (4-byte ledger_seq key,
//     value = sorted term_hash ‖ uvarint event-ID runs — see
//     AppendPackedRow). One skiplist insert per ledger instead of
//     one per (term, event) pair — the per-(term,event) 20-byte-key
//     format it replaces put tens of thousands of keys per ledger into
//     the shared commit batch's memtable insert, which dominated hot
//     commit latency. Only warmup reads this CF (queries hit the
//     in-memory mirror), so block size just needs to suit a sequential
//     scan of ~100-300KB values.
//   - OffsetsCF stores 8-byte (ledger_seq -> event_count) rows in
//     the tens-of-thousands per chunk.
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
	dataKeyLen = 4 // event_id (chunk encoded by per-Chunk DB directory)
	// packedIndexKeyLen is the per-ledger packed index key: the ledger
	// sequence. (The pre-release legacy per-(term,event) 20-byte format is
	// gone — no deployed chunks exist.)
	packedIndexKeyLen = 4
	offsetKeyLen      = 4 // ledger_seq
	offsetValLen      = 4 // per-ledger event count (uint32 BE)
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
//   - Reads (LookupKeys, FetchEvents, All) take NO HotStore-level lock — they guard
//     via chunkStore.IsClosed() and rely on the mirror's internal locks and
//     RocksDB's thread-safety.
//   - Metadata split after the caller-owned store is closed: ChunkID is
//     infallible (cached, usable post-close); EventCount and
//     Offsets return stores.ErrStoreClosed after close (Reader-interface contract).
type HotStore struct {
	chunkStore *rocksdb.Store
	chunkID    chunk.ID
	hotIdx     *HotIndex
	offsets    *ConcurrentLedgerOffsets

	// scratch holds the writer-owned flat-pairs arenas reused across ledgers
	// (single-writer ingest contract). The post-commit hook borrows views over
	// it, so the hook for ledger N must run before IngestLedgerToBatch for
	// N+1 — the hotchunk driver's ingest → commit → hook sequence.
	scratch ledgerScratch
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
	hotIdx, offsets, err := warmup(store, chunkID)
	if err != nil {
		return nil, fmt.Errorf("events: warmup chunk %s: %w", chunkID, err)
	}
	return &HotStore{
		chunkStore: store,
		chunkID:    chunkID,
		hotIdx:     hotIdx,
		offsets:    offsets,
	}, nil
}

// ChunkID returns the chunk this store serves. Infallible and usable post-close
// (the Reader exception). No production caller yet — the intended read seam for
// the v2 cutover (#772), exercised by tests until then.
func (h *HotStore) ChunkID() chunk.ID { return h.chunkID }

// EventCount is the total number of events committed to this Chunk
// so far. Equal to the next event-id IngestLedgerToBatch would assign.
// Returns (0, stores.ErrStoreClosed) after the caller-owned store is closed. The Reader interface signature
// is fallible to accommodate ColdReader's lazy metadata load; on the
// hot side the value is always live and the error is only stores.ErrStoreClosed.
func (h *HotStore) EventCount() (uint32, error) {
	if h.chunkStore.IsClosed() {
		return 0, stores.ErrStoreClosed
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
// Returns (nil, stores.ErrStoreClosed) after the caller-owned store is closed.
func (h *HotStore) Offsets() (*LedgerOffsets, error) {
	if h.chunkStore.IsClosed() {
		return nil, stores.ErrStoreClosed
	}
	return h.offsets.View(), nil
}

// LookupKeys returns bitmaps for each key, aligned positionally with
// the input slice. result[i] is nil if keys[i] has no matching
//
//	See Reader.LookupKeys for the semantics — in particular
//
// the borrowed-bitmap contract (callers must not mutate).
//
// Hot-side implementation is N hot-index lookups: dense terms answer
// from the in-memory overlay; sparse terms materialize from the
// retained window rows plus bounded preads of the sealed run files —
// cheap, but NOT I/O-free. Exposing this method satisfies the Reader
// interface so callers can program against batched lookups
// uniformly.
func (h *HotStore) LookupKeys(ctx context.Context, keys []TermKey) ([]*roaring.Bitmap, error) {
	if h.chunkStore.IsClosed() {
		return nil, stores.ErrStoreClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}
	results := make([]*roaring.Bitmap, len(keys))
	for i, key := range keys {
		bm, err := h.hotIdx.Get(key)
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
// LookupKeys, which only returns IDs the mirror knows about —
// implying RocksDB also has them. A miss indicates corruption or a
// writer/reader mismatch, not a normal not-found case.
//
// After the caller-owned store is closed, returns stores.ErrStoreClosed.
func (h *HotStore) FetchEvents(ctx context.Context, eventIDs []uint32) ([]Payload, error) {
	if h.chunkStore.IsClosed() {
		return nil, stores.ErrStoreClosed
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

	results := make([]Payload, len(eventIDs))
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
// After the caller-owned store is closed, yields (zero Payload, stores.ErrStoreClosed) and stops.
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
func (h *HotStore) FetchRange(ctx context.Context, start, count uint32) iter.Seq2[Payload, error] {
	return func(yield func(Payload, error) bool) {
		if h.chunkStore.IsClosed() {
			yield(Payload{}, stores.ErrStoreClosed)
			return
		}
		if err := ctx.Err(); err != nil {
			yield(Payload{}, err)
			return
		}
		if count == 0 {
			return
		}
		if err := validateFetchRange(start, count, h.offsets.TotalEvents(), h.chunkID); err != nil {
			yield(Payload{}, err)
			return
		}

		startKey := encodeDataKey(start)
		endKey := encodeDataKey(start + count - 1) // inclusive
		yielded := uint32(0)
		for entry, err := range h.chunkStore.IterateRange(DataCF, startKey, endKey) {
			if err != nil {
				yield(Payload{}, fmt.Errorf("events: scan chunk %s: %w", h.chunkID, err))
				return
			}
			if err := ctx.Err(); err != nil {
				yield(Payload{}, err)
				return
			}
			var p Payload
			// entry.Value is a zero-copy ref into the IterateRange
			// iterator buffer, valid only for this step; Unmarshal aliases
			// it into p.ContractEventBytes, so the yielded Payload is
			// borrowed (see the FetchRange doc). A retaining consumer clones.
			if err := p.Unmarshal(entry.Value); err != nil {
				yield(Payload{}, fmt.Errorf("events: decode event from chunk %s: %w",
					h.chunkID, err))
				return
			}
			if !yield(p, nil) {
				return
			}
			yielded++
		}
		if yielded != count {
			yield(Payload{}, fmt.Errorf(
				"events: FetchRange short scan for chunk %s: got %d of %d events at [%d, %d)",
				h.chunkID, yielded, count, start, start+count))
		}
	}
}

// All streams every event in this Chunk in chunk-relative eventID
// order — the Reader full-scan. The freeze re-derives cold event
// artifacts from raw LCMs and never calls this, so it has no
// production caller yet: it's the intended read seam for the v2
// cutover (#772), exercised by tests until then. Thin wrapper over
// FetchRange; its yielded Payloads are likewise borrowed (valid only
// for the step).
//
// The committed event count is read inside the returned closure body, so a
// concurrent ingest between r.All(ctx) returning the Seq2 and the
// consumer's first range step is included in the snapshot.
//
// After the caller-owned store is closed, yields (zero Payload, stores.ErrStoreClosed) and stops.
func (h *HotStore) All(ctx context.Context) iter.Seq2[Payload, error] {
	return func(yield func(Payload, error) bool) {
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
// caller runs AFTER b commits (decision (a)). Sequence validation happens before
// any Put; marshal and term derivation are fused per event, and on any error
// Store.Batch discards the whole WriteBatch, so a rejected ledger never leaves
// committed rows behind.
//
// payloads is produced by PayloadsFromLedgerEvents, which emits each ledger's
// events in ascending getEvents cursor order — write order here IS the cursor
// contract (event IDs are assigned by arrival position). Terms are derived
// from each payload's ContractEventBytes into the writer-owned pair arenas
// and constant-key side lanes (appendEventTerms).
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
	b *rocksdb.BatchWriter, ledgerSeq uint32, payloads []Payload,
) (func() error, error) {
	// Sequence validation BEFORE any Put. Per-event errors below are equally
	// safe: on any error Store.Batch discards the whole WriteBatch, so a
	// mid-loop failure never orphans rows — no separate staging buffer needed.
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

	startID := h.offsets.TotalEvents()
	if uint64(startID)+uint64(len(payloads)) > math.MaxUint32 {
		return nil, fmt.Errorf("chunk %s would overflow uint32 event-id space at ledger %d",
			h.chunkID, ledgerSeq)
	}

	// Marshal + derive terms + queue each event in ONE pass over the payload
	// bytes. BatchWriter.Put copies key and value synchronously, so one reused
	// value scratch AND one reused 4-byte key scratch serve every event (the
	// per-call encodeDataKey array escaped to the heap once per event — ~6k
	// 4-byte objects/ledger). Term derivation appends flat (term, eventID)
	// pairs into the writer-owned arenas — no per-ledger map, no per-event
	// key slices. Event IDs are assigned strictly increasing here; the sort
	// in buildRuns depends on that arrival order for per-term ID order.
	var scratch []byte
	var dataKey [dataKeyLen]byte
	h.scratch.reset()
	for i := range payloads {
		blob, err := payloads[i].MarshalInto(scratch[:0])
		if err != nil {
			return nil, fmt.Errorf("marshal payload %d for ledger %d: %w", i, ledgerSeq, err)
		}
		scratch = blob
		eventID := startID + uint32(i)
		binary.BigEndian.PutUint32(dataKey[:], eventID)
		b.Put(DataCF, dataKey[:], blob)
		if err := h.scratch.appendEventTerms(eventID, payloads[i].ContractEventBytes); err != nil {
			return nil, fmt.Errorf("derive terms for payload %d in ledger %d: %w", i, ledgerSeq, err)
		}
	}
	// ONE packed index row per ledger instead of a row per (term, event) —
	// the memtable insert cost of the shared commit batch scales with key
	// count, and this removes tens of thousands of keys per ledger from it.
	// A ledger with no events writes no index row. The row bytes are RETAINED
	// for the post-commit hot-index apply (the window keeps the slice;
	// BatchWriter.Put copied it into the batch already), so rowLen's exact
	// sizing is retained memory, not just churn.
	runs := h.scratch.buildRuns()
	var rowBytes []byte
	if len(runs.terms) > 0 {
		rowBytes = runs.appendRow(make([]byte, 0, runs.rowLen()))
		b.Put(IndexCF, encodePackedIndexKey(ledgerSeq), rowBytes)
	}
	//nolint:gosec // len bounded by the overflow guard above
	b.Put(OffsetsCF, encodeOffsetKey(ledgerSeq), encodeLedgerEventCount(uint32(len(payloads))))

	//nolint:gosec // len bounded by the overflow guard above
	eventCount := uint32(len(payloads))
	return func() error {
		// runs borrows the writer-owned arenas — valid here because the
		// driver runs the hook before the next IngestLedgerToBatch resets
		// them, and ApplyLedger does not retain the view.
		if rowBytes != nil {
			if err := h.hotIdx.ApplyLedger(ledgerSeq, rowBytes, runs); err != nil {
				return err
			}
		}
		h.offsets.Append(eventCount)
		return nil
	}, nil
}

// hotIndexRunDir is the run-file directory name inside the chunk DB dir —
// covered by the chunk lifecycle's directory wipe like every other chunk
// artifact. RocksDB ignores foreign subdirectories in its dir.
const hotIndexRunDir = "hotindex-runs"

// rocksdbManifest persists the hot index's live-run list in the chunk DB's
// default CF under one key. Every Put rides the store's pinned synced
// WriteOptions, so a manifest write is durable before reapSeal publishes.
type rocksdbManifest struct {
	store *rocksdb.Store
}

var hotIndexManifestKey = []byte("hotindex:manifest") //nolint:gochecknoglobals // fixed key

func (m rocksdbManifest) PutRuns(names []string, lastSealed uint32) error {
	val := make([]byte, 4, 4+len(names)*16)
	binary.BigEndian.PutUint32(val, lastSealed)
	val = append(val, strings.Join(names, ",")...)
	return m.store.Put("", hotIndexManifestKey, val)
}

func (m rocksdbManifest) GetRuns() ([]string, uint32, error) {
	val, found, err := m.store.Get("", hotIndexManifestKey)
	if err != nil || !found {
		return nil, 0, err
	}
	if len(val) < 4 {
		return nil, 0, fmt.Errorf("events: hotindex manifest value too short (%d bytes)", len(val))
	}
	lastSealed := binary.BigEndian.Uint32(val)
	rest := string(val[4:])
	if rest == "" {
		return nil, lastSealed, nil
	}
	return strings.Split(rest, ","), lastSealed, nil
}

// Shutdown drains the hot index's background seal. hotchunk.DB.Close calls it
// before closing the shared store so no seal goroutine outlives the DB.
func (h *HotStore) Shutdown() { h.hotIdx.Close() }

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
func verifyChunkConsistency(chunkStore *rocksdb.Store, total uint32, indexUpperBound uint64) error {
	// indexUpperBound is uint64 so a hostile row at eventID ==
	// MaxUint32 can't wrap max+1 to 0 and slip past this check.
	if indexUpperBound > uint64(total) {
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

// overlay returns the hot index's dense-term overlay — a PARTIAL view
// holding only promoted (dense) terms; sparse terms live in the window rows
// and sealed runs and are absent here. Test-only write hook: no production
// path reads it, and no read path should — a lookup against the overlay
// alone silently misses every sparse term (use HotIndex.Get / LookupKeys).
func (h *HotStore) overlay() *ConcurrentBitmaps { return h.hotIdx.overlay }

// ──────────────────────────────────────────────────────────────────
// Warmup — reconstructs the in-memory mirror + offsets from the
// per-Chunk DB's on-disk CFs. Called by NewWithStore.
// ──────────────────────────────────────────────────────────────────

// warmup rebuilds the in-memory mirrors for chunkID by prefix-scanning
// the chunk's two on-disk caches once each:
//
//   - events_index  → *ConcurrentBitmaps — every
//     (TermKey, eventID) row replayed into a fresh in-memory
//     bitmap mirror.
//   - events_offsets → *ConcurrentLedgerOffsets — every
//     (ledger_seq, per_ledger_count) row replayed into a fresh
//     offset cache.
//
// chunkID seeds ConcurrentLedgerOffsets.StartLedger for empty
// chunks; on-disk rows carry the full ledger sequence themselves.
// Both mirrors are empty for fresh chunks.
func warmup(
	chunkStore *rocksdb.Store, chunkID chunk.ID,
) (*HotIndex, *ConcurrentLedgerOffsets, error) {
	offsets, err := warmupOffsets(chunkStore, chunkID)
	if err != nil {
		return nil, nil, err
	}
	manifest := rocksdbManifest{store: chunkStore}
	runDir := filepath.Join(chunkStore.Path(), hotIndexRunDir)
	hotIdx, lastSealed, err := OpenHotIndex(runDir, manifest)
	if err != nil {
		return nil, nil, err
	}
	// Replay the UN-SEALED tail: only packed rows past the sealed frontier
	// are read — sealed rows are never touched (their derived runs are the
	// CRC-verified authority, and they were validated when applied live), so
	// warmup memory and time genuinely no longer scale with the chunk.
	// indexUpperBound (the consistency tripwire's input) derives from the
	// replayed tail; the tripwire is an upper bound, so a tail-only value
	// can under-approximate but never false-alarm.
	var indexUpperBound uint64
	// runs arenas are reused across rows: ApplyLedger borrows the view and
	// never retains it — the same contract the ingest-path scratch relies on.
	var runs termRuns
	for entry, ierr := range chunkStore.IterateRange(IndexCF, rocksdb.EncodeUint32(lastSealed+1), nil) {
		if ierr != nil {
			hotIdx.Close()
			return nil, nil, fmt.Errorf("events: warmup scan %s: %w", IndexCF, ierr)
		}
		if len(entry.Key) != packedIndexKeyLen {
			hotIdx.Close()
			return nil, nil, fmt.Errorf("events: warmup unexpected %s key length %d (want %d)",
				IndexCF, len(entry.Key), packedIndexKeyLen)
		}
		seq := binary.BigEndian.Uint32(entry.Key)
		runs.reset()
		if derr := DecodePackedRow(entry.Value, func(term TermKey, ids []uint32) {
			runs.addRun(term, ids)
			if last := uint64(ids[len(ids)-1]) + 1; last > indexUpperBound {
				indexUpperBound = last
			}
		}); derr != nil {
			hotIdx.Close()
			return nil, nil, fmt.Errorf("events: warmup %s ledger %d: %w", IndexCF, seq, derr)
		}
		if aerr := hotIdx.ApplyLedger(seq, append([]byte(nil), entry.Value...), runs); aerr != nil {
			hotIdx.Close()
			return nil, nil, aerr
		}
	}
	if err := verifyChunkConsistency(chunkStore, offsets.TotalEvents(), indexUpperBound); err != nil {
		hotIdx.Close()
		return nil, nil, err
	}
	// The open is validated — only now may the engine write durable index
	// state (seal runs, manifest). The replay above ran disarmed, so a
	// failed open leaves the manifest and frontier exactly as it found
	// them, and every retry faces the same tripwire over the same rows.
	hotIdx.ArmSealing()
	return hotIdx, offsets, nil
}

// warmupOffsets scans events_offsets and replays every (ledger_seq,
// event_count) row into a fresh *ConcurrentLedgerOffsets. The
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
func warmupOffsets(chunkStore *rocksdb.Store, chunkID chunk.ID) (*ConcurrentLedgerOffsets, error) {
	offsets := NewConcurrentLedgerOffsets(chunkID.FirstLedger())

	if err := scanOffsetsCF(chunkStore, func(ledger, eventCount uint32) error {
		// Each row must be the next sequential ledger and within the
		// chunk. The first test catches a gap, an out-of-order row, or a
		// wrong start; the second catches an excess row past the chunk
		// (which would otherwise append past capacity and panic).
		if expected := offsets.EndLedger(); ledger != expected || ledger > chunkID.LastLedger() {
			return fmt.Errorf("events: warmup offsets: chunk %s expected ledger %d, got %d",
				chunkID, expected, ledger)
		}
		// On-disk counts are untrusted: guard the cumulative against uint32
		// overflow, the same check the ingest path makes up front.
		if uint64(offsets.TotalEvents())+uint64(eventCount) > math.MaxUint32 {
			return fmt.Errorf("events: warmup offsets: chunk %s cumulative event count overflow at ledger %d",
				chunkID, ledger)
		}
		offsets.Append(eventCount)
		return nil
	}); err != nil {
		return nil, err
	}
	return offsets, nil
}

// scanOffsetsCF iterates the OffsetsCF, validating each row's SHAPE (key and
// value widths, big-endian decode) and yielding (ledger, count) to emit.
// It is the one trust boundary for the row format; sequencing, range, and
// overflow policy belong to each caller's accumulator (warmup's concurrent
// offsets vs the freeze's LedgerOffsets differ deliberately).
func scanOffsetsCF(store *rocksdb.Store, emit func(ledger, count uint32) error) error {
	for entry, err := range store.Iterate(OffsetsCF, nil) {
		if err != nil {
			return fmt.Errorf("events: scan %s: %w", OffsetsCF, err)
		}
		if len(entry.Key) != offsetKeyLen {
			return fmt.Errorf("events: unexpected %s key length %d (want %d)",
				OffsetsCF, len(entry.Key), offsetKeyLen)
		}
		if len(entry.Value) != offsetValLen {
			return fmt.Errorf("events: unexpected %s value length %d (want %d)",
				OffsetsCF, len(entry.Value), offsetValLen)
		}
		if err := emit(binary.BigEndian.Uint32(entry.Key), binary.BigEndian.Uint32(entry.Value)); err != nil {
			return err
		}
	}
	return nil
}

// ──────────────────────────────────────────────────────────────────
// Key encoding helpers — RocksDB key layouts for the per-Chunk DB.
// ──────────────────────────────────────────────────────────────────

func encodeDataKey(eventID uint32) []byte {
	var key [dataKeyLen]byte
	binary.BigEndian.PutUint32(key[:], eventID)
	return key[:]
}

// encodePackedIndexKey delegates to rocksdb.EncodeUint32 — the same call
// warmup's tail seek uses on this CF, so writer and reader keys agree by
// construction rather than by inspection.
func encodePackedIndexKey(ledgerSeq uint32) []byte { return rocksdb.EncodeUint32(ledgerSeq) }

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
