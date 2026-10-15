package event

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"math"

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
//   - IndexCF stores one (slab || term_hash) -> bitmap entry per term of
//     each sealed slab, loaded one sorted file per slab (see hotIndex).
//     Auto compaction is off, and the files never overlap so it would have
//     nothing to do; their index blocks sit in the block cache so a chunk's
//     many files cost bounded memory. No bloom filter: it would be the
//     largest thing in that cache, to save a few microseconds per slab on
//     an absent term.
//   - OffsetsCF stores 8-byte (ledger_seq -> event_count) rows in
//     the tens-of-thousands per chunk; small blocks waste less I/O per
//     point read.
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
		IndexCF: {
			BlockSize:                 indexCFBlockSize,
			CacheIndexAndFilterBlocks: true,
			DisableAutoCompactions:    true,
		},
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
	dataKeyLen   = 4 // event_id (chunk encoded by per-Chunk DB directory)
	offsetKeyLen = 4 // ledger_seq
	offsetValLen = 4 // per-ledger event count (uint32 BE)
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

// HotStore wraps one chunk's hot RocksDB DB plus the term index and
// ledger-offset cache that feed the query path.
//
// Atomicity: the per-Chunk DB is the source of truth. IngestLedgerToBatch queues
// data + offsets into one atomic batch, then (post-commit) the apply hook
// updates the term index and the offset cache; warmup reconstructs both from
// the on-disk CFs on next startup.
//
// Concurrency:
//
//   - Writes (IngestLedgerToBatch) are single-writer (one goroutine per chunk).
//   - Reads (LookupKeys, FetchEvents, All) take NO HotStore-level lock — they guard
//     via chunkStore.IsClosed() and rely on the index's lock-free reads and
//     RocksDB's thread-safety.
//   - Metadata split after the caller-owned store is closed: ChunkID is
//     infallible (cached, usable post-close); EventCount and
//     Offsets return stores.ErrStoreClosed after close (Reader-interface contract).
type HotStore struct {
	chunkStore *rocksdb.Store
	chunkID    chunk.ID
	index      *hotIndex
	offsets    *ConcurrentLedgerOffsets
}

// Compile-time guard: *HotStore satisfies Reader.
var _ Reader = (*HotStore)(nil)

// NewWithStore wraps an ALREADY-OPEN rocksdb.Store as an events HotStore on the
// three events CFs (CFNames()), running the mandatory warmup to rebuild the
// term index + offsets. The store is owned by the caller — in production,
// hotchunk.DB composes this facade over the shared per-chunk DB and closes that
// DB once. The store must have CFNames() registered + CFOptions() applied, and
// be writable: warmup seals a full slab it indexes again as soon as the next
// one starts. A warmup failure returns the error WITHOUT closing the
// caller-owned store.
func NewWithStore(store *rocksdb.Store, chunkID chunk.ID) (*HotStore, error) {
	index, offsets, err := warmup(store, chunkID)
	if err != nil {
		return nil, fmt.Errorf("events: warmup chunk %s: %w", chunkID, err)
	}
	return &HotStore{
		chunkStore: store,
		chunkID:    chunkID,
		index:      index,
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

// Offsets returns a point-in-time view of the ledger-offset cache,
// the query side's source for translating ledger bounds into
// event-id windows (see Reader.Offsets).
//
// Implementation: returns a *LedgerOffsets sharing the live
// backing array, capped at the count visible at call time
// (~24-byte allocation per Matches call). A concurrent IngestLedgerToBatch
// may extend the backing past the cap, but the returned view's
// slice stays bounded to what was visible when Offsets returned.
// Callers (Matches) take the view once at entry and pass it through
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

// LookupKeys returns each key's event ids in the slabs the window touches,
// aligned positionally with the input slice, and the id range those slabs
// span. See Reader.LookupKeys for the semantics.
//
// A key with no events in the covered range gets an empty bitmap, never
// nil: nil would promise the chunk has none, and the lookup read only the
// window's slabs. Every bitmap is built for the caller.
func (h *HotStore) LookupKeys(
	ctx context.Context, keys []TermKey, window IDRange,
) ([]*roaring.Bitmap, IDRange, error) {
	if h.chunkStore.IsClosed() {
		return nil, IDRange{}, stores.ErrStoreClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, IDRange{}, err
	}
	if len(keys) == 0 {
		return nil, window, nil
	}
	results, covered, err := h.index.lookup(ctx, keys, window)
	if errors.Is(err, rocksdb.ErrStoreClosed) {
		return nil, IDRange{}, stores.ErrStoreClosed
	}
	if err != nil {
		return nil, IDRange{}, fmt.Errorf("events: LookupKeys for chunk %s: %w", h.chunkID, err)
	}
	return results, covered, nil
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
// A missing row is an error: every caller passes ids that name
// stored events (see Reader.FetchEvents), implying RocksDB has
// them. A miss indicates corruption or a writer/reader mismatch,
// not a normal not-found case.
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
// caller runs AFTER b commits (decision (a)). Validation + term derivation happen
// before any Put; on any error Store.Batch discards the whole WriteBatch, so a
// rejected ledger never leaves committed rows behind.
//
// payloads is produced by PayloadsFromLedgerEvents, which emits each ledger's
// events in ascending getEvents cursor order — write order here IS the cursor
// contract (event IDs are assigned by arrival position). Terms are derived via
// TermsForBytes on each payload's ContractEventBytes.
//
// Sequence validation, before any Put or index mutation:
//
//   - ledgerSeq must lie within [chunkID.FirstLedger(), chunkID.LastLedger()] —
//     out-of-range returns ErrLedgerOutOfRange.
//   - ledgerSeq must equal the next expected ledger (StartLedger + LedgerCount).
//     Under decision (a) resume is always MaxCommittedSeq+1, so a non-expected
//     ledger is a mis-sequencing source (the ingestion loop's seq guard should
//     have caught it) — an error (ErrLedgerOutOfOrder), never silent tolerance.
//
// The apply hook fails only when the index could not seal an earlier slab
// (see hotIndex.add). The ledger is committed either way, and the next open
// indexes it again, so the caller must not go on ingesting after a failure.
func (h *HotStore) IngestLedgerToBatch(
	b *rocksdb.BatchWriter, ledgerSeq uint32, payloads []Payload,
) (func() error, error) {
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
	// ledger without any Put) and retain them for the post-commit index update.
	termKeys := make([][]TermKey, len(payloads))
	for i := range payloads {
		keys, err := TermsForBytes(payloads[i].ContractEventBytes)
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
		b.Put(DataCF, encodeDataKey(startID+uint32(i)), blob)
	}
	//nolint:gosec // len bounded by the overflow guard above
	b.Put(OffsetsCF, encodeOffsetKey(ledgerSeq), encodeLedgerEventCount(uint32(len(payloads))))

	return func() error { return h.applyLedger(startID, termKeys) }, nil
}

// applyLedger updates the index + offsets for a ledger whose rows are durable.
// It appends in memory; at a slab boundary it first waits for the previous
// slab's seal.
//
// Ordering invariant: index BEFORE offsets. A concurrent Matches call that snapshots
// offsets then reads the index must see either the prior state or a consistent
// later one. Reversing it would let a reader see an offsets count including IDs
// the index hasn't published — FetchEvents would then miss them, silently.
func (h *HotStore) applyLedger(startID uint32, termKeys [][]TermKey) error {
	if err := h.index.add(startID, termKeys); err != nil {
		return err
	}
	//nolint:gosec // len bounded by IngestLedgerToBatch's overflow guard
	h.offsets.Append(uint32(len(termKeys)))
	return nil
}

// ──────────────────────────────────────────────────────────────────
// Warmup — reconstructs the term index + offsets from the per-Chunk
// DB's on-disk CFs. Called by NewWithStore.
// ──────────────────────────────────────────────────────────────────

// warmup rebuilds the in-memory state for chunkID:
//
//   - events_offsets → *ConcurrentLedgerOffsets — every
//     (ledger_seq, per_ledger_count) row replayed into a fresh
//     offset cache.
//   - events_index + events_data → *hotIndex — the sealed slabs stay
//     in events_index; the events after them are indexed again from
//     their events_data rows, and a full slab a later event follows is
//     sealed.
//
// chunkID seeds ConcurrentLedgerOffsets.StartLedger for empty
// chunks; on-disk rows carry the full ledger sequence themselves.
func warmup(
	chunkStore *rocksdb.Store, chunkID chunk.ID,
) (*hotIndex, *ConcurrentLedgerOffsets, error) {
	offsets, err := warmupOffsets(chunkStore, chunkID)
	if err != nil {
		return nil, nil, err
	}
	sealed, err := sealedHotSlabs(chunkStore)
	if err != nil {
		return nil, nil, err
	}
	if err := verifyChunkConsistency(chunkStore, offsets.TotalEvents(), sealed); err != nil {
		return nil, nil, err
	}
	index := newHotIndex(chunkStore, sealed)
	if err := replayUnsealed(chunkStore, index, sealed<<indexSlabShift, offsets.TotalEvents()); err != nil {
		return nil, nil, err
	}
	return index, offsets, nil
}

// replayUnsealed indexes the committed events in [from, total), which the
// index must not hold yet, from their events_data rows.
func replayUnsealed(chunkStore *rocksdb.Store, index *hotIndex, from, total uint32) error {
	if from == total {
		return nil
	}
	// The scan holds the store's lifecycle lock, which a seal takes too, so
	// the events are indexed after the scan.
	var termKeys [][]TermKey
	for entry, err := range chunkStore.IterateRange(DataCF, encodeDataKey(from), encodeDataKey(total-1)) {
		if err != nil {
			return fmt.Errorf("events: warmup scan %s: %w", DataCF, err)
		}
		id := from + uint32(len(termKeys)) //nolint:gosec // below total
		if len(entry.Key) != dataKeyLen || binary.BigEndian.Uint32(entry.Key) != id {
			return fmt.Errorf("events: corrupt chunk: %s key %x where event %d belongs", DataCF, entry.Key, id)
		}
		var p Payload
		if err := p.Unmarshal(entry.Value); err != nil {
			return fmt.Errorf("events: warmup decode event %d: %w", id, err)
		}
		keys, err := TermsForBytes(p.ContractEventBytes)
		if err != nil {
			return fmt.Errorf("events: warmup derive terms for event %d: %w", id, err)
		}
		termKeys = append(termKeys, keys)
	}
	if found := len(termKeys); found != int(total-from) {
		return fmt.Errorf("events: corrupt chunk: %s holds %d of the %d committed events", DataCF, int(from)+found, total)
	}
	if err := index.add(from, termKeys); err != nil {
		return err
	}
	return index.settle()
}

// verifyChunkConsistency cross-checks the three on-disk CFs before the
// unsealed events are indexed again, turning a torn or tampered chunk into
// a loud open failure instead of a
// silently inconsistent in-memory cache. Data and offsets are written in
// one atomic batch and a slab is sealed only after its events commit, so
// under normal operation these invariants always hold; a violation means
// a bug or external corruption.
//
//   - the index may not hold a slab the offsets don't account for: a
//     sealed slab is full, so the offsets count all of its ids.
//   - the data tail matches total: event total-1 present (when total > 0)
//     and no data row at any id >= total. Together those pin the max data
//     id to exactly total-1 — one Get plus one bounded seek.
//
// Not detected here: interior data holes below the sealed slabs (a missing
// id masked by a higher present id), under-indexed terms, and wrong
// per-ledger boundaries — each would need a full scan. The atomic batch
// makes all of them impossible for the writer; an interior hole that did
// appear (corruption/tamper) is caught lazily by FetchRange's short-scan
// check on first read, or by warmup when it is among the events indexed
// again. This is a cheap open-time tripwire on denormalized
// state, not load-bearing correctness.
func verifyChunkConsistency(chunkStore *rocksdb.Store, total, sealedSlabs uint32) error {
	if sealedSlabs > total>>indexSlabShift {
		return fmt.Errorf("events: corrupt chunk: %d index slabs sealed but only %d events committed",
			sealedSlabs, total)
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
