package event

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"math"
	"sort"

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
//   - DataCF holds one sorted file per sealed window of event payloads (see
//     hotData). Event ids ascend, so the files never overlap and auto
//     compaction is off. Larger blocks give zstd more context.
//   - IndexCF holds one sorted file per sealed slab, of (slab || term
//     key) -> bitmap entries (see hotIndex). Auto compaction is off: the
//     files never overlap. Index and filter blocks sit in the block cache:
//     a chunk has many files, and the cache bounds their memory. The
//     filter answers a slab that lacks a term without a block read, and a
//     lookup probes every sealed slab in its window.
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
			Compression:            grocksdb.ZSTDCompression,
			BlockSize:              dataCFBlockSize,
			DisableAutoCompactions: true,
		},
		IndexCF: {
			BlockSize:                 indexCFBlockSize,
			BloomFilterBitsPerKey:     10,
			RibbonFilter:              true,
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

// ErrLedgerOutOfRange is returned by AddToBatch and Replay when the
// supplied ledger sequence falls outside the chunk's [FirstLedger,
// LastLedger] window.
var ErrLedgerOutOfRange = errors.New("events: ledger outside chunk range")

// ErrLedgerOutOfOrder is returned by AddToBatch and Replay when the
// supplied ledger sequence is not the next-expected one. Catches
// duplicate ingest of an already-committed ledger as well as gaps
// (skipping ahead). Both would silently corrupt the per-ledger
// offset chain if not rejected up front.
var ErrLedgerOutOfOrder = errors.New("events: ledger out of order")

// HotStore is one chunk's hot events: the event payloads, the term index
// and the ledger offsets.
//
// A ledger's commit holds only its event count (OffsetsCF). Its payloads and
// postings are kept in memory after the commit and sealed into DataCF and
// IndexCF in the background.
//
// Concurrency:
//
//   - Writes (AddToBatch, Apply) are single-writer (one goroutine per chunk).
//   - Reads (LookupTerms, FetchEvents, All) take NO HotStore-level lock; they guard
//     via chunkStore.IsClosed() and rely on the lock-free reads of the data and
//     the index and on RocksDB's thread-safety.
//   - Metadata split after the caller-owned store is closed: ChunkID is
//     infallible (cached, usable post-close); EventCount and
//     Offsets return stores.ErrStoreClosed after close (Reader-interface contract).
type HotStore struct {
	chunkStore *rocksdb.Store
	chunkID    chunk.ID
	data       *hotData
	index      *hotIndex
	offsets    *ConcurrentLedgerOffsets
	// committed is what OffsetsCF held at open, until the replay is over.
	committed *LedgerOffsets
	// terms is reused for every ledger ingested: each is applied before the
	// next is derived. The replay derives all its ledgers before applying
	// one, so each of those gets its own.
	terms ledgerTerms
}

// Compile-time guard: *HotStore satisfies Reader.
var _ Reader = (*HotStore)(nil)

// NewWithStore wraps an ALREADY-OPEN rocksdb.Store as an events HotStore on the
// three events CFs (CFNames()). It holds the sealed events only: the caller
// replays the ledgers from Unsealed on (Derive, Replay) and then calls
// Replayed. The store is owned by the caller and must be writable, because
// the replay can seal. A failure returns the error WITHOUT closing the store.
// The index keys terms with the chunk's cold index secret,
// ColdIndexSecret(catalogSecret, chunkID), so its keys are the cold build's.
func NewWithStore(store *rocksdb.Store, chunkID chunk.ID, catalogSecret [32]byte) (*HotStore, error) {
	return newWithStore(store, chunkID, catalogSecret, sealBytes, sealLedgers)
}

func newWithStore(
	store *rocksdb.Store, chunkID chunk.ID, catalogSecret [32]byte, maxBytes, maxLedgers int,
) (*HotStore, error) {
	h := &HotStore{chunkStore: store, chunkID: chunkID}
	if err := h.warmup(catalogSecret, maxBytes, maxLedgers); err != nil {
		return nil, fmt.Errorf("events: warmup chunk %s: %w", chunkID, err)
	}
	return h, nil
}

// ChunkID returns the chunk this store serves. Infallible and usable post-close
// (the Reader exception). No production caller yet — the intended read seam for
// the v2 cutover (#772), exercised by tests until then.
func (h *HotStore) ChunkID() chunk.ID { return h.chunkID }

// EventCount is the total number of events committed to this Chunk
// so far. Equal to the id Apply gives the next event.
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
// Implementation: returns a *LedgerOffsets sharing the live backing
// array, capped at the count visible at call time (~24-byte allocation
// per Matches call). A concurrent Apply may extend the backing past the
// cap, but the returned view's slice stays bounded to what was visible
// when Offsets returned.
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

// LookupTerms returns each term's event ids in the slabs the window touches,
// aligned positionally with the input slice, and the id range those slabs
// span. See Reader.LookupTerms for the semantics.
//
// A term with no events in the covered range gets an empty bitmap, never
// nil: nil would promise the chunk has none, and the lookup read only the
// window's slabs. Every bitmap is built for the caller.
func (h *HotStore) LookupTerms(
	ctx context.Context, terms []Term, window IDRange,
) ([]*roaring.Bitmap, Coverage, error) {
	if h.chunkStore.IsClosed() {
		return nil, Coverage{}, stores.ErrStoreClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, Coverage{}, err
	}
	if len(terms) == 0 {
		return nil, Coverage{IDRange: window}, nil
	}
	results, covered, err := h.index.lookup(ctx, terms, window)
	if errors.Is(err, rocksdb.ErrStoreClosed) {
		return nil, Coverage{}, stores.ErrStoreClosed
	}
	if err != nil {
		return nil, Coverage{}, fmt.Errorf("events: LookupTerms for chunk %s: %w", h.chunkID, err)
	}
	return results, Coverage{IDRange: covered}, nil
}

// FetchEvents returns the events with the given ids, positionally aligned
// with the input slice. See Reader.FetchEvents for the sorted-input
// precondition. Ids below the first one in memory are read from DataCF, the
// rest from the unsealed windows.
//
// It calls BatchMultiGet once per matchBatchSize ids: RocksDB pins every
// block a call touches until the call returns, so the slices bound what
// one request pins in the block cache. ctx is checked before each call.
//
// A missing event is an error: every caller passes ids that name stored
// events (see Reader.FetchEvents).
//
// After the caller-owned store is closed, returns stores.ErrStoreClosed.
func (h *HotStore) FetchEvents(ctx context.Context, eventIDs []uint32) ([]Payload, error) {
	if h.chunkStore.IsClosed() {
		return nil, stores.ErrStoreClosed
	}
	if len(eventIDs) == 0 {
		return nil, nil
	}
	if err := validateSortedEventIDs(eventIDs); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	first, inMemory := h.data.inMemory()
	sealed := sort.Search(len(eventIDs), func(i int) bool { return eventIDs[i] >= first })
	values := make([][]byte, 0, len(eventIDs))
	keys := make([][]byte, sealed)
	for i, id := range eventIDs[:sealed] {
		keys[i] = encodeDataKey(id)
	}
	step := max(1, matchBatchSize)
	for lo := 0; lo < len(keys); lo += step {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, err := h.chunkStore.BatchMultiGet(DataCF, keys[lo:min(lo+step, len(keys))])
		if err != nil {
			return nil, fmt.Errorf("events: batch fetch from chunk %s: %w", h.chunkID, err)
		}
		values = append(values, batch...)
	}
	for _, id := range eventIDs[sealed:] {
		// Cloned, as the cold reader's are: a payload must not keep its
		// ledger's row alive.
		values = append(values, bytes.Clone(inMemory(id)))
	}

	results := make([]Payload, len(eventIDs))
	for i, id := range eventIDs {
		if i >= len(values) || values[i] == nil {
			return nil, fmt.Errorf("events: event %d missing from chunk %s", id, h.chunkID)
		}
		if err := results[i].Unmarshal(values[i]); err != nil {
			return nil, fmt.Errorf("events: decode event %d from chunk %s: %w", id, h.chunkID, err)
		}
	}
	return results, nil
}

// FetchRange streams count events starting at chunk-relative event
// ID start, in ascending eventID order: see Reader.FetchRange.
//
// Yielded Payloads are borrowed: ContractEventBytes is valid only until the
// next step. Clone to retain.
//
// After the caller-owned store is closed, yields (zero Payload, stores.ErrStoreClosed) and stops.
// ctx is checked at entry and between steps.
//
// count == 0 yields nothing. A range past the committed event count, or an
// event missing inside it, yields an error and stops.
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
		for raw, err := range h.data.from(start, start+count) {
			if err != nil {
				yield(Payload{}, fmt.Errorf("events: scan chunk %s: %w", h.chunkID, err))
				return
			}
			if err := ctx.Err(); err != nil {
				yield(Payload{}, err)
				return
			}
			var p Payload
			if err := p.Unmarshal(raw); err != nil {
				yield(Payload{}, fmt.Errorf("events: decode event from chunk %s: %w", h.chunkID, err))
				return
			}
			if !yield(p, nil) {
				return
			}
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

// Derive builds what the store keeps of ledger seq's events, and starts
// keying their terms on another goroutine. payloads come from
// PayloadsFromLedgerEvents: event ids are assigned by position.
func (h *HotStore) Derive(seq uint32, payloads []Payload) (LedgerEvents, error) {
	e, err := MarshalLedgerEvents(seq, payloads)
	if err != nil {
		return LedgerEvents{}, err
	}
	terms := &h.terms
	if h.committed != nil {
		terms = new(ledgerTerms)
	}
	terms.reset()
	if !h.indexed(seq) {
		for i := range payloads {
			if err := terms.appendEvent(payloads[i].ContractEventBytes); err != nil {
				return LedgerEvents{}, fmt.Errorf("derive terms for payload %d in ledger %d: %w", i, seq, err)
			}
		}
	}
	terms.keyAsync(h.index.secret)
	e.terms = terms
	return e, nil
}

// indexed reports whether the index already holds replayed ledger seq's
// events, so their terms would go unused.
func (h *HotStore) indexed(seq uint32) bool {
	if h.committed == nil {
		return false
	}
	_, end, err := h.committed.EventIDs(seq)
	return err == nil && end <= h.index.unsealed.Load().Live.slab<<indexSlabShift
}

// AddToBatch queues the ledger's event count into the SHARED batch b. The
// caller runs Apply AFTER b commits. The ledger must lie within the chunk
// (ErrLedgerOutOfRange) and be the next one (ErrLedgerOutOfOrder).
func (h *HotStore) AddToBatch(b *rocksdb.BatchWriter, e LedgerEvents) error {
	if err := h.checkNext(e); err != nil {
		return err
	}
	//nolint:gosec // len bounded by checkNext
	b.Put(OffsetsCF, encodeOffsetKey(e.seq), encodeLedgerEventCount(uint32(len(e.ends))))
	return nil
}

func (h *HotStore) checkNext(e LedgerEvents) error {
	if e.seq < h.chunkID.FirstLedger() || e.seq > h.chunkID.LastLedger() {
		return fmt.Errorf("%w: ledger %d not in chunk %s [%d, %d]",
			ErrLedgerOutOfRange, e.seq, h.chunkID, h.chunkID.FirstLedger(), h.chunkID.LastLedger())
	}
	if expected := h.offsets.EndLedger(); e.seq != expected {
		return fmt.Errorf("%w: expected ledger %d, got %d", ErrLedgerOutOfOrder, expected, e.seq)
	}
	if uint64(h.offsets.TotalEvents())+uint64(len(e.ends)) > math.MaxUint32 {
		return fmt.Errorf("chunk %s would overflow uint32 event-id space at ledger %d", h.chunkID, e.seq)
	}
	return nil
}

// Apply adds a committed ledger in memory: data, then index, then offsets. A
// reader takes its bounds from the offsets, so every id below the count it
// saw is already in the data and the index.
//
// It fails only when an earlier seal failed, after which the caller must
// stop ingesting.
func (h *HotStore) Apply(e LedgerEvents) error {
	terms := e.terms
	e.terms = nil
	e.first = h.offsets.TotalEvents()
	terms.waitKeyed()
	if err := h.data.apply(e); err != nil {
		return err
	}
	if err := h.index.add(e.first, terms); err != nil {
		return err
	}
	//nolint:gosec // len bounded by checkNext
	h.offsets.Append(uint32(len(e.ends)))
	return nil
}

// Unsealed returns the next ledger to apply, which until the replay is the
// first ledger whose events were not sealed.
func (h *HotStore) Unsealed() uint32 { return h.offsets.EndLedger() }

// Replay applies a ledger committed before the open. The ledgers from
// Unsealed on are replayed in order, and each must yield the event count its
// commit recorded.
func (h *HotStore) Replay(e LedgerEvents) error {
	if err := h.checkNext(e); err != nil {
		return err
	}
	if err := checkCommitted(h.committed, &e); err != nil {
		return err
	}
	return h.Apply(e)
}

// checkCommitted checks that e holds the event count offsets record for its
// ledger.
func checkCommitted(offsets *LedgerOffsets, e *LedgerEvents) error {
	first, end, err := offsets.EventIDs(e.seq)
	if err != nil {
		return fmt.Errorf("events: corrupt chunk: %w", err)
	}
	if uint32(len(e.ends)) != end-first { //nolint:gosec // an event count
		return fmt.Errorf("events: corrupt chunk: ledger %d: %d events derived, %d committed",
			e.seq, len(e.ends), end-first)
	}
	return nil
}

// Replayed ends the replay: every committed ledger must be applied, and the
// seals the replay started must land.
func (h *HotStore) Replayed() error {
	if end, want := h.offsets.EndLedger(), h.committed.EndLedger(); end != want {
		return fmt.Errorf("events: corrupt chunk: the ledgers end at %d, the offsets at %d", end-1, want-1)
	}
	h.committed = nil
	if err := h.data.unsealed.Wait(); err != nil {
		return err
	}
	return h.index.unsealed.Wait()
}

// warmup loads the sealed state: the offsets of the ledgers whose events are
// sealed, and the postings after the sealed slabs from the sealed events.
func (h *HotStore) warmup(catalogSecret [32]byte, maxBytes, maxLedgers int) error {
	committed, err := warmupOffsets(h.chunkStore, h.chunkID)
	if err != nil {
		return err
	}
	h.committed = committed.View()
	sealedSlabs, sealed, err := sealedCounts(h.chunkStore, h.committed.TotalEvents())
	if err != nil {
		return err
	}
	unsealed, err := unsealedLedger(h.committed, sealed)
	if err != nil {
		return err
	}
	h.offsets = NewConcurrentLedgerOffsets(h.chunkID.FirstLedger())
	for seq := h.chunkID.FirstLedger(); seq < unsealed; seq++ {
		first, end, err := h.committed.EventIDs(seq)
		if err != nil {
			return err
		}
		h.offsets.Append(end - first)
	}
	h.data = newHotData(h.chunkStore, sealed, maxBytes, maxLedgers)
	h.index = newHotIndex(h.chunkStore, sealedSlabs, ColdIndexSecret(catalogSecret[:], h.chunkID))
	if from := sealedSlabs << indexSlabShift; from < sealed {
		return indexSealedEvents(h.data, h.index, from, sealed)
	}
	return nil
}

// unsealedLedger returns the first ledger with an event from id sealed on,
// the ledger after the last if there is none. The sealed events must end
// with a ledger.
func unsealedLedger(offsets *LedgerOffsets, sealed uint32) (uint32, error) {
	if sealed == offsets.TotalEvents() {
		return offsets.EndLedger(), nil
	}
	ends := offsets.Offsets()
	i := sort.Search(len(ends), func(i int) bool { return ends[i] > sealed })
	seq := offsets.StartLedger() + uint32(i) //nolint:gosec // at most a chunk's ledgers
	if first, _, err := offsets.EventIDs(seq); err != nil || first != sealed {
		return 0, fmt.Errorf("events: corrupt chunk: %d events sealed, which ends inside ledger %d", sealed, seq)
	}
	return seq, nil
}

// indexSealedEvents indexes the sealed events in [from, end), which the
// index must not hold yet.
func indexSealedEvents(data *hotData, index *hotIndex, from, end uint32) error {
	// Collect the keys first and index them after the scan: indexing can
	// seal, and a seal takes the lifecycle lock the scan holds.
	var terms ledgerTerms
	for raw, err := range data.from(from, end) {
		if err != nil {
			return err
		}
		id := from + uint32(len(terms.ends)) //nolint:gosec // below end
		var p Payload
		if err := p.Unmarshal(raw); err != nil {
			return fmt.Errorf("events: warmup decode event %d: %w", id, err)
		}
		if err := terms.appendKeyed(p.ContractEventBytes, index.secret); err != nil {
			return fmt.Errorf("events: warmup derive terms for event %d: %w", id, err)
		}
	}
	return index.add(from, &terms)
}

// sealedCounts reads how many index slabs and how many events are sealed.
// Neither may pass total, the committed event count.
func sealedCounts(store *rocksdb.Store, total uint32) (slabs, events uint32, err error) {
	if slabs, err = sealedHotSlabs(store); err != nil {
		return 0, 0, err
	}
	if events, err = sealedEvents(store); err != nil {
		return 0, 0, err
	}
	if slabs > total>>indexSlabShift {
		return 0, 0, fmt.Errorf("events: corrupt chunk: %d index slabs sealed but only %d events committed", slabs, total)
	}
	if events > total {
		return 0, 0, fmt.Errorf("events: corrupt chunk: %d events sealed but only %d committed", events, total)
	}
	return slabs, events, nil
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
