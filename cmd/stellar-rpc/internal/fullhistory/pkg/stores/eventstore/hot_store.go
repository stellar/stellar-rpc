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
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2"

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

// openHotChunk opens (or creates) chunkID's per-Chunk hot RocksDB DB
// at HotChunkDir(dataDir, chunkID). The three per-Chunk CFs are
// configured at New so they auto-create on a fresh DB and are
// rediscovered on a reopen.
//
// Unexported: OpenHotStore is the only caller and is the public way
// to open a per-Chunk hot DB (since the warmup step is mandatory
// before the store is usable).
func openHotChunk(dataDir string, chunkID chunk.ID, logger *supportlog.Entry) (*rocksdb.Store, error) {
	store, err := rocksdb.New(rocksdb.Config{
		Path:           HotChunkDir(dataDir, chunkID),
		ColumnFamilies: []string{DataCF, IndexCF, OffsetsCF},
		Logger:         logger,
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
	offsetValLen = 4      // cumulative event count (uint32 BE)
)

// ErrLedgerOutOfRange is returned by IngestLedgerEvents when the
// supplied ledger sequence falls outside the chunk's [FirstLedger,
// LastLedger] window.
var ErrLedgerOutOfRange = errors.New("events: ledger outside chunk range")

// ErrLedgerOutOfOrder is returned by IngestLedgerEvents when the
// supplied ledger sequence is not the next-expected one. Catches
// duplicate ingest of an already-committed ledger as well as gaps
// (skipping ahead). Both would silently corrupt the cumulative
// offset chain if not rejected up front.
var ErrLedgerOutOfOrder = errors.New("events: ledger out of order")

// HotStore wraps one chunk's hot RocksDB DB plus the in-memory term
// mirror and ledger-offset cache that feed the query path. Reads and
// writes share the same struct; every HotStore owns its chunkStore
// exclusively and Close releases it.
//
// Atomicity model: the per-Chunk DB is the source of truth.
// IngestLedgerEvents commits data + index + offsets to chunkStore in one
// atomic batch and then updates the in-memory mirrors. Warmup on next
// startup reconstructs the mirrors from the chunk's on-disk CFs.
//
// Concurrency model:
//
//   - Writes (IngestLedgerEvents) take h.mu to serialize against other
//     ingest calls (single-writer model). After RocksDB commits,
//     mirror and offsets updates run while the mutex is held; both
//     in-memory primitives have their own internal locks for read
//     concurrency.
//   - Reads (Lookup, FetchEvents, All) take NO HotStore-level lock.
//     They check h.closed atomically as a fast-path guard and then
//     rely on the in-memory primitives' internal locks (for the
//     mirror) and RocksDB's own thread-safety (for chunkStore).
//   - Metadata accessors (ChunkID, EventCount, NextEventID, Offsets,
//     Index) survive Close: they return values cached at Open and
//     never check h.closed. Callers can use them for logging,
//     metrics, or error context after closing the store.
//   - Close uses CompareAndSwap on h.closed for idempotency, closes
//     the mirror (freeze gate; safe to call even if the freeze
//     orchestrator already did), and releases the chunkStore.
//     Subsequent Lookup / FetchEvents / All return ErrClosed.
type HotStore struct {
	chunkStore *rocksdb.Store
	chunkID    chunk.ID

	mu      sync.Mutex // serializes IngestLedgerEvents against itself
	closed  atomic.Bool
	mirror  events.BitmapIndex
	offsets *events.LedgerOffsets
}

// Compile-time guard: *HotStore satisfies Reader.
var _ Reader = (*HotStore)(nil)

// OpenHotStore opens (or creates) chunkID's hot DB at
// HotChunkDir(dataDir, chunkID), warms up the in-memory mirror and
// offsets from disk, and returns a ready-to-use HotStore. The
// returned store owns its chunkStore; Close releases it.
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
	mirror, offsets, err := warmup(chunkStore, chunkID)
	if err != nil {
		_ = chunkStore.Close()
		return nil, fmt.Errorf("events: warmup chunk %s: %w", chunkID, err)
	}
	return &HotStore{
		chunkStore: chunkStore,
		chunkID:    chunkID,
		mirror:     mirror,
		offsets:    offsets,
	}, nil
}

// Close releases the underlying chunk store and marks the in-memory
// mirror immutable. Idempotent.
//
// Concurrency: must not be called concurrently with in-flight read
// methods on the same HotStore (Lookup, FetchEvents, All). Callers
// drain those reads before invoking Close. IngestLedgerEvents takes
// h.mu and is therefore exclusive of itself, but it does not block
// Close — Close's CAS on h.closed plus the entry guard in
// IngestLedgerEvents prevent a write from racing past a Close.
func (h *HotStore) Close() error {
	if !h.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Mark the mirror immutable. The freeze orchestrator may have
	// already done this before invoking Close — mirror.Close is
	// idempotent, so the redundant call is harmless.
	_ = h.mirror.Close()
	return h.chunkStore.Close()
}

// ChunkID returns the chunk this store serves.
func (h *HotStore) ChunkID() chunk.ID { return h.chunkID }

// EventCount is the total number of events committed to this Chunk
// so far. Equal to the next event-id IngestLedgerEvents would assign.
func (h *HotStore) EventCount() uint32 { return h.offsets.TotalEvents() }

// NextEventID is the next chunk-relative event ID IngestLedgerEvents will
// assign. Identical to EventCount; exposed under both names for the
// ingest-side and reader-side mental models.
func (h *HotStore) NextEventID() uint32 { return h.offsets.TotalEvents() }

// Offsets returns the in-memory ledger-offset cache. The coordinator
// uses this to stitch a multi-ledger query range into chunk-relative
// event-id ranges (see Reader.Offsets).
//
// Cached at Open (or rebuilt by warmup on reopen); survives Close.
// Callers must treat the returned value as read-only — mutations
// would corrupt the mirror seen by every other reader.
func (h *HotStore) Offsets() *events.LedgerOffsets { return h.offsets }

// Index returns the in-memory term mirror. Used by the freezer to
// stream every (events.TermKey, bitmap) pair into WriteColdIndex without
// rebuilding from RocksDB.
func (h *HotStore) Index() events.BitmapIndex { return h.mirror }

// Lookup returns the bitmap of event IDs in this Chunk that match
// the given term. A clone is returned so the caller can intersect /
// union freely without affecting the mirror or other readers.
// Returns (nil, ErrTermNotFound) when the term has no matching
// events. Returns (nil, ErrClosed) after Close.
func (h *HotStore) Lookup(key events.TermKey) (*roaring.Bitmap, error) {
	if h.closed.Load() {
		return nil, ErrClosed
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

// FetchEvents decodes the events_data row for each provided eventID
// and returns them positionally aligned with the input slice. See
// Reader.FetchEvents for the sorted-input precondition.
//
// Implementation: builds a sorted [][]byte of encoded eventID keys
// and calls rocksdb.Store.BatchMultiGet once. The batched API
// crosses CGO a single time regardless of key count and enables
// async_io so the kernel can overlap SST page reads — a meaningful
// win on EBS / high-random-latency storage. ctx is honored at the
// top of the call; the underlying CGO call is not cancellable
// mid-flight.
//
// A missing row is an error: eventIDs only reach this path through
// Lookup, which only returns IDs the mirror knows about — implying
// RocksDB also has them. A miss indicates corruption or a
// writer/reader mismatch, not a normal not-found case.
//
// After Close, returns ErrClosed.
func (h *HotStore) FetchEvents(ctx context.Context, eventIDs []uint32) ([]events.Payload, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(eventIDs) == 0 {
		return nil, nil
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
		if err := results[i].Unmarshal(v); err != nil {
			return nil, fmt.Errorf("events: decode event %d from chunk %s: %w", id, h.chunkID, err)
		}
	}
	return results, nil
}

// All streams every event in this Chunk in chunk-relative eventID
// order. Used by the freeze loop to dump a hot Chunk into a
// ColdWriter without buffering.
//
// After Close, yields (zero Payload, ErrClosed) and stops.
func (h *HotStore) All() iter.Seq2[events.Payload, error] {
	return func(yield func(events.Payload, error) bool) {
		if h.closed.Load() {
			yield(events.Payload{}, ErrClosed)
			return
		}
		for entry, err := range h.chunkStore.Iterate(DataCF, nil) {
			if err != nil {
				yield(events.Payload{}, fmt.Errorf("events: scan chunk %s: %w", h.chunkID, err))
				return
			}
			var p events.Payload
			if err := p.Unmarshal(entry.Value); err != nil {
				yield(events.Payload{}, fmt.Errorf("events: decode event from chunk %s: %w",
					h.chunkID, err))
				return
			}
			if !yield(p, nil) {
				return
			}
		}
	}
}

// IngestLedgerEvents commits one ledger's events to the chunk store
// atomically and then updates the in-memory mirrors.
//
// payloads is typically produced by events.LCMToPayloads. Terms are
// derived internally via events.TermsFor on each payload's
// ContractEvent.
//
// Sequence validation is performed up front, before any RocksDB
// write or mirror mutation:
//
//   - ledgerSeq must lie within [chunkID.FirstLedger(),
//     chunkID.LastLedger()] — out-of-range returns ErrLedgerOutOfRange.
//   - ledgerSeq must equal the next expected ledger (StartLedger +
//     LedgerCount) — duplicates or gaps return ErrLedgerOutOfOrder.
//
// Both checks complete before marshaling, so a rejected call leaves
// the chunk store and in-memory mirrors entirely untouched.
//
// Post-batch atomicity: once the RocksDB batch commits, the in-memory
// mirror + offsets updates are infallible by construction. Any
// failure there panics rather than returning an error, because a
// returned error would leave on-disk state ahead of in-memory state
// with no clean recovery short of close + reopen.
//
//nolint:cyclop // sequential pipeline: validate -> marshal -> batch -> mirror updates
func (h *HotStore) IngestLedgerEvents(ledgerSeq uint32, payloads []events.Payload) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed.Load() {
		return ErrClosed
	}

	// Validate ledger sequence BEFORE any disk write or mirror mutation.
	// Failing the offsets.Append check after the RocksDB batch has
	// committed would leave events orphaned under a bad ledger key.
	if ledgerSeq < h.chunkID.FirstLedger() || ledgerSeq > h.chunkID.LastLedger() {
		return fmt.Errorf("%w: ledger %d not in chunk %s [%d, %d]",
			ErrLedgerOutOfRange, ledgerSeq, h.chunkID,
			h.chunkID.FirstLedger(), h.chunkID.LastLedger())
	}
	expected := h.offsets.StartLedger() + uint32(h.offsets.LedgerCount()) //nolint:gosec
	if ledgerSeq != expected {
		return fmt.Errorf("%w: expected ledger %d, got %d",
			ErrLedgerOutOfOrder, expected, ledgerSeq)
	}

	// Pre-marshal payloads outside the batch callback so we surface
	// any serialization error before queuing a single Put.
	payloadBytes := make([][]byte, len(payloads))
	for i := range payloads {
		b, err := payloads[i].Marshal()
		if err != nil {
			return fmt.Errorf("events: marshal payload %d for ledger %d: %w", i, ledgerSeq, err)
		}
		payloadBytes[i] = b
	}

	// Pre-derive term keys per payload so the post-commit mirror
	// update doesn't re-hash. Surfacing TermsFor errors here (pre-batch)
	// cleanly rejects the ledger commit without touching disk —
	// MarshalBinary failure on stellar-core-validated XDR is a
	// corruption signal worth aborting on.
	termKeys := make([][]events.TermKey, len(payloads))
	for i := range payloads {
		keys, err := events.TermsFor(payloads[i].ContractEvent)
		if err != nil {
			return fmt.Errorf("events: derive terms for payload %d in ledger %d: %w", i, ledgerSeq, err)
		}
		termKeys[i] = keys
	}

	startID := h.offsets.TotalEvents()
	if uint64(startID)+uint64(len(payloads)) > math.MaxUint32 {
		return fmt.Errorf("events: chunk %s would overflow uint32 event-id space at ledger %d",
			h.chunkID, ledgerSeq)
	}

	// Atomic batch on the per-Chunk DB.
	err := h.chunkStore.Batch(func(b *rocksdb.BatchWriter) error {
		for i, blob := range payloadBytes {
			eventID := startID + uint32(i)
			b.Put(DataCF, encodeDataKey(eventID), blob)
			for _, key := range termKeys[i] {
				b.Put(IndexCF, encodeIndexKey(key, eventID), nil)
			}
		}
		// On-disk shape matches the in-memory API: per-ledger event
		// count, not cumulative. Warmup replays directly via
		// offsets.Append(ledger, eventCount) — no delta arithmetic.
		//nolint:gosec // bounds-checked above
		eventCount := uint32(len(payloads))
		b.Put(OffsetsCF, encodeOffsetKey(ledgerSeq), encodeOffsetValue(eventCount))
		return nil
	})
	if err != nil {
		return fmt.Errorf("events: commit ledger %d to chunk %s: %w", ledgerSeq, h.chunkID, err)
	}

	// Phase 3: batch is durable on disk — apply to the in-memory mirrors.
	//
	// Both mirror.AddTo and offsets.Append are typed-fallible but cannot
	// be meaningfully recovered from here: returning an error after the
	// disk batch is committed would leave on-disk state ahead of
	// in-memory state, violating the type-level atomicity contract.
	// The only recovery from that is close + reopen (warmup rebuilds
	// in-memory from on-disk). We panic instead so an invariant
	// violation surfaces immediately.
	for i, keys := range termKeys {
		eventID := startID + uint32(i)
		for _, key := range keys {
			if err := h.mirror.AddTo(key, eventID); err != nil {
				// mirror.AddTo only fails with ErrClosed. Reachable only
				// if the freeze orchestrator closed the mirror while
				// ingest was still running — a coordination bug. Panic
				// to surface it loudly.
				panic(fmt.Sprintf("events: mirror.AddTo: chunk %s ledger %d: %v",
					h.chunkID, ledgerSeq, err))
			}
		}
	}
	if err := h.offsets.Append(ledgerSeq, uint32(len(payloads))); err != nil { //nolint:gosec // bounds-checked just above
		// offsets.Append only fails on sequence mismatch, which the
		// up-front validation already ruled out under h.mu. Reachable
		// only if a future refactor breaks that invariant — defense in
		// depth.
		panic(fmt.Sprintf("events: offsets.Append: chunk %s ledger %d: %v",
			h.chunkID, ledgerSeq, err))
	}
	return nil
}

// ──────────────────────────────────────────────────────────────────
// Warmup — reconstructs the in-memory mirror + offsets from the
// per-Chunk DB's on-disk CFs. Called only by OpenHotStore.
// ──────────────────────────────────────────────────────────────────

// warmup rebuilds the in-memory mirrors for chunkID by prefix-scanning
// the chunk's two on-disk caches once each:
//
//   - events_index  → events.BitmapIndex — every (events.TermKey, eventID) row
//     replayed into a fresh in-memory bitmap mirror.
//   - events_offsets → *events.LedgerOffsets — every (ledger_seq,
//     cumulative_count) row replayed into a fresh offset cache.
//
// chunkID seeds events.LedgerOffsets.StartLedger for empty chunks; on-disk
// rows carry the full ledger sequence themselves. Both mirrors are
// empty for fresh chunks.
func warmup(chunkStore *rocksdb.Store, chunkID chunk.ID) (events.BitmapIndex, *events.LedgerOffsets, error) {
	mirror, err := warmupIndex(chunkStore)
	if err != nil {
		return nil, nil, err
	}
	offsets, err := warmupOffsets(chunkStore, chunkID)
	if err != nil {
		return nil, nil, err
	}
	return mirror, offsets, nil
}

// warmupIndex scans the events_index CF and replays every (events.TermKey,
// eventID) row into a fresh events.MemBitmaps. Design doc §12 step 3.
func warmupIndex(chunkStore *rocksdb.Store) (events.BitmapIndex, error) {
	mirror := events.NewMemBitmaps()

	for entry, err := range chunkStore.Iterate(IndexCF, nil) {
		if err != nil {
			return nil, fmt.Errorf("events: warmup scan %s: %w", IndexCF, err)
		}
		if len(entry.Key) != indexKeyLen {
			return nil, fmt.Errorf("events: warmup unexpected %s key length %d (want %d)",
				IndexCF, len(entry.Key), indexKeyLen)
		}
		var term events.TermKey
		copy(term[:], entry.Key[0:16])
		eventID := binary.BigEndian.Uint32(entry.Key[16:20])
		if err := mirror.AddTo(term, eventID); err != nil {
			return nil, fmt.Errorf("events: warmup add term: %w", err)
		}
	}
	return mirror, nil
}

// warmupOffsets scans events_offsets and replays every (ledger_seq,
// event_count) row into a fresh *events.LedgerOffsets. The on-disk
// shape matches LedgerOffsets.Append's input directly (per-ledger
// counts, not cumulative), so no delta arithmetic is needed.
//
// Iteration order is byte-sorted == numeric-sorted under the
// big-endian uint32 key encoding, so rows arrive in ledger order and
// events.LedgerOffsets.Append's "must be next sequential ledger" contract
// holds without an explicit sort.
func warmupOffsets(chunkStore *rocksdb.Store, chunkID chunk.ID) (*events.LedgerOffsets, error) {
	offsets := events.NewLedgerOffsets(chunkID.FirstLedger())

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
		if err := offsets.Append(ledger, eventCount); err != nil {
			return nil, fmt.Errorf("events: warmup offsets: %w", err)
		}
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

func encodeOffsetValue(cumulative uint32) []byte {
	var val [offsetValLen]byte
	binary.BigEndian.PutUint32(val[:], cumulative)
	return val[:]
}
