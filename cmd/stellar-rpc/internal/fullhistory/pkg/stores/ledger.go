package stores

import "iter"

// LedgerEntry is one (sequence, bytes) pair stored in or returned
// from a LedgerStore.
//
// The store stores Bytes verbatim and returns them verbatim — it has
// no opinion on what the bytes are, what encoding they use, or
// whether they have been compressed.
// Callers that need a particular shape (e.g., XDR-marshaled
// LedgerCloseMeta) are responsible for marshaling on the write side
// and unmarshaling on the read side.
//
// The store treats Bytes as an opaque byte container the size of
// whatever the caller hands it.
type LedgerEntry struct {
	Seq   uint32
	Bytes []byte
}

// LedgerStore is the typed contract for the unified service's hot
// ledger store.
// One concrete impl today: rocksdb.LedgerStore.
//
// Storage model:
//
//   - Key: ledger sequence (uint32, encoded big-endian on disk).
//   - Value: caller-supplied opaque bytes.
//   - One row per ledger sequence.
//
// The store maintains the (min, max) sequence range in memory and
// exposes it via GetLedgerRange so callers (most importantly a
// federated reader deciding "hot or cold") can short-circuit
// out-of-range queries without a disk hit.
type LedgerStore interface {
	// Open brings the underlying store up.
	// Idempotent; creates the on-disk directory (with parents) if
	// missing.
	// Initializes the in-memory range bounds via a single
	// FirstLastKey call at the layer below.
	Open() error

	// Close drains the active memtable and tears the underlying
	// store down.
	// Idempotent.
	// Waits for any in-flight method to release the wrapper's
	// lifecycle read-lock before tearing down — graceful Close
	// gives the next graceful Open zero WAL to replay.
	Close() error

	// AddLedgers writes a batch of (sequence, bytes) pairs.
	//
	// All entries land or none do — the underlying RocksDB Write
	// is atomic. One fsync per call regardless of len(entries).
	//
	// Empty slice is a no-op and returns nil.
	// A single-entry slice dispatches to a direct Put internally;
	// multi-entry slices go through the underlying Layer-1 batch.
	//
	// Overwrites any prior value at the same sequence.
	// On success, widens the in-memory range bounds to include the
	// written sequences.
	AddLedgers(entries []LedgerEntry) error

	// DeleteLedgers removes the given sequences.
	//
	// Missing sequences are silently skipped (idempotent).
	// All deletes commit together atomically; one fsync per call
	// regardless of len(seqs).
	//
	// Empty slice is a no-op and returns nil.
	//
	// On success, the in-memory range bounds may shrink: if any
	// deleted sequence equals the current minSeq or maxSeq, the
	// bounds are recomputed via a single FirstLastKey call at the
	// layer below (O(1) on store size, not O(N)).
	DeleteLedgers(seqs []uint32) error

	// GetLedgerRaw returns the bytes stored under seq.
	// Returns (nil, stores.ErrNotFound) when seq is absent from
	// the store.
	GetLedgerRaw(seq uint32) ([]byte, error)

	// IterateLedgers yields the (seq, bytes) pairs in [start, end]
	// (inclusive), in ascending sequence order.
	//
	// The iterator seeks directly to start at the layer below, so
	// keys below start are never touched.
	// Walks forward until the first key with seq > end and stops.
	//
	// Gap handling: yields every key that exists in [start, end].
	// If the keyspace has holes — sequences 100, 102, 105 — the
	// iterator yields 100, 102, 105 with no signal about the
	// missing 101, 103, 104.
	// Callers that need strict contiguity check
	// `entry.Seq == prevSeq + 1` themselves and decide what a gap
	// means in their context.
	//
	// Per-iteration errors ride in the tuple; no separate Err
	// method. Resource cleanup is internal to the producer closure.
	IterateLedgers(start, end uint32) iter.Seq2[LedgerEntry, error]

	// GetLedgerRange returns the smallest and largest sequences
	// currently in the store.
	// Returns (0, 0) on a fresh store with no entries (callers can
	// disambiguate by treating (0, 0) as empty; in practice ledger
	// sequence 0 never occurs, so the empty sentinel never
	// collides with a real range).
	//
	// O(1), no disk IO — reads the in-memory bounds maintained by
	// AddLedgers and DeleteLedgers.
	// Primarily consumed by a federated reader deciding hot-vs-cold
	// routing.
	GetLedgerRange() (minSeq, maxSeq uint32)
}
