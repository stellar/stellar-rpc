package stores

// MetaStoreEntry is the sealed marker interface every metastore entry
// type implements.
// Concrete implementers (ChunkEntry, IndexEntry) live in this file;
// the sealing method is unexported so external packages cannot define
// their own entry types — the schema is closed to what this package
// declares.
//
// New entry types are added here as the metastore schema grows.
// Hot-streaming-side keys (a hypothetical "hot:*" family that the
// streaming workflow may produce later) would land as additional
// implementers; the concrete-impl side in pkg/rocksdb type-switches
// over MetaStoreEntry and adds a case per new variant.
type MetaStoreEntry interface{ isMetaStoreEntry() }

// MetaStoreKey is the sealed marker interface for descriptors used
// to read or delete entries.
// Mirrors MetaStoreEntry — same sealed-via-unexported-method pattern,
// same growth path.
type MetaStoreKey interface{ isMetaStoreKey() }

// ChunkArtifactKind identifies which of a chunk's three artifacts an
// entry refers to.
// Stored on disk via the unexported key-suffix mapping inside the
// concrete impl; callers always pass the typed constant.
type ChunkArtifactKind uint8

const (
	// ChunkArtifactLFS — the chunk's ledger pack file ("LFS" — ledger
	// pack file in project shorthand).
	ChunkArtifactLFS ChunkArtifactKind = iota

	// ChunkArtifactTxHashRaw — the chunk's raw transaction-hash .bin
	// file (consumed by build_txhash_index, then deleted by
	// cleanup_txhash).
	ChunkArtifactTxHashRaw

	// ChunkArtifactEvents — the chunk's events cold-segment artifact.
	ChunkArtifactEvents
)

// ChunkEntry is one (chunk artifact → value) flag.
//
// Value is a caller-defined uint8 enum — the metastore stores the
// byte verbatim and never inspects its meaning.
// Typical use: a state-machine value owned by whichever package is
// running the lifecycle (the backfill task, the streaming
// orchestrator).
// Example caller-side enum:
//
//	type ChunkLFSState uint8
//	const (
//	    ChunkLFSInProgress ChunkLFSState = iota + 1 // start at 1 so 0=unset stays distinguishable
//	    ChunkLFSDone
//	)
//	m.AddEntries([]stores.MetaStoreEntry{
//	    stores.ChunkEntry{ChunkID: 42, Kind: stores.ChunkArtifactLFS, Value: uint8(ChunkLFSDone)},
//	})
type ChunkEntry struct {
	ChunkID uint32
	Kind    ChunkArtifactKind
	Value   uint8
}

func (ChunkEntry) isMetaStoreEntry() {}

// IndexEntry is one (tx-index artifact → value) flag.
// Same value semantics as ChunkEntry — caller-defined uint8 enum.
type IndexEntry struct {
	IndexID uint32
	Value   uint8
}

func (IndexEntry) isMetaStoreEntry() {}

// ChunkKey identifies a ChunkEntry for read or delete.
// The schema is symmetric: a key has the same identifying fields as
// its entry, minus the value.
type ChunkKey struct {
	ChunkID uint32
	Kind    ChunkArtifactKind
}

func (ChunkKey) isMetaStoreKey() {}

// IndexKey identifies an IndexEntry for read or delete.
type IndexKey struct {
	IndexID uint32
}

func (IndexKey) isMetaStoreKey() {}

// MetaStore is the typed contract for the unified service's general-
// purpose state key/value store.
//
// The interface is intentionally broad — three families of operation
// (multi-row entries, two singletons, one specific atomic transition)
// each get a small typed surface.
// Collapsing them under fewer generic methods would push routing and
// value-encoding decisions onto every caller, which is exactly what
// this typed surface is here to hide.
// One concrete impl today: rocksdb.MetaStore.
//
// Two record families:
//
//  1. Multi-row entries (ChunkEntry, IndexEntry) — many rows per
//     family, identified by typed fields.
//     Written and deleted via AddEntries / DeleteEntries with
//     mixed-type slices; read via the typed GetChunkEntry /
//     GetIndexEntry.
//
//  2. Singletons (last-committed-ledger, ledgers-per-tx-index) —
//     one row per concept, at a fixed key.
//     Exposed as typed methods (UpdateLastCommittedLedger, etc.)
//     rather than as marker-interface entry types.
//
// Singletons-as-methods is a deliberate scope choice: the only
// reason to put them in the MetaStoreEntry marker interface would be
// to commit a singleton atomically alongside chunk/index entries in
// one transaction (e.g., "mark these 50 chunks done AND advance
// last-committed-ledger, all-or-nothing").
// No design today requires that.
// If a future slice does — most likely a streaming-side per-ledger
// commit pattern — the migration is mechanical: declare
// LastCommittedLedgerEntry / ConfigLedgersPerTxIndexEntry types
// satisfying MetaStoreEntry, add the corresponding cases in the
// pkg/rocksdb impl, and let callers either route through AddEntries
// or keep using the convenience Update*/Get* methods (which would
// stay as one-line wrappers).
//
// Atomicity contract for batched methods:
// AddEntries / DeleteEntries / MarkTxHashIndexComplete each commit
// as a single transaction — every queued write lands or none do,
// one fsync per call regardless of how many records are touched.
//
//nolint:interfacebloat // see preceding doc comment — 11 methods is the deliberate shape.
type MetaStore interface {
	// Open brings the underlying store up. Idempotent; creates the
	// on-disk directory (with parents) if missing.
	Open() error

	// Close drains the active memtable and tears the underlying store
	// down. Idempotent. Waits for any in-flight method to release the
	// wrapper's lifecycle read-lock before tearing down — graceful
	// Close gives the next graceful Open zero WAL to replay.
	Close() error

	// AddEntries writes any combination of MetaStoreEntry types
	// atomically.
	// Empty slice is a no-op.
	AddEntries(entries []MetaStoreEntry) error

	// DeleteEntries removes the given keys atomically.
	// Missing keys are silently skipped (idempotent).
	// Empty slice is a no-op.
	// Used today by random-pruning paths in the streaming side.
	DeleteEntries(keys []MetaStoreKey) error

	// GetChunkEntry returns the value stored under
	// (chunkID, kind), or stores.ErrNotFound if absent.
	GetChunkEntry(chunkID uint32, kind ChunkArtifactKind) (uint8, error)

	// GetIndexEntry returns the value stored under indexID, or
	// stores.ErrNotFound if absent.
	GetIndexEntry(indexID uint32) (uint8, error)

	// UpdateLastCommittedLedger sets the streaming-side checkpoint to
	// seq. Overwrites any prior value.
	UpdateLastCommittedLedger(seq uint32) error

	// GetLastCommittedLedger returns the most recent
	// last-committed-ledger value, or stores.ErrNotFound if never set
	// (fresh service install).
	GetLastCommittedLedger() (uint32, error)

	// UpdateConfigLedgersPerTxIndex sets the immutability marker.
	// Intended to be called once on the very first run; subsequent
	// service starts read the value and fail fast if the operator's
	// TOML disagrees.
	// This method does NOT enforce write-once at the storage layer —
	// that policy lives at the call site so misuse is detectable.
	UpdateConfigLedgersPerTxIndex(n uint32) error

	// GetConfigLedgersPerTxIndex returns the configured
	// ledgers-per-tx-index, or stores.ErrNotFound if never set.
	GetConfigLedgersPerTxIndex() (uint32, error)

	// MarkTxHashIndexComplete is the cleanup_txhash atomic transition:
	// set index:<txIndexID>:txhash = value AND delete chunk:*:txhashRaw
	// for every chunk in this tx-index, in one transaction.
	//
	// Internally reads the stored ledgersPerTxIndex config and uses
	// pkg/geometry to compute the chunk range; if
	// ledgersPerTxIndex has never been written, returns an error
	// (the caller is responsible for setting the immutability
	// marker before invoking cleanup).
	MarkTxHashIndexComplete(txIndexID uint32, value uint8) error
}
