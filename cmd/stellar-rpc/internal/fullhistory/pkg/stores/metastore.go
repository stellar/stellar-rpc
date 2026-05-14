package stores

// MetaStoreEntry — sealed marker for typed metastore writes.
// Implementers: ChunkEntry, IndexEntry.
type MetaStoreEntry interface{ isMetaStoreEntry() }

// MetaStoreKey — sealed marker for typed metastore reads/deletes.
// Implementers: ChunkKey, IndexKey.
type MetaStoreKey interface{ isMetaStoreKey() }

// ChunkArtifactKind selects which of a chunk's three artifacts an
// entry refers to.
type ChunkArtifactKind uint8

const (
	ChunkArtifactLFS ChunkArtifactKind = iota
	ChunkArtifactTxHashRaw
	ChunkArtifactEvents
)

// ChunkEntry — one (chunk artifact → uint8 value). Value is caller-
// defined; the store treats it as an opaque byte.
type ChunkEntry struct {
	ChunkID uint32
	Kind    ChunkArtifactKind
	Value   uint8
}

func (ChunkEntry) isMetaStoreEntry() {}

// IndexEntry — one (tx-index → uint8 value). Same value semantics
// as ChunkEntry.
type IndexEntry struct {
	IndexID uint32
	Value   uint8
}

func (IndexEntry) isMetaStoreEntry() {}

// ChunkKey identifies a ChunkEntry for read or delete.
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

// MetaStore is the typed contract for the service-wide
// key/value/state store. Multi-row entries (Chunk/Index) plus typed
// singletons (last-committed-ledger, ledgers-per-tx-index) plus the
// one atomic transition cleanup_txhash needs.
// AddEntries / DeleteEntries / MarkTxHashIndexComplete each commit
// as one transaction; one fsync per call regardless of size.
//
//nolint:interfacebloat
type MetaStore interface {
	Open() error
	Close() error

	// AddEntries writes any mix of MetaStoreEntry types atomically.
	// Empty slice is a no-op on an open store.
	AddEntries(entries []MetaStoreEntry) error

	// DeleteEntries removes the given keys atomically. Missing keys
	// are silently skipped. Empty slice is a no-op on an open store.
	DeleteEntries(keys []MetaStoreKey) error

	// GetChunkArtifactState — (chunkID, kind) → state value, or
	// ErrNotFound.
	GetChunkArtifactState(chunkID uint32, kind ChunkArtifactKind) (uint8, error)

	// GetTxHashIndexState — tx-hash-index ID → state value, or
	// ErrNotFound.
	GetTxHashIndexState(indexID uint32) (uint8, error)

	// UpdateLastCommittedLedger overwrites the streaming checkpoint.
	UpdateLastCommittedLedger(seq uint32) error

	// GetLastCommittedLedger returns the streaming checkpoint, or
	// ErrNotFound on a fresh install.
	GetLastCommittedLedger() (uint32, error)

	// UpdateConfigLedgersPerTxIndex sets the immutability marker.
	// Write-once enforcement lives at the call site, not here.
	UpdateConfigLedgersPerTxIndex(n uint32) error

	// GetConfigLedgersPerTxIndex returns the marker, or ErrNotFound.
	GetConfigLedgersPerTxIndex() (uint32, error)

	// MarkTxHashIndexComplete is the cleanup_txhash atomic transition:
	// set index:<txIndexID>:txhash = value AND delete the chunk's
	// txhashRaw entries for every chunk in this tx-index, in one
	// transaction. Returns an error wrapping ErrNotFound if the
	// immutability marker has never been written.
	MarkTxHashIndexComplete(txIndexID uint32, value uint8) error
}
