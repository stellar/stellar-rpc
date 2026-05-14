package stores

// TxHashToLedgerSeqEntry is one (transaction hash → ledger sequence)
// mapping written to or read from a TxHashStore.
//
// Hash is the 32-byte SHA-256 of the canonical transaction envelope —
// the same hash a caller hands to getTransaction(byHash).
// LedgerSeq is the sequence number of the ledger that included the
// transaction.
type TxHashToLedgerSeqEntry struct {
	Hash      [32]byte
	LedgerSeq uint32
}

// TxHashStore is the typed contract for the unified service's hot
// txhash store.
// One concrete impl today: rocksdb.TxHashStore (16-CF nibble-routed
// RocksDB).
//
// Writes are atomic at the granularity of one method call:
// AddEntries / RemoveEntries fsync-then-return as a single
// transaction across however many CFs the entries touch.
//
// Open and Close form the two-phase lifecycle every fullhistory
// store follows.
// New() (from the concrete-impl package) constructs the store; Open()
// brings the backing RocksDB up; Close() drains the active memtable
// and tears down.
// All methods on a closed store return stores.ErrStoreClosed.
type TxHashStore interface {
	// Open brings the underlying store up — opens RocksDB,
	// configures the 16 column families, and warms any per-store
	// state.
	// Idempotent: a second Open on the same instance returns the
	// same result without re-opening.
	// Creates the on-disk directory (with parents) if missing.
	Open() error

	// Close drains the active memtable to an SST file and tears
	// the underlying store down.
	// Idempotent: a second Close is a no-op.
	// Waits for any in-flight AddEntries / RemoveEntries / Get to
	// release the wrapper's lifecycle read-lock before tearing the
	// RocksDB down — a graceful Close is durable, post-Close ops
	// return stores.ErrStoreClosed cleanly.
	Close() error

	// AddEntries writes a batch of (txhash → ledgerSeq) entries
	// atomically.
	//
	// All entries land or none do — the underlying RocksDB Write
	// is atomic across however many of the 16 column families the
	// hashes' high-nibbles cover.
	// One fsync per call regardless of len(entries).
	//
	// Empty slice is a no-op and returns nil.
	// A single-entry slice is treated identically to a multi-entry
	// slice from the caller's perspective; the concrete impl
	// optimizes the single-entry path to a direct Put internally.
	AddEntries(entries []TxHashToLedgerSeqEntry) error

	// RemoveEntries deletes a batch of (txhash) entries atomically.
	//
	// Idempotent: hashes that aren't currently in the store are
	// silently skipped — RemoveEntries returns no error for those.
	// All deletes commit together (atomic across CFs) or none do.
	// One fsync per call regardless of len(hashes).
	//
	// Empty slice is a no-op and returns nil.
	// A single-hash slice is treated identically to a multi-hash
	// slice from the caller's perspective; the concrete impl
	// optimizes the single-hash path to a direct Delete internally.
	RemoveEntries(hashes [][32]byte) error

	// Get returns the ledger sequence the given transaction hash
	// was committed in.
	//
	// Returns (0, stores.ErrNotFound) when the hash is not present
	// in the store — callers fall back to the cold RecSplit index
	// via the federated reader.
	// Other errors (closed store, RocksDB-side failure) come back
	// as-is or wrapped from the underlying impl.
	Get(hash [32]byte) (uint32, error)
}
