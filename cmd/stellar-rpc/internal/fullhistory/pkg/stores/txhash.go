package stores

// TxHashToLedgerSeqEntry — one (txhash → ledgerSeq) mapping.
// Hash is the 32-byte SHA-256 of the canonical transaction envelope.
type TxHashToLedgerSeqEntry struct {
	Hash      [32]byte
	LedgerSeq uint32
}

// TxHashStore is the typed contract for the hot txhash store.
// Writes are atomic at the granularity of one method call: one
// fsync covers however many CFs the entries touch.
type TxHashStore interface {
	Open() error
	Close() error

	// AddEntries writes a batch atomically. Empty slice is a no-op
	// on an open store. One fsync per call.
	AddEntries(entries []TxHashToLedgerSeqEntry) error

	// RemoveEntries deletes by hash atomically. Missing hashes are
	// silently skipped. Empty slice is a no-op on an open store.
	RemoveEntries(hashes [][32]byte) error

	// Get returns the ledger sequence the hash was committed in,
	// or (0, ErrNotFound) on miss.
	Get(hash [32]byte) (uint32, error)
}
