package stores

// TxHashToLedgerSeqEntry — one (txhash → ledgerSeq) mapping.
// Hash is the 32-byte SHA-256 of the canonical transaction envelope.
type TxHashToLedgerSeqEntry struct {
	Hash      [32]byte
	LedgerSeq uint32
}

// TxHashHotStore is the typed contract for the hot txhash store.
// Writes are atomic at the granularity of one method call: one
// fsync covers however many CFs the entries touch.
type TxHashHotStore interface {
	Close() error

	// AddEntries writes a batch atomically. Empty slice is a no-op
	// on an open store. One fsync per call.
	AddEntries(entries []TxHashToLedgerSeqEntry) error

	// Get returns the ledger sequence the hash was committed in,
	// or (0, ErrNotFound) on miss.
	Get(hash [32]byte) (uint32, error)
}
