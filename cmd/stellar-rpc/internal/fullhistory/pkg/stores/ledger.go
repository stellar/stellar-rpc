package stores

import "iter"

// LedgerEntry — one (sequence, opaque bytes) pair.
// The store stores Bytes verbatim and has no opinion on encoding.
// By convention, the streaming write path compresses Bytes with
// zstd before AddLedgers and readers decompress after GetLedgerRaw
// or IterateLedgers; that's a caller-side contract, not enforced
// here.
type LedgerEntry struct {
	Seq   uint32
	Bytes []byte
}

// LedgerHotStore is the typed contract for the hot ledger store.
// Key: ledger sequence (uint32). Value: caller-supplied opaque bytes.
// One row per sequence.
type LedgerHotStore interface {
	Close() error

	// AddLedgers writes a batch atomically. Empty slice is a no-op
	// on an open store. Overwrites any prior value at the same seq.
	AddLedgers(entries []LedgerEntry) error

	// GetLedgerRaw returns the bytes stored under seq, or
	// (nil, ErrNotFound) on miss.
	GetLedgerRaw(seq uint32) ([]byte, error)

	// IterateLedgers yields (seq, bytes) in [start, end] inclusive,
	// ascending. Gaps in the keyspace are visible to the caller as
	// missing sequences between yielded entries; the iterator has
	// no opinion on whether a gap is fatal.
	IterateLedgers(start, end uint32) iter.Seq2[LedgerEntry, error]
}
