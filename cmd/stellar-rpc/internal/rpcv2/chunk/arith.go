package chunk

// Signed pre-genesis chunk arithmetic — the single home for the chunk↔ledger maps
// that run in int64 so the pre-genesis sentinel (-1 = "nothing complete") never
// underflows the uint32 domain. Keeping all of it here (rather than split across
// lifecycle progress and this package) means there is one -1 convention, not two.

// PreGenesisLedger is the last-committed ledger when nothing is complete
// (FirstLedgerSeq-1) — the ledger-domain image of the -1 chunk sentinel.
const PreGenesisLedger uint32 = FirstLedgerSeq - 1

// LastLedgerOf maps a signed chunk index to its last ledger: c < 0 ⇒
// PreGenesisLedger (the sub-genesis sentinel); c >= 0 ⇒ ID(c).LastLedger().
func LastLedgerOf(c int64) uint32 {
	if c < 0 {
		return PreGenesisLedger
	}
	return ID(c).LastLedger() //nolint:gosec // c >= 0 and bounded by real chunk ids
}

// SignedIDOfLedger maps a ledger to its chunk, signed so a sub-genesis ledger
// yields -1 instead of panicking.
func SignedIDOfLedger(ledger uint32) int64 {
	if ledger < FirstLedgerSeq {
		return -1
	}
	return int64(IDFromLedger(ledger))
}

// LastCompleteChunkAt is the inverse of ID.LastLedger: the largest chunk
// whose last ledger is <= ledger. Returns SIGNED int64 so a sub-genesis ledger
// (the sub-genesis sentinel) maps to -1 ("before the first chunk") rather than
// wrapping; the cast-before-subtract keeps it in int64 (uint32 ledger-1 would
// underflow for ledger 0).
func LastCompleteChunkAt(ledger uint32) int64 {
	return (int64(ledger)+1-int64(FirstLedgerSeq))/int64(LedgersPerChunk) - 1
}
