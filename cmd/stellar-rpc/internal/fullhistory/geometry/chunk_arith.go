package geometry

import "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"

// Signed pre-genesis chunk arithmetic — the single home for the chunk↔ledger maps
// that run in int64 so the pre-genesis sentinel (-1 = "nothing complete") never
// underflows the uint32 domain. Keeping all of it here (rather than split across
// lifecycle progress and this package) means there is one -1 convention, not two.

// PreGenesisLedger is the last-committed ledger when nothing is complete
// (FirstLedgerSeq-1) — the ledger-domain image of the -1 chunk sentinel.
const PreGenesisLedger uint32 = chunk.FirstLedgerSeq - 1

// ChunkLastLedger maps a signed chunk index to its last ledger: c < 0 ⇒
// PreGenesisLedger (the sub-genesis sentinel); c >= 0 ⇒ chunk.ID(c).LastLedger().
// The signed-domain companion of ChunkFirstLedger below.
func ChunkLastLedger(c int64) uint32 {
	if c < 0 {
		return PreGenesisLedger
	}
	return chunk.ID(c).LastLedger() //nolint:gosec // c >= 0 and bounded by real chunk ids
}

// ChunkIDOfLedger maps a ledger to its chunk, signed so a sub-genesis ledger
// yields -1 instead of panicking.
func ChunkIDOfLedger(ledger uint32) int64 {
	if ledger < chunk.FirstLedgerSeq {
		return -1
	}
	return int64(chunk.IDFromLedger(ledger))
}

// LastCompleteChunkAt is the inverse of chunk.ID.LastLedger: the largest chunk
// whose last ledger is <= ledger. Returns SIGNED int64 so a sub-genesis ledger
// (the sub-genesis sentinel) maps to -1 ("before the first chunk") rather than
// wrapping; the cast-before-subtract keeps it in int64 (uint32 ledger-1 would
// underflow for ledger 0).
func LastCompleteChunkAt(ledger uint32) int64 {
	return (int64(ledger)+1-int64(chunk.FirstLedgerSeq))/int64(chunk.LedgersPerChunk) - 1
}

// ChunkFirstLedger maps a non-negative signed chunk index to its first ledger.
// It is the signed-domain companion of chunk.ID.FirstLedger used after a
// max(..., 0) clamp.
func ChunkFirstLedger(c int64) uint32 {
	return chunk.ID(c).FirstLedger() //nolint:gosec // c >= 0 (clamped) and bounded by real chunk ids
}
