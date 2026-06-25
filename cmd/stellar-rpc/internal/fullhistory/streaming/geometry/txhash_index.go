package geometry

import (
	"errors"
	"fmt"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// Tx-hash-index arithmetic lives here, not in pkg/chunk: pkg/chunk is pure chunk
// geometry with no index concept, so the chunk<->index mapping is parameterized
// by chunks_per_txhash_index (cpi). A tx-hash index covers a contiguous run of
// cpi chunks: index i owns chunks [i*cpi, i*cpi + cpi - 1].

// MaxChunksPerTxhashIndex bounds cpi so an index's ledger span always fits the
// cold tx-hash index's on-disk format, which stores each ledger as a
// txhash.ColdPayloadSize-byte offset from the index's first ledger — capping the
// span at 2^(8*ColdPayloadSize) ledgers. The bound is derived from that same
// constant so the two can never drift: a larger cpi would pass
// NewTxHashIndexLayout yet make every index build fail (txhash.BuildColdIndex
// rejects an over-budget span), and cpi is immutable once pinned. With a 3-byte
// payload and 10k-ledger chunks this is ~1,677. See gettransaction-full-history-
// design.md §6.2.
const MaxChunksPerTxhashIndex uint32 = (uint32(1) << (8 * txhash.ColdPayloadSize)) / chunk.LedgersPerChunk

// TxHashIndexLayout is the tx-hash-index arithmetic bound to one
// chunks_per_txhash_index value. The value is immutable for a deployment (pinned
// in config:chunks_per_txhash_index on first start), so a TxHashIndexLayout is
// constructed once and shared.
type TxHashIndexLayout struct {
	cpi uint32 // chunks_per_txhash_index; > 0, <= MaxChunksPerTxhashIndex
}

// NewTxHashIndexLayout validates cpi and returns the index arithmetic for it.
func NewTxHashIndexLayout(chunksPerIndex uint32) (TxHashIndexLayout, error) {
	if chunksPerIndex == 0 {
		return TxHashIndexLayout{}, errors.New("streaming: chunks_per_txhash_index must be > 0")
	}
	if chunksPerIndex > MaxChunksPerTxhashIndex {
		return TxHashIndexLayout{}, fmt.Errorf(
			"streaming: chunks_per_txhash_index %d exceeds max %d",
			chunksPerIndex, MaxChunksPerTxhashIndex,
		)
	}
	return TxHashIndexLayout{cpi: chunksPerIndex}, nil
}

// ChunksPerIndex returns the configured cpi.
func (l TxHashIndexLayout) ChunksPerIndex() uint32 { return l.cpi }

// TxHashIndexID returns the index containing chunk c: c / cpi.
func (l TxHashIndexLayout) TxHashIndexID(c chunk.ID) TxHashIndexID {
	return TxHashIndexID(uint32(c) / l.cpi)
}

// FirstChunk returns the lowest chunk in index id: id * cpi.
func (l TxHashIndexLayout) FirstChunk(id TxHashIndexID) chunk.ID {
	return chunk.ID(uint32(id) * l.cpi)
}

// LastChunk returns the highest chunk in index id: (id+1)*cpi - 1.
func (l TxHashIndexLayout) LastChunk(id TxHashIndexID) chunk.ID {
	return chunk.ID((uint32(id)+1)*l.cpi - 1)
}

// IsTerminalCoverage reports whether a coverage's hi equals its index's last
// chunk — the derived "terminal"/finalized property (marked nowhere). When such
// a coverage is frozen its index is finalized: .bin inputs were demoted in the
// same commit and it is never rebuilt again.
func (l TxHashIndexLayout) IsTerminalCoverage(cov TxHashIndexCoverage) bool {
	return cov.Hi == l.LastChunk(cov.Index)
}

// LastCompleteChunkAt is the inverse of chunk.ID.LastLedger: the largest chunk
// whose last ledger is <= ledger. Returns SIGNED int64 so a sub-genesis ledger
// (the watermark sentinel) maps to -1 ("before the first chunk") rather than
// wrapping; the cast-before-subtract keeps it in int64 (uint32 ledger-1 would
// underflow for ledger 0).
func LastCompleteChunkAt(ledger uint32) int64 {
	return (int64(ledger)+1-int64(chunk.FirstLedgerSeq))/int64(chunk.LedgersPerChunk) - 1
}

// ChunkFirstLedger maps a non-negative signed chunk index to its first ledger.
// It is the signed-domain companion of chunk.ID.FirstLedger used by
// effectiveRetentionFloor after the max(..., 0) clamp.
func ChunkFirstLedger(c int64) uint32 {
	return chunk.ID(c).FirstLedger() //nolint:gosec // c >= 0 (clamped) and bounded by real chunk ids
}
