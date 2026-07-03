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

// ChunksPerTxhashIndex is the fixed number of chunks each tx-hash index covers
// (1000 = 10M ledgers per index). It was once a settable, metastore-pinned
// config field; it is now a compile-time constant. Changing it would invalidate
// every existing index boundary, so it is set once, here, for all deployments.
// It aliases txhash.DefaultChunksPerIndex so the streaming index layout and the
// cold index builder always agree on the index size.
const ChunksPerTxhashIndex uint32 = txhash.DefaultChunksPerIndex

// MaxChunksPerTxhashIndex bounds cpi so an index's ledger span always fits the
// cold tx-hash index's on-disk format, which stores each ledger as a
// txhash.ColdPayloadSize-byte offset from the index's first ledger — capping the
// span at 2^(8*ColdPayloadSize) ledgers. The bound is derived from that same
// constant so the two can never drift: a larger cpi would pass
// NewTxHashIndexLayout yet make every index build fail (txhash.BuildColdIndex
// rejects an over-budget span). With a 3-byte payload and 10k-ledger chunks this
// is ~1,677 — well above the fixed ChunksPerTxhashIndex of 1000. See
// gettransaction-full-history-design.md §6.2.
const MaxChunksPerTxhashIndex uint32 = (uint32(1) << (8 * txhash.ColdPayloadSize)) / chunk.LedgersPerChunk

// TxHashIndexLayout is the tx-hash-index arithmetic bound to one
// chunks_per_txhash_index value — the fixed ChunksPerTxhashIndex constant in
// production, so a TxHashIndexLayout is constructed once and shared. The type
// stays parameterized so the arithmetic can be exercised at other index sizes in
// tests.
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
