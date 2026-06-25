package streaming

import (
	"errors"
	"fmt"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// Window arithmetic lives here, not in pkg/chunk: pkg/chunk is pure chunk
// geometry with no window/index concept, so the chunk<->window mapping is
// parameterized by chunks_per_txhash_index (cpi). A window is a contiguous run
// of cpi chunks: window w owns chunks [w*cpi, w*cpi + cpi - 1].

// MaxChunksPerTxhashIndex bounds cpi so a window's ledger span always fits a
// uint32 seq: floor(2^32 / LedgersPerChunk). See gettransaction-full-history-
// design.md §6.2.
const MaxChunksPerTxhashIndex uint32 = ^uint32(0) / chunk.LedgersPerChunk

// Windows is window arithmetic bound to one chunks_per_txhash_index value. The
// value is immutable for a deployment (pinned in config:chunks_per_txhash_index
// on first start), so a Windows is constructed once and shared.
type Windows struct {
	cpi uint32 // chunks_per_txhash_index; > 0, <= MaxChunksPerTxhashIndex
}

// NewWindows validates cpi and returns the window arithmetic for it.
func NewWindows(chunksPerIndex uint32) (Windows, error) {
	if chunksPerIndex == 0 {
		return Windows{}, errors.New("streaming: chunks_per_txhash_index must be > 0")
	}
	if chunksPerIndex > MaxChunksPerTxhashIndex {
		return Windows{}, fmt.Errorf(
			"streaming: chunks_per_txhash_index %d exceeds max %d",
			chunksPerIndex, MaxChunksPerTxhashIndex,
		)
	}
	return Windows{cpi: chunksPerIndex}, nil
}

// ChunksPerIndex returns the configured cpi.
func (w Windows) ChunksPerIndex() uint32 { return w.cpi }

// WindowID returns the window containing chunk c: c / cpi.
func (w Windows) WindowID(c chunk.ID) WindowID {
	return WindowID(uint32(c) / w.cpi)
}

// FirstChunk returns the lowest chunk in window id: id * cpi.
func (w Windows) FirstChunk(id WindowID) chunk.ID {
	return chunk.ID(uint32(id) * w.cpi)
}

// LastChunk returns the highest chunk in window id: (id+1)*cpi - 1.
func (w Windows) LastChunk(id WindowID) chunk.ID {
	return chunk.ID((uint32(id)+1)*w.cpi - 1)
}

// IsTerminalCoverage reports whether a coverage's hi equals its window's last
// chunk — the derived "terminal"/finalized property (marked nowhere). When such
// a coverage is frozen its window is finalized: .bin inputs were demoted in the
// same commit and it is never rebuilt again.
func (w Windows) IsTerminalCoverage(cov IndexCoverage) bool {
	return cov.Hi == w.LastChunk(cov.Window)
}

// lastCompleteChunkAt is the inverse of chunk.ID.LastLedger: the largest chunk
// whose last ledger is <= ledger. Returns SIGNED int64 so a sub-genesis ledger
// (the watermark sentinel) maps to -1 ("before the first chunk") rather than
// wrapping; the cast-before-subtract keeps it in int64 (uint32 ledger-1 would
// underflow for ledger 0).
func lastCompleteChunkAt(ledger uint32) int64 {
	return (int64(ledger)+1-int64(chunk.FirstLedgerSeq))/int64(chunk.LedgersPerChunk) - 1
}

// chunkFirstLedger maps a non-negative signed chunk index to its first ledger.
// It is the signed-domain companion of chunk.ID.FirstLedger used by
// effectiveRetentionFloor after the max(..., 0) clamp.
func chunkFirstLedger(c int64) uint32 {
	return chunk.ID(c).FirstLedger() //nolint:gosec // c >= 0 (clamped) and bounded by real chunk ids
}
