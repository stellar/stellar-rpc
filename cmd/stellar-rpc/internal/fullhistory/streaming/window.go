package streaming

import (
	"errors"
	"fmt"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// Window arithmetic lives here, not in pkg/chunk: pkg/chunk deliberately has no
// window/index concept (it is pure chunk geometry), so the chunk<->window
// mapping is parameterized by chunks_per_txhash_index (cpi). A window is a
// contiguous run of cpi chunks: window w owns chunks [w*cpi, w*cpi + cpi - 1].

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

// ChunksIn returns the number of chunks in any window (always cpi). Present so
// callers don't reach for the raw field.
func (w Windows) ChunksIn() uint32 { return w.cpi }

// IsTerminalCoverage reports whether a coverage's hi equals its window's last
// chunk — the derived "terminal"/finalized property (marked nowhere). A frozen
// terminal coverage means its window is finalized: its .bin inputs were
// demoted in the same commit, and it is never rebuilt again.
func (w Windows) IsTerminalCoverage(cov IndexCoverage) bool {
	return cov.Hi == w.LastChunk(cov.Window)
}
