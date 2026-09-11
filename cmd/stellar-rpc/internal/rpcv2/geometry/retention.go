package geometry

import (
	"math"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// Retention is the daemon's retention policy: keep size chunks back from the
// highest complete chunk, never below earliestChunk (size 0 = full history). The
// fields are unexported so every Retention carries a validated earliestChunk
// (config_validate.go rejects a non-chunk-aligned earliest_ledger).
type Retention struct {
	size          uint32   // retention_chunks
	earliestChunk chunk.ID // earliest_ledger, chunk-aligned
}

// NewRetention binds the retention_chunks knob to the pinned, validated earliest
// chunk.
func NewRetention(size uint32, earliestChunk chunk.ID) Retention {
	return Retention{size: size, earliestChunk: earliestChunk}
}

// FloorAt is the lowest chunk still in retention given the highest complete chunk
// as a signed frontier (-1 = nothing complete): max(frontier-size+1, earliestChunk),
// or earliestChunk when size is 0 (full history). Signed so LastCompleteChunkAt's -1
// flows straight in.
func (r Retention) FloorAt(frontier int64) chunk.ID {
	sliding := int64(r.earliestChunk)
	if r.size > 0 {
		sliding = frontier - int64(r.size) + 1
	}
	return chunk.ID(max(sliding, int64(r.earliestChunk))) //nolint:gosec // max >= earliestChunk >= 0, fits uint32
}

// RetentionWindow is the configured window's width in ledgers — the
// retention_chunks knob in ledger units. 0 means full history (nothing is ever
// pruned); a size whose ledger count exceeds uint32 clamps to the maximum.
func (r Retention) RetentionWindow() uint32 {
	ledgers := min(uint64(r.size)*uint64(chunk.LedgersPerChunk), math.MaxUint32)
	return uint32(ledgers) //nolint:gosec // min clamps to MaxUint32
}
