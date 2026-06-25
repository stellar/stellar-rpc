package streaming

import (
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// RetentionGate is the reader-side retention contract (design "Reader retention
// contract", gettx §8.2 / §8.5): a read for any seq below the effective floor is
// not-found regardless of what's on disk. Retention — not the on-disk file set —
// is the source of truth for availability, which lets prune/sweep unlink a chunk
// the instant it passes the floor without coordinating with the index lifecycle
// (a stale .idx pointing at a pruned .pack is masked). The gate may err LOW
// harmlessly — a wrongly-admitted seq still hits the reader's missing-file rule —
// so it anchors on the same live completeThrough the prune scan uses; widening
// history is catch-up's job, not the gate's.
type RetentionGate struct {
	floor uint32 // first ledger of the lowest in-retention chunk
}

// NewRetentionGate pins the floor for one (completeThrough, retentionChunks,
// earliest) snapshot. A shortened retentionChunks raises the floor at once — no
// per-chunk state to migrate.
func NewRetentionGate(through, retentionChunks, earliest uint32) RetentionGate {
	return RetentionGate{floor: effectiveRetentionFloor(through, retentionChunks, earliest)}
}

// Floor is the effective retention floor, exposed for the reader's coverage
// filtering (§8.2: skip a window wholly below it) and for tests.
func (g RetentionGate) Floor() uint32 { return g.floor }

// Admits reports whether seq is within retention; false ⟹ not-found regardless
// of on-disk state.
func (g RetentionGate) Admits(seq uint32) bool { return seq >= g.floor }

// WindowBelowFloor reports whether a whole window sits below the floor — its .idx
// need not be probed and the prune scan may sweep it. A window straddling the
// floor is NOT below it: Admits masks its below-floor tail.
func (g RetentionGate) WindowBelowFloor(w WindowID, windows Windows) bool {
	return windows.LastChunk(w).LastLedger() < g.floor
}

// ChunkBelowFloor reports whether a whole chunk sits below the floor — the same
// "past retention" predicate the discard and prune scans use (eligibility.go).
func (g RetentionGate) ChunkBelowFloor(c chunk.ID) bool {
	return c.LastLedger() < g.floor
}

// effectiveRetentionFloor is the chunk-aligned lower bound of the retention
// window: the HIGHER of the sliding floor (retentionChunks back from the last
// complete chunk) and the fixed earliest_ledger. slidingChunk is signed so a
// young store / large retentionChunks clamps to chunk 0 instead of underflowing.
func effectiveRetentionFloor(upperBound, retentionChunks, earliest uint32) uint32 {
	sliding := uint32(chunk.FirstLedgerSeq) // GenesisLedger
	if retentionChunks > 0 {
		slidingChunk := lastCompleteChunkAt(upperBound) - int64(retentionChunks) + 1
		sliding = chunkFirstLedger(max(slidingChunk, 0))
	}
	return max(sliding, earliest)
}
