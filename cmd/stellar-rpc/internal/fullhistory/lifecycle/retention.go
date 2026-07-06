package lifecycle

import (
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// RetentionFloor is the lowest chunk still within retention; anything below is
// eligible for discard/prune. It is the reader-side retention contract (design
// "Reader retention contract", gettx §8.2 / §8.5): availability is decided by
// retention, not the on-disk file set, so prune/sweep can unlink a chunk the
// instant it passes the floor without coordinating with the index lifecycle. The
// floor may err LOW harmlessly (a wrongly-retained chunk still hits the reader's
// missing-file rule), so it anchors on the live last-committed ledger; widening history
// is backfill's job, not the floor's.
type RetentionFloor struct {
	chunk chunk.ID // lowest in-retention chunk
}

// RetentionFloorAt pins the floor from an already-computed floor ledger, so the
// tick derives EffectiveRetentionFloor once and shares it between the gauge and
// the gate rather than recomputing it per scan.
func RetentionFloorAt(floorLedger uint32) RetentionFloor {
	return RetentionFloor{chunk: chunk.IDFromLedger(floorLedger)}
}

// Excludes reports whether chunk c is below the floor (past retention). The scans
// use it on a chunk directly and, since an index is below the floor exactly when
// its last chunk is, as Excludes(layout.LastChunk(idx)) for a whole index.
func (f RetentionFloor) Excludes(c chunk.ID) bool { return c < f.chunk }

// FirstChunk is the lowest in-retention chunk — the single floor→chunk boundary
// definition shared by prune (the gate), the lifecycle plan range, and startup
// backfill, so the three can never disagree on where retention begins.
func (f RetentionFloor) FirstChunk() chunk.ID { return f.chunk }

// EffectiveRetentionFloor is the chunk-aligned lower bound of the retention
// window: the HIGHER of the sliding floor (retentionChunks back from the last
// complete chunk) and the fixed earliest_ledger. slidingChunk is signed so a
// young store / large retentionChunks clamps to chunk 0 instead of underflowing.
func EffectiveRetentionFloor(upperBound, retentionChunks, earliest uint32) uint32 {
	sliding := uint32(chunk.FirstLedgerSeq) // GenesisLedger
	if retentionChunks > 0 {
		slidingChunk := geometry.LastCompleteChunkAt(upperBound) - int64(retentionChunks) + 1
		sliding = geometry.ChunkFirstLedger(max(slidingChunk, 0))
	}
	return max(sliding, earliest)
}
