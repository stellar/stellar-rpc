package fullhistory

import (
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
)

// RetentionFloor is the lowest chunk still within retention; any chunk below it
// is eligible for discard/prune. It is the reader-side retention contract
// (design "Reader retention contract", gettx §8.2 / §8.5): availability is
// decided by retention, not the on-disk file set, which lets prune/sweep unlink
// a chunk the instant it passes the floor without coordinating with the index
// lifecycle (a stale .idx pointing at a pruned .pack is masked). The floor may
// err LOW harmlessly — a wrongly-retained chunk still hits the reader's
// missing-file rule — so it anchors on the same live completeThrough the prune
// scan uses; widening history is catch-up's job, not the floor's.
type RetentionFloor struct {
	chunk chunk.ID // lowest in-retention chunk
}

// NewRetentionFloor pins the floor for one (through, retentionChunks, earliest)
// snapshot. A shortened retentionChunks raises the floor at once — no per-chunk
// state to migrate.
func NewRetentionFloor(through, retentionChunks, earliest uint32) RetentionFloor {
	return RetentionFloor{chunk: retentionFloorChunk(through, retentionChunks, earliest)}
}

// Excludes reports whether chunk c is below the floor — past retention, eligible
// for discard/prune. The discard and prune scans (eligibility.go) use it on a
// chunk directly and, since an index is below the floor exactly when its last
// chunk is, as Excludes(layout.LastChunk(idx)) for a whole tx-hash index. (The
// reader's seq-level admit predicate and the ledger-seq floor for §8.2 coverage
// filtering return with the read path, #772.)
func (f RetentionFloor) Excludes(c chunk.ID) bool { return c < f.chunk }

// retentionFloorChunk is the retention window's lower bound as a chunk id (the
// design's retentionFloorChunk): the HIGHER of the sliding floor (retentionChunks
// back from the last complete chunk) and the fixed earliest_ledger. slidingChunk is
// signed so a young store / large retentionChunks clamps to chunk 0 instead of
// underflowing. Both terms are chunk-first-ledgers, so IDFromLedger is exact.
func retentionFloorChunk(upperBound, retentionChunks, earliest uint32) chunk.ID {
	sliding := uint32(chunk.FirstLedgerSeq) // GenesisLedger
	if retentionChunks > 0 {
		slidingChunk := geometry.LastCompleteChunkAt(upperBound) - int64(retentionChunks) + 1
		sliding = geometry.ChunkFirstLedger(max(slidingChunk, 0))
	}
	return chunk.IDFromLedger(max(sliding, earliest))
}
