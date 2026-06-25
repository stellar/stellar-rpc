package streaming

import (
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// The reader retention contract (design "Reader retention contract", gettx
// §8.5 / §9):
//
//	A read for any seq below the effective retention floor is not-found,
//	regardless of whether the file still exists on disk.
//
// Retention — not the on-disk file set — is the source of truth for
// availability, so prune and sweep may unlink a chunk the instant it passes the
// floor without coordinating with the index lifecycle (sweep.go,
// eligibility.go); a stale .idx pointing at a pruned .pack is masked.
//
// The floor has two roles with OPPOSITE safe directions (design "Lifecycle").
// The RETENTION role here (and the prune scan) may err LOW harmlessly — a
// wrongly-admitted seq still hits the reader's missing-file rule — so it anchors
// on the same live completeThrough the prune scan uses. The PRODUCTION role
// (catch-up's plan range, not this file) must NOT err low: it never consults the
// floor below existing storage; widening storage is catch-up's job,
// producibility enforced lazily by buildTxhashIndex's .bin precondition.
//
// retentionFloorFor derives the gate's floor from the SAME
// (completeThrough, RetentionChunks, earliest_ledger) the prune and discard
// scans use, so a read and a concurrent prune agree on the floor within a tick.
func retentionFloorFor(through, retentionChunks, earliest uint32) uint32 {
	return effectiveRetentionFloor(through, retentionChunks, earliest)
}

// seqWithinRetention reports whether seq is at or above the effective retention
// floor. false ⟹ not-found whatever is on disk. The floor is chunk-aligned
// (first ledger of the lowest in-retention chunk), so within a straddling
// window a seq is in-range in the floor chunk or above and not-found below.
func seqWithinRetention(seq, through, retentionChunks, earliest uint32) bool {
	return seq >= retentionFloorFor(through, retentionChunks, earliest)
}

// RetentionGate is the reader-facing handle consulted before serving any seq.
// It pins one (completeThrough, RetentionChunks, earliest_ledger) snapshot so
// every seq a single read examines is judged against one floor; the serving
// side derives a fresh gate per request.
type RetentionGate struct {
	floor uint32
}

// NewRetentionGate builds the gate for one snapshot. retentionChunks/earliest
// are the same knobs the prune scan reads. A shortened retentionChunks raises
// the floor immediately — no per-chunk state to migrate.
func NewRetentionGate(through, retentionChunks, earliest uint32) RetentionGate {
	return RetentionGate{floor: retentionFloorFor(through, retentionChunks, earliest)}
}

// Floor is the gate's effective retention floor — first ledger of the lowest
// in-retention chunk. Exposed for the reader's coverage filtering (the §8.2
// retention gate, skipping a window wholly below Floor) and for tests.
func (g RetentionGate) Floor() uint32 { return g.floor }

// Admits reports whether a read for seq is within retention. false ⟹ not-found
// regardless of on-disk state.
func (g RetentionGate) Admits(seq uint32) bool { return seq >= g.floor }

// WindowBelowFloor reports whether an entire window sits below the floor (its
// last chunk's last ledger is below it): the .idx need not be probed and the
// prune scan may sweep it. A window straddling the floor is NOT below it — it
// still holds in-retention seqs, so the reader probes it and lets Admits mask
// the below-floor tail.
func (g RetentionGate) WindowBelowFloor(w WindowID, windows Windows) bool {
	return windows.LastChunk(w).LastLedger() < g.floor
}

// ChunkBelowFloor reports whether an entire chunk sits below the floor (last
// ledger below it). Same predicate the discard and prune scans use
// (eligibility.go), surfaced on the gate so reader and lifecycle share one
// definition of "past retention".
func (g RetentionGate) ChunkBelowFloor(c chunk.ID) bool {
	return c.LastLedger() < g.floor
}

// effectiveRetentionFloor is the chunk-aligned lower bound of the retention
// window: the HIGHER of the sliding floor (lastCompleteChunkAt(upperBound) -
// retentionChunks + 1, when retentionChunks > 0) and the fixed earliest-ledger
// floor. slidingChunk is signed int64 as the underflow guard: a young store or
// large retentionChunks drives it negative, which max(..., 0) clamps to chunk 0.
func effectiveRetentionFloor(upperBound, retentionChunks, earliest uint32) uint32 {
	sliding := uint32(chunk.FirstLedgerSeq) // GenesisLedger
	if retentionChunks > 0 {
		slidingChunk := lastCompleteChunkAt(upperBound) - int64(retentionChunks) + 1
		sliding = chunkFirstLedger(max(slidingChunk, 0))
	}
	return max(sliding, earliest)
}
