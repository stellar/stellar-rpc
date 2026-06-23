package streaming

import (
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// The reader retention contract (design "Reader retention contract",
// gettransaction §8.5 / §9). It is the single storage-side rule that lets the
// prune and sweep stages remove a chunk's files the instant it passes the
// retention floor WITHOUT coordinating with the index lifecycle:
//
//	A read for any seq below the effective retention floor is not-found,
//	regardless of whether the underlying file still exists on disk.
//
// A read may land on a .pack that pruning has since deleted, or on one that
// pruning is about to delete; a below-floor read is not-found either way. From
// the storage layer's perspective, retention — not the set of files on disk —
// is the source of truth for "is this data available?", and that is the entire
// property prune/sweep rely on to unlink unilaterally (sweep.go,
// eligibility.go).
//
// The floor plays two roles with OPPOSITE safe directions, and the system
// keeps them strictly separate (design "Lifecycle"):
//
//   - RETENTION role (this gate, the prune scan): erring LOW is harmless. A
//     gate that admits a seq an instant after pruning removed its data returns
//     not-found via the reader's missing-file rule; a gate that rejects a seq an
//     instant before pruning gets to it merely anticipates the prune. Either way
//     the answer a reader sees is correct, so this role anchors on the same live
//     completeThrough the prune scan uses.
//   - PRODUCTION role (catch-up's plan range, NOT this file): erring low is
//     DANGEROUS — it would demand chunks from a bulk source nobody validated it
//     can produce. Production therefore never consults the floor below existing
//     storage; extending the bottom of storage (retention widening) is
//     exclusively catch-up's job, where producibility is enforced lazily per
//     chunk by the cold ingest (no pre-flight gate). This gate is a retention
//     consumer by construction (a read is harmless to reject), so it uses the
//     floor directly.
//
// retentionFloorFor is the gate's floor: effectiveRetentionFloor evaluated at
// the SAME (completeThrough, RetentionChunks, earliest_ledger) the prune and
// discard scans use, so a read and a concurrent prune agree on where the floor
// sits within one tick's snapshot. Sliding the floor is therefore atomic from
// the reader's perspective: shortening retention raises the floor and both the
// gate and the prune scan observe the higher value on the next derivation.
func retentionFloorFor(through, retentionChunks, earliest uint32) uint32 {
	return effectiveRetentionFloor(through, retentionChunks, earliest)
}

// seqWithinRetention reports whether seq is at or above the effective retention
// floor — the reader retention contract's admit/reject decision for one seq.
// false means the read MUST resolve to not-found no matter what is on disk;
// this is what makes it safe for pruning to unlink a chunk's files the moment
// the chunk passes the floor.
//
// The comparison is "seq >= floor", chunk-aligned through effectiveRetentionFloor:
// the floor is the first ledger of the lowest in-retention chunk, so a seq in a
// straddling window resolves in-range when it sits in the floor chunk or above
// and not-found when it sits in a below-floor chunk of the SAME window — the
// window-straddling case (gettransaction §8.5: a stale .idx whose lo references
// pruned chunks is tolerated precisely because this gate masks them).
func seqWithinRetention(seq, through, retentionChunks, earliest uint32) bool {
	return seq >= retentionFloorFor(through, retentionChunks, earliest)
}

// RetentionGate is the reader-facing handle the query-routing layer consults
// before serving any seq: it pins one (completeThrough, RetentionChunks,
// earliest_ledger) snapshot so every seq a single read examines is judged
// against one floor. The serving side derives a fresh gate per request (or per
// coverage refresh) — how it obtains completeThrough is the query-routing
// design's concern; this type only fixes the contract's arithmetic so the read
// path and the prune stage cannot drift.
type RetentionGate struct {
	floor uint32
}

// NewRetentionGate builds the gate for one snapshot of ingestion progress and
// the retention config. through is completeThrough; retentionChunks/earliest are
// the same knobs the prune scan reads. A shortened retentionChunks yields a
// higher floor immediately — no per-chunk state to migrate.
func NewRetentionGate(through, retentionChunks, earliest uint32) RetentionGate {
	return RetentionGate{floor: retentionFloorFor(through, retentionChunks, earliest)}
}

// Floor is the gate's effective retention floor — the first ledger of the
// lowest in-retention chunk. Exposed for the reader's coverage filtering (it
// skips a window's .idx probe when the window is wholly below Floor, the §8.2
// retention gate) and for tests.
func (g RetentionGate) Floor() uint32 { return g.floor }

// Admits reports whether a read for seq is within retention. false ⟹ the read
// is not-found regardless of on-disk state — the contract pruning relies on.
func (g RetentionGate) Admits(seq uint32) bool { return seq >= g.floor }

// ChunkBelowFloor reports whether an entire chunk sits below the floor — its
// last ledger is below the floor. This is the same predicate the discard and
// prune scans use (eligibility.go: last < floor), surfaced on the gate so the
// reader and the lifecycle share one definition of "past retention" rather than
// each open-coding the comparison.
func (g RetentionGate) ChunkBelowFloor(c chunk.ID) bool {
	return c.LastLedger() < g.floor
}
