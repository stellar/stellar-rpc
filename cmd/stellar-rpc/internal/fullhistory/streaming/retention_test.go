package streaming

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/geometry"
)

// ---------------------------------------------------------------------------
// Reader retention contract (retention.go): a chunk below the floor is past
// retention — not-found regardless of on-disk state. Excludes is the whole
// surface; these are pure-arithmetic unit tests plus one tied to a real index
// layout.
// ---------------------------------------------------------------------------

// through = chunk 100's last ledger, retain 10 chunks ⇒ floor = chunk 91
// (effectiveRetentionFloor: 100-10+1 = 91). Anything below chunk 91 is excluded.
func TestRetentionFloor_ExcludesBelow(t *testing.T) {
	floor := NewRetentionFloor(chunk.ID(100).LastLedger(), 10, 0)

	assert.True(t, floor.Excludes(90), "one chunk below the floor => excluded")
	assert.False(t, floor.Excludes(91), "the floor chunk itself => retained")
	assert.False(t, floor.Excludes(100), "above the floor => retained")
	assert.True(t, floor.Excludes(0), "genesis chunk, far below => excluded")
}

// Shortening retention raises the floor immediately — no per-chunk state to
// migrate. The SAME (through, earliest) with a smaller retentionChunks excludes
// chunks that were retained before.
func TestRetentionFloor_ShorteningRaisesFloorImmediately(t *testing.T) {
	through := chunk.ID(100).LastLedger()

	wide := NewRetentionFloor(through, 50, 0)   // floor = chunk 51
	narrow := NewRetentionFloor(through, 10, 0) // floor = chunk 91

	assert.False(t, wide.Excludes(60), "chunk 60 retained under the wide retention")
	assert.True(t, narrow.Excludes(60), "shortening retention excludes it at once")
}

// A whole tx-hash index is below the floor exactly when its last chunk is, so
// callers test Excludes(layout.LastChunk(idx)) — no index-specific method needed.
func TestRetentionFloor_ExcludesIndexByLastChunk(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4) // indexes: 0=[0,3], 1=[4,7], 2=[8,11]
	layout := cat.TxHashIndexLayout()

	// through = chunk 11's last ledger, retain 4 chunks ⇒ floor = chunk 8
	// (11-4+1 = 8). Index 2 ([8,11]) starts at the floor.
	floor := NewRetentionFloor(chunk.ID(11).LastLedger(), 4, 0)

	// Index 0 ([0,3]) and index 1 ([4,7]) are wholly below the floor; index 2
	// ([8,11]) is the floor index — at it, not below.
	assert.True(t, floor.Excludes(layout.LastChunk(0)))
	assert.True(t, floor.Excludes(layout.LastChunk(1)))
	assert.False(t, floor.Excludes(layout.LastChunk(2)))

	// Chunk 7 is below the floor; chunk 8 is the floor chunk.
	assert.True(t, floor.Excludes(7))
	assert.False(t, floor.Excludes(8))
}

// retention_chunks = 0 means "full history": the sliding floor is disabled, so
// the floor pins at earliest_ledger (never below genesis) and does NOT move with
// `through`. Exercises the earliest-wins branch (the other tests pass earliest=0,
// below genesis, so the sliding floor always wins).
func TestRetentionFloor_FullHistoryPinsAtEarliest(t *testing.T) {
	through := chunk.ID(100).LastLedger()

	// earliest at chunk 50: the fixed floor wins; no sliding floor applies.
	floor := NewRetentionFloor(through, 0, chunk.ID(50).FirstLedger())
	assert.True(t, floor.Excludes(49), "below earliest => excluded")
	assert.False(t, floor.Excludes(50), "the earliest chunk is retained")

	// earliest = genesis: full history from chunk 0.
	atGenesis := NewRetentionFloor(through, 0, chunk.FirstLedgerSeq)
	assert.False(t, atGenesis.Excludes(0), "genesis chunk retained under full history")

	// The full-history floor is independent of `through`: a much higher tip does
	// not raise it (there is no sliding window to slide).
	higher := NewRetentionFloor(chunk.ID(1_000).LastLedger(), 0, chunk.ID(50).FirstLedger())
	assert.False(t, higher.Excludes(50), "full-history floor does not move with the tip")
	assert.True(t, higher.Excludes(49), "still excludes below earliest regardless of the tip")
}

// A young store — or a retention_chunks larger than the history that exists —
// must clamp the floor to chunk 0, not underflow the signed chunk arithmetic
// into a giant floor that would exclude everything.
func TestRetentionFloor_YoungStoreClampsToGenesis(t *testing.T) {
	// Only 4 complete chunks exist (0..3) but we ask to retain 1000: the sliding
	// floor 3-1000+1 = -996 must clamp to chunk 0, not wrap.
	floor := NewRetentionFloor(chunk.ID(3).LastLedger(), 1000, 0)
	assert.False(t, floor.Excludes(0), "chunk 0 is at the clamped floor, not below it")
}

// ---------------------------------------------------------------------------
// Scenario: a window STRADDLING the floor serves in-range seqs and not-found
// below. A finalized window's frozen .idx covers [lo, hi] including chunks the
// floor has since risen past; the gate masks those below-floor chunks. This is
// the stale-.idx case gettransaction §8.5 tolerates because the reader gate
// makes below-floor reads not-found regardless of what the .idx resolves.
// ---------------------------------------------------------------------------

func TestReaderRetention_WindowStraddlingFloorServesInRangeNotBelow(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4) // window 0 = chunks [0,3]
	wins := cat.TxHashIndexLayout()

	// Window 0 was finalized at terminal coverage [0,3] when the floor sat at
	// genesis. Its frozen .idx hashes chunks 0..3 — a static, stale-lo artifact.
	for c := chunk.ID(0); c <= 3; c++ {
		freezeKinds(t, cat, c, geometry.KindLedgers, geometry.KindEvents)
	}
	freezeCoverage(t, cat, 0, 0, 3)
	fk, ok, err := cat.FrozenTxHashIndex(0)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, wins.IsTerminalCoverage(fk), "window 0 is finalized")

	// The floor later rose to chunk 2 (its first ledger). Window 0 now STRADDLES
	// the floor: chunks 0,1 below it, chunks 2,3 in range. The .idx still claims
	// lo=0, but the reader gate is the source of truth.
	through := chunk.ID(3).LastLedger()
	// Pick retentionChunks so the sliding floor lands on chunk 2:
	// geometry.LastCompleteChunkAt(through)=3, floor chunk = 3-retention+1 = 2 ⇒ retention=2.
	floor := NewRetentionFloor(through, 2, 0)

	// (The seq-level reader masking — a below-floor read is not-found even though
	// the stale .idx still hashes chunks 0,1 — returns with the read path, #772;
	// RetentionFloor here exposes only the chunk-granularity prune predicate.)

	// The straddling window's frozen .idx is NOT swept: the window is not wholly
	// below the floor (its last chunk, 3, is in range), so only its below-floor
	// chunk artifacts (chunks 0,1) are pruned.
	assert.False(t, floor.Excludes(wins.LastChunk(0)),
		"a straddling window is not wholly below the floor — its .idx is kept")
	cfg, _ := lifecycleTestConfig(t, cat, 2)
	pops, err := eligiblePruneOps(cfg, cat, through)
	require.NoError(t, err)
	for _, op := range pops {
		require.NoError(t, op())
	}

	// The window's frozen .idx coverage survives the prune (index family).
	survives, ok, err := cat.FrozenTxHashIndex(0)
	require.NoError(t, err)
	require.True(t, ok, "the straddling window keeps its frozen coverage")
	require.Equal(t, fk.Key, survives.Key)

	// The below-floor chunks 0,1 ARE pruned (chunk family); the in-range chunks
	// 2,3 survive — exactly the data the gate admits.
	for c := chunk.ID(0); c <= 1; c++ {
		ledgers, serr := cat.State(c, geometry.KindLedgers)
		require.NoError(t, serr)
		assert.Equal(t, geometry.State(""), ledgers, "below-floor chunk %s pruned", c)
	}
	for c := chunk.ID(2); c <= 3; c++ {
		ledgers, serr := cat.State(c, geometry.KindLedgers)
		require.NoError(t, serr)
		assert.Equal(t, geometry.StateFrozen, ledgers, "in-range chunk %s survives", c)
	}
	assertQuiescent(t, cfg, cat, through)
}
