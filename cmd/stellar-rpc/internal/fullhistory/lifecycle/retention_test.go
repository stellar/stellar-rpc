package lifecycle

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// ---------------------------------------------------------------------------
// Scenario: a window STRADDLING the floor serves in-range seqs and not-found
// below. A finalized window's frozen .idx covers [lo, hi] including chunks the
// floor has since risen past; the below-floor chunks are pruned, the .idx kept.
// (The floor arithmetic itself is tested in the geometry package.)
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

	// The floor later rose to chunk 2. Window 0 now STRADDLES it: chunks 0,1 below,
	// chunks 2,3 in range.
	through := chunk.ID(3).LastLedger()
	cfg := lifecycleTestConfig(t, cat, 2) // retention 2 ⇒ floor lands on chunk 2
	floor := floorFor(t, cfg, through)
	require.Equal(t, chunk.ID(2), floor)

	// The straddling window's frozen .idx is NOT swept: its last chunk (3) is in
	// range, so only its below-floor chunk artifacts (chunks 0,1) are pruned.
	assert.GreaterOrEqual(t, wins.LastChunk(0), floor,
		"a straddling window is not wholly below the floor — its .idx is kept")
	pops, _, err := eligiblePruneOps(cat, floor, nil)
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
	// 2,3 survive — exactly the data the floor admits.
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
