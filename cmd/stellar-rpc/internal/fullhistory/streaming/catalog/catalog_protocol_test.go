package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/geometry"
)

// ---------------------------------------------------------------------------
// Index commit batch atomicity: promote + demote + terminal land together.
// ---------------------------------------------------------------------------

func TestCommitIndexPromoteAndDemote(t *testing.T) {
	cat, _ := testCatalog(t)

	// First coverage [5100,5349] becomes frozen.
	cov1, err := cat.MarkTxHashIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	require.NoError(t, cat.CommitTxHashIndex(cov1))

	// Next boundary: [5100,5350]. Commit promotes it and demotes [5100,5349].
	cov2, err := cat.MarkTxHashIndexFreezing(5, 5100, 5350)
	require.NoError(t, err)
	require.NoError(t, cat.CommitTxHashIndex(cov2))

	// Exactly one frozen coverage — the new one.
	frozen, ok, err := cat.FrozenTxHashIndex(5)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, chunk.ID(5350), frozen.Hi)

	// The predecessor is now "pruning".
	keys, err := cat.TxHashIndexKeys(5)
	require.NoError(t, err)
	states := map[string]geometry.State{}
	for _, k := range keys {
		states[k.Key] = k.State
	}
	require.Equal(t, geometry.StatePruning, states[geometry.TxHashIndexKey(5, 5100, 5349)])
	require.Equal(t, geometry.StateFrozen, states[geometry.TxHashIndexKey(5, 5100, 5350)])
}

func TestCommitIndexTerminalDemotesTxhashKeys(t *testing.T) {
	cat, _ := testCatalog(t)

	// Index 0 (chunks 0..999). Mark a few chunks' .bin frozen.
	for _, c := range []chunk.ID{0, 1, 500, 999} {
		require.NoError(t, cat.MarkChunkFreezing(c, geometry.KindTxHash))
		require.NoError(t, cat.FlipChunkFrozen(c, geometry.KindTxHash))
	}
	// A non-txhash key in the index must NOT be demoted.
	require.NoError(t, cat.FlipChunkFrozen(500, geometry.KindLedgers))

	// Terminal build covers the whole index [0,999] => hi == last chunk.
	cov, err := cat.MarkTxHashIndexFreezing(0, 0, 999)
	require.NoError(t, err)
	require.True(t, cat.txhashIndex.IsTerminalCoverage(cov))
	require.NoError(t, cat.CommitTxHashIndex(cov))

	// Every present txhash key in the index demoted to "pruning".
	for _, c := range []chunk.ID{0, 1, 500, 999} {
		s, err := cat.State(c, geometry.KindTxHash)
		require.NoError(t, err)
		require.Equal(t, geometry.StatePruning, s, "chunk %d txhash", c)
	}
	// The ledgers key is untouched.
	ledgers, err := cat.State(500, geometry.KindLedgers)
	require.NoError(t, err)
	require.Equal(t, geometry.StateFrozen, ledgers)

	// And the index coverage is frozen.
	frozen, ok, err := cat.FrozenTxHashIndex(0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, chunk.ID(999), frozen.Hi)
}

func TestCommitIndexNonTerminalLeavesTxhashKeys(t *testing.T) {
	cat, _ := testCatalog(t)

	require.NoError(t, cat.MarkChunkFreezing(0, geometry.KindTxHash))
	require.NoError(t, cat.FlipChunkFrozen(0, geometry.KindTxHash))

	// Non-terminal: hi (5) < index's last chunk (999).
	cov, err := cat.MarkTxHashIndexFreezing(0, 0, 5)
	require.NoError(t, err)
	require.False(t, cat.txhashIndex.IsTerminalCoverage(cov))
	require.NoError(t, cat.CommitTxHashIndex(cov))

	// txhash key NOT demoted — the index is still filling.
	s, err := cat.State(0, geometry.KindTxHash)
	require.NoError(t, err)
	require.Equal(t, geometry.StateFrozen, s)
}

// A terminal coverage that starts AFTER its index's first chunk must demote only
// the .bin inputs inside its own [Lo,Hi] range. Chunks below Lo belong to a
// different (lower) coverage's build and must keep their frozen .bin — the new
// .idx cannot answer their ledgers, so a sweep deleting them would strand that
// lower build.
func TestCommitIndexTerminalDemotesOnlyCoverageRange(t *testing.T) {
	cat, _ := testCatalog(t)

	// Index 5 spans chunks [5000,5999]. Freeze .bin inputs both BELOW the
	// coverage's Lo and WITHIN it.
	below := []chunk.ID{5100, 5200}
	within := []chunk.ID{5500, 5999}
	for _, c := range []chunk.ID{5100, 5200, 5500, 5999} {
		require.NoError(t, cat.MarkChunkFreezing(c, geometry.KindTxHash))
		require.NoError(t, cat.FlipChunkFrozen(c, geometry.KindTxHash))
	}

	// A TERMINAL coverage starting after the index's first chunk: [5500,5999].
	cov, err := cat.MarkTxHashIndexFreezing(5, 5500, 5999)
	require.NoError(t, err)
	require.True(t, cat.txhashIndex.IsTerminalCoverage(cov))
	require.NoError(t, cat.CommitTxHashIndex(cov))

	// .bin inputs inside [Lo,Hi] are demoted...
	for _, c := range within {
		s, err := cat.State(c, geometry.KindTxHash)
		require.NoError(t, err)
		require.Equal(t, geometry.StatePruning, s, "chunk %d txhash inside coverage", c)
	}
	// ...but inputs below Lo are untouched.
	for _, c := range below {
		s, err := cat.State(c, geometry.KindTxHash)
		require.NoError(t, err)
		require.Equal(t, geometry.StateFrozen, s, "chunk %d txhash below coverage must survive", c)
	}
}

// An out-of-order or retried build whose range the frozen coverage already spans
// must be REFUSED, so FrozenTxHashIndex never regresses to a shorter index (which,
// if the predecessor was terminal, may have already demoted the .bin inputs
// needed to rebuild the longer one).
func TestCommitIndexRejectsStaleCoverage(t *testing.T) {
	cat, _ := testCatalog(t)

	// A longer coverage [5000,5500] is frozen first.
	long, err := cat.MarkTxHashIndexFreezing(5, 5000, 5500)
	require.NoError(t, err)
	require.NoError(t, cat.CommitTxHashIndex(long))

	// A shorter, fully-contained build commits afterward — it must be refused.
	short, err := cat.MarkTxHashIndexFreezing(5, 5000, 5400)
	require.NoError(t, err)
	require.Error(t, cat.CommitTxHashIndex(short),
		"a coverage the frozen one already spans must be refused")

	// The frozen coverage is unchanged, and only it is frozen.
	frozen, ok, err := cat.FrozenTxHashIndex(5)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, chunk.ID(5500), frozen.Hi)

	// A strictly-longer coverage is NOT stale — it commits and promotes.
	longer, err := cat.MarkTxHashIndexFreezing(5, 5000, 5600)
	require.NoError(t, err)
	require.NoError(t, cat.CommitTxHashIndex(longer))
	frozen, ok, err = cat.FrozenTxHashIndex(5)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, chunk.ID(5600), frozen.Hi)
}

// MarkChunkFreezing must refuse to re-materialize a "pruning" key (a sweep owns
// that artifact); resurrecting it would race the sweep into a frozen key with no
// file. A "freezing"/absent key stays idempotently re-materializable.
func TestMarkChunkFreezingRefusesPruning(t *testing.T) {
	cat, _ := testCatalog(t)

	require.NoError(t, cat.store.Put(geometry.ChunkKey(7, geometry.KindTxHash), string(geometry.StatePruning)))
	require.Error(t, cat.MarkChunkFreezing(7, geometry.KindTxHash),
		"re-marking a pruning key freezing must be refused")

	// The key is unchanged — still pruning, not resurrected.
	s, err := cat.State(7, geometry.KindTxHash)
	require.NoError(t, err)
	require.Equal(t, geometry.StatePruning, s)

	// An absent then "freezing" key is still accepted twice (idempotent).
	require.NoError(t, cat.MarkChunkFreezing(8, geometry.KindLedgers))
	require.NoError(t, cat.MarkChunkFreezing(8, geometry.KindLedgers))
	s, err = cat.State(8, geometry.KindLedgers)
	require.NoError(t, err)
	require.Equal(t, geometry.StateFreezing, s)
}

// CommitTxHashIndex is documented crash-safe to re-run on the same coverage (the
// hasPrev && prev.Key == cov.Key branch in protocol.go): a re-commit of an
// already-landed batch must be a no-op overwrite, leaving exactly one frozen
// coverage and nothing demoted against itself. This exercises that branch,
// which no other test touched.
func TestCommitIndexReCommitIsIdempotent(t *testing.T) {
	cat, _ := testCatalog(t)

	cov, err := cat.MarkTxHashIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	require.NoError(t, cat.CommitTxHashIndex(cov))

	// Second commit on the SAME coverage: the predecessor IS cov, so the demote
	// branch is skipped and the promote is an idempotent overwrite.
	require.NoError(t, cat.CommitTxHashIndex(cov))

	// Exactly one frozen coverage remains, and it is cov — not demoted against
	// itself, no debris.
	keys, err := cat.TxHashIndexKeys(5)
	require.NoError(t, err)
	require.Len(t, keys, 1, "exactly one coverage key in the index")
	require.Equal(t, cov.Key, keys[0].Key)
	require.Equal(t, geometry.StateFrozen, keys[0].State, "re-commit must leave it frozen, not pruning")

	frozen, ok, err := cat.FrozenTxHashIndex(5)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, chunk.ID(5349), frozen.Hi)
}

func TestMarkRequiresKinds(t *testing.T) {
	cat, _ := testCatalog(t)
	require.Error(t, cat.MarkChunkFreezing(1))
	require.Error(t, cat.FlipChunkFrozen(1))
}
