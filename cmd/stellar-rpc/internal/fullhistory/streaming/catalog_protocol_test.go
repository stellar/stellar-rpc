package streaming

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
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
	states := map[string]State{}
	for _, k := range keys {
		states[k.Key] = k.State
	}
	require.Equal(t, StatePruning, states[txhashIndexKey(5, 5100, 5349)])
	require.Equal(t, StateFrozen, states[txhashIndexKey(5, 5100, 5350)])
}

func TestCommitIndexTerminalDemotesTxhashKeys(t *testing.T) {
	cat, _ := testCatalog(t)

	// Index 0 (chunks 0..999). Mark a few chunks' .bin frozen.
	for _, c := range []chunk.ID{0, 1, 500, 999} {
		require.NoError(t, cat.MarkChunkFreezing(c, KindTxHash))
		require.NoError(t, cat.FlipChunkFrozen(c, KindTxHash))
	}
	// A non-txhash key in the index must NOT be demoted.
	require.NoError(t, cat.FlipChunkFrozen(500, KindLedgers))

	// Terminal build covers the whole index [0,999] => hi == last chunk.
	cov, err := cat.MarkTxHashIndexFreezing(0, 0, 999)
	require.NoError(t, err)
	require.True(t, cat.txhashIndex.IsTerminalCoverage(cov))
	require.NoError(t, cat.CommitTxHashIndex(cov))

	// Every present txhash key in the index demoted to "pruning".
	for _, c := range []chunk.ID{0, 1, 500, 999} {
		s, err := cat.State(c, KindTxHash)
		require.NoError(t, err)
		require.Equal(t, StatePruning, s, "chunk %d txhash", c)
	}
	// The ledgers key is untouched.
	ledgers, err := cat.State(500, KindLedgers)
	require.NoError(t, err)
	require.Equal(t, StateFrozen, ledgers)

	// And the index coverage is frozen.
	frozen, ok, err := cat.FrozenTxHashIndex(0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, chunk.ID(999), frozen.Hi)
}

func TestCommitIndexNonTerminalLeavesTxhashKeys(t *testing.T) {
	cat, _ := testCatalog(t)

	require.NoError(t, cat.MarkChunkFreezing(0, KindTxHash))
	require.NoError(t, cat.FlipChunkFrozen(0, KindTxHash))

	// Non-terminal: hi (5) < index's last chunk (999).
	cov, err := cat.MarkTxHashIndexFreezing(0, 0, 5)
	require.NoError(t, err)
	require.False(t, cat.txhashIndex.IsTerminalCoverage(cov))
	require.NoError(t, cat.CommitTxHashIndex(cov))

	// txhash key NOT demoted — the index is still filling.
	s, err := cat.State(0, KindTxHash)
	require.NoError(t, err)
	require.Equal(t, StateFrozen, s)
}

// CommitTxHashIndex's finalization is one atomic batch: promote-new + demote-prev (+
// demote terminal txhash keys) land together or not at all. We prove it by
// fault-injecting a failure INSIDE the batch callback (which makes metastore
// drop the whole batch) and then asserting NOTHING the batch would have written
// is observable: the predecessor is still the unique frozen coverage, the new
// coverage is still "freezing", and the in-index txhash keys are still frozen.
// Rewriting CommitTxHashIndex as separate non-atomic Puts would leave some of those
// writes durable here and fail this test.
func TestCommitIndexBatchIsAtomic(t *testing.T) {
	cat, _ := testCatalog(t)

	// Predecessor [0,499] frozen.
	prev, err := cat.MarkTxHashIndexFreezing(0, 0, 499)
	require.NoError(t, err)
	require.NoError(t, cat.CommitTxHashIndex(prev))

	// A terminal txhash input that a successful terminal commit would demote.
	require.NoError(t, cat.MarkChunkFreezing(0, KindTxHash))
	require.NoError(t, cat.FlipChunkFrozen(0, KindTxHash))

	// The new TERMINAL coverage [0,999] — exercises all three batch puts at once.
	cov, err := cat.MarkTxHashIndexFreezing(0, 0, 999)
	require.NoError(t, err)
	require.True(t, cat.txhashIndex.IsTerminalCoverage(cov))

	// Fail the batch from inside its callback: metastore drops the whole batch.
	cat.hooks.failCommitBatch = func() bool { return true }
	err = cat.CommitTxHashIndex(cov)
	require.Error(t, err, "CommitTxHashIndex must surface the injected batch failure")
	cat.hooks.failCommitBatch = nil

	// All-or-nothing: the failed batch wrote NOTHING.
	// (1) The predecessor is still the index's unique frozen coverage.
	frozen, ok, err := cat.FrozenTxHashIndex(0)
	require.NoError(t, err, "must not observe two frozen coverages")
	require.True(t, ok)
	require.Equal(t, chunk.ID(499), frozen.Hi, "predecessor still the unique frozen coverage")
	// (2) The new coverage is still merely "freezing" (its promote did not land).
	v, ok, err := cat.get(cov.Key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, string(StateFreezing), v)
	// (3) The terminal txhash input was not demoted.
	s, err := cat.State(0, KindTxHash)
	require.NoError(t, err)
	require.Equal(t, StateFrozen, s)

	// And a clean re-commit (no fault) lands the whole batch.
	require.NoError(t, cat.CommitTxHashIndex(cov))
	frozen, ok, err = cat.FrozenTxHashIndex(0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, chunk.ID(999), frozen.Hi)
	prevState, ok, err := cat.get(prev.Key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, string(StatePruning), prevState)
	s, err = cat.State(0, KindTxHash)
	require.NoError(t, err)
	require.Equal(t, StatePruning, s)
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
	require.Equal(t, StateFrozen, keys[0].State, "re-commit must leave it frozen, not pruning")

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
