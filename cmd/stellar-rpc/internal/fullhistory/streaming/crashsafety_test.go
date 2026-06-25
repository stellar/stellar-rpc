package streaming

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// CRASH-SAFETY tests — drive the real protocol to a crash point and assert the
// recovery invariants on the durable state that survives: (A) every file on
// disk has its meta key; (B) key absent => file gone.
//
// These check the states a crash actually LEAVES BEHIND. The mid-operation
// ordering once asserted via in-method fault-injection hooks ("never unlink
// under a frozen key", "the commit batch is all-or-nothing") is now covered
// structurally by the sweep and commit tests; exhaustive per-write crash
// coverage is the job of the fault-injection harness tracked in the follow-up
// issue.
// ---------------------------------------------------------------------------

// Crash instant: file written but key not yet flipped to "frozen".
//
// Reproduces the mark-then-write protocol stopped after barrierNewFile but
// before FlipChunkFrozen / CommitTxHashIndex. The key is "freezing", the file is on
// disk. INV-3 disk->meta must still hold: the file is reachable from its key.
func TestCrashSafety_FileWrittenKeyNotFlipped(t *testing.T) {
	cat, root := testCatalog(t)

	// Per-chunk: mark freezing, write+barrier the file, then "crash" before the
	// flip.
	require.NoError(t, cat.MarkChunkFreezing(4, KindLedgers))
	lfsPath := cat.layout.LedgerPackPath(4)
	writeArtifact(t, lfsPath)
	require.NoError(t, barrierNewFile(lfsPath, true))
	// <-- crash here: no FlipChunkFrozen.

	// Index: mark freezing, write+barrier the file, "crash" before CommitTxHashIndex.
	cov, err := cat.MarkTxHashIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	idxPath := cat.layout.TxHashIndexFilePath(cov)
	writeArtifact(t, idxPath)
	require.NoError(t, barrierNewFile(idxPath, true))
	// <-- crash here: no CommitTxHashIndex.

	// INV-3 (disk -> meta): every file on disk has its key.
	assertEveryFileHasKey(t, cat, root)

	// The keys are observable as "freezing" — the recovery signal.
	s, err := cat.State(4, KindLedgers)
	require.NoError(t, err)
	require.Equal(t, StateFreezing, s)

	keys, err := cat.TxHashIndexKeys(5)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, StateFreezing, keys[0].State)

	// Recovery for the index "freezing" debris is the sweep: delete file + key.
	require.NoError(t, cat.SweepTxHashIndexKey(keys[0]))
	require.NoFileExists(t, idxPath)
	// And after the sweep, INV-3 still holds for what remains.
	assertEveryFileHasKey(t, cat, root)
}
