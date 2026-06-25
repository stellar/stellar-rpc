package streaming

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// CRASH-SAFETY tests — interpose at the two dangerous instants and assert both
// invariants: (A) every file on disk has its meta key; (B) key absent => file
// gone.
// ---------------------------------------------------------------------------

// Crash instant (i): file written but key not yet flipped to "frozen".
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

// Crash instant (ii): inside the REAL sweep, between the durable unlink and the
// key delete.
//
// Earlier this test hand-replayed the sweep steps and stopped before the final
// delete — which stays green no matter how SweepChunkArtifacts orders its own
// steps, because the test never runs that code. We now fire a hook from INSIDE
// SweepChunkArtifacts at the exact instant after unlink+fsync and before the
// key-delete batch, and assert the EXIT-side invariant there: file gone =>
// key still present. If the key delete were reordered ahead of the unlink, the
// file would still be on disk when the hook fires and the in-hook assertion
// fails. (Verified by experiment: moving the delete batch above the unlink loop
// turns this test red.)
func TestCrashSafety_SweepUnlinkDurableKeyNotDeleted(t *testing.T) {
	cat, root := testCatalog(t)

	// A frozen ledgers (one file) + frozen events (three files) for chunk 6.
	lfsPath := cat.layout.LedgerPackPath(6)
	writeArtifact(t, lfsPath)
	require.NoError(t, cat.MarkChunkFreezing(6, KindLedgers))
	require.NoError(t, cat.FlipChunkFrozen(6, KindLedgers))

	eventsPaths := cat.layout.EventsPaths(6)
	for _, p := range eventsPaths {
		writeArtifact(t, p)
	}
	require.NoError(t, cat.MarkChunkFreezing(6, KindEvents))
	require.NoError(t, cat.FlipChunkFrozen(6, KindEvents))

	refs := []ArtifactRef{
		{Chunk: 6, Kind: KindLedgers, State: StateFrozen},
		{Chunk: 6, Kind: KindEvents, State: StateFrozen},
	}
	allPaths := append([]string{lfsPath}, eventsPaths...)

	// The hook fires once, between the durable unlink and the key delete.
	fired := false
	cat.hooks.beforeKeyDelete = func() {
		fired = true
		for _, p := range allPaths {
			require.NoFileExists(t, p, "EXIT invariant: file must be unlinked before its key is deleted")
		}
		// ...and the keys must still be present (they are about to be deleted).
		for _, ref := range refs {
			ok, err := cat.has(ref.Key())
			require.NoError(t, err)
			require.True(t, ok, "key %q must still exist at the pre-delete instant", ref.Key())
		}
	}

	require.NoError(t, cat.SweepChunkArtifacts(refs))
	require.True(t, fired, "beforeKeyDelete hook must have fired inside SweepChunkArtifacts")

	// After the sweep both invariants hold globally.
	assertEveryFileHasKey(t, cat, root) // (A), vacuous — files gone
	for _, ref := range refs {          // (B) key absent => file gone
		s, err := cat.State(ref.Chunk, ref.Kind)
		require.NoError(t, err)
		require.Equal(t, State(""), s)
	}
	for _, p := range allPaths {
		require.NoFileExists(t, p)
	}
}

// Index-side twin of the EXIT-invariant test: fire INSIDE SweepTxHashIndexKey, between
// the durable unlink and the key delete, and assert file-gone => key-present.
func TestCrashSafety_SweepIndexUnlinkDurableKeyNotDeleted(t *testing.T) {
	cat, root := testCatalog(t)

	cov, err := cat.MarkTxHashIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	idxPath := cat.layout.TxHashIndexFilePath(cov)
	writeArtifact(t, idxPath)
	require.NoError(t, cat.CommitTxHashIndex(cov))

	frozen, ok, err := cat.FrozenTxHashIndex(5)
	require.NoError(t, err)
	require.True(t, ok)

	fired := false
	cat.hooks.beforeKeyDelete = func() {
		fired = true
		require.NoFileExists(t, idxPath, "EXIT invariant: idx file must be unlinked before its key is deleted")
		ok, err := cat.has(frozen.Key)
		require.NoError(t, err)
		require.True(t, ok, "coverage key must still exist at the pre-delete instant")
	}

	require.NoError(t, cat.SweepTxHashIndexKey(frozen))
	require.True(t, fired, "beforeKeyDelete hook must have fired inside SweepTxHashIndexKey")

	require.NoFileExists(t, idxPath)
	keys, err := cat.TxHashIndexKeys(5)
	require.NoError(t, err)
	require.Empty(t, keys)
	assertEveryFileHasKey(t, cat, root)
}

// Never-unlink-under-a-frozen-key, asserted at the instant it matters: fire
// INSIDE SweepTxHashIndexKey between the frozen->pruning demote and the unlink, and
// require the durable value to be "pruning" — never "frozen". If the demote
// were dropped (or moved after the unlink), the value here would still be
// "frozen" and this fails. The same hook also confirms the file is still on
// disk at this instant (the demote precedes any unlink).
func TestSweepIndex_NeverUnlinksUnderFrozenKey(t *testing.T) {
	cat, _ := testCatalog(t)

	cov, err := cat.MarkTxHashIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	idxPath := cat.layout.TxHashIndexFilePath(cov)
	writeArtifact(t, idxPath)
	require.NoError(t, cat.CommitTxHashIndex(cov))

	frozen, ok, err := cat.FrozenTxHashIndex(5)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, StateFrozen, frozen.State)

	fired := false
	cat.hooks.beforeUnlink = func() {
		fired = true
		v, ok, err := cat.get(frozen.Key)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, string(StatePruning), v,
			"value at the pre-unlink instant must be pruning, never frozen")
		require.FileExists(t, idxPath, "file must still be on disk before the unlink")
	}

	require.NoError(t, cat.SweepTxHashIndexKey(frozen))
	require.True(t, fired, "beforeUnlink hook must have fired inside SweepTxHashIndexKey")

	require.NoFileExists(t, idxPath)
	keys, err := cat.TxHashIndexKeys(5)
	require.NoError(t, err)
	require.Empty(t, keys)
}

// Per-chunk twin of the never-unlink-under-frozen-key assertion: fire INSIDE
// SweepChunkArtifacts between the demote batch and the unlinks; every "frozen"
// ref must read "pruning" by then. Dropping the demote batch leaves them
// "frozen" here and this fails.
func TestSweepChunk_NeverUnlinksUnderFrozenKey(t *testing.T) {
	cat, _ := testCatalog(t)

	lfsPath := cat.layout.LedgerPackPath(6)
	writeArtifact(t, lfsPath)
	require.NoError(t, cat.MarkChunkFreezing(6, KindLedgers))
	require.NoError(t, cat.FlipChunkFrozen(6, KindLedgers))

	ref := ArtifactRef{Chunk: 6, Kind: KindLedgers, State: StateFrozen}

	fired := false
	cat.hooks.beforeUnlink = func() {
		fired = true
		v, ok, err := cat.get(ref.Key())
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, string(StatePruning), v,
			"value at the pre-unlink instant must be pruning, never frozen")
		require.FileExists(t, lfsPath, "file must still be on disk before the unlink")
	}

	require.NoError(t, cat.SweepChunkArtifacts([]ArtifactRef{ref}))
	require.True(t, fired, "beforeUnlink hook must have fired inside SweepChunkArtifacts")

	require.NoFileExists(t, lfsPath)
	s, err := cat.State(6, KindLedgers)
	require.NoError(t, err)
	require.Equal(t, State(""), s)
}
