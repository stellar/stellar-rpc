package streaming

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ---------------------------------------------------------------------------
// Reader retention contract (retention.go): a seq below the floor is not-found
// regardless of on-disk state. These are pure-arithmetic unit tests; the
// straddling-window scenario below ties the gate to real on-disk artifacts.
// ---------------------------------------------------------------------------

func TestRetentionGate_AdmitsAtAndAboveFloor(t *testing.T) {
	// through = chunk 100's last ledger, retain 10 chunks ⇒ floor = chunk 91's
	// first ledger (effectiveRetentionFloor: 100-10+1 = 91).
	through := chunk.ID(100).LastLedger()
	gate := NewRetentionGate(through, 10, 0)
	require.Equal(t, chunk.ID(91).FirstLedger(), gate.Floor())

	tests := []struct {
		name string
		seq  uint32
		want bool
	}{
		{"one below the floor => not-found", gate.Floor() - 1, false},
		{"exactly the floor => admitted", gate.Floor(), true},
		{"floor chunk's last ledger => admitted", chunk.ID(91).LastLedger(), true},
		{"well above the floor => admitted", chunk.ID(100).FirstLedger(), true},
		{"genesis (far below) => not-found", chunk.FirstLedgerSeq, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, gate.Admits(tc.seq))
			// The free function and the gate agree (one definition).
			assert.Equal(t, tc.want, seqWithinRetention(tc.seq, through, 10, 0))
		})
	}
}

// Shortening retention raises the floor immediately in the gate — no per-chunk
// state to migrate. The SAME (through, earliest) with a smaller retentionChunks
// yields a higher floor, so seqs that were admitted become not-found at once.
func TestRetentionGate_ShorteningRaisesFloorImmediately(t *testing.T) {
	through := chunk.ID(100).LastLedger()

	wide := NewRetentionGate(through, 50, 0)   // floor = chunk 51
	narrow := NewRetentionGate(through, 10, 0) // floor = chunk 91
	require.Equal(t, chunk.ID(51).FirstLedger(), wide.Floor())
	require.Equal(t, chunk.ID(91).FirstLedger(), narrow.Floor())

	// A seq in chunk 60: inside the wide window, below the narrowed floor.
	seq := chunk.ID(60).FirstLedger()
	assert.True(t, wide.Admits(seq), "in range under the wide retention")
	assert.False(t, narrow.Admits(seq), "shortening retention makes it not-found at once")
}

// WindowBelowFloor / ChunkBelowFloor: a window or chunk wholly below the floor
// is past retention; one straddling it is not.
func TestRetentionGate_WindowAndChunkBelowFloor(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4) // windows: 0=[0,3], 1=[4,7], 2=[8,11]
	wins := cat.Windows()

	// through = chunk 11's last ledger, retain 4 chunks ⇒ floor = chunk 8's first
	// ledger (11-4+1 = 8). Window 2 starts at the floor.
	through := chunk.ID(11).LastLedger()
	gate := NewRetentionGate(through, 4, 0)
	require.Equal(t, chunk.ID(8).FirstLedger(), gate.Floor())

	// Window 0 ([0,3]) and window 1 ([4,7]) are wholly below the floor (chunk 8);
	// window 2 ([8,11]) is the floor window — at it, not below.
	assert.True(t, gate.WindowBelowFloor(0, wins))
	assert.True(t, gate.WindowBelowFloor(1, wins))
	assert.False(t, gate.WindowBelowFloor(2, wins))

	// Chunk 7 is below the floor; chunk 8 is the floor chunk.
	assert.True(t, gate.ChunkBelowFloor(7))
	assert.False(t, gate.ChunkBelowFloor(8))
}

// ---------------------------------------------------------------------------
// Scenario: a window STRADDLING the floor serves in-range seqs and not-found
// below. A finalized window's frozen .idx covers [lo, hi] including chunks the
// floor has since risen past; the gate masks those below-floor chunks. This is
// the stale-.idx case gettransaction §8.5 tolerates because the reader gate
// makes below-floor reads not-found regardless of what the .idx resolves.
// ---------------------------------------------------------------------------

func TestReaderRetention_WindowStraddlingFloorServesInRangeNotBelow(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4) // window 0 = chunks [0,3]
	wins := cat.Windows()

	// Window 0 was finalized at terminal coverage [0,3] when the floor sat at
	// genesis. Its frozen .idx hashes chunks 0..3 — a static, stale-lo artifact.
	for c := chunk.ID(0); c <= 3; c++ {
		freezeKinds(t, cat, c, KindLedgers, KindEvents)
	}
	freezeCoverage(t, cat, 0, 0, 3)
	fk, ok, err := cat.FrozenCoverage(0)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, wins.IsTerminalCoverage(fk), "window 0 is finalized")

	// The floor later rose to chunk 2 (its first ledger). Window 0 now STRADDLES
	// the floor: chunks 0,1 below it, chunks 2,3 in range. The .idx still claims
	// lo=0, but the reader gate is the source of truth.
	through := chunk.ID(3).LastLedger()
	// Pick retentionChunks so the sliding floor lands on chunk 2:
	// lastCompleteChunkAt(through)=3, floor chunk = 3-retention+1 = 2 ⇒ retention=2.
	gate := NewRetentionGate(through, 2, 0)
	require.Equal(t, chunk.ID(2).FirstLedger(), gate.Floor(),
		"the floor straddles window 0 at chunk 2")

	// A seq in chunk 2 or 3 (in range) is admitted even though the .idx's lo is a
	// now-pruned chunk 0; a seq in chunk 0 or 1 is not-found regardless of the
	// .idx still hashing it.
	assert.True(t, gate.Admits(chunk.ID(2).FirstLedger()), "floor chunk: in range")
	assert.True(t, gate.Admits(chunk.ID(3).LastLedger()), "above the floor: in range")
	assert.False(t, gate.Admits(chunk.ID(1).LastLedger()), "below the floor: not-found")
	assert.False(t, gate.Admits(chunk.ID(0).FirstLedger()), "below the floor: not-found")

	// The straddling window's frozen .idx is NOT swept (the window is not wholly
	// below the floor) — only its below-floor chunk artifacts (chunks 0,1) are
	// pruned. The .idx therefore keeps serving the in-range tail (chunks 2,3),
	// with the gate masking the now-pruned chunks 0,1 it still hashes.
	assert.False(t, gate.WindowBelowFloor(0, wins),
		"a straddling window is not wholly below the floor — its .idx is kept")
	cfg, _ := lifecycleTestConfig(t, cat, 2)
	pops, err := eligiblePruneOps(cfg, cat, through)
	require.NoError(t, err)
	for _, op := range pops {
		require.NoError(t, op())
	}

	// The window's frozen .idx coverage survives the prune (index family).
	survives, ok, err := cat.FrozenCoverage(0)
	require.NoError(t, err)
	require.True(t, ok, "the straddling window keeps its frozen coverage")
	require.Equal(t, fk.Key, survives.Key)

	// The below-floor chunks 0,1 ARE pruned (chunk family); the in-range chunks
	// 2,3 survive — exactly the data the gate admits.
	for c := chunk.ID(0); c <= 1; c++ {
		ledgers, serr := cat.State(c, KindLedgers)
		require.NoError(t, serr)
		assert.Equal(t, State(""), ledgers, "below-floor chunk %s pruned", c)
	}
	for c := chunk.ID(2); c <= 3; c++ {
		ledgers, serr := cat.State(c, KindLedgers)
		require.NoError(t, serr)
		assert.Equal(t, StateFrozen, ledgers, "in-range chunk %s survives", c)
	}
	assertQuiescent(t, cfg, cat, through)
}

// ---------------------------------------------------------------------------
// Scenario: retention WIDENING at the next startup. A window finalized at a
// NARROW coverage [lo, last] (a higher old floor) is re-derived by backfill at
// the new wider coverage [lo', last]: the resolver emits the wider IndexBuild
// plus .bin re-materialization for the newly-in-range chunks, and the terminal
// CommitIndex demotes the old coverage and promotes the wider one as the unique
// frozen. Extending the bottom of storage is backfill's job (runBackfill), never
// a tick's.
// ---------------------------------------------------------------------------

func TestReaderRetention_WideningReDerivesAndDemotesOldCoverage(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4) // window 0 = chunks [0,3]
	wins := cat.Windows()

	// Prior run, narrow retention: the floor sat at chunk 2, so window 0 was
	// finalized at the narrow TERMINAL coverage [2,3] (lo raised to the floor
	// chunk). Chunks 2,3 have ledgers/events frozen; chunks 0,1 were pruned (no keys).
	for c := chunk.ID(2); c <= 3; c++ {
		freezeKinds(t, cat, c, KindLedgers, KindEvents)
	}
	freezeCoverage(t, cat, 0, 2, 3) // narrow terminal coverage
	narrow, ok, err := cat.FrozenCoverage(0)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, wins.IsTerminalCoverage(narrow), "narrow coverage [2,3] is terminal")
	require.Equal(t, chunk.ID(2), narrow.Lo)

	// Retention widened: the new floor is genesis (chunk 0), so the desired
	// coverage for window 0 is the wider [0,3]. resolve at the wider range
	// re-derives. Chunks 0,1 are fully pruned ⇒ every kind requested (bulk
	// refetch); chunks 2,3 keep their frozen ledgers/events but need their .bin.
	plan, err := resolve(resolveCfg(cat), 0, 3)
	require.NoError(t, err)

	// One terminal index build at the WIDER coverage [0,3].
	require.Equal(t, []IndexBuild{{Window: 0, Lo: 0, Hi: 3}}, plan.IndexBuilds,
		"widening re-derives the window at its new wider terminal coverage")
	require.True(t, wins.IsTerminalCoverage(IndexCoverage{Window: 0, Lo: 0, Hi: 3}))

	// The newly-in-range chunks 0,1 need all kinds (fully pruned ⇒ bulk refetch);
	// chunks 2,3 need only their .bin (ledgers/events still frozen from local .pack).
	require.Equal(t, []chunk.ID{0, 1, 2, 3}, chunkSet(plan))
	for _, c := range []chunk.ID{0, 1} {
		cb, found := findChunkBuild(plan, c)
		require.True(t, found)
		assert.Equal(t, AllArtifacts(), cb.Artifacts,
			"fully-pruned chunk %s refetches every kind from the bulk source", c)
	}
	for _, c := range []chunk.ID{2, 3} {
		cb, found := findChunkBuild(plan, c)
		require.True(t, found)
		assert.Equal(t, NewArtifactSet(KindTxHash), cb.Artifacts,
			"covered chunk %s rebuilds only its .bin from the local .pack", c)
	}

	// Now drive the terminal CommitIndex for the wider coverage (what the
	// executor's IndexBuild does once the .bins are present). It must demote the
	// old narrow coverage and promote the wider one as the window's UNIQUE frozen.
	for c := chunk.ID(0); c <= 1; c++ {
		freezeKinds(t, cat, c, KindLedgers, KindEvents) // the refetch landed
	}
	wider, err := cat.MarkIndexFreezing(0, 0, 3)
	require.NoError(t, err)
	require.NoError(t, cat.CommitIndex(wider))

	// The window's unique frozen coverage is now the wider [0,3]; the old [2,3]
	// was demoted to "pruning".
	got, ok, err := cat.FrozenCoverage(0)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, chunk.ID(0), got.Lo, "the wider coverage is now the frozen one")
	assert.Equal(t, chunk.ID(3), got.Hi)
	assert.True(t, wins.IsTerminalCoverage(got))

	covs, err := cat.AllIndexKeys()
	require.NoError(t, err)
	var oldState, newState State
	for _, c := range covs {
		switch c.Key {
		case narrow.Key:
			oldState = c.State
		case wider.Key:
			newState = c.State
		}
	}
	assert.Equal(t, StatePruning, oldState, "the old narrow coverage was demoted")
	assert.Equal(t, StateFrozen, newState, "the wider coverage is frozen")
}

// The widening flows through backfill's runBackfill (resolve + executePlan),
// not a tick: a seamed runIndex performs the real terminal CommitIndex so the
// demote/promote happens on the production path. This is the "at the next
// startup" half of the contract.
func TestReaderRetention_WideningRunsThroughBackfill(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4) // window 0 = chunks [0,3]

	// Prior narrow finalization at [2,3].
	for c := chunk.ID(2); c <= 3; c++ {
		freezeKinds(t, cat, c, KindLedgers, KindEvents)
	}
	freezeCoverage(t, cat, 0, 2, 3)
	narrow, _, err := cat.FrozenCoverage(0)
	require.NoError(t, err)

	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 2,
		Process: ProcessConfig{Backend: zeroTxBackend(t)}, // bulk source for the refetch
		runChunk: func(_ context.Context, cb ChunkBuild, _ ExecConfig) error {
			// Simulate the freeze: flip every requested kind frozen (and demote
			// nothing — the index build owns that).
			kinds := []Kind{}
			for _, k := range []Kind{KindLedgers, KindEvents, KindTxHash} {
				if cb.Artifacts.Has(k) {
					kinds = append(kinds, k)
				}
			}
			if err := cat.MarkChunkFreezing(cb.Chunk, kinds...); err != nil {
				return err
			}
			return cat.FlipChunkFrozen(cb.Chunk, kinds...)
		},
		runIndex: func(_ context.Context, ib IndexBuild, _ ExecConfig) error {
			// The real terminal commit: mark-then-commit, which demotes the old
			// coverage and any in-window chunk:txhash keys.
			cov, merr := cat.MarkIndexFreezing(ib.Window, ib.Lo, ib.Hi)
			if merr != nil {
				return merr
			}
			return cat.CommitIndex(cov)
		},
	}

	// backfill widens the bottom of storage to chunk 0 by backfilling [0,3].
	require.NoError(t, runBackfill(context.Background(), cfg, 0, 3))

	// The window finalized at the wider [0,3]; the old [2,3] is demoted/swept-bound.
	got, ok, err := cat.FrozenCoverage(0)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, chunk.ID(0), got.Lo)
	assert.Equal(t, chunk.ID(3), got.Hi)
	require.NotEqual(t, narrow.Key, got.Key, "the frozen coverage is the wider one, not the old narrow one")
}

// ---------------------------------------------------------------------------
// Scenario: retention SHORTENING prunes the newly-out-of-range chunks
// immediately. The prune scan reads the floor live from (through,
// RetentionChunks), so a smaller RetentionChunks raises the floor and the next
// tick sweeps the chunks that just fell past it — keys and files alike.
// ---------------------------------------------------------------------------

func TestReaderRetention_ShorteningPrunesNewlyOutOfRangeChunks(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 1) // one-chunk windows: window c == chunk c
	wins := cat.Windows()

	// Chunks 0..5 fully frozen, each its own terminal one-chunk window, with a
	// real .pack on disk. Live chunk 6 (positional ⇒ through = chunk 5's last).
	for c := chunk.ID(0); c <= 5; c++ {
		freezeKinds(t, cat, c, KindLedgers, KindEvents, KindTxHash)
		writeArtifact(t, cat.layout.LedgerPackPath(c))
		freezeCoverage(t, cat, wins.WindowID(c), c, c)
	}
	live := openLiveHotDB(t, cat, 6)
	t.Cleanup(func() { _ = live.Close() })

	through, err := deriveCompleteThrough(cat)
	require.NoError(t, err)
	require.Equal(t, chunk.ID(5).LastLedger(), through)

	// Under wide retention (5 chunks) the floor would be chunk 1's first ledger,
	// so only chunk 0 would be past it — documenting the pre-shortening floor.
	require.Equal(t, chunk.ID(1).FirstLedger(),
		effectiveRetentionFloor(through, 5, 0), "the wide-retention floor is chunk 1")

	// Now SHORTEN retention to 2 chunks: floor = chunk 4's first ledger. Chunks
	// 0..3 are now past retention and must be swept on the next tick.
	cfg, rec := lifecycleTestConfig(t, cat, 2)
	require.Equal(t, chunk.ID(4).FirstLedger(),
		effectiveRetentionFloor(through, 2, 0), "shortening raised the floor to chunk 4")

	runTickForCatalog(context.Background(), t, cfg, cat)
	require.False(t, rec.fired(), "a shortening prune tick never aborts: %v", rec.last.Load())

	// Chunks 0..3 (newly out of range) are gone — keys and files.
	for c := chunk.ID(0); c <= 3; c++ {
		ledgers, serr := cat.State(c, KindLedgers)
		require.NoError(t, serr)
		assert.Equal(t, State(""), ledgers, "chunk %s key swept by the shortened floor", c)
		assert.NoFileExists(t, cat.layout.LedgerPackPath(c), "chunk %s pack swept", c)
		_, hasFrozen, ferr := cat.FrozenCoverage(wins.WindowID(c))
		require.NoError(t, ferr)
		assert.False(t, hasFrozen, "chunk %s window's index swept (wholly past the floor)", c)
	}
	// Chunks 4,5 (the new retention window) survive.
	for c := chunk.ID(4); c <= 5; c++ {
		ledgers, serr := cat.State(c, KindLedgers)
		require.NoError(t, serr)
		assert.Equal(t, StateFrozen, ledgers, "chunk %s within the shortened retention survives", c)
		assert.FileExists(t, cat.layout.LedgerPackPath(c))
	}

	assertQuiescent(t, cfg, cat, through)
}

// ---------------------------------------------------------------------------
// Scenario: the prune scan's redundant-input branch cleans a WIDENED-then-
// NARROWED window. A widening backfill re-froze (or left mid-write) a finalized
// window's chunk:c:txhash .bin keys, then retention narrowed back before the
// rebuild. The resolver schedules nothing (desired ⊆ stored), so re-
// materialization will never repair those keys; the prune scan's redundant-
// input branch demotes and sweeps them — "frozen" and "freezing" alike — because
// the window's terminal .idx provably covers their chunks.
// ---------------------------------------------------------------------------

func TestReaderRetention_RedundantInputCleanupOfWidenedThenNarrowedWindow(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4) // window 0 = chunks [0,3]
	wins := cat.Windows()

	// Window 0 is finalized at terminal coverage [0,3] (the post-widening final
	// .idx). ledgers/events frozen for all four chunks; a real .pack each.
	for c := chunk.ID(0); c <= 3; c++ {
		freezeKinds(t, cat, c, KindLedgers, KindEvents)
		writeArtifact(t, cat.layout.LedgerPackPath(c))
	}
	freezeCoverage(t, cat, 0, 0, 3)
	fk, ok, err := cat.FrozenCoverage(0)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, wins.IsTerminalCoverage(fk), "window 0 is finalized at [0,3]")

	// The abandoned widening left behind chunk:c:txhash .bin keys inside this
	// finalized window: chunk 1's is "frozen" (re-froze fully), chunk 2's is
	// "freezing" (crashed mid-write). Both are provably redundant — the terminal
	// .idx already covers chunks 1 and 2 — and the resolver never re-materializes
	// a covered window.
	freezeKinds(t, cat, 1, KindTxHash) // chunk:1:txhash = "frozen"
	writeArtifact(t, cat.layout.TxHashBinPath(1))
	require.NoError(t, cat.MarkChunkFreezing(2, KindTxHash)) // chunk:2:txhash = "freezing"
	writeArtifact(t, cat.layout.TxHashBinPath(2))

	// The resolver schedules NOTHING for this window (desired [0,3] ⊆ stored
	// [0,3]) — so these keys would never be repaired by re-materialization.
	plan, err := resolve(resolveCfg(cat), 0, 3)
	require.NoError(t, err)
	require.True(t, plan.Empty(), "a covered finalized window schedules no work, got %+v", plan)

	// The prune scan's redundant-input branch sweeps both, frozen and freezing
	// alike. A live chunk 4 keeps the window below the partition (not required for
	// the prune scan, but matches steady state).
	cfg, rec := lifecycleTestConfig(t, cat, 0) // full history; nothing past the floor
	through := chunk.ID(3).LastLedger()
	pops, err := eligiblePruneOps(cfg, cat, through)
	require.NoError(t, err)
	require.NotEmpty(t, pops, "the redundant chunk:txhash keys are scheduled for sweep")
	for _, op := range pops {
		require.NoError(t, op())
	}
	require.False(t, rec.fired())

	// Both redundant chunk:txhash keys (and their .bin files) are gone.
	for _, c := range []chunk.ID{1, 2} {
		st, serr := cat.State(c, KindTxHash)
		require.NoError(t, serr)
		assert.Equal(t, State(""), st, "chunk %s redundant txhash key swept", c)
		assert.NoFileExists(t, cat.layout.TxHashBinPath(c), "chunk %s .bin swept", c)
	}
	// The window's terminal .idx coverage and the chunks' ledgers/events survive — the
	// .idx is what serves these chunks now.
	survives, ok, err := cat.FrozenCoverage(0)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, fk.Key, survives.Key, "the terminal .idx coverage is untouched")
	for c := chunk.ID(0); c <= 3; c++ {
		ledgers, serr := cat.State(c, KindLedgers)
		require.NoError(t, serr)
		assert.Equal(t, StateFrozen, ledgers, "chunk %s ledgers survives", c)
	}

	assertQuiescent(t, cfg, cat, through)
}
