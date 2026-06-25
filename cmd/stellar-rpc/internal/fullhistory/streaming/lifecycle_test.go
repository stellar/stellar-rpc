package streaming

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/geometry"
)

// ---------------------------------------------------------------------------
// End-to-end tick harness: real catalog + real hotchunk DBs.
// ---------------------------------------------------------------------------

// TestRunLifecycleTick_BoundaryFreezesFoldsDiscards is the "one boundary, end to
// end" walk: chunk 0 just closed (its full hot DB is on disk, ready), chunk 1 is
// the new live chunk. One tick must:
//   - freeze chunk 0's cold artifacts FROM its hot DB (via processChunk's hot
//     branch),
//   - fold chunk 0 into its window's index (terminal coverage, cpi=1),
//   - discard chunk 0's hot DB (cold artifacts now fully serve it),
//   - leave the live chunk 1 untouched.
//
// Then re-running the tick is a no-op (quiescence).
func TestRunLifecycleTick_BoundaryFreezesFoldsDiscards(t *testing.T) {
	t.Parallel()                            // full-chunk ingest; isolated TempDir/catalog — overlap with the other heavy tests to fit the gate's go-test timeout
	cat, _ := smallTxHashIndexCatalog(t, 1) // window w == chunk w; a one-chunk window finalizes immediately
	cfg, rec := lifecycleTestConfig(t, cat, 0)

	// Chunk 0: just-closed, full hot DB on disk. Chunk 1: the new live chunk.
	ingestFullHotChunk(t, cat, 0)
	live := openLiveHotDB(t, cat, 1) // the live chunk's hot DB (held open by "ingestion")
	t.Cleanup(func() { _ = live.Close() })

	runTickForCatalog(context.Background(), t, cfg, cat)
	require.False(t, rec.fired(), "a healthy tick never aborts: %v", rec.last.Load())

	// Chunk 0's cold artifacts are all frozen.
	for _, kind := range []geometry.Kind{geometry.KindLedgers, geometry.KindEvents} {
		state, err := cat.State(0, kind)
		require.NoError(t, err)
		assert.Equal(t, geometry.StateFrozen, state, "chunk 0 %s frozen", kind)
	}
	// The window's index is terminal and covers chunk 0.
	covered, err := indexCovers(0, cat)
	require.NoError(t, err)
	assert.True(t, covered, "the window index folded chunk 0 in")
	fk, ok, err := cat.FrozenTxHashIndex(cat.TxHashIndexLayout().TxHashIndexID(0))
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, cat.TxHashIndexLayout().IsTerminalCoverage(fk), "a one-chunk window is terminal")

	// Chunk 0's hot DB is discarded (cold artifacts fully serve it).
	has, err := hotKeyExists(cat, 0)
	require.NoError(t, err)
	assert.False(t, has, "chunk 0's hot key is gone")

	// The live chunk 1 is untouched: its hot key still "ready", no cold artifacts.
	hotState, err := cat.HotState(1)
	require.NoError(t, err)
	assert.Equal(t, geometry.HotReady, hotState, "the live chunk's hot key is untouched")
	lfs1, err := cat.State(1, geometry.KindLedgers)
	require.NoError(t, err)
	assert.Equal(t, geometry.State(""), lfs1, "the live chunk is not frozen")

	// Quiescence: re-running the tick produces no work.
	through, err := deriveCompleteThrough(cat)
	require.NoError(t, err)
	assertQuiescent(t, cfg, cat, through)
}

// TestRunLifecycleTick_DiscardGatedOnIndexCoverage: a complete chunk whose cold
// ledgers+events are frozen but whose window index does NOT yet cover it keeps its
// hot DB (it still serves tx lookups). Only once a terminal coverage exists does
// the discard fire. cpi=2 so a single chunk does NOT finalize the window.
func TestRunLifecycleTick_DiscardGatedOnIndexCoverage(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 2) // window 0 = chunks [0,1]
	cfg, _ := lifecycleTestConfig(t, cat, 0)

	// Pre-freeze chunk 0's ledgers+events+txhash directly (no hot dependence), and
	// leave it with a "ready" hot DB on disk. The window is NOT finalized (cpi=2,
	// only chunk 0 present), so no terminal coverage exists.
	freezeKinds(t, cat, 0, geometry.KindLedgers, geometry.KindEvents, geometry.KindTxHash)
	makeReadyHotDirNoData(t, cat, 0)
	// A live chunk 1 above it so chunk 0 is below the partition boundary.
	require.NoError(t, cat.PutHotTransient(1))

	through := chunk.ID(0).LastLedger() // chunk 0 complete via cold
	// txhash is frozen, ledgers/events frozen, but the window has no FROZEN coverage
	// yet => indexCovers(0) is false => NOT discarded (still needed for lookups via
	// its .bin/hot DB until the index folds it in).
	ops, err := eligibleDiscardOps(cfg, cat, through)
	require.NoError(t, err)
	require.Empty(t, ops, "no index coverage yet: the hot DB stays")

	// Now finalize the window's index so it covers chunk 0 (terminal needs chunk
	// 1's .bin too; build a non-terminal-but-covering frozen coverage [0,0]).
	freezeCoverage(t, cat, 0, 0, 0)
	covered, err := indexCovers(0, cat)
	require.NoError(t, err)
	require.True(t, covered)

	ops, err = eligibleDiscardOps(cfg, cat, through)
	require.NoError(t, err)
	require.Len(t, ops, 1, "covered + nothing pending => discard eligible")
	require.NoError(t, ops[0]())

	has, err := hotKeyExists(cat, 0)
	require.NoError(t, err)
	assert.False(t, has, "the now-covered chunk's hot DB is discarded")
}

// TestRunLifecycleTick_PastFloorPrune: a chunk wholly below the effective
// retention floor has its artifact files and hot DB swept, regardless of state.
func TestRunLifecycleTick_PastFloorPrune(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 1)
	cfg, rec := lifecycleTestConfig(t, cat, 2) // retain ~2 chunks

	// completeThrough will be chunk 5's last ledger (positional: live chunk 6).
	// floor = geometry.LastCompleteChunkAt(through)-retention+1 = 5-2+1 = chunk 4's first
	// ledger. So chunks 0..3 are wholly past the floor and must be swept.
	for c := chunk.ID(0); c <= 5; c++ {
		freezeKinds(t, cat, c, geometry.KindLedgers, geometry.KindEvents, geometry.KindTxHash)
		writeArtifact(t, cat.Layout().LedgerPackPath(c))
		freezeCoverage(t, cat, cat.TxHashIndexLayout().TxHashIndexID(c), c, c) // each one-chunk window terminal
	}
	// A past-floor hot DB too (chunk 1).
	makeReadyHotDirNoData(t, cat, 1)
	live := openLiveHotDB(t, cat, 6) // live chunk
	t.Cleanup(func() { _ = live.Close() })

	through, err := deriveCompleteThrough(cat)
	require.NoError(t, err)
	require.Equal(t, chunk.ID(5).LastLedger(), through)
	floor := effectiveRetentionFloor(through, cfg.RetentionChunks, 0)
	require.Equal(t, chunk.ID(4).FirstLedger(), floor, "floor anchors 2 chunks back")

	runTickForCatalog(context.Background(), t, cfg, cat)
	require.False(t, rec.fired(), "prune tick never aborts: %v", rec.last.Load())

	// Chunks 0..3 (wholly below the floor) are gone: keys and files.
	for c := chunk.ID(0); c <= 3; c++ {
		ledgers, serr := cat.State(c, geometry.KindLedgers)
		require.NoError(t, serr)
		assert.Equal(t, geometry.State(""), ledgers, "chunk %s ledgers key swept", c)
		assert.NoFileExists(t, cat.Layout().LedgerPackPath(c), "chunk %s pack swept", c)
		has, herr := hotKeyExists(cat, c)
		require.NoError(t, herr)
		assert.False(t, has, "chunk %s hot key swept", c)
	}
	// Chunk 4 (the floor chunk) and 5 are within retention and survive.
	for c := chunk.ID(4); c <= 5; c++ {
		ledgers, serr := cat.State(c, geometry.KindLedgers)
		require.NoError(t, serr)
		assert.Equal(t, geometry.StateFrozen, ledgers, "chunk %s in retention survives", c)
	}

	assertQuiescent(t, cfg, cat, through)
}

// TestRunLifecycleTick_PrunesTransientIndexDebris: a "freezing" index key (a
// crashed build attempt) is swept regardless of window, even within retention.
func TestRunLifecycleTick_PrunesTransientIndexDebris(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 2)
	cfg, rec := lifecycleTestConfig(t, cat, 0)

	// A crashed build left a "freezing" coverage key (no commit).
	_, err := cat.MarkTxHashIndexFreezing(0, 0, 0)
	require.NoError(t, err)

	through, err := deriveCompleteThrough(cat)
	require.NoError(t, err)
	ops, err := eligiblePruneOps(cfg, cat, through)
	require.NoError(t, err)
	require.Len(t, ops, 1, "the freezing debris is swept")
	require.NoError(t, ops[0]())
	require.False(t, rec.fired())

	covs, err := cat.AllTxHashIndexKeys()
	require.NoError(t, err)
	require.Empty(t, covs, "the freezing index key is gone")
}

// ---------------------------------------------------------------------------
// CLEAN SHUTDOWN: a ctx cancelled mid-tick returns WITHOUT fatal.
// ---------------------------------------------------------------------------

// TestRunLifecycleTick_CleanShutdownNoFatal: when executePlan returns because
// ctx was cancelled, the tick must NOT call Fatalf — cancellation is a shutdown,
// never an op failure. The plan stage's work is real (a backend-only chunk that
// the cancelled ctx aborts), so executePlan genuinely returns an error here.
func TestRunLifecycleTick_CleanShutdownNoFatal(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 1)
	rec := &fatalRecorder{}

	// A READY live chunk 1 so chunk 0 sits BELOW the partition and counts as
	// complete (positional term => through = chunk 0's last ledger), making the
	// plan range [0,0] non-empty. Chunk 0 has no frozen artifacts, so resolve
	// schedules a ChunkBuild whose seamed execution we cancel mid-flight.
	readyHot(t, cat, 1)                        // live chunk (ready + dir)
	require.NoError(t, cat.PutHotTransient(0)) // chunk 0 in storage, below live

	// Block the chunk build long enough to cancel, then make it observe the cancel.
	started := make(chan struct{})
	cfg := LifecycleConfig{
		ExecConfig: ExecConfig{
			Catalog: cat,
			Logger:  silentLogger(),
			Workers: 1,
			runChunk: func(ctx context.Context, _ ChunkBuild, _ ExecConfig) error {
				close(started)
				<-ctx.Done() // wait for the cancel, then return the ctx error
				return ctx.Err()
			},
		},
		RetentionChunks: 0,
		Fatalf:          rec.fatalf,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runLifecycleTick(ctx, cfg, cat, 0) // lastChunk 0: plan range [0,0], the build we cancel
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the chunk build never started")
	}
	cancel() // shutdown mid-tick

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the tick did not return after ctx cancellation")
	}
	require.False(t, rec.fired(), "a cancelled ctx is a clean shutdown, NOT an op failure — no Fatalf")
}

// TestRunLifecycleTick_GenuineFailureAborts: when a plan op fails for a real
// reason (NOT ctx cancellation), the tick aborts via Fatalf per the error policy.
func TestRunLifecycleTick_GenuineFailureAborts(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 1)
	rec := &fatalRecorder{}

	readyHot(t, cat, 1)                        // ready live chunk => through = chunk 0 last ledger
	require.NoError(t, cat.PutHotTransient(0)) // chunk 0 below live, no frozen artifacts

	cfg := LifecycleConfig{
		ExecConfig: ExecConfig{
			Catalog: cat,
			Logger:  silentLogger(),
			Workers: 1,
			runChunk: func(context.Context, ChunkBuild, ExecConfig) error {
				return assertErr // a genuine, non-cancellation failure
			},
		},
		Fatalf: rec.fatalf,
	}
	runLifecycleTick(context.Background(), cfg, cat, 0) // lastChunk 0: plan range [0,0], the failing build
	require.True(t, rec.fired(), "a genuine op failure aborts the daemon")
}
