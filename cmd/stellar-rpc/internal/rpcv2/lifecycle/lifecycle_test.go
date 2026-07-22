package lifecycle

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/storage/chunk"
)

// tickMetricsRecorder counts the two gauges the lifecycle tick could touch, to pin
// which one it owns. Embeds NopMetrics for every other signal (Discard/Prune/etc.).
type tickMetricsRecorder struct {
	observability.NopMetrics

	mu             sync.Mutex
	lastCommitted  int
	retentionFloor int
}

func (r *tickMetricsRecorder) LastCommitted(uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastCommitted++
}

func (r *tickMetricsRecorder) RetentionFloor(uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.retentionFloor++
}

// TestRunLifecycleTick_DoesNotReEmitLastCommitted: the lifecycle tick owns the
// retention-floor gauge but must NOT re-emit last-committed — its chunk-aligned value
// would regress the ingestion loop's mid-chunk last-committed on every tick.
func TestRunLifecycleTick_DoesNotReEmitLastCommitted(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 1)
	cfg := lifecycleTestConfig(t, cat, 0)
	rec := &tickMetricsRecorder{}
	cfg.Metrics = rec

	// A cheap tick with no full ingest: chunk 0 is already frozen + index-covered with
	// a leftover "ready" hot DB, so the plan stage is a no-op and the discard scan
	// retires chunk 0. A live chunk 1 keeps chunk 0 below the partition.
	freezeKinds(t, cat, 0, geometry.KindLedgers, geometry.KindEvents, geometry.KindTxHash)
	freezeCoverage(t, cat, cat.TxHashIndexLayout().TxHashIndexID(0), 0, 0)
	makeReadyHotDirNoData(t, cat, 0)
	live := openLiveHotDB(t, cat, 1)
	t.Cleanup(func() { _ = live.Close() })

	require.NoError(t, runLifecycle(context.Background(), cfg, cat, chunk.ID(0)))

	assert.Positive(t, rec.retentionFloor, "the tick owns and emits the retention-floor gauge")
	assert.Zero(t, rec.lastCommitted, "the tick must NOT re-emit last-committed (ingestion owns it)")
}

// ---------------------------------------------------------------------------
// End-to-end tick harness: real catalog + real hotchunk DBs.
// ---------------------------------------------------------------------------

// TestRunLifecycleTick_BoundaryFreezesRebuildsDiscards is the "one boundary, end to
// end" walk: chunk 0 just closed (its full hot DB is on disk, ready), chunk 1 is
// the new live chunk. One tick must:
//   - freeze chunk 0's cold artifacts FROM its hot DB (via processChunk's hot
//     branch),
//   - rebuild chunk 0's window index (terminal coverage, cpi=1),
//   - discard chunk 0's hot DB (cold artifacts now fully serve it),
//   - leave the live chunk 1 untouched.
//
// Then re-running the tick is a no-op (quiescence).
func TestRunLifecycleTick_BoundaryFreezesRebuildsDiscards(t *testing.T) {
	// full-chunk ingest on an isolated TempDir/catalog; overlaps the other heavy
	// tests to fit the gate's go-test timeout.
	t.Parallel()
	cat, _ := smallTxHashIndexCatalog(t, 1) // window w == chunk w; a one-chunk window finalizes immediately
	cfg := lifecycleTestConfig(t, cat, 0)

	// Chunk 0: just-closed, full hot DB on disk. Chunk 1: the new live chunk.
	ingestFullHotChunk(t, cat, 0)
	live := openLiveHotDB(t, cat, 1) // the live chunk's hot DB (held open by "ingestion")
	t.Cleanup(func() { _ = live.Close() })

	// Chunk 0 is the boundary chunk ingestion hands over.
	require.NoError(t, runLifecycle(context.Background(), cfg, cat, 0), "a healthy tick never fails")

	// Chunk 0's cold artifacts are all frozen.
	for _, kind := range []geometry.Kind{geometry.KindLedgers, geometry.KindEvents} {
		state, err := cat.State(0, kind)
		require.NoError(t, err)
		assert.Equal(t, geometry.StateFrozen, state, "chunk 0 %s frozen", kind)
	}
	// The window's index is terminal and covers chunk 0.
	covered, err := cat.FrozenIndexCovers(0)
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

	// Quiescence: re-running the tick produces no work. The fixture pins the
	// frontier: chunk 0 durable, live chunk 1 empty ⇒ through = chunk 0's last ledger.
	assertQuiescent(t, cfg, cat, chunk.ID(0).LastLedger())
}

// TestRunLifecycleTick_DiscardGatedOnIndexCoverage: a complete chunk whose cold
// ledgers+events are frozen but whose window index does NOT yet cover it keeps its
// hot DB (it still serves tx lookups). Only once a terminal coverage exists does
// the discard fire. cpi=2 so a single chunk does NOT finalize the window.
func TestRunLifecycleTick_DiscardGatedOnIndexCoverage(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 2) // window 0 = chunks [0,1]
	cfg := lifecycleTestConfig(t, cat, 0)

	// Pre-freeze chunk 0's ledgers+events+txhash directly (no hot dependence), and
	// leave it with a "ready" hot DB on disk. The window is NOT finalized (cpi=2,
	// only chunk 0 present), so no terminal coverage exists.
	freezeKinds(t, cat, 0, geometry.KindLedgers, geometry.KindEvents, geometry.KindTxHash)
	makeReadyHotDirNoData(t, cat, 0)
	// A live chunk 1 above it so chunk 0 is below the partition boundary.
	require.NoError(t, cat.PutHotTransient(1))

	lastChunk := chunk.ID(0) // chunk 0 complete via cold
	// txhash is frozen, ledgers/events frozen, but the window has no FROZEN coverage
	// yet => indexCovers(0) is false => NOT discarded (still needed for lookups via
	// its .bin/hot DB until the index folds it in).
	ops, err := eligibleDiscardOps(cat, floorFor(t, cfg, lastChunk.LastLedger()), lastChunk)
	require.NoError(t, err)
	require.Empty(t, ops, "no index coverage yet: the hot DB stays")

	// Now finalize the window's index so it covers chunk 0 (terminal needs chunk
	// 1's .bin too; build a non-terminal-but-covering frozen coverage [0,0]).
	freezeCoverage(t, cat, 0, 0, 0)
	covered, err := cat.FrozenIndexCovers(0)
	require.NoError(t, err)
	require.True(t, covered)

	ops, err = eligibleDiscardOps(cat, floorFor(t, cfg, lastChunk.LastLedger()), lastChunk)
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
	cfg := lifecycleTestConfig(t, cat, 2) // retain ~2 chunks

	// CompleteThrough will be chunk 5's last ledger (positional: live chunk 6).
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

	through := chunk.ID(5).LastLedger()
	floor := floorFor(t, cfg, through)
	require.Equal(t, chunk.ID(4), floor, "floor anchors 2 chunks back")

	// Chunk 5 is the last complete chunk — the boundary id ingestion hands over.
	require.NoError(t, runLifecycle(context.Background(), cfg, cat, 5), "prune tick never fails")

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
	cfg := lifecycleTestConfig(t, cat, 0)

	// A crashed build left a "freezing" coverage key (no commit).
	_, err := cat.MarkTxHashIndexFreezing(0, 0, 0)
	require.NoError(t, err)

	// Nothing durable and no hot keys ⇒ through sits at the pre-genesis sentinel.
	ops, weights, err := eligiblePruneOps(cat, floorFor(t, cfg, geometry.PreGenesisLedger))
	require.NoError(t, err)
	require.Len(t, ops, 1, "the freezing debris is swept")
	require.Equal(t, []int{1}, weights, "one index artifact swept")
	require.NoError(t, ops[0]())

	covs, err := cat.AllTxHashIndexKeys()
	require.NoError(t, err)
	require.Empty(t, covs, "the freezing index key is gone")
}

// ---------------------------------------------------------------------------
// ERROR PLUMBING: a failing tick RETURNS its error (no Fatalf / os.Exit).
// supervise — not the tick — classifies ctx-cancel-is-clean vs restart (tested at
// the daemon level: TestRunDaemon_LoadValidateWireStartCleanShutdown, TestSupervise_*).
// ---------------------------------------------------------------------------

// TestRunLifecycleTick_FailureReturnsError: when a plan op fails, runLifecycle
// returns the wrapped error rather than aborting the process — so Loop can
// propagate it up through the errgroup to supervise. The chunk-0 build is
// GENUINELY unproducible: chunk 0 sits below a READY live chunk 1 (so it counts as
// complete and the plan range [0,0] is non-empty), has no frozen artifacts, and
// its hot key is "transient" (not a ready read source). With no bulk Backend
// configured, backfillSource has no source for chunk 0 and RunBackfill fails;
// MaxRetries defaults to 0, so it fails fast.
func TestRunLifecycleTick_FailureReturnsError(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 1)
	cfg := lifecycleTestConfig(t, cat, 0)      // hot tier read by path, no Backend
	makeReadyHotDirNoData(t, cat, 1)           // ready live chunk => through = chunk 0 last ledger
	require.NoError(t, cat.PutHotTransient(0)) // chunk 0 below live, no frozen artifacts, not a ready source

	err := runLifecycle(context.Background(), cfg, cat, 0) // plan range [0,0], the failing build
	require.Error(t, err, "a genuine op failure surfaces up the call stack")
}
