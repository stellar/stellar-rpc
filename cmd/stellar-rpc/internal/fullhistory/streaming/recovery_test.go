package streaming

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
)

// ---------------------------------------------------------------------------
// Surgical recovery test helpers.
// ---------------------------------------------------------------------------

// mustState reads a per-chunk artifact key's State, asserting no error.
//
//nolint:unparam // kind varies in later slices (events/txhash)
func mustState(t *testing.T, cat *Catalog, c chunk.ID, kind Kind) State {
	t.Helper()
	s, err := cat.State(c, kind)
	require.NoError(t, err)
	return s
}

// mustHotState reads a hot:chunk key's HotState, asserting no error.
func mustHotState(t *testing.T, cat *Catalog, c chunk.ID) HotState {
	t.Helper()
	s, err := cat.HotState(c)
	require.NoError(t, err)
	return s
}

// ---------------------------------------------------------------------------
// The demotion batch: atomic, idempotent, scoped to the range, never creating
// absent keys.
// ---------------------------------------------------------------------------

func TestSurgicalRecovery_DemotesColdAndHot(t *testing.T) {
	cat, _ := testCatalog(t)

	// In-range frozen cold artifacts on chunks 5 and 6.
	freezeKinds(t, cat, 5, KindLedgers)
	freezeKinds(t, cat, 6, KindLedgers)
	// In-range ready hot DBs on chunks 5 and 6 (the live chunk 6 included).
	readyHot(t, cat, 5)
	readyHot(t, cat, 6)

	// Out-of-range keys that MUST stay untouched.
	freezeKinds(t, cat, 9, KindLedgers)
	readyHot(t, cat, 9)

	plan, err := cat.SurgicalRecovery(RecoveryRequest{Lo: 5, Hi: 6, Tier: RecoverColdAndHot})
	require.NoError(t, err)
	require.False(t, plan.Empty())

	// Cold artifacts in range -> "freezing".
	require.Equal(t, StateFreezing, mustState(t, cat, 5, KindLedgers))
	require.Equal(t, StateFreezing, mustState(t, cat, 6, KindLedgers))

	// Hot DBs in range -> "transient" (the live chunk's included).
	require.Equal(t, HotTransient, mustHotState(t, cat, 5))
	require.Equal(t, HotTransient, mustHotState(t, cat, 6))

	// Out-of-range keys untouched.
	require.Equal(t, StateFrozen, mustState(t, cat, 9, KindLedgers))
	require.Equal(t, HotReady, mustHotState(t, cat, 9))
}

func TestSurgicalRecovery_Idempotent_ReRunIsNoOp(t *testing.T) {
	cat, _ := testCatalog(t)

	freezeKinds(t, cat, 2, KindLedgers)
	readyHot(t, cat, 2)
	readyHot(t, cat, 3)

	req := RecoveryRequest{Lo: 2, Hi: 3, Tier: RecoverColdAndHot}

	first, err := cat.SurgicalRecovery(req)
	require.NoError(t, err)

	// Capture the full key snapshot after the first apply.
	before := snapshotAllKeys(t, cat)

	// Re-run the EXACT same recovery — a no-op: every demote re-writes the same
	// value, so the snapshot is byte-identical.
	second, err := cat.SurgicalRecovery(req)
	require.NoError(t, err)
	after := snapshotAllKeys(t, cat)

	require.Equal(t, before, after, "re-running surgical recovery must be a no-op")
	require.Len(t, second.ColdKeys, len(first.ColdKeys))
	require.Len(t, second.HotKeys, len(first.HotKeys))
}

// TestSurgicalRecovery_BatchIsAtomic proves ApplySurgicalRecovery commits its
// cold/hot demotions in ONE all-or-nothing batch — the core property the
// design's "commits atomically or not at all" / "no interruption analysis"
// claim rests on. We fault-inject a failure INSIDE the batch callback (which
// makes metastore drop the whole batch) and assert the FULL key snapshot is
// byte-identical before and after: not a single demotion leaked. Rewriting
// ApplySurgicalRecovery as separate non-atomic per-key Puts would leave some
// demotions durable here and fail this test.
func TestSurgicalRecovery_BatchIsAtomic(t *testing.T) {
	cat, _ := testCatalog(t)

	// A fixture spanning both demotion families: frozen cold artifacts and ready
	// hot DBs (the live chunk's included) — so a partial-commit impl would leak at
	// least one of them.
	freezeKinds(t, cat, 5, KindLedgers)
	freezeKinds(t, cat, 6, KindLedgers)
	readyHot(t, cat, 5)
	readyHot(t, cat, 6)

	req := RecoveryRequest{Lo: 5, Hi: 6, Tier: RecoverColdAndHot}

	// The plan is composed against durable state first; planning does not mutate.
	plan, err := PlanSurgicalRecovery(cat, req)
	require.NoError(t, err)
	require.False(t, plan.Empty())
	require.NotEmpty(t, plan.ColdKeys)
	require.NotEmpty(t, plan.HotKeys)

	before := snapshotAllKeys(t, cat)

	// Fail the batch from inside its callback: metastore drops the whole batch.
	cat.hooks.failCommitBatch = func() bool { return true }
	err = cat.ApplySurgicalRecovery(plan)
	require.Error(t, err, "ApplySurgicalRecovery must surface the injected batch failure")
	cat.hooks.failCommitBatch = nil

	// All-or-nothing: the failed batch wrote NOTHING — every cold/hot key is
	// still exactly as seeded.
	after := snapshotAllKeys(t, cat)
	require.Equal(t, before, after,
		"a dropped recovery batch must leave every demotion key unchanged (atomicity)")

	// And a clean re-apply (no fault) lands the whole batch.
	require.NoError(t, cat.ApplySurgicalRecovery(plan))
	require.Equal(t, StateFreezing, mustState(t, cat, 5, KindLedgers))
	require.Equal(t, StateFreezing, mustState(t, cat, 6, KindLedgers))
	require.Equal(t, HotTransient, mustHotState(t, cat, 5))
	require.Equal(t, HotTransient, mustHotState(t, cat, 6))
}

// snapshotAllKeys returns a map of every meta-store key to its value, for
// no-op / atomicity assertions. It walks the chunk + hot key families.
func snapshotAllKeys(t *testing.T, cat *Catalog) map[string]string {
	t.Helper()
	m := map[string]string{}
	refs, err := cat.ChunkArtifactKeys()
	require.NoError(t, err)
	for _, r := range refs {
		m[r.Key()] = string(r.State)
	}
	hots, err := cat.HotChunkKeys()
	require.NoError(t, err)
	for _, id := range hots {
		m[hotChunkKey(id)] = string(mustHotState(t, cat, id))
	}
	return m
}

func TestSurgicalRecovery_HotOnly_LeavesColdUntouched(t *testing.T) {
	cat, _ := testCatalog(t)

	// The case-4 fixture: cold artifacts survive on durable storage; only the
	// hot DBs are lost. A hot-only recovery must NOT touch any cold key.
	freezeKinds(t, cat, 5, KindLedgers)
	readyHot(t, cat, 5)
	readyHot(t, cat, 6)

	plan, err := cat.SurgicalRecovery(RecoveryRequest{Lo: 5, Hi: 6, Tier: RecoverHotOnly})
	require.NoError(t, err)

	require.Empty(t, plan.ColdKeys, "hot-only recovery must not list cold keys")
	require.Len(t, plan.HotKeys, 2)

	// Cold keys are exactly as seeded.
	require.Equal(t, StateFrozen, mustState(t, cat, 5, KindLedgers))

	// Only the hot keys were demoted.
	require.Equal(t, HotTransient, mustHotState(t, cat, 5))
	require.Equal(t, HotTransient, mustHotState(t, cat, 6))
}

func TestSurgicalRecovery_NeverCreatesAbsentKeys(t *testing.T) {
	cat, _ := testCatalog(t)

	// Seed only chunk 5; recover a DISJOINT range [20, 25] that matches nothing.
	freezeKinds(t, cat, 5, KindLedgers)
	readyHot(t, cat, 5)

	plan, err := cat.SurgicalRecovery(RecoveryRequest{Lo: 20, Hi: 25, Tier: RecoverColdAndHot})
	require.NoError(t, err)
	require.True(t, plan.Empty(), "a range matching no keys yields an empty plan")

	// No key was conjured for any chunk in [20, 25].
	for c := chunk.ID(20); c <= 25; c++ {
		require.Equal(t, State(""), mustState(t, cat, c, KindLedgers))
		require.Equal(t, HotState(""), mustHotState(t, cat, c))
	}
	// The seeded chunk is untouched.
	require.Equal(t, StateFrozen, mustState(t, cat, 5, KindLedgers))
	require.Equal(t, HotReady, mustHotState(t, cat, 5))
}

func TestSurgicalRecovery_RangeValidation(t *testing.T) {
	cat, _ := testCatalog(t)
	_, err := cat.SurgicalRecovery(RecoveryRequest{Lo: 7, Hi: 3, Tier: RecoverColdAndHot})
	require.Error(t, err)
	require.Contains(t, err.Error(), "lo")
}

// TestSurgicalRecovery_ColdBoundary proves the cold-key range predicate is
// inclusive at both endpoints and excludes strictly-out-of-range chunks.
func TestSurgicalRecovery_ColdBoundary(t *testing.T) {
	cat, _ := testCatalog(t)

	// Frozen cold artifacts at the range edges and just outside [10, 20].
	for _, c := range []chunk.ID{9, 10, 20, 21} {
		freezeKinds(t, cat, c, KindLedgers)
	}

	plan, err := PlanSurgicalRecovery(cat, RecoveryRequest{Lo: 10, Hi: 20, Tier: RecoverColdAndHot})
	require.NoError(t, err)

	selected := map[string]bool{}
	for _, ref := range plan.ColdKeys {
		selected[ref.Key()] = true
	}
	require.True(t, selected[chunkKey(10, KindLedgers)], "chunk 10 is the low edge (inclusive)")
	require.True(t, selected[chunkKey(20, KindLedgers)], "chunk 20 is the high edge (inclusive)")
	require.False(t, selected[chunkKey(9, KindLedgers)], "chunk 9 is below the range")
	require.False(t, selected[chunkKey(21, KindLedgers)], "chunk 21 is above the range")
}

// ---------------------------------------------------------------------------
// Self-correcting watermark. Demoting hot keys regresses deriveWatermark to the
// last frozen boundary; demoting strictly below the live chunk leaves it
// unchanged. No manual rewind.
// ---------------------------------------------------------------------------

// TestSurgicalRecovery_SelfCorrectingWatermark_RegressesToLastFrozenBoundary
// is the design's case-3/4 claim made concrete: a demotion reaching the live
// chunk rewinds the derived watermark to the last frozen boundary, with NO
// stored pointer to edit.
func TestSurgicalRecovery_SelfCorrectingWatermark_RegressesToLastFrozenBoundary(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq)) // genesis floor

	// Cold history: chunks 0..2 fully durable (frozen). Last frozen boundary is
	// chunk 2's last ledger.
	makeChunkDurable(t, cat, 0)
	makeChunkDurable(t, cat, 1)
	makeChunkDurable(t, cat, 2)

	// Live chunk 3: a real hot DB committed mid-chunk. The watermark must reflect
	// this committed frontier BEFORE recovery.
	live := chunk.ID(3)
	db := openLiveHotDB(t, cat, live)
	committed := live.FirstLedger() + 4321
	require.NoError(t, db.Ledgers().AddLedgers(ledger.Entry{Seq: committed, Bytes: []byte("live")}))
	require.NoError(t, db.Close())

	probe := NewRocksHotProbe(cat.layout.HotChunkPath, silentLogger())
	before, err := deriveWatermark(cat, probe)
	require.NoError(t, err)
	require.Equal(t, committed, before, "watermark reflects the live DB's committed frontier")

	// Recovery reaches the live chunk (range [3, 3]): its hot key -> "transient".
	// The hot dir is left in place; demotion is pure key surgery.
	_, err = cat.SurgicalRecovery(RecoveryRequest{Lo: live, Hi: live, Tier: RecoverColdAndHot})
	require.NoError(t, err)

	// deriveWatermark now ignores the demoted (no-longer-"ready") live key and
	// lands at chunk 2's last ledger — the last frozen boundary. No rewind edit.
	after, err := deriveWatermark(cat, probe)
	require.NoError(t, err)
	require.Equal(t, chunk.ID(2).LastLedger(), after,
		"demoting the live hot key regresses the watermark to the last frozen boundary")
	require.Less(t, after, before, "the watermark strictly regressed")
}

// TestSurgicalRecovery_DemotionBelowLiveLeavesWatermarkUnchanged proves the
// other half of the uniformity claim: a demotion strictly BELOW the live chunk
// leaves the watermark put — those chunks are not the highest "ready" key, and
// the live chunk's "ready" DB still pins the bound.
func TestSurgicalRecovery_DemotionBelowLiveLeavesWatermarkUnchanged(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	makeChunkDurable(t, cat, 0)
	makeChunkDurable(t, cat, 1)

	// Two ready hot chunks: a lower one (2) and the live one (5) with a real DB.
	readyHot(t, cat, 2)
	live := chunk.ID(5)
	db := openLiveHotDB(t, cat, live)
	committed := live.FirstLedger() + 100
	require.NoError(t, db.Ledgers().AddLedgers(ledger.Entry{Seq: committed, Bytes: []byte("live")}))
	require.NoError(t, db.Close())

	probe := NewRocksHotProbe(cat.layout.HotChunkPath, silentLogger())
	before, err := deriveWatermark(cat, probe)
	require.NoError(t, err)
	require.Equal(t, committed, before)

	// Demote ONLY the lower hot chunk 2 (strictly below the live chunk 5).
	_, err = cat.SurgicalRecovery(RecoveryRequest{Lo: 2, Hi: 2, Tier: RecoverHotOnly})
	require.NoError(t, err)
	require.Equal(t, HotTransient, mustHotState(t, cat, 2))

	after, err := deriveWatermark(cat, probe)
	require.NoError(t, err)
	require.Equal(t, before, after,
		"demoting a hot key strictly below the live chunk leaves the watermark unchanged")
}

// TestSurgicalRecovery_CatchupReDerivesFreezingColdArtifacts proves the cold
// half heals through existing machinery: a chunk whose artifacts were demoted to
// "freezing" is no longer counted durable by highestDurableChunk — which is
// exactly the signal that makes backfill's per-chunk resolver re-materialize it
// (rule 1, overwriting in place). We assert the durable-chunk frontier regresses
// past the demoted chunk.
func TestSurgicalRecovery_CatchupReDerivesFreezingColdArtifacts(t *testing.T) {
	cat, _ := testCatalog(t)

	// Chunks 0..3 durable; the durable frontier is 3.
	for c := chunk.ID(0); c <= 3; c++ {
		makeChunkDurable(t, cat, c)
	}
	frontier, err := highestDurableChunk(cat)
	require.NoError(t, err)
	require.Equal(t, int64(3), frontier)

	// Taint chunks 2..3 (cold only). Their artifacts drop to "freezing".
	_, err = cat.SurgicalRecovery(RecoveryRequest{Lo: 2, Hi: 3, Tier: RecoverColdAndHot})
	require.NoError(t, err)
	require.Equal(t, StateFreezing, mustState(t, cat, 2, KindLedgers))
	require.Equal(t, StateFreezing, mustState(t, cat, 3, KindLedgers))

	// The durable frontier regresses to chunk 1 — chunks 2 and 3 are now
	// re-derivable "freezing" debris, not durable truth. Catch-up's resolver will
	// schedule their re-materialization; we assert the watermark/frontier input
	// that drives it.
	frontier, err = highestDurableChunk(cat)
	require.NoError(t, err)
	require.Equal(t, int64(1), frontier,
		"demoting cold artifacts to freezing regresses the durable-chunk frontier")
}

// ---------------------------------------------------------------------------
// Hot-volume-loss detection (case 4) — the fatal already exists; verify it.
// ---------------------------------------------------------------------------

// TestHotVolumeLoss_DeriveWatermarkFatalOnReadyKeyMissingDir is the case-4
// fatal: a "ready" hot key whose dir is gone is hot-volume loss, surfaced as
// ErrHotVolumeLost — never silently healed.
func TestHotVolumeLoss_DeriveWatermarkFatalOnReadyKeyMissingDir(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	// A ready hot key WITHOUT its dir (the lost-volume shape: meta survived, the
	// ephemeral hot tree did not). readyHot creates the dir; do it by hand and
	// then remove the dir to simulate loss.
	live := chunk.ID(4)
	require.NoError(t, cat.PutHotTransient(live))
	require.NoError(t, cat.FlipHotReady(live))
	require.NoError(t, os.RemoveAll(cat.layout.HotChunkPath(live)))

	probe := NewRocksHotProbe(cat.layout.HotChunkPath, silentLogger())
	_, err := deriveWatermark(cat, probe)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHotVolumeLost,
		"a ready hot key with a missing dir must fatal as ErrHotVolumeLost")
}

// TestHotVolumeLoss_OpenHotTierFatalOnReadyKeyMissingDir is the same fatal at
// the OTHER detection site — openHotTierForChunk, which a later open would hit
// if derivation somehow didn't.
func TestHotVolumeLoss_OpenHotTierFatalOnReadyKeyMissingDir(t *testing.T) {
	cat, _ := testCatalog(t)

	live := chunk.ID(4)
	require.NoError(t, cat.PutHotTransient(live))
	require.NoError(t, cat.FlipHotReady(live))
	require.NoError(t, os.RemoveAll(cat.layout.HotChunkPath(live)))

	_, err := openHotTierForChunk(cat, live, silentLogger())
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHotVolumeLost,
		"opening a ready hot key with a missing dir must fatal as ErrHotVolumeLost")
}

// TestHotVolumeLoss_RecoveryThenWatermarkHealsForward ties case 4 end to end:
// the operator demotes the orphaned hot key (hot-only), the fatal stops firing
// (it checks "ready" keys), and the watermark falls to the last frozen boundary
// for re-ingestion to fill forward.
func TestHotVolumeLoss_RecoveryThenWatermarkHealsForward(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	// Durable cold history through chunk 2 (survives on durable storage).
	for c := chunk.ID(0); c <= 2; c++ {
		makeChunkDurable(t, cat, c)
	}

	// Orphaned live hot key: "ready" with a missing dir (the lost NVMe).
	live := chunk.ID(3)
	require.NoError(t, cat.PutHotTransient(live))
	require.NoError(t, cat.FlipHotReady(live))
	require.NoError(t, os.RemoveAll(cat.layout.HotChunkPath(live)))

	probe := NewRocksHotProbe(cat.layout.HotChunkPath, silentLogger())

	// Before recovery: the fatal fires.
	_, err := deriveWatermark(cat, probe)
	require.ErrorIs(t, err, ErrHotVolumeLost)

	// Operator runs the case-4 (hot-only) recovery over the orphaned chunk.
	_, err = cat.SurgicalRecovery(RecoveryRequest{Lo: live, Hi: live, Tier: RecoverHotOnly})
	require.NoError(t, err)
	require.Equal(t, HotTransient, mustHotState(t, cat, live))

	// After recovery: no "ready" key with a missing dir, so the fatal no longer
	// fires; the watermark falls to the last frozen boundary (chunk 2's last
	// ledger) for captive core to re-ingest the lost tail forward.
	after, err := deriveWatermark(cat, probe)
	require.NoError(t, err)
	require.Equal(t, chunk.ID(2).LastLedger(), after,
		"after hot-only recovery the watermark heals to the last frozen boundary")
}

// ---------------------------------------------------------------------------
// Operator entrypoint — RunSurgicalRecovery: stopped-daemon-only (flock) and
// the end-to-end open/demote/close happy path.
// ---------------------------------------------------------------------------

// recoveryConfig builds a Config rooted at a temp dir, enough for
// RunSurgicalRecovery (which only needs the data dir + cpi default).
func recoveryConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		Service:   ServiceConfig{DefaultDataDir: t.TempDir()},
		Streaming: StreamingConfig{EarliestLedger: "genesis"},
	}
}

func TestRunSurgicalRecovery_RefusesWhileDaemonRunning(t *testing.T) {
	cfg := recoveryConfig(t)
	paths := cfg.WithDefaults().ResolvePaths()

	// Hold one of the storage-root flocks (the hot tree — any root would do;
	// RunSurgicalRecovery takes them all) to stand in for ANOTHER process that
	// owns it. This proves the ErrRootLocked fail-fast fires whenever a root is
	// already held; it is the same guard a daemon will trip ONCE the daemon-side
	// LockRoots wiring lands (today the daemon does not take these flocks, so the
	// live-daemon guard is instead RocksDB's metastore single-writer LOCK — see
	// the STOPPED-DAEMON-ONLY note in recovery.go).
	held, err := LockRoots(paths.HotStorage)
	require.NoError(t, err)
	defer held.Release()

	_, err = RunSurgicalRecovery(cfg, RecoveryRequest{Lo: 1, Hi: 2, Tier: RecoverColdAndHot}, silentLogger(), nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRootLocked,
		"recovery against a running daemon must fail fast with ErrRootLocked")
}

func TestRunSurgicalRecovery_HappyPath_OpensDemotesCloses(t *testing.T) {
	cfg := recoveryConfig(t)
	paths := cfg.WithDefaults().ResolvePaths()

	// Seed durable state through a catalog on the SAME meta path the entrypoint
	// will reopen, then CLOSE it (RocksDB is single-writer; the entrypoint takes
	// the lock + reopens).
	seedStore, err := metastore.New(paths.Catalog, silentLogger())
	require.NoError(t, err)
	seedCat := NewCatalog(seedStore, NewLayout(paths.DataDir))
	freezeKinds(t, seedCat, 5, KindLedgers)
	require.NoError(t, seedCat.PutHotTransient(5))
	require.NoError(t, seedCat.FlipHotReady(5))
	require.NoError(t, seedStore.Close())

	// Run the entrypoint: it locks every root, reopens the store, commits the
	// demotion batch, and releases.
	plan, err := RunSurgicalRecovery(cfg,
		RecoveryRequest{Lo: 5, Hi: 5, Tier: RecoverColdAndHot}, silentLogger(), nil)
	require.NoError(t, err)
	require.False(t, plan.Empty())
	require.Len(t, plan.ColdKeys, 1)
	require.Len(t, plan.HotKeys, 1)

	// The entrypoint released its locks, so a fresh reopen sees the demotions.
	verifyStore, err := metastore.New(paths.Catalog, silentLogger())
	require.NoError(t, err)
	defer func() { _ = verifyStore.Close() }()
	verifyCat := NewCatalog(verifyStore, NewLayout(paths.DataDir))

	require.Equal(t, StateFreezing, mustState(t, verifyCat, 5, KindLedgers))
	require.Equal(t, HotTransient, mustHotState(t, verifyCat, 5))
}

func TestRunSurgicalRecovery_EmptyRangeReportsErrRecoveryEmptyRange(t *testing.T) {
	cfg := recoveryConfig(t)
	paths := cfg.WithDefaults().ResolvePaths()

	// Open and immediately close the store so the path exists but holds no keys.
	store, err := metastore.New(paths.Catalog, silentLogger())
	require.NoError(t, err)
	require.NoError(t, store.Close())

	plan, err := RunSurgicalRecovery(cfg,
		RecoveryRequest{Lo: 1, Hi: 9, Tier: RecoverColdAndHot}, silentLogger(), nil)
	require.ErrorIs(t, err, ErrRecoveryEmptyRange,
		"a range matching no keys reports ErrRecoveryEmptyRange")
	require.True(t, plan.Empty())

	// Sanity: lock files were created under each root (and released).
	_, statErr := os.Stat(filepath.Join(paths.HotStorage, lockFileName))
	require.NoError(t, statErr)
}
