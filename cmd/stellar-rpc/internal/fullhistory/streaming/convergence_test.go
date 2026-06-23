package streaming

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// =============================================================================
// Crash-injection + convergence suite — the design's strongest validation
// (design-docs/full-history-streaming-workflow.md "Convergence", "Scenario
// coverage", "What a bug looks like").
//
// Each case (1) CONSTRUCTS a durable crash / partial-completion state on a real
// Catalog + real hotchunk DB + temp artifact dirs — by driving the REAL protocol
// ops (MarkChunkFreezing, SurgicalRecovery, the hot-tier open/ingest) to a chunk
// boundary and then STOPPING before the next op runs, and/or by directly
// planting the durable keys+files a crash at that instant would leave. (2) runs
// the REAL convergence path — a lifecycle tick (runLifecycleTick) and/or a
// re-derivation (deriveCompleteThrough / deriveWatermark). (3) ASSERTS the
// system converges to quiescence satisfying INV-2..4 by calling the REAL
// Catalog.Audit and requiring report.Clean(), PLUS idempotency (re-running the
// convergence op changes nothing) and that the derived watermark equals the
// durable state.
//
// The point of using the real ops + real audit (rather than hand-rolled
// assertions) is the design's "None of the invariants reference the phase
// scans": a bug in freeze / discard / prune / sweep surfaces here as a genuine
// Audit violation, not something the same code that produced it judges
// acceptable.
//
// CAVEAT — INV-1's deep byte-compare (audit_test.go's DeepDeriver) is NOT wired
// here — this suite asserts INV-1 only structurally (no orphan/dangling/
// duplicate, single canonical state); content re-derivation is audit_test.go's
// job.
// =============================================================================

// convergenceHarness bundles the catalog, its lifecycle config (real production
// primitives — a real RocksHotProbe over the catalog's hot layout), a fatal
// recorder, and a probe so a case can run real ticks and derivations.
type convergenceHarness struct {
	cat   *Catalog
	cfg   LifecycleConfig
	rec   *fatalRecorder
	probe HotProbe
}

// newConvergenceHarness builds a harness over a catalog with the genesis
// earliest_ledger pin and the given retention width.
//
//nolint:unparam // retentionChunks varies across slices' convergence tests
func newConvergenceHarness(t *testing.T, retentionChunks uint32) *convergenceHarness {
	t.Helper()
	cat, _ := testCatalog(t)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))
	cfg, rec := lifecycleTestConfig(t, cat, retentionChunks)
	return &convergenceHarness{
		cat:   cat,
		cfg:   cfg,
		rec:   rec,
		probe: cfg.Process.HotProbe,
	}
}

// tick runs one real lifecycle tick — driven the way ingestion would, with the
// highest complete chunk derived from the catalog as lastChunk — and asserts it
// did not abort the daemon.
func (h *convergenceHarness) tick(t *testing.T) {
	t.Helper()
	runTickForCatalog(context.Background(), t, h.cfg, h.cat)
	require.False(t, h.rec.fired(), "convergence tick must not abort the daemon: %v", h.rec.last.Load())
}

// auditClean runs the REAL audit and requires zero violations. retentionChunks
// matches the harness so INV-4 checks against the EXACT floor the daemon
// enforces.
func (h *convergenceHarness) auditClean(t *testing.T) AuditReport {
	t.Helper()
	report, err := h.cat.Audit(AuditOptions{RetentionChunks: h.cfg.RetentionChunks})
	require.NoError(t, err, "audit must complete (error only for I/O)")
	require.True(t, report.Clean(),
		"after convergence the store must satisfy INV-2..4; violations:\n%s", violationsString(report))
	return report
}

// requireQuiescent asserts re-running the tick's three derivations schedules no
// further work (idempotency: convergence reached a fixed point).
func (h *convergenceHarness) requireQuiescent(t *testing.T) {
	t.Helper()
	through, err := deriveCompleteThrough(h.cat)
	require.NoError(t, err)
	assertQuiescent(t, h.cfg, h.cat, through)
}

// requireWatermarkMatchesDurable asserts the derived watermark equals the
// expected durable frontier — the design's "the startup derivation equals
// exactly the durable state".
func (h *convergenceHarness) requireWatermarkMatchesDurable(t *testing.T, want uint32) {
	t.Helper()
	got, err := deriveWatermark(h.cat, h.probe)
	require.NoError(t, err, "watermark derivation must succeed at quiescence")
	require.Equal(t, want, got, "derived watermark must equal the durable frontier")
}

func violationsString(r AuditReport) string {
	s := ""
	var sSb111 strings.Builder
	for _, v := range r.Violations {
		sSb111.WriteString("  - " + v.String() + "\n")
	}
	s += sSb111.String()
	if s == "" {
		return "  (none)"
	}
	return s
}

// =============================================================================
// Per-chunk artifact crash states (freezing / pruning) — the "freezing" tail
// is re-materialized by the freeze stage from its still-present hot DB
// (processChunk's hot branch, the design's "freeze from a live hot DB"); the
// "pruning" demoted artifact is swept by the prune scan.
// =============================================================================

// TestConvergence_PerChunkFreezingReMaterializesFromHotDB constructs the
// per-chunk "freezing" crash state WITHIN retention (a crashed freeze that
// marked the key but did not finish): chunk 0's ledgers are "freezing" with a
// complete hot DB still behind the chunk. The freeze stage re-derives the cold
// artifact FROM that hot DB (backfillSource's hot branch), then discards the
// now-redundant hot DB — converging to a clean, quiescent store satisfying
// INV-2..4.
func TestConvergence_PerChunkFreezingReMaterializesFromHotDB(t *testing.T) {
	// full-chunk ingest; isolated TempDir/catalog — overlaps the other heavy
	// tests to fit the gate's go-test timeout.
	t.Parallel()
	h := newConvergenceHarness(t, 0) // a chunk finalizes at chunk 0

	// Chunk 0: a COMPLETE hot DB on disk (every ledger ingested, write handle
	// closed — the just-closed-chunk shape). This is the source the freeze stage
	// re-materializes from.
	ingestFullHotChunk(t, h.cat, 0)
	// The live chunk 1 above the partition (held open by "ingestion").
	live := openLiveHotDB(t, h.cat, 1)
	t.Cleanup(func() { _ = live.Close() })

	// Now plant the crash: chunk 0's cold artifact marked "freezing" (a crashed
	// freeze that pre-marked but did not fsync+flip). Mark via the REAL protocol.
	require.NoError(t, h.cat.MarkChunkFreezing(0, KindLedgers))
	require.Equal(t, StateFreezing, mustState(t, h.cat, 0, KindLedgers))

	// Converge: one real tick. The freeze stage's resolver sees the non-frozen
	// key, re-materializes chunk 0 from its hot DB, and the discard stage retires
	// the hot DB.
	h.tick(t)
	h.auditClean(t)
	h.requireQuiescent(t)

	// The chunk is now frozen and its hot DB discarded.
	require.Equal(t, StateFrozen, mustState(t, h.cat, 0, KindLedgers))
	has, err := h.cat.Has(hotChunkKey(0))
	require.NoError(t, err)
	require.False(t, has, "chunk 0's hot DB was discarded after the freeze")

	// Idempotency.
	before := snapshotAllKeys(t, h.cat)
	h.tick(t)
	require.Equal(t, before, snapshotAllKeys(t, h.cat), "second tick is a no-op")
	h.auditClean(t)
}

// TestConvergence_PerChunkPruningArtifactSwept constructs the per-chunk
// "pruning" crash state: a recovery-demoted ledger artifact whose sweep did not
// run, sitting in-retention. The prune scan sweeps it (file + key), converging
// to INV-2..4 clean.
func TestConvergence_PerChunkPruningArtifactSwept(t *testing.T) {
	h := newConvergenceHarness(t, 0)

	// A live chunk 1 above the partition so chunk 0 is below it and complete.
	require.NoError(t, h.cat.PutHotTransient(1))

	// The crash leftover: a chunk:0:ledgers key demoted to "pruning" with its pack
	// file still on disk (a demotion whose sweep did not unlink).
	writeArtifact(t, h.cat.layout.LedgerPackPath(0))
	require.NoError(t, h.cat.store.Put(chunkKey(0, KindLedgers), string(StatePruning)))

	// Before convergence the audit FAILS (a "pruning" key surviving quiescence is
	// an INV-2 violation) — proving the suite catches the bug class.
	pre, err := h.cat.Audit(AuditOptions{RetentionChunks: h.cfg.RetentionChunks})
	require.NoError(t, err)
	require.False(t, pre.Clean(), "the unswept pruning artifact must be a detectable violation pre-convergence")

	// Converge: the prune scan sweeps the "pruning" ref.
	h.tick(t)
	h.auditClean(t)
	h.requireQuiescent(t)

	require.Equal(t, State(""), mustState(t, h.cat, 0, KindLedgers), "the pruning key is swept")
	require.NoFileExists(t, h.cat.layout.LedgerPackPath(0), "the pruning file is unlinked")

	before := snapshotAllKeys(t, h.cat)
	h.tick(t)
	require.Equal(t, before, snapshotAllKeys(t, h.cat))
	h.auditClean(t)
}

// =============================================================================
// Boundary crash — recovered by the watermark refinement. A crash at a chunk
// boundary can leave the just-completed chunk's hot key "ready" and C+1's hot
// key "transient". deriveWatermark's ONE read of the highest *ready* chunk
// recovers the chunk-level frontier the "transient" key no longer advertises.
// =============================================================================

// TestConvergence_BoundaryCrashWatermarkRefinement plants the boundary-crash
// durable state the design's progress.go describes: chunk 0's hot DB complete
// and "ready" (the just-completed chunk), chunk 1's hot key "transient" (the next
// bracket's key was written — close-before-create-key — but the crash hit before
// it became "ready", so its completion no key now advertises). The POSITIONAL
// term under-counts here (highest *ready* is chunk 0, so positional = -1); the
// design's recovery is deriveWatermark's ONE MaxCommittedSeq read of the highest
// ready chunk, which supplies chunk 0's frontier. We assert that refinement, then
// that ingestion resuming (chunk 1 becomes "ready") lets a tick converge.
func TestConvergence_BoundaryCrashWatermarkRefinement(t *testing.T) {
	// full-chunk ingest; isolated TempDir/catalog — overlaps the other heavy
	// tests to fit the gate's go-test timeout.
	t.Parallel()
	h := newConvergenceHarness(t, 0)

	// Chunk 0: a complete, "ready" hot DB (every ledger committed). Chunk 1:
	// "transient" only (the next bracket opened its key but crashed before "ready").
	ingestFullHotChunk(t, h.cat, 0) // closes the write handle, leaves key "ready" + full dir
	require.Equal(t, HotReady, mustHotState(t, h.cat, 0))
	require.NoError(t, h.cat.PutHotTransient(1))
	require.Equal(t, HotTransient, mustHotState(t, h.cat, 1))

	// completeThrough alone under-counts (positional term sees no ready chunk above
	// chunk 0): it lands at the genesis sentinel.
	through, err := deriveCompleteThrough(h.cat)
	require.NoError(t, err)
	require.Equal(t, preGenesisLedger, through, "completeThrough under-counts at a boundary crash")

	// The WATERMARK refinement recovers the real frontier: deriveWatermark's one
	// MaxCommittedSeq read of the highest ready chunk (chunk 0) yields chunk 0's
	// last committed seq — the design's boundary-crash recovery.
	h.requireWatermarkMatchesDurable(t, chunk.ID(0).LastLedger())

	// Pre-resume the store is already INV-2..4 clean (chunk 0's hot DB is the live
	// tier from the lifecycle's view; nothing is orphaned or dangling).
	h.auditClean(t)

	// Ingestion resumes: chunk 1's bracket completes ("ready"), moving the partition
	// above chunk 0. Now a tick freezes chunk 0 from its ready hot DB and discards
	// the hot DB — converging to INV-2..4 clean and quiescent.
	live := openLiveHotDB(t, h.cat, 1)
	t.Cleanup(func() { _ = live.Close() })
	h.tick(t)
	h.auditClean(t)
	h.requireQuiescent(t)
	require.Equal(t, StateFrozen, mustState(t, h.cat, 0, KindLedgers))
}

// =============================================================================
// Surgical recovery (case 3, tainted cold data) — the operator demotes the
// tainted range to "freezing"/"transient" (one atomic batch), then the next
// startup converges: backfill re-derives the "freezing" cold artifacts from the
// surviving hot DB (or the bulk backend in production). We drive the demotion
// through the REAL SurgicalRecovery and the re-derivation through a REAL tick.
// =============================================================================

// TestConvergence_SurgicalRecoveryCase3ReDerives ties case 3 end to end on real
// state: a fully-converged chunk 0 (frozen cold) is tainted by a cold+hot
// surgical recovery (cold -> "freezing"); the next tick re-derives the cold
// artifact from a re-ingested hot DB, returning to INV-2..4 clean.
func TestConvergence_SurgicalRecoveryCase3ReDerives(t *testing.T) {
	// full-chunk ingest; isolated TempDir/catalog — overlaps the other heavy
	// tests to fit the gate's go-test timeout.
	t.Parallel()
	h := newConvergenceHarness(t, 0)

	// Converged steady state for chunk 0: frozen cold artifact, served PURELY by
	// cold (no hot DB — the hot tier was already discarded in steady state). A live
	// chunk 1 sits above the partition.
	live := openLiveHotDB(t, h.cat, 1)
	t.Cleanup(func() { _ = live.Close() })
	freezeChunkArtifacts(t, h.cat, 0, KindLedgers)
	h.auditClean(t) // sanity: the pre-recovery state is already clean and quiescent

	// Operator runs the case-3 recovery over chunk 0 (cold + hot). The present cold
	// key (ledgers) drops to "freezing" — one atomic batch. There is no hot key for
	// chunk 0 to demote (it was discarded in steady state), so the recovery's hot
	// tier is a no-op for this chunk; the cold demotion is what regresses it.
	plan, err := h.cat.SurgicalRecovery(RecoveryRequest{Lo: 0, Hi: 0, Tier: RecoverColdAndHot})
	require.NoError(t, err)
	require.False(t, plan.Empty())
	require.Equal(t, StateFreezing, mustState(t, h.cat, 0, KindLedgers))

	// Re-ingestion refills the chunk's hot tail (the design's "captive core
	// re-ingests the un-frozen tail forward" / "openHotDB wipes and recreates one
	// when re-ingestion re-opens that chunk") — the local source the freeze stage
	// re-derives the cold artifact from (production uses the bulk backend).
	ingestFullHotChunk(t, h.cat, 0)
	require.Equal(t, HotReady, mustHotState(t, h.cat, 0))

	// Converge: the tick re-materializes chunk 0's cold artifact, then discards the
	// hot DB. Back to INV-2..4 clean and quiescent.
	h.tick(t)
	h.auditClean(t)
	h.requireQuiescent(t)
	require.Equal(t, StateFrozen, mustState(t, h.cat, 0, KindLedgers))

	before := snapshotAllKeys(t, h.cat)
	h.tick(t)
	require.Equal(t, before, snapshotAllKeys(t, h.cat))
	h.auditClean(t)
}

// =============================================================================
// Hot-volume loss (case 4) — a "ready" hot key whose dir is gone is FATAL
// (ErrHotVolumeLost), never silently healed; the operator demotes it hot-only
// to "transient", the fatal stops, the watermark falls to the last frozen
// boundary, and re-ingestion fills forward. We assert BOTH halves.
// =============================================================================

// TestConvergence_HotVolumeLossCase4 plants the case-4 state (cold survives,
// hot dir gone), asserts the fatal fires, runs the REAL hot-only recovery, then
// asserts the watermark heals to the last frozen boundary, a re-ingested hot DB
// converges, and the audit is clean.
func TestConvergence_HotVolumeLossCase4(t *testing.T) {
	h := newConvergenceHarness(t, 0)

	// Durable cold history through chunk 0 (survives on durable storage): frozen
	// ledgers. Chunk 0's last ledger is the last frozen boundary the watermark must
	// heal to.
	freezeChunkArtifacts(t, h.cat, 0, KindLedgers)

	// The lost live chunk 1: "ready" with its hot dir GONE (the ephemeral volume
	// died while the meta store survived).
	live := chunk.ID(1)
	require.NoError(t, h.cat.PutHotTransient(live))
	require.NoError(t, h.cat.FlipHotReady(live))
	require.NoError(t, os.RemoveAll(h.cat.layout.HotChunkPath(live)))

	// Half 1: the fatal fires (ready key + missing dir = ErrHotVolumeLost). It is
	// NOT silently healed — derivation REFUSES rather than guessing.
	_, err := deriveWatermark(h.cat, h.probe)
	require.ErrorIs(t, err, ErrHotVolumeLost,
		"a ready hot key with a missing dir must fatal as ErrHotVolumeLost")

	// Half 2: the operator runs the case-4 (hot-only) recovery over the orphaned
	// chunk. The hot key -> "transient"; the fatal stops firing.
	_, err = h.cat.SurgicalRecovery(RecoveryRequest{Lo: live, Hi: live, Tier: RecoverHotOnly})
	require.NoError(t, err)
	require.Equal(t, HotTransient, mustHotState(t, h.cat, live))

	// The watermark heals to chunk 0's last ledger — the last frozen boundary; no
	// "ready" key with a missing dir remains.
	h.requireWatermarkMatchesDurable(t, chunk.ID(0).LastLedger())

	// Re-ingestion opens a fresh hot DB for the lost chunk and fills it forward.
	db := openLiveHotDB(t, h.cat, live)
	committed := live.FirstLedger() + 3
	require.NoError(t, db.Ledgers().AddLedgers(ledger.Entry{Seq: committed, Bytes: []byte("refill")}))
	require.NoError(t, db.Close())

	// The watermark now reflects the re-ingested frontier. The convergence value of
	// this case lives in the two halves above — the ErrHotVolumeLost fatal and the
	// watermark healing to the last frozen boundary — NOT in the tick: the cold
	// history survived intact and the re-ingested chunk is the new live tier, so
	// nothing is dirty for the tick to repair.
	h.requireWatermarkMatchesDurable(t, committed)
	h.auditClean(t) // already clean BEFORE the tick — the recovery left nothing dirty
	before := snapshotAllKeys(t, h.cat)
	h.tick(t)
	require.Equal(t, before, snapshotAllKeys(t, h.cat),
		"case 4's post-reingest tick is a no-op: nothing below the live chunk is tainted")
	h.auditClean(t)
	h.requireQuiescent(t)
}

// =============================================================================
// Retention widen / shorten — the floor recomputes; convergence prunes below a
// raised floor (shorten) and the next tick is a no-op once below-floor data is
// gone.
// =============================================================================

// TestConvergence_RetentionShortenPrunesBelowRaisedFloor seeds several finalized
// chunks, then SHORTENS retention so a higher floor leaves the lowest chunks
// wholly below it. One tick prunes them (keys + files + hot DBs) and the store
// converges to INV-2..4 clean against the NEW (shorter) retention.
func TestConvergence_RetentionShortenPrunesBelowRaisedFloor(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	// Six finalized chunks (0..5) with real files, plus a live chunk 6.
	for c := chunk.ID(0); c <= 5; c++ {
		freezeChunkArtifacts(t, cat, c, KindLedgers)
		writeArtifact(t, cat.layout.LedgerPackPath(c))
	}
	makeReadyHotDirNoData(t, cat, 1) // a below-floor hot DB too
	live := openLiveHotDB(t, cat, 6)
	t.Cleanup(func() { _ = live.Close() })

	// Shorten retention to 2 chunks. through = chunk 5's last ledger, so floor =
	// lastCompleteChunkAt(through)-2+1 = chunk 4's first ledger; chunks 0..3 fall
	// wholly below it and must be pruned.
	cfg, rec := lifecycleTestConfig(t, cat, 2)
	h := &convergenceHarness{cat: cat, cfg: cfg, rec: rec, probe: cfg.Process.HotProbe}

	h.tick(t)
	h.auditClean(t)
	h.requireQuiescent(t)

	for c := chunk.ID(0); c <= 3; c++ {
		require.Equal(t, State(""), mustState(t, cat, c, KindLedgers), "chunk %s pruned below the raised floor", c)
		require.NoFileExists(t, cat.layout.LedgerPackPath(c), "chunk %s pack pruned", c)
		has, herr := cat.Has(hotChunkKey(c))
		require.NoError(t, herr)
		require.False(t, has, "chunk %s hot key pruned", c)
	}
	for c := chunk.ID(4); c <= 5; c++ {
		require.Equal(t, StateFrozen, mustState(t, cat, c, KindLedgers), "chunk %s in retention survives", c)
	}

	before := snapshotAllKeys(t, cat)
	h.tick(t)
	require.Equal(t, before, snapshotAllKeys(t, cat))
	h.auditClean(t)
}

// TestConvergence_RetentionWidenIsTickNoOpAuditClean asserts the widen-side
// claim from the tick's perspective: a lowered floor does NOT make the tick
// prune (it never does) NOR materialize new bottom storage (that is backfill's
// job). The tick over already-converged storage with a wider retention window is
// a clean no-op, and the store stays INV-2..4 clean — the bottom-extension is
// deferred to the next backfill, not the tick.
func TestConvergence_RetentionWidenIsTickNoOpAuditClean(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	// Chunks 3..5 finalized (the existing bottom of storage is chunk 3), live 6.
	for c := chunk.ID(3); c <= 5; c++ {
		freezeChunkArtifacts(t, cat, c, KindLedgers)
		writeArtifact(t, cat.layout.LedgerPackPath(c))
	}
	live := openLiveHotDB(t, cat, 6)
	t.Cleanup(func() { _ = live.Close() })

	// A WIDE retention (100 chunks) lowers the floor below chunk 3, but the tick's
	// production range is raised to lowestMaterializedChunk (chunk 3): it must NOT
	// try to materialize chunks 0..2 (no source) and must NOT prune anything.
	cfg, rec := lifecycleTestConfig(t, cat, 100)
	h := &convergenceHarness{cat: cat, cfg: cfg, rec: rec, probe: cfg.Process.HotProbe}

	before := snapshotAllKeys(t, cat)
	h.tick(t)
	require.False(t, rec.fired(), "widening must not fail the tick (no source for the new bottom): %v", rec.last.Load())
	require.Equal(t, before, snapshotAllKeys(t, cat),
		"the tick neither prunes nor materializes on a widen — that is backfill's job")
	h.auditClean(t)
	h.requireQuiescent(t)
}

// =============================================================================
// Young network — no complete chunk exists yet. The tick produces nothing (the
// freeze stage's range is empty), and the empty store trivially satisfies
// INV-2..4. The convergence here is "no spurious work, no fatal".
// =============================================================================

// TestConvergence_YoungNetworkNoOp seeds a network younger than one complete
// chunk: only a live (transient/ready) hot chunk 0, no frozen artifacts, no
// complete chunk below the live one. A tick must do nothing and the audit must
// be clean.
func TestConvergence_YoungNetworkNoOp(t *testing.T) {
	h := newConvergenceHarness(t, 0)

	// A live chunk 0's hot DB, mid-ingest (a few ledgers, not the whole chunk), so
	// nothing below it is complete and no chunk has frozen.
	db := openLiveHotDB(t, h.cat, 0)
	require.NoError(t, db.Ledgers().AddLedgers(ledger.Entry{Seq: chunk.ID(0).FirstLedger() + 2, Bytes: []byte("young")}))
	t.Cleanup(func() { _ = db.Close() })

	// completeThrough is the genesis sentinel (no frozen, the only ready chunk is
	// the live one whose predecessor is below genesis), so the freeze range is
	// empty and the tick is a pure no-op.
	through, err := deriveCompleteThrough(h.cat)
	require.NoError(t, err)
	require.Equal(t, preGenesisLedger, through, "no complete chunk exists on a young network")

	before := snapshotAllKeys(t, h.cat)
	h.tick(t)
	require.Equal(t, before, snapshotAllKeys(t, h.cat), "a young-network tick is a no-op")
	h.auditClean(t)
	h.requireQuiescent(t)
}
