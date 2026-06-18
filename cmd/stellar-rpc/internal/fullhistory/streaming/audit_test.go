package streaming

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
)

// testCatalogCPI is testCatalog with a caller-chosen chunks_per_txhash_index, so
// a test can build a SMALL window (e.g. cpi=2: window 0 = chunks {0,1}) and reach
// the "terminal/finalized window" branch without materializing 1000 chunks.
func testCatalogCPI(t *testing.T, cpi uint32) (*Catalog, string) {
	t.Helper()
	metaDir := t.TempDir()
	artifactRoot := t.TempDir()

	store, err := metastore.New(filepath.Join(metaDir, "rocksdb"), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	windows, err := NewWindows(cpi)
	require.NoError(t, err)
	return NewCatalog(store, NewLayout(artifactRoot), windows), artifactRoot
}

// freezeChunkArtifacts marks+writes+freezes every per-chunk artifact kind for a
// chunk (ledgers, events, txhash) and writes the real files, so the audit's INV-3
// disk<->meta walk sees a fully materialized chunk.
func freezeChunkArtifacts(t *testing.T, cat *Catalog, c chunk.ID, kinds ...Kind) {
	t.Helper()
	if len(kinds) == 0 {
		kinds = AllKinds()
	}
	require.NoError(t, cat.MarkChunkFreezing(c, kinds...))
	for _, kind := range kinds {
		for _, p := range cat.layout.ArtifactPaths(c, kind) {
			writeArtifact(t, p)
		}
	}
	require.NoError(t, cat.FlipChunkFrozen(c, kinds...))
}

// freezeIndex marks+writes+commits a frozen index coverage and writes its .idx.
func freezeIndex(t *testing.T, cat *Catalog, w WindowID, lo, hi chunk.ID) IndexCoverage {
	t.Helper()
	cov, err := cat.MarkIndexFreezing(w, lo, hi)
	require.NoError(t, err)
	writeArtifact(t, cat.layout.IndexFilePath(cov))
	require.NoError(t, cat.CommitIndex(cov))
	cov.State = StateFrozen
	return cov
}

// hasViolation reports whether the report contains a violation for inv whose key
// matches wantKey (empty wantKey matches any).
func hasViolation(r AuditReport, inv Invariant, wantKey string) bool {
	for _, v := range r.Violations {
		if v.Invariant != inv {
			continue
		}
		if wantKey == "" || v.Key == wantKey {
			return true
		}
	}
	return false
}

func countInvariant(r AuditReport, inv Invariant) int {
	n := 0
	for _, v := range r.Violations {
		if v.Invariant == inv {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// Clean store — a fully materialized, finalized, in-retention chunk set yields
// zero violations across every invariant.
// ---------------------------------------------------------------------------

func TestAudit_CleanStoreNoViolations(t *testing.T) {
	cat, _ := testCatalogCPI(t, 2) // window 0 = {0,1}, window 1 = {2,3}
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	// Window 0 finalized: chunks 0,1 frozen (ledgers+events), terminal index covers
	// {0,1}, so the .bin keys are demoted/swept (we never create them, matching a
	// finalized window). Use ledgers+events only — txhash is gone post-finalization.
	freezeChunkArtifacts(t, cat, 0, KindLedgers, KindEvents)
	freezeChunkArtifacts(t, cat, 1, KindLedgers, KindEvents)
	freezeIndex(t, cat, 0, 0, 1) // terminal: hi==1==LastChunk(window 0)

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err)
	require.True(t, report.Clean(), "expected clean audit, got: %v", report.Violations)
}

// ---------------------------------------------------------------------------
// INV-2 — single canonical state.
// ---------------------------------------------------------------------------

func TestAudit_INV2_TwoFrozenIndexKeysInOneWindow(t *testing.T) {
	cat, _ := testCatalogCPI(t, 4) // window 0 = {0,1,2,3}
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	// Two NON-terminal frozen coverages in window 0. CommitIndex demotes a
	// predecessor, so to force the forbidden co-existence we write the second
	// frozen key directly (simulating a commit batch that failed to demote).
	cov1 := freezeIndex(t, cat, 0, 0, 1)
	cov2, err := cat.MarkIndexFreezing(0, 0, 2)
	require.NoError(t, err)
	writeArtifact(t, cat.layout.IndexFilePath(cov2))
	require.NoError(t, cat.store.Put(cov2.Key, string(StateFrozen))) // bug: predecessor not demoted

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err)
	require.True(t, hasViolation(report, InvSingleCanonicalState, ""),
		"expected INV-2 two-frozen violation; cov1=%s cov2=%s", cov1.Key, cov2.Key)
}

// TestAudit_INV2_TwoFrozenKeysPlusHotPlusTxhashStillCompletes is the regression
// for the abort-on-duplicate bug: a window with TWO frozen index keys whose
// other clause-3 (orphan hot) and clause-4 (leftover txhash) inputs ALSO route
// through frozen-coverage resolution. Before the fix, clause 3 (pendingArtifacts
// -> indexCovers) and clause 4 (txhashRedundantInFinalizedWindow) called
// Catalog.FrozenCoverage, which ERRORS on two frozen keys; Audit returned a
// zero-value report (Clean()==true) plus an error, discarding the clause-1
// violation. After the fix the audit completes (err==nil) and records all three
// INV-2 breaches against the duplicate-tolerant frozen-coverage view.
func TestAudit_INV2_TwoFrozenKeysPlusHotPlusTxhashStillCompletes(t *testing.T) {
	cat, _ := testCatalogCPI(t, 2) // window 0 = {0,1}
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	// Window 0 finalized: chunks 0,1 frozen (ledgers+events) and a TERMINAL frozen
	// coverage [0,1] (hi==1==LastChunk(window 0)).
	freezeChunkArtifacts(t, cat, 0, KindLedgers, KindEvents)
	freezeChunkArtifacts(t, cat, 1, KindLedgers, KindEvents)
	freezeIndex(t, cat, 0, 0, 1)

	// Bug 1: a SECOND frozen coverage [0,0] in the same window (a commit batch that
	// failed to demote its predecessor) — clause-1 two-frozen violation.
	cov2, err := cat.MarkIndexFreezing(0, 0, 0)
	require.NoError(t, err)
	writeArtifact(t, cat.layout.IndexFilePath(cov2))
	require.NoError(t, cat.store.Put(cov2.Key, string(StateFrozen)))

	// Bug 2: a "ready" hot DB for the fully-served chunk 0 — clause-3 orphan-hot.
	readyHot(t, cat, 0)

	// Bug 3: a leftover per-chunk txhash key for chunk 0 in the finalized window —
	// clause-4 leftover-txhash.
	require.NoError(t, cat.MarkChunkFreezing(0, KindTxHash))
	writeArtifact(t, cat.layout.TxHashBinPath(0))
	require.NoError(t, cat.FlipChunkFrozen(0, KindTxHash))

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err, "audit must complete (err only for I/O), not abort on the uniqueness breach")
	require.False(t, report.Clean(), "a multiply-corrupted store must not report Clean")

	// All three INV-2 breaches must be present — clause 1 (two frozen), clause 3
	// (orphan hot), clause 4 (leftover txhash) — proving the full walk finished.
	require.True(t, hasViolation(report, InvSingleCanonicalState, hotChunkKey(0)),
		"expected clause-3 orphan-hot INV-2 violation: %v", report.Violations)
	require.True(t, hasViolation(report, InvSingleCanonicalState, chunkKey(0, KindTxHash)),
		"expected clause-4 leftover-txhash INV-2 violation: %v", report.Violations)
	require.GreaterOrEqual(t, countInvariant(report, InvSingleCanonicalState), 3,
		"expected at least 3 INV-2 violations (two-frozen + orphan-hot + leftover-txhash): %v",
		report.Violations)
}

func TestAudit_INV2_FreezingArtifactWithinRetentionIsViolation(t *testing.T) {
	cat, _ := testCatalogCPI(t, 1000)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	// A "freezing" ledgers key for chunk 0, and a fully-frozen chunk 5 so
	// completeThrough advances ABOVE chunk 0 (chunk 0 is within
	// [floor, completeThrough]). Re-materialization was skipped -> INV-2.
	freezeChunkArtifacts(t, cat, 5, KindLedgers, KindEvents, KindTxHash)
	require.NoError(t, cat.MarkChunkFreezing(0, KindLedgers))
	writeArtifact(t, cat.layout.LedgerPackPath(0))

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err)
	require.True(t, hasViolation(report, InvSingleCanonicalState, chunkKey(0, KindLedgers)),
		"expected INV-2 within-retention freezing violation: %v", report.Violations)
}

func TestAudit_INV2_FreezingArtifactAboveCompleteThroughIsTolerated(t *testing.T) {
	cat, root := testCatalogCPI(t, 1000)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	// No frozen chunks at all => completeThrough is pre-genesis. A "freezing" key
	// for chunk 3 lies ABOVE completeThrough — the tolerated hot-volume-loss tail.
	require.NoError(t, cat.MarkChunkFreezing(3, KindLedgers))
	writeArtifact(t, cat.layout.LedgerPackPath(3))

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err)
	require.False(t, hasViolation(report, InvSingleCanonicalState, chunkKey(3, KindLedgers)),
		"above-completeThrough freezing key must be tolerated: %v", report.Violations)
	_ = root
}

func TestAudit_INV2_PruningArtifactIsAlwaysViolation(t *testing.T) {
	cat, _ := testCatalogCPI(t, 1000)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	// A "pruning" key surviving quiescence — the sweep should have finished it.
	// No completeThrough carve-out applies to "pruning" (only "freezing").
	require.NoError(t, cat.MarkChunkFreezing(7, KindEvents))
	require.NoError(t, cat.store.Put(chunkKey(7, KindEvents), string(StatePruning)))

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err)
	require.True(t, hasViolation(report, InvSingleCanonicalState, chunkKey(7, KindEvents)),
		"expected INV-2 pruning violation: %v", report.Violations)
}

func TestAudit_INV2_OrphanHotForFullyServedChunk(t *testing.T) {
	cat, _ := testCatalogCPI(t, 2) // window 0 = {0,1}
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	// Chunk 0 fully served by cold artifacts (ledgers+events frozen, terminal index
	// covers it) yet a "ready" hot DB persists — the discard scan missed it.
	freezeChunkArtifacts(t, cat, 0, KindLedgers, KindEvents)
	freezeChunkArtifacts(t, cat, 1, KindLedgers, KindEvents)
	freezeIndex(t, cat, 0, 0, 1)
	readyHot(t, cat, 0)

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err)
	require.True(t, hasViolation(report, InvSingleCanonicalState, hotChunkKey(0)),
		"expected INV-2 orphan-hot violation: %v", report.Violations)
}

func TestAudit_INV2_TransientHotIsTolerated(t *testing.T) {
	cat, _ := testCatalogCPI(t, 2)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	freezeChunkArtifacts(t, cat, 0, KindLedgers, KindEvents)
	freezeChunkArtifacts(t, cat, 1, KindLedgers, KindEvents)
	freezeIndex(t, cat, 0, 0, 1)
	// A "transient" hot key for the same fully-served chunk is the tolerated
	// in-flight bracket — NOT an orphan, and its missing dir is NOT a dangling key.
	require.NoError(t, cat.PutHotTransient(0))

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err)
	require.False(t, hasViolation(report, InvSingleCanonicalState, hotChunkKey(0)),
		"transient hot key must be tolerated by INV-2: %v", report.Violations)
	require.False(t, hasViolation(report, InvDiskMatchesMeta, hotChunkKey(0)),
		"transient hot key with no dir must be tolerated by INV-3: %v", report.Violations)
}

func TestAudit_INV2_TxhashKeyInFinalizedWindow(t *testing.T) {
	cat, _ := testCatalogCPI(t, 2) // window 0 = {0,1}
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	freezeChunkArtifacts(t, cat, 0, KindLedgers, KindEvents)
	freezeChunkArtifacts(t, cat, 1, KindLedgers, KindEvents)
	freezeIndex(t, cat, 0, 0, 1) // terminal -> window finalized
	// A per-chunk txhash key left behind in the finalized window (finalization
	// demotion did not complete).
	require.NoError(t, cat.MarkChunkFreezing(0, KindTxHash))
	writeArtifact(t, cat.layout.TxHashBinPath(0))
	require.NoError(t, cat.FlipChunkFrozen(0, KindTxHash))

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err)
	require.True(t, hasViolation(report, InvSingleCanonicalState, chunkKey(0, KindTxHash)),
		"expected INV-2 leftover-txhash violation: %v", report.Violations)
}

// ---------------------------------------------------------------------------
// INV-3 — disk matches meta-store, both directions.
// ---------------------------------------------------------------------------

func TestAudit_INV3_OrphanFileNoKey(t *testing.T) {
	cat, _ := testCatalogCPI(t, 1000)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	// A file on disk at chunk 9's ledgers path with NO meta key — orphan.
	orphan := cat.layout.LedgerPackPath(9)
	writeArtifact(t, orphan)

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err)
	found := false
	for _, v := range report.Violations {
		if v.Invariant == InvDiskMatchesMeta && v.Path == orphan {
			found = true
		}
	}
	require.True(t, found, "expected INV-3 orphan-file violation for %s: %v", orphan, report.Violations)
}

func TestAudit_INV3_DuplicateArtifactIsOrphan(t *testing.T) {
	cat, _ := testCatalogCPI(t, 1000)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	// Chunk 0 events frozen (three legit files). A stray FOURTH events file the
	// meta store does not name is a duplicate -> orphan.
	freezeChunkArtifacts(t, cat, 0, KindEvents)
	dupe := filepath.Join(filepath.Dir(cat.layout.EventsPaths(0)[0]), "00000000-events.dupe")
	writeArtifact(t, dupe)

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err)
	found := false
	for _, v := range report.Violations {
		if v.Invariant == InvDiskMatchesMeta && v.Path == dupe {
			found = true
		}
	}
	require.True(t, found, "expected INV-3 duplicate-artifact orphan for %s: %v", dupe, report.Violations)
}

func TestAudit_INV3_DanglingKeyNoFile(t *testing.T) {
	cat, _ := testCatalogCPI(t, 1000)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	// A "frozen" ledgers key for chunk 2 but no file on disk — dangling key.
	require.NoError(t, cat.MarkChunkFreezing(2, KindLedgers))
	require.NoError(t, cat.FlipChunkFrozen(2, KindLedgers))

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err)
	require.True(t, hasViolation(report, InvDiskMatchesMeta, chunkKey(2, KindLedgers)),
		"expected INV-3 dangling-key violation: %v", report.Violations)
}

func TestAudit_INV3_PruningKeyNoFileIsTolerated(t *testing.T) {
	cat, _ := testCatalogCPI(t, 1000)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	// A "pruning" key whose file the sweep already unlinked (before deleting the
	// key) is the legitimate mid-sweep window, NOT a dangling key.
	require.NoError(t, cat.MarkChunkFreezing(2, KindLedgers))
	require.NoError(t, cat.store.Put(chunkKey(2, KindLedgers), string(StatePruning)))

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err)
	require.False(t, hasViolation(report, InvDiskMatchesMeta, chunkKey(2, KindLedgers)),
		"pruning key with no file must NOT be an INV-3 dangling key: %v", report.Violations)
}

func TestAudit_INV3_OrphanHotDir(t *testing.T) {
	cat, _ := testCatalogCPI(t, 1000)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	// A hot DB directory on disk for chunk 4 with no hot:chunk key — orphan tier.
	require.NoError(t, os.MkdirAll(cat.layout.HotChunkPath(4), 0o755))

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err)
	found := false
	for _, v := range report.Violations {
		if v.Invariant == InvDiskMatchesMeta && v.Path == cat.layout.HotChunkPath(4) {
			found = true
		}
	}
	require.True(t, found, "expected INV-3 orphan-hot-dir violation: %v", report.Violations)
}

// ---------------------------------------------------------------------------
// INV-4 — retention bound.
// ---------------------------------------------------------------------------

func TestAudit_INV4_ChunkBelowFloor(t *testing.T) {
	cat, _ := testCatalogCPI(t, 1000)
	// Pin earliest_ledger to chunk 5's first ledger -> floor is chunk 5's first
	// ledger, so chunk 0..4 are wholly below the floor.
	require.NoError(t, cat.PutEarliestLedger(chunk.ID(5).FirstLedger()))

	// A frozen chunk 1 below the floor (its files exist so INV-3 is clean) — but
	// it's below floor, so INV-4 fires.
	freezeChunkArtifacts(t, cat, 1, KindLedgers, KindEvents, KindTxHash)

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err)
	require.True(t, hasViolation(report, InvRetentionBound, chunkKey(1, KindLedgers)),
		"expected INV-4 below-floor violation: %v", report.Violations)
}

func TestAudit_INV4_StraddlingFloorNotFlagged(t *testing.T) {
	cat, _ := testCatalogCPI(t, 1000)
	// earliest at chunk 0 first ledger + 1 (mid chunk 0). floor =
	// effectiveRetentionFloor with earliest just above genesis; chunk 0's last
	// ledger is ABOVE that, so chunk 0 straddles and must NOT be flagged.
	require.NoError(t, cat.PutEarliestLedger(chunk.ID(0).FirstLedger()+1))
	freezeChunkArtifacts(t, cat, 0, KindLedgers, KindEvents, KindTxHash)

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err)
	require.Equal(t, 0, countInvariant(report, InvRetentionBound),
		"a chunk straddling the floor must not be an INV-4 violation: %v", report.Violations)
}

// ---------------------------------------------------------------------------
// INV-1 — deep mode.
// ---------------------------------------------------------------------------

type fakeDeriver struct {
	bytesFor map[string][]byte // keyed by chunkKey(c, kind)
	declined map[string]bool
	err      error
}

func (f *fakeDeriver) DeriveArtifact(c chunk.ID, kind Kind) ([]byte, bool, error) {
	if f.err != nil {
		return nil, false, f.err
	}
	k := chunkKey(c, kind)
	if f.declined[k] {
		return nil, false, nil
	}
	b, ok := f.bytesFor[k]
	return b, ok, nil
}

func TestAudit_INV1_DeepByteMatchClean(t *testing.T) {
	cat, _ := testCatalogCPI(t, 1000)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))
	freezeChunkArtifacts(t, cat, 0, KindLedgers)
	// writeArtifact writes "artifact"; deriver returns the same bytes -> match.
	dv := &fakeDeriver{bytesFor: map[string][]byte{chunkKey(0, KindLedgers): []byte("artifact")}}

	report, err := cat.Audit(AuditOptions{Deep: dv})
	require.NoError(t, err)
	require.Equal(t, 0, countInvariant(report, InvReadCorrectness), "%v", report.Violations)
	require.Equal(t, 1, report.DeepChecked)
}

func TestAudit_INV1_DeepByteMismatch(t *testing.T) {
	cat, _ := testCatalogCPI(t, 1000)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))
	freezeChunkArtifacts(t, cat, 0, KindLedgers)
	dv := &fakeDeriver{bytesFor: map[string][]byte{chunkKey(0, KindLedgers): []byte("DIFFERENT")}}

	report, err := cat.Audit(AuditOptions{Deep: dv})
	require.NoError(t, err)
	require.True(t, hasViolation(report, InvReadCorrectness, chunkKey(0, KindLedgers)),
		"expected INV-1 byte-mismatch violation: %v", report.Violations)
}

func TestAudit_INV1_DeclinedSampleNotChecked(t *testing.T) {
	cat, _ := testCatalogCPI(t, 1000)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))
	freezeChunkArtifacts(t, cat, 0, KindLedgers)
	dv := &fakeDeriver{declined: map[string]bool{chunkKey(0, KindLedgers): true}}

	report, err := cat.Audit(AuditOptions{Deep: dv})
	require.NoError(t, err)
	require.Equal(t, 0, report.DeepChecked)
	require.Equal(t, 0, countInvariant(report, InvReadCorrectness))
}

func TestAudit_INV1_DeriverErrorSurfaces(t *testing.T) {
	cat, _ := testCatalogCPI(t, 1000)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))
	freezeChunkArtifacts(t, cat, 0, KindLedgers)
	dv := &fakeDeriver{err: errors.New("backend down")}

	_, err := cat.Audit(AuditOptions{Deep: dv})
	require.Error(t, err)
	require.Contains(t, err.Error(), "backend down")
}

func TestAudit_INV1_NoDeriverSkipsDeep(t *testing.T) {
	cat, _ := testCatalogCPI(t, 1000)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))
	freezeChunkArtifacts(t, cat, 0, KindLedgers)

	report, err := cat.Audit(AuditOptions{}) // no Deep
	require.NoError(t, err)
	require.Equal(t, 0, report.DeepChecked)
}
