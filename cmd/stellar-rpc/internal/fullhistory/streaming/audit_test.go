package streaming

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// freezeChunkArtifacts marks+writes+freezes every per-chunk artifact kind for a
// chunk (currently ledgers) and writes the real files, so the audit's INV-3
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
// Clean store — a fully materialized, in-retention chunk set yields zero
// violations across every invariant.
// ---------------------------------------------------------------------------

func TestAudit_CleanStoreNoViolations(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	freezeChunkArtifacts(t, cat, 0, KindLedgers, KindEvents)
	freezeChunkArtifacts(t, cat, 1, KindLedgers, KindEvents)

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err)
	require.True(t, report.Clean(), "expected clean audit, got: %v", report.Violations)
}

// TestAudit_INV3_OrphanEventsFileNoKey confirms the INV-3 disk->meta walk now
// covers the events tree: a stray events cold-segment file with no meta key is
// flagged as an orphan.
func TestAudit_INV3_OrphanEventsFileNoKey(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	// An events file on disk at chunk 9's events path with NO meta key — orphan.
	orphan := cat.layout.EventsPaths(9)[0]
	writeArtifact(t, orphan)

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err)
	require.True(t, hasViolation(report, InvDiskMatchesMeta, ""),
		"a keyless events file must be flagged as an orphan: %v", report.Violations)
}

// ---------------------------------------------------------------------------
// INV-2 — single canonical state.
// ---------------------------------------------------------------------------

func TestAudit_INV2_FreezingArtifactWithinRetentionIsViolation(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	// A "freezing" ledgers key for chunk 0, and a fully-frozen chunk 5 so
	// completeThrough advances ABOVE chunk 0 (chunk 0 is within
	// [floor, completeThrough]). Re-materialization was skipped -> INV-2.
	freezeChunkArtifacts(t, cat, 5, KindLedgers)
	require.NoError(t, cat.MarkChunkFreezing(0, KindLedgers))
	writeArtifact(t, cat.layout.LedgerPackPath(0))

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err)
	require.True(t, hasViolation(report, InvSingleCanonicalState, chunkKey(0, KindLedgers)),
		"expected INV-2 within-retention freezing violation: %v", report.Violations)
}

func TestAudit_INV2_FreezingArtifactAboveCompleteThroughIsTolerated(t *testing.T) {
	cat, root := testCatalog(t)
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
	cat, _ := testCatalog(t)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	// A "pruning" key surviving quiescence — the sweep should have finished it.
	// No completeThrough carve-out applies to "pruning" (only "freezing").
	require.NoError(t, cat.MarkChunkFreezing(7, KindLedgers))
	require.NoError(t, cat.store.Put(chunkKey(7, KindLedgers), string(StatePruning)))

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err)
	require.True(t, hasViolation(report, InvSingleCanonicalState, chunkKey(7, KindLedgers)),
		"expected INV-2 pruning violation: %v", report.Violations)
}

func TestAudit_INV2_OrphanHotForFullyServedChunk(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	// Chunk 0 fully served by cold artifacts (ledgers + events frozen) yet a
	// "ready" hot DB persists — the discard scan missed it.
	freezeChunkArtifacts(t, cat, 0, KindLedgers, KindEvents)
	freezeChunkArtifacts(t, cat, 1, KindLedgers, KindEvents)
	readyHot(t, cat, 0)

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err)
	require.True(t, hasViolation(report, InvSingleCanonicalState, hotChunkKey(0)),
		"expected INV-2 orphan-hot violation: %v", report.Violations)
}

func TestAudit_INV2_TransientHotIsTolerated(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	freezeChunkArtifacts(t, cat, 0, KindLedgers)
	freezeChunkArtifacts(t, cat, 1, KindLedgers)
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

// ---------------------------------------------------------------------------
// INV-3 — disk matches meta-store, both directions.
// ---------------------------------------------------------------------------

func TestAudit_INV3_OrphanFileNoKey(t *testing.T) {
	cat, _ := testCatalog(t)
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
	cat, _ := testCatalog(t)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))

	// Chunk 0 ledgers frozen (one legit .pack). A stray SECOND file the meta store
	// does not name (in the same bucket dir) is a duplicate -> orphan.
	freezeChunkArtifacts(t, cat, 0, KindLedgers)
	dupe := filepath.Join(filepath.Dir(cat.layout.LedgerPackPath(0)), "00000000.dupe")
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
	cat, _ := testCatalog(t)
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
	cat, _ := testCatalog(t)
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
	cat, _ := testCatalog(t)
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
	cat, _ := testCatalog(t)
	// Pin earliest_ledger to chunk 5's first ledger -> floor is chunk 5's first
	// ledger, so chunk 0..4 are wholly below the floor.
	require.NoError(t, cat.PutEarliestLedger(chunk.ID(5).FirstLedger()))

	// A frozen chunk 1 below the floor (its files exist so INV-3 is clean) — but
	// it's below floor, so INV-4 fires.
	freezeChunkArtifacts(t, cat, 1, KindLedgers)

	report, err := cat.Audit(AuditOptions{})
	require.NoError(t, err)
	require.True(t, hasViolation(report, InvRetentionBound, chunkKey(1, KindLedgers)),
		"expected INV-4 below-floor violation: %v", report.Violations)
}

func TestAudit_INV4_StraddlingFloorNotFlagged(t *testing.T) {
	cat, _ := testCatalog(t)
	// earliest at chunk 0 first ledger + 1 (mid chunk 0). floor =
	// effectiveRetentionFloor with earliest just above genesis; chunk 0's last
	// ledger is ABOVE that, so chunk 0 straddles and must NOT be flagged.
	require.NoError(t, cat.PutEarliestLedger(chunk.ID(0).FirstLedger()+1))
	freezeChunkArtifacts(t, cat, 0, KindLedgers)

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
	cat, _ := testCatalog(t)
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
	cat, _ := testCatalog(t)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))
	freezeChunkArtifacts(t, cat, 0, KindLedgers)
	dv := &fakeDeriver{bytesFor: map[string][]byte{chunkKey(0, KindLedgers): []byte("DIFFERENT")}}

	report, err := cat.Audit(AuditOptions{Deep: dv})
	require.NoError(t, err)
	require.True(t, hasViolation(report, InvReadCorrectness, chunkKey(0, KindLedgers)),
		"expected INV-1 byte-mismatch violation: %v", report.Violations)
}

func TestAudit_INV1_DeclinedSampleNotChecked(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))
	freezeChunkArtifacts(t, cat, 0, KindLedgers)
	dv := &fakeDeriver{declined: map[string]bool{chunkKey(0, KindLedgers): true}}

	report, err := cat.Audit(AuditOptions{Deep: dv})
	require.NoError(t, err)
	require.Equal(t, 0, report.DeepChecked)
	require.Equal(t, 0, countInvariant(report, InvReadCorrectness))
}

func TestAudit_INV1_DeriverErrorSurfaces(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))
	freezeChunkArtifacts(t, cat, 0, KindLedgers)
	dv := &fakeDeriver{err: errors.New("backend down")}

	_, err := cat.Audit(AuditOptions{Deep: dv})
	require.Error(t, err)
	require.Contains(t, err.Error(), "backend down")
}

func TestAudit_INV1_NoDeriverSkipsDeep(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))
	freezeChunkArtifacts(t, cat, 0, KindLedgers)

	report, err := cat.Audit(AuditOptions{}) // no Deep
	require.NoError(t, err)
	require.Equal(t, 0, report.DeepChecked)
}
