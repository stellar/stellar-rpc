package backfill

import (
	"os"
	"testing"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/cf"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/fsutil"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/logging"
)

func TestReconcilerFreshStart(t *testing.T) {
	mock := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	r := NewReconciler(ReconcilerConfig{
		Meta:             mock,
		Logger:           log,
		TxHashBase:       t.TempDir(),
		ConfiguredRanges: map[uint32]bool{0: true},
	})

	if err := r.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestReconcilerCompleteWithLeftoverRaw(t *testing.T) {
	dir := t.TempDir()
	mock := NewMockMetaStore()
	mock.SetRangeState(0, RangeStateComplete)
	log := logging.NewTestLogger("TEST")

	// Create leftover raw/ directory
	rawDir := RawTxHashDir(dir, 0)
	os.MkdirAll(rawDir, 0755)
	os.WriteFile(RawTxHashPath(dir, 0, 0), []byte("data"), 0644)

	r := NewReconciler(ReconcilerConfig{
		Meta:             mock,
		Logger:           log,
		TxHashBase:       dir,
		ConfiguredRanges: map[uint32]bool{0: true},
	})

	if err := r.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// raw/ should be deleted
	if fsutil.IsDir(rawDir) {
		t.Error("raw/ directory should have been deleted")
	}
}

func TestReconcilerRecSplitMissingRaw(t *testing.T) {
	dir := t.TempDir()
	mock := NewMockMetaStore()
	mock.SetRangeState(0, RangeStateRecSplitBuilding)
	log := logging.NewTestLogger("TEST")

	// Do NOT create raw/ directory — this is the fatal case

	r := NewReconciler(ReconcilerConfig{
		Meta:             mock,
		Logger:           log,
		TxHashBase:       dir,
		ConfiguredRanges: map[uint32]bool{0: true},
	})

	err := r.Run()
	if err == nil {
		t.Fatal("expected error for missing raw/ directory")
	}
}

func TestReconcilerRecSplitDeletesPartialIdx(t *testing.T) {
	dir := t.TempDir()
	mock := NewMockMetaStore()
	mock.SetRangeState(0, RangeStateRecSplitBuilding)
	mock.SetRecSplitCFDone(0, 0) // CF 0 is done
	// CF 1 is NOT done
	log := logging.NewTestLogger("TEST")

	// Create raw/ directory (required)
	rawDir := RawTxHashDir(dir, 0)
	os.MkdirAll(rawDir, 0755)

	// Create index directory with partial .idx files
	indexDir := RecSplitIndexDir(dir, 0)
	os.MkdirAll(indexDir, 0755)
	// CF 0 has done flag → its .idx should NOT be deleted
	cf0Path := RecSplitIndexPath(dir, 0, "0")
	os.WriteFile(cf0Path, []byte("complete"), 0644)
	// CF 1 has NO done flag → its .idx SHOULD be deleted
	cf1Path := RecSplitIndexPath(dir, 0, "1")
	os.WriteFile(cf1Path, []byte("partial"), 0644)

	r := NewReconciler(ReconcilerConfig{
		Meta:             mock,
		Logger:           log,
		TxHashBase:       dir,
		ConfiguredRanges: map[uint32]bool{0: true},
	})

	if err := r.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// CF 0's .idx should still exist
	if !fsutil.FileExists(cf0Path) {
		t.Error("CF 0 .idx should NOT be deleted (has done flag)")
	}
	// CF 1's .idx should be deleted
	if fsutil.FileExists(cf1Path) {
		t.Error("CF 1 .idx should be deleted (no done flag)")
	}
}

func TestReconcilerOrphanAbort(t *testing.T) {
	mock := NewMockMetaStore()
	// Two ranges not in config
	mock.SetRangeState(5, RangeStateIngesting)
	mock.SetRangeState(6, RangeStateIngesting)
	log := logging.NewTestLogger("TEST")

	r := NewReconciler(ReconcilerConfig{
		Meta:             mock,
		Logger:           log,
		TxHashBase:       t.TempDir(),
		ConfiguredRanges: map[uint32]bool{0: true}, // Only range 0 configured
	})

	err := r.Run()
	if err == nil {
		t.Fatal("expected error for multiple orphans")
	}
}

func TestReconcilerSingleOrphanOK(t *testing.T) {
	mock := NewMockMetaStore()
	mock.SetRangeState(5, RangeStateIngesting) // Single orphan
	log := logging.NewTestLogger("TEST")

	r := NewReconciler(ReconcilerConfig{
		Meta:             mock,
		Logger:           log,
		TxHashBase:       t.TempDir(),
		ConfiguredRanges: map[uint32]bool{0: true},
	})

	// Single orphan should NOT abort
	if err := r.Run(); err != nil {
		t.Fatalf("Run should not fail for single orphan: %v", err)
	}
}

func TestReconcilerScenarioB4(t *testing.T) {
	dir := t.TempDir()
	mock := NewMockMetaStore()
	mock.SetRangeState(0, RangeStateRecSplitBuilding)
	// All 16 CFs done
	for i := 0; i < cf.Count; i++ {
		mock.SetRecSplitCFDone(0, i)
	}
	log := logging.NewTestLogger("TEST")

	// Create raw/ (will be cleaned up)
	rawDir := RawTxHashDir(dir, 0)
	os.MkdirAll(rawDir, 0755)

	r := NewReconciler(ReconcilerConfig{
		Meta:             mock,
		Logger:           log,
		TxHashBase:       dir,
		ConfiguredRanges: map[uint32]bool{0: true},
	})

	if err := r.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// State should be updated to COMPLETE
	state, _ := mock.GetRangeState(0)
	if state != RangeStateComplete {
		t.Errorf("state = %q, want %q", state, RangeStateComplete)
	}

	// raw/ should be deleted
	if fsutil.IsDir(rawDir) {
		t.Error("raw/ should be deleted after all CFs done")
	}
}
