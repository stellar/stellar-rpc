package backfill

import (
	"os"
	"testing"

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
	mock.SetIndexTxHash(0)
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

func TestReconcilerOrphanAbort(t *testing.T) {
	mock := NewMockMetaStore()
	// Two indexes not in config
	mock.SetIndexTxHash(5)
	mock.SetIndexTxHash(6)
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
	mock.SetIndexTxHash(5) // Single orphan
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
