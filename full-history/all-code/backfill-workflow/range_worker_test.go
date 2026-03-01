package backfill

import (
	"context"
	"os"
	"testing"

	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
)

func TestRangeWorkerNewRange(t *testing.T) {
	// Fresh range — should run ingestion + RecSplit for all chunks.
	// Use 1 instance for simplicity.
	ledgersDir := t.TempDir()
	txhashDir := t.TempDir()
	meta := NewMockMetaStore()
	log := NewTestLogger("TEST")

	worker := NewRangeWorker(RangeWorkerConfig{
		RangeID:       0,
		NumInstances:  1, // Single instance processes all 1000 chunks
		LedgersBase:   ledgersDir,
		TxHashBase:    txhashDir,
		FlushInterval: 100,
		Meta:          meta,
		Memory:        NewNopMemoryMonitor(1.0),
		Factory:       newMockLedgerSourceFactory(),
		Logger:        log,
		Tracker:       NewProgressTracker(int(helpers.ChunksPerRange)),
	})

	stats, err := worker.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// All 1000 chunks should be processed
	if stats.ChunksCompleted != int(helpers.ChunksPerRange) {
		t.Errorf("ChunksCompleted = %d, want %d", stats.ChunksCompleted, helpers.ChunksPerRange)
	}

	// Range should end in COMPLETE state
	state, _ := meta.GetRangeState(0)
	if state != RangeStateComplete {
		t.Errorf("range state = %q, want %q", state, RangeStateComplete)
	}
}

func TestRangeWorkerAlreadyComplete(t *testing.T) {
	// Range already complete — should return immediately.
	meta := NewMockMetaStore()
	meta.SetRangeState(0, RangeStateComplete)
	log := NewTestLogger("TEST")

	worker := NewRangeWorker(RangeWorkerConfig{
		RangeID:       0,
		NumInstances:  1,
		LedgersBase:   t.TempDir(),
		TxHashBase:    t.TempDir(),
		FlushInterval: 100,
		Meta:          meta,
		Memory:        NewNopMemoryMonitor(1.0),
		Factory:       newMockLedgerSourceFactory(),
		Logger:        log,
		Tracker:       NewProgressTracker(0),
	})

	stats, err := worker.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Nothing should have been processed
	if stats.ChunksCompleted != 0 {
		t.Errorf("ChunksCompleted = %d, want 0", stats.ChunksCompleted)
	}
}

func TestRangeWorkerResumeIngestion(t *testing.T) {
	// Range partially ingested — should resume with skip-set.
	ledgersDir := t.TempDir()
	txhashDir := t.TempDir()
	meta := NewMockMetaStore()
	log := NewTestLogger("TEST")

	// Mark first 500 chunks as done and create their .bin files on disk.
	// In a real scenario these .bin files would exist from the previous
	// ingestion run. RecSplit reads ALL 1000 .bin files, so they must exist.
	meta.SetRangeState(0, RangeStateIngesting)
	rawDir := RawTxHashDir(txhashDir, 0)
	os.MkdirAll(rawDir, 0755)
	for c := uint32(0); c < 500; c++ {
		meta.SetChunkComplete(0, c)
		// Create empty .bin file (valid: 0 entries)
		os.WriteFile(RawTxHashPath(txhashDir, 0, c), []byte{}, 0644)
	}

	worker := NewRangeWorker(RangeWorkerConfig{
		RangeID:       0,
		NumInstances:  1,
		LedgersBase:   ledgersDir,
		TxHashBase:    txhashDir,
		FlushInterval: 100,
		Meta:          meta,
		Memory:        NewNopMemoryMonitor(1.0),
		Factory:       newMockLedgerSourceFactory(),
		Logger:        log,
		Tracker:       NewProgressTracker(int(helpers.ChunksPerRange)),
	})

	stats, err := worker.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 500 chunks processed (the other 500 were skipped)
	if stats.ChunksCompleted != 500 {
		t.Errorf("ChunksCompleted = %d, want 500", stats.ChunksCompleted)
	}

	// Range should end in COMPLETE state (ingestion + RecSplit)
	state, _ := meta.GetRangeState(0)
	if state != RangeStateComplete {
		t.Errorf("range state = %q, want %q", state, RangeStateComplete)
	}
}
