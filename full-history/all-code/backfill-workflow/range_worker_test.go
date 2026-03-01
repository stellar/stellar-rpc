package backfill

import (
	"context"
	"os"
	"testing"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/geometry"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/memory"
)

func TestRangeWorkerNewRange(t *testing.T) {
	// Fresh range — should run ingestion + RecSplit for all chunks.
	// Use 1 instance for simplicity.
	geo := geometry.TestGeometry()
	ledgersDir := t.TempDir()
	txhashDir := t.TempDir()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	worker := NewRangeWorker(RangeWorkerConfig{
		RangeID:       0,
		NumInstances:  1, // Single instance processes all chunks
		LedgersBase:   ledgersDir,
		TxHashBase:    txhashDir,
		FlushInterval: 100,
		Meta:          meta,
		Memory:        memory.NewNopMonitor(1.0),
		Factory:       newMockLedgerSourceFactory(),
		Logger:        log,
		Tracker:       NewProgressTracker(int(geo.ChunksPerRange)),
		Geo:           geo,
	})

	stats, err := worker.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// All chunks should be processed
	if stats.ChunksCompleted != int(geo.ChunksPerRange) {
		t.Errorf("ChunksCompleted = %d, want %d", stats.ChunksCompleted, geo.ChunksPerRange)
	}

	// Range should end in COMPLETE state
	state, _ := meta.GetRangeState(0)
	if state != RangeStateComplete {
		t.Errorf("range state = %q, want %q", state, RangeStateComplete)
	}
}

func TestRangeWorkerAlreadyComplete(t *testing.T) {
	// Range already complete — should return immediately.
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()
	meta.SetRangeState(0, RangeStateComplete)
	log := logging.NewTestLogger("TEST")

	worker := NewRangeWorker(RangeWorkerConfig{
		RangeID:       0,
		NumInstances:  1,
		LedgersBase:   t.TempDir(),
		TxHashBase:    t.TempDir(),
		FlushInterval: 100,
		Meta:          meta,
		Memory:        memory.NewNopMonitor(1.0),
		Factory:       newMockLedgerSourceFactory(),
		Logger:        log,
		Tracker:       NewProgressTracker(0),
		Geo:           geo,
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
	geo := geometry.TestGeometry()
	ledgersDir := t.TempDir()
	txhashDir := t.TempDir()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	// Mark first half of chunks as done and create their .bin files on disk.
	// RecSplit reads ALL .bin files in the range, so they must exist.
	halfChunks := geo.ChunksPerRange / 2
	meta.SetRangeState(0, RangeStateIngesting)
	rawDir := RawTxHashDir(txhashDir, 0)
	os.MkdirAll(rawDir, 0755)
	for c := uint32(0); c < halfChunks; c++ {
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
		Memory:        memory.NewNopMonitor(1.0),
		Factory:       newMockLedgerSourceFactory(),
		Logger:        log,
		Tracker:       NewProgressTracker(int(geo.ChunksPerRange)),
		Geo:           geo,
	})

	stats, err := worker.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Half the chunks should be processed (the other half were skipped)
	if stats.ChunksCompleted != int(halfChunks) {
		t.Errorf("ChunksCompleted = %d, want %d", stats.ChunksCompleted, halfChunks)
	}

	// Range should end in COMPLETE state (ingestion + RecSplit)
	state, _ := meta.GetRangeState(0)
	if state != RangeStateComplete {
		t.Errorf("range state = %q, want %q", state, RangeStateComplete)
	}
}
