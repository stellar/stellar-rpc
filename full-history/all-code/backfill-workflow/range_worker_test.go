package backfill

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
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
		Tracker:       NewProgressTracker(),
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
		Tracker:       NewProgressTracker(),
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
		Tracker:       NewProgressTracker(),
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

// =============================================================================
// OnIngestionDone / Semaphore Tests
// =============================================================================

func TestOnIngestionDoneTimingNewRange(t *testing.T) {
	// Verify OnIngestionDone fires after ingestion completes (meta state =
	// RECSPLIT_BUILDING at callback time) and before RecSplit runs (final
	// state = COMPLETE after Run returns).
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()

	var stateAtCallback string
	var callbackCalled atomic.Bool

	worker := NewRangeWorker(RangeWorkerConfig{
		RangeID:       0,
		NumInstances:  1,
		LedgersBase:   t.TempDir(),
		TxHashBase:    t.TempDir(),
		FlushInterval: 100,
		Meta:          meta,
		Memory:        memory.NewNopMonitor(1.0),
		Factory:       newMockLedgerSourceFactory(),
		Logger:        logging.NewTestLogger("TEST"),
		Tracker:       NewProgressTracker(),
		OnIngestionDone: func() {
			callbackCalled.Store(true)
			stateAtCallback, _ = meta.GetRangeState(0)
		},
		Geo: geo,
	})

	_, err := worker.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if !callbackCalled.Load() {
		t.Fatal("OnIngestionDone was not called")
	}

	// At callback time, ingestion is done → state should be RECSPLIT_BUILDING
	if stateAtCallback != RangeStateRecSplitBuilding {
		t.Errorf("state at OnIngestionDone = %q, want %q",
			stateAtCallback, RangeStateRecSplitBuilding)
	}

	// After Run, RecSplit should have completed
	finalState, _ := meta.GetRangeState(0)
	if finalState != RangeStateComplete {
		t.Errorf("final state = %q, want %q", finalState, RangeStateComplete)
	}
}

func TestOnIngestionDoneAlreadyComplete(t *testing.T) {
	// Verify OnIngestionDone fires even for already-complete ranges.
	// The defer in Run() ensures the callback fires on every exit path.
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()
	meta.SetRangeState(0, RangeStateComplete)

	var callbackCalled atomic.Bool

	worker := NewRangeWorker(RangeWorkerConfig{
		RangeID:       0,
		NumInstances:  1,
		LedgersBase:   t.TempDir(),
		TxHashBase:    t.TempDir(),
		FlushInterval: 100,
		Meta:          meta,
		Memory:        memory.NewNopMonitor(1.0),
		Factory:       newMockLedgerSourceFactory(),
		Logger:        logging.NewTestLogger("TEST"),
		Tracker:       NewProgressTracker(),
		OnIngestionDone: func() {
			callbackCalled.Store(true)
		},
		Geo: geo,
	})

	_, err := worker.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if !callbackCalled.Load() {
		t.Fatal("OnIngestionDone should be called for already-complete ranges")
	}
}

func TestOnIngestionDoneRecSplitResume(t *testing.T) {
	// Verify OnIngestionDone fires immediately when resuming at RecSplit
	// phase (all chunks already ingested, state = RECSPLIT_BUILDING).
	geo := geometry.TestGeometry()
	txhashDir := t.TempDir()
	meta := NewMockMetaStore()

	// Set up: all chunks complete, state = RECSPLIT_BUILDING
	meta.SetRangeState(0, RangeStateRecSplitBuilding)
	rawDir := RawTxHashDir(txhashDir, 0)
	os.MkdirAll(rawDir, 0755)
	for c := uint32(0); c < geo.ChunksPerRange; c++ {
		meta.SetChunkComplete(0, c)
		os.WriteFile(RawTxHashPath(txhashDir, 0, c), []byte{}, 0644)
	}

	var stateAtCallback string
	var callbackCalled atomic.Bool

	worker := NewRangeWorker(RangeWorkerConfig{
		RangeID:       0,
		NumInstances:  1,
		LedgersBase:   t.TempDir(),
		TxHashBase:    txhashDir,
		FlushInterval: 100,
		Meta:          meta,
		Memory:        memory.NewNopMonitor(1.0),
		Factory:       newMockLedgerSourceFactory(),
		Logger:        logging.NewTestLogger("TEST"),
		Tracker:       NewProgressTracker(),
		OnIngestionDone: func() {
			callbackCalled.Store(true)
			stateAtCallback, _ = meta.GetRangeState(0)
		},
		Geo: geo,
	})

	_, err := worker.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if !callbackCalled.Load() {
		t.Fatal("OnIngestionDone should be called for RecSplit resume")
	}

	// Callback fires before RecSplit starts — state should still be RECSPLIT_BUILDING
	if stateAtCallback != RangeStateRecSplitBuilding {
		t.Errorf("state at callback = %q, want %q",
			stateAtCallback, RangeStateRecSplitBuilding)
	}

	// RecSplit should complete
	finalState, _ := meta.GetRangeState(0)
	if finalState != RangeStateComplete {
		t.Errorf("final state = %q, want %q", finalState, RangeStateComplete)
	}
}

func TestOnIngestionDoneCalledExactlyOnce(t *testing.T) {
	// Verify the callback fires exactly once despite multiple call paths:
	// explicit signalIngestionDone() after ingestion + defer signalIngestionDone().
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()

	var callCount atomic.Int32

	worker := NewRangeWorker(RangeWorkerConfig{
		RangeID:       0,
		NumInstances:  1,
		LedgersBase:   t.TempDir(),
		TxHashBase:    t.TempDir(),
		FlushInterval: 100,
		Meta:          meta,
		Memory:        memory.NewNopMonitor(1.0),
		Factory:       newMockLedgerSourceFactory(),
		Logger:        logging.NewTestLogger("TEST"),
		Tracker:       NewProgressTracker(),
		OnIngestionDone: func() {
			callCount.Add(1)
		},
		Geo: geo,
	})

	_, err := worker.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if count := callCount.Load(); count != 1 {
		t.Errorf("OnIngestionDone called %d times, want exactly 1", count)
	}
}

func TestSemaphorePatternTwoRanges(t *testing.T) {
	// End-to-end test of the orchestrator's semaphore pattern:
	// 2 ranges with parallelism=1, both complete successfully.
	// Mirrors the orchestrator's ingestSem + OnIngestionDone dispatch loop.
	//
	// With parallelism=1, the main goroutine blocks on the second sem acquire
	// until the first range's OnIngestionDone releases the slot. This proves
	// the semaphore is released during the range lifecycle (not only at exit).
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()

	// Pre-create temp dirs on the test goroutine.
	type rangeDirs struct{ ledgers, txhash string }
	dirs := [2]rangeDirs{
		{t.TempDir(), t.TempDir()},
		{t.TempDir(), t.TempDir()},
	}

	ingestSem := make(chan struct{}, 1) // parallelism=1
	var wg sync.WaitGroup

	for rangeID := uint32(0); rangeID < 2; rangeID++ {
		// Acquire ingestion slot — blocks until a slot is free
		ingestSem <- struct{}{}

		wg.Add(1)
		go func(rID uint32) {
			defer wg.Done()

			worker := NewRangeWorker(RangeWorkerConfig{
				RangeID:         rID,
				NumInstances:    1,
				LedgersBase:     dirs[rID].ledgers,
				TxHashBase:      dirs[rID].txhash,
				FlushInterval:   100,
				Meta:            meta,
				Memory:          memory.NewNopMonitor(1.0),
				Factory:         newMockLedgerSourceFactory(),
				Logger:          logging.NewTestLogger("TEST"),
				Tracker:         NewProgressTracker(),
				OnIngestionDone: func() { <-ingestSem },
				Geo:             geo,
			})

			_, err := worker.Run(context.Background())
			if err != nil {
				t.Errorf("range %d: %v", rID, err)
			}
		}(rangeID)
	}

	wg.Wait()

	// Both ranges should be complete
	for rID := uint32(0); rID < 2; rID++ {
		state, _ := meta.GetRangeState(rID)
		if state != RangeStateComplete {
			t.Errorf("range %d state = %q, want %q", rID, state, RangeStateComplete)
		}
	}
}
