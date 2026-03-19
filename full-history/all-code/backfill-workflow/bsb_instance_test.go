package backfill

import (
	"context"
	"testing"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/geometry"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/memory"
)

// mockLedgerSourceFactory creates mockLedgerSources for testing.
type mockLedgerSourceFactory struct{}

func newMockLedgerSourceFactory() *mockLedgerSourceFactory {
	return &mockLedgerSourceFactory{}
}

func (f *mockLedgerSourceFactory) Create(_ context.Context, startLedger, endLedger uint32) (LedgerSource, error) {
	source := newMockLedgerSource(startLedger, endLedger)
	return source, nil
}

func TestBSBInstanceAllSkipped(t *testing.T) {
	// All chunks in skip-set — should exit immediately without creating source.
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	firstChunk := uint32(0)
	lastChunk := uint32(4) // 5 chunks

	skipSet := make(map[uint32]bool)
	for c := firstChunk; c <= lastChunk; c++ {
		skipSet[c] = true
	}

	instance := NewBSBInstance(BSBInstanceConfig{
		InstanceID:    0,
		IndexID:       0,
		FirstChunkID:  firstChunk,
		LastChunkID:   lastChunk,
		SkipSet:       skipSet,
		LedgersBase:   t.TempDir(),
		TxHashBase:    t.TempDir(),
		Meta:          meta,
		Memory:        memory.NewNopMonitor(1.0),
		Factory:       newMockLedgerSourceFactory(),
		Logger:        log,
		Geo:           geo,
	})

	stats, err := instance.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if stats.ChunksProcessed != 0 {
		t.Errorf("ChunksProcessed = %d, want 0", stats.ChunksProcessed)
	}
	if stats.ChunksSkipped != 5 {
		t.Errorf("ChunksSkipped = %d, want 5", stats.ChunksSkipped)
	}
}

func TestBSBInstanceNoneSkipped(t *testing.T) {
	// No chunks skipped — should process all.
	geo := geometry.TestGeometry()
	ledgersDir := t.TempDir()
	txhashDir := t.TempDir()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	chunkID := uint32(0) // Single chunk for speed

	instance := NewBSBInstance(BSBInstanceConfig{
		InstanceID:    0,
		IndexID:       0,
		FirstChunkID:  chunkID,
		LastChunkID:   chunkID,
		SkipSet:       make(map[uint32]bool),
		LedgersBase:   ledgersDir,
		TxHashBase:    txhashDir,
		Meta:          meta,
		Memory:        memory.NewNopMonitor(1.0),
		Factory:       newMockLedgerSourceFactory(),
		Logger:        log,
		Geo:           geo,
	})

	stats, err := instance.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if stats.ChunksProcessed != 1 {
		t.Errorf("ChunksProcessed = %d, want 1", stats.ChunksProcessed)
	}
	if stats.ChunksSkipped != 0 {
		t.Errorf("ChunksSkipped = %d, want 0", stats.ChunksSkipped)
	}
	if stats.TotalLedgers != int64(geo.ChunkSize) {
		t.Errorf("TotalLedgers = %d, want %d", stats.TotalLedgers, geo.ChunkSize)
	}

	// Verify chunk was marked complete
	lfsDone, _ := meta.IsChunkLFSDone(chunkID)
	txDone, _ := meta.IsChunkTxHashDone(chunkID)
	if !lfsDone || !txDone {
		t.Error("chunk should be marked complete (both lfs and txhash flags)")
	}
}

func TestBSBInstancePartialSkip(t *testing.T) {
	// Some chunks skipped, some not.
	geo := geometry.TestGeometry()
	ledgersDir := t.TempDir()
	txhashDir := t.TempDir()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	// 3 chunks, skip middle one
	skipSet := map[uint32]bool{1: true}

	instance := NewBSBInstance(BSBInstanceConfig{
		InstanceID:    0,
		IndexID:       0,
		FirstChunkID:  0,
		LastChunkID:   2,
		SkipSet:       skipSet,
		LedgersBase:   ledgersDir,
		TxHashBase:    txhashDir,
		Meta:          meta,
		Memory:        memory.NewNopMonitor(1.0),
		Factory:       newMockLedgerSourceFactory(),
		Logger:        log,
		Geo:           geo,
	})

	stats, err := instance.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if stats.ChunksProcessed != 2 {
		t.Errorf("ChunksProcessed = %d, want 2", stats.ChunksProcessed)
	}
	if stats.ChunksSkipped != 1 {
		t.Errorf("ChunksSkipped = %d, want 1", stats.ChunksSkipped)
	}
}
