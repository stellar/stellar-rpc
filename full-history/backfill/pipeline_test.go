package backfill

import (
	"context"
	"testing"

	"github.com/stellar/stellar-rpc/full-history/pkg/geometry"
	"github.com/stellar/stellar-rpc/full-history/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/pkg/memory"
)

type mockLedgerSourceFactory struct{}

func newMockLedgerSourceFactory() *mockLedgerSourceFactory {
	return &mockLedgerSourceFactory{}
}

func (f *mockLedgerSourceFactory) Create(_ context.Context, startLedger, endLedger uint32) (LedgerSource, error) {
	return newMockLedgerSource(startLedger, endLedger), nil
}

func newTestPipelineConfig(geo geometry.Geometry, meta *MockMetaStore, log logging.Logger) *Config {
	return &Config{
		Service: ServiceConfig{DefaultDataDir: "/data/test"},
		Backfill: BackfillConfig{
			ChunksPerTxHashIndex: int(geo.ChunksPerTxHashIndex),
			BSB:                  &BSBConfig{BucketPath: "test-bucket"},
		},
		ImmutableStorage: ImmutableStorageConfig{
			Ledgers:     StoragePathConfig{Path: "/tmp/test/ledgers"},
			Events:      StoragePathConfig{Path: "/tmp/test/events"},
			TxHashRaw:   StoragePathConfig{Path: "/tmp/test/txhash/raw"},
			TxHashIndex: StoragePathConfig{Path: "/tmp/test/txhash/index"},
		},
		EffectiveStartLedger: geometry.FirstLedger,
		EffectiveEndLedger:   geo.IndexLastLedger(0),
		Workers:              2,
		VerifyRecSplit:       true,
		MaxRetries:           1,
	}
}

func TestPipeline_BuildDAG_AllTasksRegistered(t *testing.T) {
	geo := geometry.TestGeometry() // 5 chunks per index
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")
	cfg := newTestPipelineConfig(geo, meta, log)

	// 2 indexes.
	cfg.EffectiveEndLedger = geo.IndexLastLedger(1)

	pl := NewPipeline(PipelineConfig{
		Cfg:     cfg,
		Meta:    meta,
		Logger:  log,
		Memory:  memory.NewNopMonitor(1.0),
		Factory: newMockLedgerSourceFactory(),
		Geo:     geo,
	})

	startChunkID := uint32(0)
	endChunkID := geo.IndexLastChunk(1)
	tracker := NewProgressTracker()
	tracker.RegisterIndex(0, int(geo.ChunksPerTxHashIndex))
	tracker.RegisterIndex(1, int(geo.ChunksPerTxHashIndex))

	dag := pl.buildDAG(startChunkID, endChunkID, tracker)

	// 10 process_chunk + 2 build_txhash_index + 2 cleanup_txhash = 14 tasks.
	expected := 10 + 2 + 2
	if dag.Len() != expected {
		t.Errorf("DAG tasks = %d, want %d", dag.Len(), expected)
	}
}

func TestPipeline_NoTriageNoReconciler(t *testing.T) {
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")
	cfg := newTestPipelineConfig(geo, meta, log)

	// Even with some chunks complete, ALL tasks are still registered.
	meta.SetChunkLFS(0)
	meta.SetChunkTxHash(0)
	meta.SetChunkEvents(0)

	pl := NewPipeline(PipelineConfig{
		Cfg:     cfg,
		Meta:    meta,
		Logger:  log,
		Memory:  memory.NewNopMonitor(1.0),
		Factory: newMockLedgerSourceFactory(),
		Geo:     geo,
	})

	startChunkID := uint32(0)
	endChunkID := geo.IndexLastChunk(0)
	tracker := NewProgressTracker()
	tracker.RegisterIndex(0, int(geo.ChunksPerTxHashIndex))

	dag := pl.buildDAG(startChunkID, endChunkID, tracker)

	// 5 process_chunk + 1 build + 1 cleanup = 7 tasks (all registered even if some done).
	expected := 5 + 1 + 1
	if dag.Len() != expected {
		t.Errorf("DAG tasks = %d, want %d", dag.Len(), expected)
	}
}

func TestPipelineStartupReport(t *testing.T) {
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")
	cfg := newTestPipelineConfig(geo, meta, log)

	pl := NewPipeline(PipelineConfig{
		Cfg:     cfg,
		Meta:    meta,
		Logger:  log,
		Memory:  memory.NewNopMonitor(1.0),
		Factory: newMockLedgerSourceFactory(),
		Geo:     geo,
	})

	_ = pl.Run(context.Background())

	if !log.HasMessage("CONFIGURATION") {
		t.Error("startup report should include CONFIGURATION header")
	}
	if !log.HasMessage("default_data_dir") {
		t.Error("startup report should log default_data_dir")
	}
	if !log.HasMessage("Task graph") {
		t.Error("should log task graph size")
	}
}

func TestPipelineCancellation(t *testing.T) {
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")
	cfg := newTestPipelineConfig(geo, meta, log)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	pl := NewPipeline(PipelineConfig{
		Cfg:     cfg,
		Meta:    meta,
		Logger:  log,
		Memory:  memory.NewNopMonitor(1.0),
		Factory: newMockLedgerSourceFactory(),
		Geo:     geo,
	})

	err := pl.Run(ctx)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}
