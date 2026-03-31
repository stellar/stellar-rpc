package backfill

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stellar/stellar-rpc/full-history/pkg/fsutil"
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

// txMockLedgerSourceFactory creates mock sources with a configurable
// number of transactions per ledger (needed for txhash .bin production).
type txMockLedgerSourceFactory struct {
	txCount int
}

func (f *txMockLedgerSourceFactory) Create(_ context.Context, start, end uint32) (LedgerSource, error) {
	return newMockLedgerSourceWithTx(start, end, f.txCount), nil
}

// cancellingFactory returns valid sources for the first N calls, then cancels
// the pipeline context and returns an error. Used for crash recovery tests.
type cancellingFactory struct {
	mu       sync.Mutex
	txCount  int
	cancel   context.CancelFunc
	calls    int
	cancelAt int // 0-based: first cancelAt calls succeed, cancelAt-th fails
}

func (f *cancellingFactory) Create(_ context.Context, start, end uint32) (LedgerSource, error) {
	f.mu.Lock()
	n := f.calls
	f.calls++
	f.mu.Unlock()

	if n >= f.cancelAt {
		f.cancel()
		return nil, fmt.Errorf("cancelled after %d chunks", f.cancelAt)
	}
	return newMockLedgerSourceWithTx(start, end, f.txCount), nil
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

// =============================================================================
// Integration Tests — Full Pipeline With Real RocksDB
// =============================================================================

// newIntegrationConfig builds a Config wired to real temp directories.
// Each storage type gets its own isolated temp dir.
func newIntegrationConfig(t *testing.T, geo geometry.Geometry) (*Config, integrationDirs) {
	t.Helper()
	dirs := integrationDirs{
		ledgers:     t.TempDir(),
		events:      t.TempDir(),
		txhashRaw:   t.TempDir(),
		txhashIndex: t.TempDir(),
	}
	cfg := &Config{
		Service:  ServiceConfig{DefaultDataDir: t.TempDir()},
		Backfill: BackfillConfig{
			ChunksPerTxHashIndex: int(geo.ChunksPerTxHashIndex),
			BSB:                  &BSBConfig{BucketPath: "test-bucket"},
		},
		ImmutableStorage: ImmutableStorageConfig{
			Ledgers:     StoragePathConfig{Path: dirs.ledgers},
			Events:      StoragePathConfig{Path: dirs.events},
			TxHashRaw:   StoragePathConfig{Path: dirs.txhashRaw},
			TxHashIndex: StoragePathConfig{Path: dirs.txhashIndex},
		},
		EffectiveStartLedger: geometry.FirstLedger,
		EffectiveEndLedger:   geo.IndexLastLedger(0), // 1 index = 5 chunks
		Workers:              2,
		VerifyRecSplit:       true,
		MaxRetries:           1,
	}
	return cfg, dirs
}

type integrationDirs struct {
	ledgers, events, txhashRaw, txhashIndex string
}

// verifyCleanFinalState checks the full post-pipeline state: files + meta flags.
func verifyCleanFinalState(t *testing.T, meta BackfillMetaStore, dirs integrationDirs, geo geometry.Geometry) {
	t.Helper()
	numChunks := geo.ChunksPerTxHashIndex

	// Ledger .pack files must exist.
	for c := uint32(0); c < numChunks; c++ {
		if !fsutil.FileExists(LedgerPackPath(dirs.ledgers, c)) {
			t.Errorf("chunk %d: ledger .pack missing", c)
		}
	}

	// StreamHash index must exist.
	if !fsutil.FileExists(StreamHashIndexPath(dirs.txhashIndex, 0)) {
		t.Error("StreamHash index missing for index 0")
	}

	// Raw .bin files must be gone (cleaned up by flow or cleanup_txhash).
	for c := uint32(0); c < numChunks; c++ {
		if fsutil.FileExists(RawTxHashPath(dirs.txhashRaw, c)) {
			t.Errorf("chunk %d: raw .bin should be deleted", c)
		}
	}

	// Meta flags: lfs + events per chunk.
	for c := uint32(0); c < numChunks; c++ {
		lfsDone, err := meta.IsChunkLFSDone(c)
		if err != nil {
			t.Fatalf("chunk %d lfs check: %v", c, err)
		}
		if !lfsDone {
			t.Errorf("chunk %d: lfs flag missing", c)
		}

		eventsDone, err := meta.IsChunkEventsDone(c)
		if err != nil {
			t.Fatalf("chunk %d events check: %v", c, err)
		}
		if !eventsDone {
			t.Errorf("chunk %d: events flag missing", c)
		}

		// chunk:C:txhash must be deleted after cleanup.
		txDone, err := meta.IsChunkTxHashDone(c)
		if err != nil {
			t.Fatalf("chunk %d txhash check: %v", c, err)
		}
		if txDone {
			t.Errorf("chunk %d: txhash key should be deleted", c)
		}
	}

	// index:0:txhash must be set.
	indexDone, err := meta.IsIndexTxHashDone(0)
	if err != nil {
		t.Fatalf("index 0 txhash check: %v", err)
	}
	if !indexDone {
		t.Error("index 0: txhash flag missing")
	}
}

// TestPipeline_Integration_FullRoundTrip runs the full pipeline end-to-end with
// real RocksDB, real file I/O, and TestGeometry (10-ledger chunks, 5 chunks/index).
// After verifying the final state, it re-runs the pipeline and confirms no errors.
func TestPipeline_Integration_FullRoundTrip(t *testing.T) {
	geo := geometry.TestGeometry()

	// Real RocksDB meta store.
	metaDir := t.TempDir()
	meta, err := NewRocksDBMetaStore(metaDir)
	if err != nil {
		t.Fatalf("create meta store: %v", err)
	}
	defer meta.Close()

	cfg, dirs := newIntegrationConfig(t, geo)
	log := logging.NewTestLogger("INTEG")
	factory := &txMockLedgerSourceFactory{txCount: 2}

	// --- Run 1: full pipeline ---
	pl := NewPipeline(PipelineConfig{
		Cfg:           cfg,
		Meta:          meta,
		Logger:        log,
		Memory:        memory.NewNopMonitor(1.0),
		Factory:       factory,
		Geo:           geo,
		UseStreamHash: true,
	})

	if err := pl.Run(context.Background()); err != nil {
		t.Fatalf("Run 1: %v", err)
	}

	verifyCleanFinalState(t, meta, dirs, geo)

	// --- Run 2: idempotent re-run — must complete without errors ---
	//
	// After a complete run, the StreamHash flow deletes chunk:C:txhash keys.
	// On re-run, process_chunk would see txhash as missing and try the
	// LFS-first path. However, LFSLedgerIterator uses production ChunkSize
	// (10K), which is incompatible with TestGeometry (ChunkSize=10).
	// Pre-set chunk:txhash keys so all process_chunk tasks no-op.
	for c := uint32(0); c < geo.ChunksPerTxHashIndex; c++ {
		if err := meta.SetChunkTxHash(c); err != nil {
			t.Fatalf("pre-set chunk txhash %d: %v", c, err)
		}
	}

	pl2 := NewPipeline(PipelineConfig{
		Cfg:           cfg,
		Meta:          meta,
		Logger:        log,
		Memory:        memory.NewNopMonitor(1.0),
		Factory:       factory,
		Geo:           geo,
		UseStreamHash: true,
	})

	if err := pl2.Run(context.Background()); err != nil {
		t.Fatalf("Run 2 (re-run): %v", err)
	}

	// Final state after re-run must be identical (cleanup_txhash re-deletes
	// the chunk:txhash keys we set above).
	verifyCleanFinalState(t, meta, dirs, geo)
}

// TestPipeline_Integration_CrashRecovery cancels the pipeline after 3 chunks,
// verifies partial state, then re-runs to completion.
func TestPipeline_Integration_CrashRecovery(t *testing.T) {
	geo := geometry.TestGeometry()

	metaDir := t.TempDir()
	meta, err := NewRocksDBMetaStore(metaDir)
	if err != nil {
		t.Fatalf("create meta store: %v", err)
	}
	defer meta.Close()

	cfg, dirs := newIntegrationConfig(t, geo)
	// Single worker so chunk processing order is deterministic.
	cfg.Workers = 1

	log := logging.NewTestLogger("CRASH")

	// Factory that cancels after 3 Create() calls.
	ctx1, cancel1 := context.WithCancel(context.Background())
	factory1 := &cancellingFactory{
		txCount:  2,
		cancel:   cancel1,
		cancelAt: 3, // chunks 0, 1, 2 succeed; chunk 3 fails
	}

	// --- Run 1: partial run (crash) ---
	pl := NewPipeline(PipelineConfig{
		Cfg:           cfg,
		Meta:          meta,
		Logger:        log,
		Memory:        memory.NewNopMonitor(1.0),
		Factory:       factory1,
		Geo:           geo,
		UseStreamHash: true,
	})

	err = pl.Run(ctx1)
	if err == nil {
		t.Fatal("Run 1 should have failed due to cancellation")
	}

	// Verify partial state: at least some chunks completed.
	completedChunks := 0
	for c := uint32(0); c < geo.ChunksPerTxHashIndex; c++ {
		done, _ := meta.IsChunkLFSDone(c)
		if done {
			completedChunks++
		}
	}
	if completedChunks == 0 {
		t.Fatal("expected at least some chunks to complete before cancellation")
	}
	if completedChunks == int(geo.ChunksPerTxHashIndex) {
		t.Fatal("expected partial completion, not all chunks")
	}

	// Index must NOT be set (not all chunks completed).
	indexDone, _ := meta.IsIndexTxHashDone(0)
	if indexDone {
		t.Error("index flag should not be set after partial run")
	}

	// --- Run 2: resume to completion ---
	factory2 := &txMockLedgerSourceFactory{txCount: 2}

	pl2 := NewPipeline(PipelineConfig{
		Cfg:           cfg,
		Meta:          meta,
		Logger:        log,
		Memory:        memory.NewNopMonitor(1.0),
		Factory:       factory2,
		Geo:           geo,
		UseStreamHash: true,
	})

	if err := pl2.Run(context.Background()); err != nil {
		t.Fatalf("Run 2 (resume): %v", err)
	}

	// Final state must match a clean run.
	verifyCleanFinalState(t, meta, dirs, geo)
}
