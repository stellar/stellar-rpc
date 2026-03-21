package backfill

import (
	"context"
	"os"
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

func TestPipelineSingleIndex(t *testing.T) {
	geo := geometry.TestGeometry()
	immutableDir := t.TempDir()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	cfg := &Config{
		Backfill: BackfillConfig{
			StartLedger: geometry.FirstLedger,
			EndLedger:   geo.RangeLastLedger(0),
			Workers:     40,
			BSB:         &BSBConfig{},
		},
		ImmutableStores: ImmutableConfig{
			ImmutableBase: immutableDir,
		},
	}

	pl := NewPipeline(PipelineConfig{
		Cfg:     cfg,
		Meta:    meta,
		Logger:  log,
		Memory:  memory.NewNopMonitor(1.0),
		Factory: newMockLedgerSourceFactory(),
		Geo:     geo,
	})

	err := pl.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	done, _ := meta.IsIndexTxHashDone(0)
	if !done {
		t.Error("index 0 should have txhash key set")
	}
}

func TestPipelineStartupReportFresh(t *testing.T) {
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	cfg := &Config{
		Backfill: BackfillConfig{
			StartLedger: geometry.FirstLedger,
			EndLedger:   geo.RangeLastLedger(0),
			Workers:     40,
			BSB: &BSBConfig{
				BucketPath: "test-bucket/ledgers",
			},
		},
		Service: ServiceConfig{DataDir: "/data/test"},
		ImmutableStores: ImmutableConfig{
			ImmutableBase: t.TempDir(),
		},
	}

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
	if !log.HasMessage("data_dir") {
		t.Error("startup report should log data_dir")
	}
	if !log.HasMessage("bucket_path") {
		t.Error("startup report should log BSB bucket_path")
	}
	if !log.HasMessage("INDEX STATE REPORT") {
		t.Error("startup report should include INDEX STATE REPORT header")
	}
	if !log.HasMessage("FRESH BACKFILL") {
		t.Error("fresh backfill should be identified as FRESH BACKFILL")
	}
	if !log.HasMessage("Task graph") {
		t.Error("should log task graph size")
	}
}

func TestPipelineStartupReportResume(t *testing.T) {
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	chunks := geo.ChunksForIndex(0)
	halfChunks := len(chunks) / 2
	for i := 0; i < halfChunks; i++ {
		meta.SetChunkFlags(chunks[i])
	}

	cfg := &Config{
		Backfill: BackfillConfig{
			StartLedger: geometry.FirstLedger,
			EndLedger:   geo.RangeLastLedger(0),
			Workers:     40,
			BSB:         &BSBConfig{},
		},
		ImmutableStores: ImmutableConfig{
			ImmutableBase: t.TempDir(),
		},
	}

	pl := NewPipeline(PipelineConfig{
		Cfg:     cfg,
		Meta:    meta,
		Logger:  log,
		Memory:  memory.NewNopMonitor(1.0),
		Factory: newMockLedgerSourceFactory(),
		Geo:     geo,
	})

	_ = pl.Run(context.Background())

	if !log.HasMessage("INGESTING") {
		t.Error("resume should show INGESTING state")
	}
	if !log.HasMessage("RESUMING") {
		t.Error("resume should show RESUMING status")
	}
	if !log.HasMessage("gap") {
		t.Error("resume should show chunk gaps for incomplete index")
	}
}

func TestPipelineMixedStateResume(t *testing.T) {
	geo := geometry.TestGeometry()
	immutableDir := t.TempDir()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	// Index 0: COMPLETE
	meta.SetIndexTxHash(0)

	// Index 1: All chunks done, no txhash index yet
	chunks1 := geo.ChunksForIndex(1)
	rawDir1 := RawTxHashDir(immutableDir, 1)
	os.MkdirAll(rawDir1, 0755)
	for _, c := range chunks1 {
		meta.SetChunkFlags(c)
		os.WriteFile(RawTxHashPath(immutableDir, 1, c), []byte{}, 0644)
	}

	// Index 2: Half chunks done
	chunks2 := geo.ChunksForIndex(2)
	halfChunks := len(chunks2) / 2
	rawDir2 := RawTxHashDir(immutableDir, 2)
	os.MkdirAll(rawDir2, 0755)
	for i := 0; i < halfChunks; i++ {
		meta.SetChunkFlags(chunks2[i])
		os.WriteFile(RawTxHashPath(immutableDir, 2, chunks2[i]), []byte{}, 0644)
	}

	// Index 3: NEW

	cfg := &Config{
		Backfill: BackfillConfig{
			StartLedger: geometry.FirstLedger,
			EndLedger:   geo.RangeLastLedger(3),
			Workers:     40,
			BSB:         &BSBConfig{},
		},
		ImmutableStores: ImmutableConfig{
			ImmutableBase: immutableDir,
		},
	}

	pl := NewPipeline(PipelineConfig{
		Cfg:     cfg,
		Meta:    meta,
		Logger:  log,
		Memory:  memory.NewNopMonitor(1.0),
		Factory: newMockLedgerSourceFactory(),
		Geo:     geo,
	})

	err := pl.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	for indexID := uint32(0); indexID < 4; indexID++ {
		done, _ := meta.IsIndexTxHashDone(indexID)
		if !done {
			t.Errorf("index %d should have txhash key set", indexID)
		}
	}

	if !log.HasMessage("RESUMING") {
		t.Error("startup report should show RESUMING status")
	}
}

func TestPipelineCancellation(t *testing.T) {
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	cfg := &Config{
		Backfill: BackfillConfig{
			StartLedger: geometry.FirstLedger,
			EndLedger:   geo.RangeLastLedger(0),
			Workers:     40,
			BSB:         &BSBConfig{},
		},
		ImmutableStores: ImmutableConfig{
			ImmutableBase: t.TempDir(),
		},
	}

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

func TestPipelineAllComplete(t *testing.T) {
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	meta.SetIndexTxHash(0)
	meta.SetIndexTxHash(1)

	cfg := &Config{
		Backfill: BackfillConfig{
			StartLedger: geometry.FirstLedger,
			EndLedger:   geo.RangeLastLedger(1),
			Workers:     40,
			BSB:         &BSBConfig{},
		},
		ImmutableStores: ImmutableConfig{
			ImmutableBase: t.TempDir(),
		},
	}

	pl := NewPipeline(PipelineConfig{
		Cfg:     cfg,
		Meta:    meta,
		Logger:  log,
		Memory:  memory.NewNopMonitor(1.0),
		Factory: newMockLedgerSourceFactory(),
		Geo:     geo,
	})

	err := pl.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if !log.HasMessage("Task graph: 0 tasks") {
		t.Error("all-complete run should have 0 tasks in DAG")
	}
}

func TestPipelineRecSplitResumeOnly(t *testing.T) {
	geo := geometry.TestGeometry()
	immutableDir := t.TempDir()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	chunks := geo.ChunksForIndex(0)
	rawDir := RawTxHashDir(immutableDir, 0)
	os.MkdirAll(rawDir, 0755)
	for _, c := range chunks {
		meta.SetChunkFlags(c)
		os.WriteFile(RawTxHashPath(immutableDir, 0, c), []byte{}, 0644)
	}

	cfg := &Config{
		Backfill: BackfillConfig{
			StartLedger: geometry.FirstLedger,
			EndLedger:   geo.RangeLastLedger(0),
			Workers:     40,
			BSB:         &BSBConfig{},
		},
		ImmutableStores: ImmutableConfig{
			ImmutableBase: immutableDir,
		},
	}

	pl := NewPipeline(PipelineConfig{
		Cfg:     cfg,
		Meta:    meta,
		Logger:  log,
		Memory:  memory.NewNopMonitor(1.0),
		Factory: newMockLedgerSourceFactory(),
		Geo:     geo,
	})

	err := pl.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if !log.HasMessage("Task graph: 1 tasks") {
		t.Error("RecSplit-only resume should have 1 task in DAG")
	}

	done, _ := meta.IsIndexTxHashDone(0)
	if !done {
		t.Error("index 0 should have txhash key set")
	}
}
