package backfill

import (
	"context"
	"os"
	"testing"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/geometry"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/memory"
)

func TestOrchestratorSingleRange(t *testing.T) {
	// Single range with BSB backend.
	geo := geometry.TestGeometry()
	ledgersDir := t.TempDir()
	txhashDir := t.TempDir()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	cfg := &Config{
		Backfill: BackfillConfig{
			StartLedger:    geometry.FirstLedger,
			EndLedger:      geo.RangeLastLedger(0),
			Workers: 40,
			BSB:     &BSBConfig{},
		},
		ImmutableStores: ImmutableConfig{
			LedgersBase: ledgersDir,
			TxHashBase:  txhashDir,
		},
	}

	orch := NewOrchestrator(OrchestratorConfig{
		Cfg:     cfg,
		Meta:    meta,
		Logger:  log,
		Memory:  memory.NewNopMonitor(1.0),
		Factory: newMockLedgerSourceFactory(),
		Geo:     geo,
	})

	err := orch.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Range 0 should be complete (txhashindex key set)
	done, _ := meta.IsIndexTxHashIndexDone(0)
	if !done {
		t.Error("index 0 should have txhashindex key set")
	}
}

func TestOrchestratorStartupReportFresh(t *testing.T) {
	// Fresh backfill — startup report should indicate no prior state.
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	cfg := &Config{
		Backfill: BackfillConfig{
			StartLedger:    geometry.FirstLedger,
			EndLedger:      geo.RangeLastLedger(0),
			Workers: 40,
			BSB: &BSBConfig{
				BucketPath: "test-bucket/ledgers",
			},
		},
		Service: ServiceConfig{DataDir: "/data/test"},
		ImmutableStores: ImmutableConfig{
			LedgersBase: t.TempDir(),
			TxHashBase:  t.TempDir(),
		},
	}

	orch := NewOrchestrator(OrchestratorConfig{
		Cfg:     cfg,
		Meta:    meta,
		Logger:  log,
		Memory:  memory.NewNopMonitor(1.0),
		Factory: newMockLedgerSourceFactory(),
		Geo:     geo,
	})

	_ = orch.Run(context.Background())

	// Verify config was logged
	if !log.HasMessage("CONFIGURATION") {
		t.Error("startup report should include CONFIGURATION header")
	}
	if !log.HasMessage("data_dir") {
		t.Error("startup report should log data_dir")
	}
	if !log.HasMessage("bucket_path") {
		t.Error("startup report should log BSB bucket_path")
	}

	// Verify range state report
	if !log.HasMessage("RANGE STATE REPORT") {
		t.Error("startup report should include RANGE STATE REPORT header")
	}
	if !log.HasMessage("FRESH BACKFILL") {
		t.Error("fresh backfill should be identified as FRESH BACKFILL")
	}

	// Verify task graph was logged
	if !log.HasMessage("Task graph") {
		t.Error("should log task graph size")
	}
}

func TestOrchestratorStartupReportResume(t *testing.T) {
	// Partially completed backfill — startup report should show resume state.
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	// Pre-set some chunks done for range 0.
	chunks := geo.ChunksForIndex(0)
	halfChunks := len(chunks) / 2
	for i := 0; i < halfChunks; i++ {
		meta.SetChunkFlags(chunks[i])
	}

	cfg := &Config{
		Backfill: BackfillConfig{
			StartLedger:    geometry.FirstLedger,
			EndLedger:      geo.RangeLastLedger(0),
			Workers: 40,
			BSB:     &BSBConfig{},
		},
		ImmutableStores: ImmutableConfig{
			LedgersBase: t.TempDir(),
			TxHashBase:  t.TempDir(),
		},
	}

	orch := NewOrchestrator(OrchestratorConfig{
		Cfg:     cfg,
		Meta:    meta,
		Logger:  log,
		Memory:  memory.NewNopMonitor(1.0),
		Factory: newMockLedgerSourceFactory(),
		Geo:     geo,
	})

	_ = orch.Run(context.Background())

	// Should show INGESTING state with chunk progress
	if !log.HasMessage("INGESTING") {
		t.Error("resume should show INGESTING range state")
	}
	if !log.HasMessage("RESUMING") {
		t.Error("resume should show RESUMING status")
	}
	if !log.HasMessage("gap") {
		t.Error("resume should show chunk gaps for incomplete range")
	}
}

func TestOrchestratorMixedStateResume(t *testing.T) {
	// Multi-range crash recovery test with multiple resume states:
	//   Range 0: COMPLETE          → skip entirely (0 tasks)
	//   Range 1: All chunks done   → build task only (RecSplit resume)
	//   Range 2: Half chunks done  → process + build tasks
	//   Range 3: NEW               → process + build tasks (fresh)
	geo := geometry.TestGeometry()
	txhashDir := t.TempDir()
	ledgersDir := t.TempDir()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	// --- Pre-seed Range 0: COMPLETE ---
	meta.SetIndexTxHashIndex(0)

	// --- Pre-seed Range 1: All chunks done, no txhashindex yet ---
	// All chunks were ingested before crash — create empty .bin files.
	chunks1 := geo.ChunksForIndex(1)
	rawDir1 := RawTxHashDir(txhashDir, 1)
	os.MkdirAll(rawDir1, 0755)
	for _, c := range chunks1 {
		meta.SetChunkFlags(c)
		os.WriteFile(RawTxHashPath(txhashDir, 1, c), []byte{}, 0644)
	}

	// --- Pre-seed Range 2: Half chunks done ---
	chunks2 := geo.ChunksForIndex(2)
	halfChunks := len(chunks2) / 2
	rawDir2 := RawTxHashDir(txhashDir, 2)
	os.MkdirAll(rawDir2, 0755)
	for i := 0; i < halfChunks; i++ {
		meta.SetChunkFlags(chunks2[i])
		os.WriteFile(RawTxHashPath(txhashDir, 2, chunks2[i]), []byte{}, 0644)
	}

	// --- Range 3: NEW (no pre-seeding needed) ---

	cfg := &Config{
		Backfill: BackfillConfig{
			StartLedger:    geometry.FirstLedger,
			EndLedger:      geo.RangeLastLedger(3),
			Workers: 40,
			BSB:     &BSBConfig{},
		},
		ImmutableStores: ImmutableConfig{
			LedgersBase: ledgersDir,
			TxHashBase:  txhashDir,
		},
	}

	orch := NewOrchestrator(OrchestratorConfig{
		Cfg:     cfg,
		Meta:    meta,
		Logger:  log,
		Memory:  memory.NewNopMonitor(1.0),
		Factory: newMockLedgerSourceFactory(),
		Geo:     geo,
	})

	err := orch.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// All 4 ranges should be COMPLETE (txhashindex key set)
	for rID := uint32(0); rID < 4; rID++ {
		done, _ := meta.IsIndexTxHashIndexDone(rID)
		if !done {
			t.Errorf("index %d should have txhashindex key set", rID)
		}
	}

	// Startup report should show RESUMING with multiple state categories
	if !log.HasMessage("RESUMING") {
		t.Error("startup report should show RESUMING status")
	}
	if !log.HasMessage("COMPLETE") {
		t.Error("startup report should mention COMPLETE range")
	}
	if !log.HasMessage("INGESTING") {
		t.Error("startup report should mention INGESTING range")
	}
	if !log.HasMessage("NOT YET STARTED") {
		t.Error("startup report should mention NOT YET STARTED range")
	}
}

func TestOrchestratorCancellation(t *testing.T) {
	// Verify orchestrator respects context cancellation.
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	cfg := &Config{
		Backfill: BackfillConfig{
			StartLedger:    geometry.FirstLedger,
			EndLedger:      geo.RangeLastLedger(0),
			Workers: 40,
			BSB:     &BSBConfig{},
		},
		ImmutableStores: ImmutableConfig{
			LedgersBase: t.TempDir(),
			TxHashBase:  t.TempDir(),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	orch := NewOrchestrator(OrchestratorConfig{
		Cfg:     cfg,
		Meta:    meta,
		Logger:  log,
		Memory:  memory.NewNopMonitor(1.0),
		Factory: newMockLedgerSourceFactory(),
		Geo:     geo,
	})

	err := orch.Run(ctx)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestOrchestratorAllComplete(t *testing.T) {
	// All ranges already complete — DAG should be empty, pipeline exits quickly.
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	meta.SetIndexTxHashIndex(0)
	meta.SetIndexTxHashIndex(1)

	cfg := &Config{
		Backfill: BackfillConfig{
			StartLedger:    geometry.FirstLedger,
			EndLedger:      geo.RangeLastLedger(1),
			Workers: 40,
			BSB:     &BSBConfig{},
		},
		ImmutableStores: ImmutableConfig{
			LedgersBase: t.TempDir(),
			TxHashBase:  t.TempDir(),
		},
	}

	orch := NewOrchestrator(OrchestratorConfig{
		Cfg:     cfg,
		Meta:    meta,
		Logger:  log,
		Memory:  memory.NewNopMonitor(1.0),
		Factory: newMockLedgerSourceFactory(),
		Geo:     geo,
	})

	err := orch.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Should log "Task graph: 0 tasks"
	if !log.HasMessage("Task graph: 0 tasks") {
		t.Error("all-complete run should have 0 tasks in DAG")
	}
}

func TestOrchestratorRecSplitResumeOnly(t *testing.T) {
	// All chunks done but no txhashindex — DAG should contain only the build task.
	geo := geometry.TestGeometry()
	txhashDir := t.TempDir()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	chunks := geo.ChunksForIndex(0)
	rawDir := RawTxHashDir(txhashDir, 0)
	os.MkdirAll(rawDir, 0755)
	for _, c := range chunks {
		meta.SetChunkFlags(c)
		os.WriteFile(RawTxHashPath(txhashDir, 0, c), []byte{}, 0644)
	}

	cfg := &Config{
		Backfill: BackfillConfig{
			StartLedger:    geometry.FirstLedger,
			EndLedger:      geo.RangeLastLedger(0),
			Workers: 40,
			BSB:     &BSBConfig{},
		},
		ImmutableStores: ImmutableConfig{
			LedgersBase: t.TempDir(),
			TxHashBase:  txhashDir,
		},
	}

	orch := NewOrchestrator(OrchestratorConfig{
		Cfg:     cfg,
		Meta:    meta,
		Logger:  log,
		Memory:  memory.NewNopMonitor(1.0),
		Factory: newMockLedgerSourceFactory(),
		Geo:     geo,
	})

	err := orch.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Should have 1 task (build only, no process tasks)
	if !log.HasMessage("Task graph: 1 tasks") {
		t.Error("RecSplit-only resume should have 1 task in DAG")
	}

	done, _ := meta.IsIndexTxHashIndexDone(0)
	if !done {
		t.Error("index 0 should have txhashindex key set")
	}
}
