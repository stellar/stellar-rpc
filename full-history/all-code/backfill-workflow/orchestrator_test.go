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

	// Range 0 should be complete
	state, _ := meta.GetRangeState(0)
	if state != RangeStateComplete {
		t.Errorf("range 0 state = %q, want %q", state, RangeStateComplete)
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

	// Pre-set range 0 as INGESTING with some chunks done.
	meta.SetRangeState(0, RangeStateIngesting)
	halfChunks := geo.ChunksPerIndex / 2
	for c := uint32(0); c < halfChunks; c++ {
		meta.SetChunkComplete(0, c)
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
	// Multi-range crash recovery test with all 4 resume states:
	//   Range 0: COMPLETE          → skip entirely (0 tasks)
	//   Range 1: RECSPLIT_BUILDING → build task only (1 task, no deps)
	//   Range 2: INGESTING         → process + build tasks (5/10 chunks done)
	//   Range 3: NEW               → process + build tasks (fresh)
	//
	// parallel_ranges=2 exercises bounded concurrency with mixed states.
	geo := geometry.TestGeometry()
	txhashDir := t.TempDir()
	ledgersDir := t.TempDir()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	// --- Pre-seed Range 0: COMPLETE ---
	meta.SetRangeState(0, RangeStateComplete)

	// --- Pre-seed Range 1: RECSPLIT_BUILDING ---
	// Simulate crash during RecSplit: 5 stale CF done flags + raw/ with .bin files.
	meta.SetRangeState(1, RangeStateRecSplitBuilding)
	for i := 0; i < 5; i++ {
		meta.SetRecSplitCFDone(1, i) // Stale flags from prior crashed run
	}
	// All chunks were ingested before crash — create empty .bin files.
	firstChunk1 := geo.RangeFirstChunk(1)
	rawDir1 := RawTxHashDir(txhashDir, 1)
	os.MkdirAll(rawDir1, 0755)
	for c := uint32(0); c < geo.ChunksPerIndex; c++ {
		meta.SetChunkComplete(1, firstChunk1+c)
		os.WriteFile(RawTxHashPath(txhashDir, 1, firstChunk1+c), []byte{}, 0644)
	}

	// --- Pre-seed Range 2: INGESTING (half done) ---
	meta.SetRangeState(2, RangeStateIngesting)
	firstChunk2 := geo.RangeFirstChunk(2)
	halfChunks := geo.ChunksPerIndex / 2
	rawDir2 := RawTxHashDir(txhashDir, 2)
	os.MkdirAll(rawDir2, 0755)
	for c := uint32(0); c < halfChunks; c++ {
		meta.SetChunkComplete(2, firstChunk2+c)
		os.WriteFile(RawTxHashPath(txhashDir, 2, firstChunk2+c), []byte{}, 0644)
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

	// All 4 ranges should be COMPLETE
	for rID := uint32(0); rID < 4; rID++ {
		state, _ := meta.GetRangeState(rID)
		if state != RangeStateComplete {
			t.Errorf("range %d state = %q, want %q", rID, state, RangeStateComplete)
		}
	}

	// Range 1: stale CF flags should have been cleared and all 16 re-set
	// by the all-or-nothing RecSplit rerun.
	for cfIdx := 0; cfIdx < 16; cfIdx++ {
		done, _ := meta.IsRecSplitCFDone(1, cfIdx)
		if !done {
			t.Errorf("range 1 CF %d: done flag not set after RecSplit rerun", cfIdx)
		}
	}

	// Range 2: all 16 CF done flags should be set (ingestion resumed → RecSplit)
	for cfIdx := 0; cfIdx < 16; cfIdx++ {
		done, _ := meta.IsRecSplitCFDone(2, cfIdx)
		if !done {
			t.Errorf("range 2 CF %d: done flag not set after RecSplit", cfIdx)
		}
	}

	// Range 3: all 16 CF done flags should be set (fresh range → complete)
	for cfIdx := 0; cfIdx < 16; cfIdx++ {
		done, _ := meta.IsRecSplitCFDone(3, cfIdx)
		if !done {
			t.Errorf("range 3 CF %d: done flag not set after RecSplit", cfIdx)
		}
	}

	// Startup report should show RESUMING with all 4 state categories
	if !log.HasMessage("RESUMING") {
		t.Error("startup report should show RESUMING status")
	}
	if !log.HasMessage("COMPLETE") {
		t.Error("startup report should mention COMPLETE range")
	}
	if !log.HasMessage("RECSPLIT_BUILDING") {
		t.Error("startup report should mention RECSPLIT_BUILDING range")
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

	meta.SetRangeState(0, RangeStateComplete)
	meta.SetRangeState(1, RangeStateComplete)

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
	// Range in RECSPLIT_BUILDING — DAG should contain only the build task.
	geo := geometry.TestGeometry()
	txhashDir := t.TempDir()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	meta.SetRangeState(0, RangeStateRecSplitBuilding)
	firstChunk := geo.RangeFirstChunk(0)
	rawDir := RawTxHashDir(txhashDir, 0)
	os.MkdirAll(rawDir, 0755)
	for c := uint32(0); c < geo.ChunksPerIndex; c++ {
		meta.SetChunkComplete(0, firstChunk+c)
		os.WriteFile(RawTxHashPath(txhashDir, 0, firstChunk+c), []byte{}, 0644)
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

	state, _ := meta.GetRangeState(0)
	if state != RangeStateComplete {
		t.Errorf("range 0 state = %q, want %q", state, RangeStateComplete)
	}
}
