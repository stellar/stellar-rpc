package backfill

import (
	"context"
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
			ParallelRanges: 1,
			FlushInterval:  100,
			BSB: &BSBConfig{
				NumInstancesPerRange: 1, // 1 instance for test speed
			},
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
			ParallelRanges: 1,
			FlushInterval:  100,
			BSB: &BSBConfig{
				NumInstancesPerRange: 1,
				BucketPath:           "test-bucket/ledgers",
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
}

func TestOrchestratorStartupReportResume(t *testing.T) {
	// Partially completed backfill — startup report should show resume state.
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	// Pre-set range 0 as INGESTING with some chunks done.
	meta.SetRangeState(0, RangeStateIngesting)
	halfChunks := geo.ChunksPerRange / 2
	for c := uint32(0); c < halfChunks; c++ {
		meta.SetChunkComplete(0, c)
	}

	cfg := &Config{
		Backfill: BackfillConfig{
			StartLedger:    geometry.FirstLedger,
			EndLedger:      geo.RangeLastLedger(0),
			ParallelRanges: 1,
			FlushInterval:  100,
			BSB:            &BSBConfig{NumInstancesPerRange: 1},
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

func TestOrchestratorCancellation(t *testing.T) {
	// Verify orchestrator respects context cancellation.
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()
	log := logging.NewTestLogger("TEST")

	cfg := &Config{
		Backfill: BackfillConfig{
			StartLedger:    geometry.FirstLedger,
			EndLedger:      geo.RangeLastLedger(0),
			ParallelRanges: 1,
			FlushInterval:  100,
			BSB: &BSBConfig{
				NumInstancesPerRange: 1,
			},
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
