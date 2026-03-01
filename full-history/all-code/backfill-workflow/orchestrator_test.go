package backfill

import (
	"context"
	"testing"
)

func TestOrchestratorSingleRange(t *testing.T) {
	// Single range with BSB backend.
	ledgersDir := t.TempDir()
	txhashDir := t.TempDir()
	meta := NewMockMetaStore()
	log := NewTestLogger("TEST")

	cfg := &Config{
		Backfill: BackfillConfig{
			StartLedger:    2,
			EndLedger:      10_000_001,
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
		Memory:  NewNopMemoryMonitor(1.0),
		Factory: newMockLedgerSourceFactory(),
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

func TestOrchestratorCancellation(t *testing.T) {
	// Verify orchestrator respects context cancellation.
	meta := NewMockMetaStore()
	log := NewTestLogger("TEST")

	cfg := &Config{
		Backfill: BackfillConfig{
			StartLedger:    2,
			EndLedger:      10_000_001,
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
		Memory:  NewNopMemoryMonitor(1.0),
		Factory: newMockLedgerSourceFactory(),
	})

	err := orch.Run(ctx)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}
