package backfill

import (
	"sync"
	"testing"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/memory"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/stats"
)

func TestLatencyStatsEmpty(t *testing.T) {
	ls := stats.NewLatencyStats()
	summary := ls.Summary()
	if summary.P50 != 0 || summary.P99 != 0 {
		t.Errorf("empty stats should return zero percentiles, got p50=%v p99=%v", summary.P50, summary.P99)
	}
	if ls.Count() != 0 {
		t.Errorf("empty stats count = %d, want 0", ls.Count())
	}
}

func TestLatencyStatsSingleSample(t *testing.T) {
	ls := stats.NewLatencyStats()
	ls.Add(10 * time.Millisecond)

	summary := ls.Summary()
	if summary.P50 != 10*time.Millisecond {
		t.Errorf("single sample p50 = %v, want 10ms", summary.P50)
	}
	if summary.P99 != 10*time.Millisecond {
		t.Errorf("single sample p99 = %v, want 10ms", summary.P99)
	}
}

func TestLatencyStatsKnownValues(t *testing.T) {
	ls := stats.NewLatencyStats()

	// Add 100 samples: 1ms, 2ms, ..., 100ms
	for i := 1; i <= 100; i++ {
		ls.Add(time.Duration(i) * time.Millisecond)
	}

	if ls.Count() != 100 {
		t.Errorf("count = %d, want 100", ls.Count())
	}

	summary := ls.Summary()

	// p50 should be around 50ms (index ~49.5 → interpolated between 50ms and 51ms)
	if summary.P50 < 49*time.Millisecond || summary.P50 > 52*time.Millisecond {
		t.Errorf("p50 = %v, expected ~50ms", summary.P50)
	}

	// p90 should be around 90ms
	if summary.P90 < 89*time.Millisecond || summary.P90 > 92*time.Millisecond {
		t.Errorf("p90 = %v, expected ~90ms", summary.P90)
	}

	// p99 should be around 99ms
	if summary.P99 < 98*time.Millisecond || summary.P99 > 100*time.Millisecond {
		t.Errorf("p99 = %v, expected ~99ms", summary.P99)
	}

	if summary.Min != 1*time.Millisecond {
		t.Errorf("min = %v, want 1ms", summary.Min)
	}
	if summary.Max != 100*time.Millisecond {
		t.Errorf("max = %v, want 100ms", summary.Max)
	}
}

func TestLatencyStatsConcurrentAdd(t *testing.T) {
	ls := stats.NewLatencyStats()
	var wg sync.WaitGroup

	// 10 goroutines each adding 100 samples
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				ls.Add(time.Duration(i) * time.Microsecond)
			}
		}()
	}
	wg.Wait()

	if ls.Count() != 1000 {
		t.Errorf("count = %d, want 1000", ls.Count())
	}

	// Summary should not panic on concurrent data
	summary := ls.Summary()
	if summary.P50 < 0 {
		t.Error("p50 should be non-negative")
	}
}

func TestLatencyPercentilesString(t *testing.T) {
	lp := stats.LatencyPercentiles{
		P50: 1200 * time.Microsecond,
		P90: 3400 * time.Microsecond,
		P95: 5100 * time.Microsecond,
		P99: 12700 * time.Microsecond,
	}
	s := lp.String()
	if s == "" {
		t.Error("String() should not be empty")
	}
	// Should contain all percentile labels
	for _, label := range []string{"p50=", "p90=", "p95=", "p99="} {
		if !containsSubstring(s, label) {
			t.Errorf("String() missing %q: %s", label, s)
		}
	}
}

func TestProgressTracker(t *testing.T) {
	pt := NewProgressTracker()
	rp := pt.RegisterRange(0, 100)

	// Record a chunk completion
	rp.RecordChunkComplete(ChunkWriteStats{
		ChunkID:          42,
		LedgersProcessed: 10000,
		TxCount:          25000,
		LFSWriteTime:    5 * time.Millisecond,
		TxHashWriteTime: 3 * time.Millisecond,
		FsyncTime:       1 * time.Millisecond,
	})

	if rp.CompletedChunks() != 1 {
		t.Errorf("CompletedChunks() = %d, want 1", rp.CompletedChunks())
	}
	if pt.SessionChunks() != 1 {
		t.Errorf("Tracker SessionChunks() = %d, want 1", pt.SessionChunks())
	}

	// Record a BSB GetLedger call
	rp.RecordBSBGetLedger(45 * time.Millisecond)

	// LogProgress should not panic
	log := logging.NewTestLogger("TEST")
	mem := memory.NewNopMonitor(24.3)
	pt.LogProgress(log, mem)

	if !log.HasMessage("Progress") {
		t.Error("LogProgress should log a progress message")
	}
	if !log.HasMessage("Range 0000") {
		t.Error("LogProgress should include per-range line")
	}
}

func TestProgressTrackerMultiRange(t *testing.T) {
	pt := NewProgressTracker()
	rp0 := pt.RegisterRange(0, 1000)
	rp1 := pt.RegisterRange(1, 1000)

	rp0.RecordChunkComplete(ChunkWriteStats{LedgersProcessed: 10000, TxCount: 500})
	rp0.RecordChunkComplete(ChunkWriteStats{LedgersProcessed: 10000, TxCount: 600})
	rp1.RecordChunkComplete(ChunkWriteStats{LedgersProcessed: 10000, TxCount: 400})

	if pt.SessionChunks() != 3 {
		t.Errorf("SessionChunks() = %d, want 3", pt.SessionChunks())
	}
	if pt.TotalLedgers() != 30000 {
		t.Errorf("TotalLedgers() = %d, want 30000", pt.TotalLedgers())
	}
	if pt.TotalTx() != 1500 {
		t.Errorf("TotalTx() = %d, want 1500", pt.TotalTx())
	}

	// Completed ranges keep stats in tracker
	rp0.SetPhase(PhaseComplete)
	if pt.SessionChunks() != 3 {
		t.Errorf("after complete, SessionChunks() = %d, want 3", pt.SessionChunks())
	}
}

func TestSeedCompleted(t *testing.T) {
	pt := NewProgressTracker()
	rp := pt.RegisterRange(0, 1000)

	// Seed with 279 chunks from a prior run
	rp.SeedCompleted(279)

	if rp.CompletedChunks() != 279 {
		t.Errorf("CompletedChunks() = %d, want 279", rp.CompletedChunks())
	}

	// New chunk completions should add on top
	rp.RecordChunkComplete(ChunkWriteStats{LedgersProcessed: 10000, TxCount: 500})
	if rp.CompletedChunks() != 280 {
		t.Errorf("CompletedChunks() after record = %d, want 280", rp.CompletedChunks())
	}

	if pt.SessionChunks() != 1 {
		t.Errorf("Tracker SessionChunks() = %d, want 1 (excludes seeded)", pt.SessionChunks())
	}
}

func TestRangeProgressPhases(t *testing.T) {
	pt := NewProgressTracker()
	rp := pt.RegisterRange(0, 100)

	// Default phase is QUEUED after registration
	log := logging.NewTestLogger("TEST")
	mem := memory.NewNopMonitor(1.0)
	pt.LogProgress(log, mem)
	if !log.HasMessage("QUEUED") {
		t.Error("default phase should be QUEUED")
	}

	// Switch to INGESTING
	rp.SetPhase(PhaseIngesting)
	logIng := logging.NewTestLogger("TEST")
	pt.LogProgress(logIng, mem)
	if !logIng.HasMessage("INGESTING") {
		t.Error("phase should be INGESTING after SetPhase")
	}

	// Switch to RECSPLIT — counting sub-phase (default)
	rp.SetPhase(PhaseRecSplit)

	log2 := logging.NewTestLogger("TEST")
	pt.LogProgress(log2, mem)
	if !log2.HasMessage("RECSPLIT:COUNTING") {
		t.Error("default sub-phase should be COUNTING")
	}

	// Switch to building sub-phase with 2 CFs done
	rp.SetRecSplitSubPhase(RecSplitSubPhaseBuilding)
	rp.RecordRecSplitCFDone()
	rp.RecordRecSplitCFDone()

	log3 := logging.NewTestLogger("TEST")
	pt.LogProgress(log3, mem)
	if !log3.HasMessage("RECSPLIT:BUILDING") {
		t.Error("sub-phase should be BUILDING")
	}
	if !log3.HasMessage("2/16") {
		t.Error("should show 2/16 CFs done")
	}
}

func TestChunkStatus(t *testing.T) {
	tests := []struct {
		name   string
		status ChunkStatus
		want   bool
	}{
		{"both done", ChunkStatus{LFSDone: true, TxHashDone: true}, true},
		{"lfs only", ChunkStatus{LFSDone: true, TxHashDone: false}, false},
		{"txhash only", ChunkStatus{LFSDone: false, TxHashDone: true}, false},
		{"neither", ChunkStatus{LFSDone: false, TxHashDone: false}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.status.IsComplete(); got != tt.want {
				t.Errorf("IsComplete() = %v, want %v", got, tt.want)
			}
		})
	}
}

func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
