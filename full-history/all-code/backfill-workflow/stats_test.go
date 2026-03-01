package backfill

import (
	"sync"
	"testing"
	"time"
)

func TestLatencyStatsEmpty(t *testing.T) {
	ls := NewLatencyStats()
	summary := ls.Summary()
	if summary.P50 != 0 || summary.P99 != 0 {
		t.Errorf("empty stats should return zero percentiles, got p50=%v p99=%v", summary.P50, summary.P99)
	}
	if ls.Count() != 0 {
		t.Errorf("empty stats count = %d, want 0", ls.Count())
	}
}

func TestLatencyStatsSingleSample(t *testing.T) {
	ls := NewLatencyStats()
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
	ls := NewLatencyStats()

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
	ls := NewLatencyStats()
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
	lp := LatencyPercentiles{
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
	pt := NewProgressTracker(100)

	// Record a chunk completion
	pt.RecordChunkComplete(ChunkWriteStats{
		ChunkID:          42,
		LedgersProcessed: 10000,
		TxCount:          25000,
		LFSWriteTime:    5 * time.Millisecond,
		TxHashWriteTime: 3 * time.Millisecond,
		FsyncTime:       1 * time.Millisecond,
	})

	if pt.CompletedChunks() != 1 {
		t.Errorf("CompletedChunks() = %d, want 1", pt.CompletedChunks())
	}

	// Record a BSB GetLedger call
	pt.RecordBSBGetLedger(45 * time.Millisecond)

	// LogProgress should not panic
	log := NewTestLogger("TEST")
	mem := NewNopMemoryMonitor(24.3)
	pt.LogProgress(log, mem)

	if !log.HasMessage("Progress") {
		t.Error("LogProgress should log a progress message")
	}
	if !log.HasMessage("THROUGHPUT") {
		t.Error("LogProgress should include THROUGHPUT line")
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
