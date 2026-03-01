package stats

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/format"
)

// =============================================================================
// LatencyStats — Thread-Safe Percentile Tracking
// =============================================================================
//
// LatencyStats collects duration samples from concurrent goroutines and computes
// percentiles (p50, p90, p95, p99). It uses a mutex-protected slice for storage.

// LatencyPercentiles holds computed percentile values.
type LatencyPercentiles struct {
	P50 time.Duration
	P90 time.Duration
	P95 time.Duration
	P99 time.Duration
	Min time.Duration
	Max time.Duration
}

// String formats percentiles as a compact log-friendly string.
func (lp LatencyPercentiles) String() string {
	return fmt.Sprintf("p50=%s  p90=%s  p95=%s  p99=%s",
		format.FormatDuration(lp.P50),
		format.FormatDuration(lp.P90),
		format.FormatDuration(lp.P95),
		format.FormatDuration(lp.P99))
}

// LatencyStats collects latency samples and computes percentiles.
// Safe for concurrent use from multiple goroutines.
type LatencyStats struct {
	mu      sync.Mutex
	samples []time.Duration
}

// NewLatencyStats creates a new LatencyStats collector.
func NewLatencyStats() *LatencyStats {
	return &LatencyStats{
		samples: make([]time.Duration, 0, 1024),
	}
}

// Add records a latency sample. Safe for concurrent use.
func (ls *LatencyStats) Add(d time.Duration) {
	ls.mu.Lock()
	ls.samples = append(ls.samples, d)
	ls.mu.Unlock()
}

// Count returns the number of samples recorded.
func (ls *LatencyStats) Count() int {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return len(ls.samples)
}

// Summary computes and returns percentiles from all recorded samples.
// Returns zero values if no samples have been recorded.
func (ls *LatencyStats) Summary() LatencyPercentiles {
	ls.mu.Lock()
	// Copy to avoid holding lock during sort
	n := len(ls.samples)
	if n == 0 {
		ls.mu.Unlock()
		return LatencyPercentiles{}
	}
	sorted := make([]time.Duration, n)
	copy(sorted, ls.samples)
	ls.mu.Unlock()

	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	return LatencyPercentiles{
		P50: percentile(sorted, 50),
		P90: percentile(sorted, 90),
		P95: percentile(sorted, 95),
		P99: percentile(sorted, 99),
		Min: sorted[0],
		Max: sorted[n-1],
	}
}

// percentile returns the p-th percentile from a sorted slice.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	rank := p / 100.0 * float64(len(sorted)-1)
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))
	if lower == upper {
		return sorted[lower]
	}
	frac := rank - float64(lower)
	return time.Duration(float64(sorted[lower])*(1-frac) + float64(sorted[upper])*frac)
}
