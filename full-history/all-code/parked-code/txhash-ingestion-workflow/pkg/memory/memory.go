// =============================================================================
// pkg/memory/memory.go - RAM Monitoring and Memory Utilities
// =============================================================================
//
// This package provides utilities for monitoring process memory usage:
//   - RSS (Resident Set Size) monitoring
//   - Memory threshold warnings (100 GB default)
//   - Periodic memory logging
//   - RecSplit memory estimation
//
// =============================================================================

package memory

import (
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/cf"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/interfaces"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/types"
)

// =============================================================================
// Constants
// =============================================================================

const (
	// DefaultRAMWarningThresholdGB is the RSS threshold that triggers a warning.
	// If process memory exceeds this, a warning is logged.
	DefaultRAMWarningThresholdGB = 100

	// MemoryCheckIntervalBatches is how often to check memory (every N batches).
	MemoryCheckIntervalBatches = 10
)

// =============================================================================
// MemoryMonitor - Track Process Memory Usage
// =============================================================================

// MemoryMonitor tracks and reports process memory usage.
//
// USAGE:
//
//	monitor := NewMemoryMonitor(logger, 100) // 100 GB threshold
//	defer monitor.Stop()
//
//	// At checkpoints:
//	monitor.Check()
//
//	// Get current usage:
//	rssGB := monitor.CurrentRSSGB()
type MemoryMonitor struct {
	mu sync.Mutex

	// logger for warnings
	logger interfaces.Logger

	// warningThresholdGB is the RSS threshold for warnings
	warningThresholdGB float64

	// warningLogged tracks if we've logged a warning (to avoid spam)
	warningLogged bool

	// peakRSSBytes is the maximum RSS observed
	peakRSSBytes int64

	// lastCheck is when we last checked memory
	lastCheck time.Time

	// checkCount is the number of checks performed
	checkCount int
}

// NewMemoryMonitor creates a new MemoryMonitor.
//
// PARAMETERS:
//   - logger: Logger for warning messages
//   - warningThresholdGB: RSS threshold in GB for warnings
func NewMemoryMonitor(logger interfaces.Logger, warningThresholdGB float64) *MemoryMonitor {
	return &MemoryMonitor{
		logger:             logger,
		warningThresholdGB: warningThresholdGB,
	}
}

// Check reads current memory usage and logs a warning if threshold exceeded.
//
// BEHAVIOR:
//   - Gets current RSS
//   - Updates peak if necessary
//   - Logs warning if > threshold (once per breach)
//   - Returns current RSS in bytes
func (m *MemoryMonitor) Check() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	rss := GetRSSBytes()
	m.checkCount++
	m.lastCheck = time.Now()

	// Update peak
	if rss > m.peakRSSBytes {
		m.peakRSSBytes = rss
	}

	// Check threshold
	rssGB := float64(rss) / float64(types.GB)
	if rssGB > m.warningThresholdGB && !m.warningLogged {
		m.logger.Error("MEMORY WARNING: RSS %.2f GB exceeds threshold %.0f GB",
			rssGB, m.warningThresholdGB)
		m.warningLogged = true
	}

	// Reset warning if we drop below threshold
	if rssGB < m.warningThresholdGB*0.9 {
		m.warningLogged = false
	}

	return rss
}

// CurrentRSSGB returns the current RSS in gigabytes.
func (m *MemoryMonitor) CurrentRSSGB() float64 {
	return float64(GetRSSBytes()) / float64(types.GB)
}

// PeakRSSGB returns the peak RSS observed in gigabytes.
func (m *MemoryMonitor) PeakRSSGB() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return float64(m.peakRSSBytes) / float64(types.GB)
}

// LogSummary logs a summary of memory usage.
func (m *MemoryMonitor) LogSummary(logger interfaces.Logger) {
	m.mu.Lock()
	defer m.mu.Unlock()

	currentRSS := GetRSSBytes()
	logger.Info("")
	logger.Info("MEMORY SUMMARY:")
	logger.Info("  Current RSS:       %.2f GB", float64(currentRSS)/float64(types.GB))
	logger.Info("  Peak RSS:          %.2f GB", float64(m.peakRSSBytes)/float64(types.GB))
	logger.Info("  Warning Threshold: %.0f GB", m.warningThresholdGB)
	logger.Info("  Checks Performed:  %d", m.checkCount)
	logger.Info("")
}

// Stop performs cleanup (currently a no-op, but here for future extensibility).
func (m *MemoryMonitor) Stop() {
	// No-op for now
}

// Compile-time interface check
var _ interfaces.MemoryMonitor = (*MemoryMonitor)(nil)

// =============================================================================
// MemorySnapshot - Point-in-Time Memory Snapshot
// =============================================================================

// MemorySnapshot captures memory statistics at a point in time.
//
// This is useful for logging detailed memory breakdowns at key points
// in the workflow (e.g., before/after compaction, before/after RecSplit).
type MemorySnapshot struct {
	// Timestamp when snapshot was taken
	Timestamp time.Time

	// RSS is Resident Set Size in bytes (actual RAM used)
	RSS int64

	// HeapAlloc is Go heap allocation in bytes
	HeapAlloc uint64

	// HeapSys is Go heap system memory in bytes
	HeapSys uint64

	// HeapInuse is Go heap memory in use in bytes
	HeapInuse uint64

	// StackInuse is Go stack memory in use in bytes
	StackInuse uint64

	// NumGC is the number of completed GC cycles
	NumGC uint32

	// GCPauseTotal is the total GC pause time
	GCPauseTotal time.Duration
}

// TakeMemorySnapshot captures current memory statistics.
func TakeMemorySnapshot() MemorySnapshot {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return MemorySnapshot{
		Timestamp:    time.Now(),
		RSS:          GetRSSBytes(),
		HeapAlloc:    memStats.HeapAlloc,
		HeapSys:      memStats.HeapSys,
		HeapInuse:    memStats.HeapInuse,
		StackInuse:   memStats.StackInuse,
		NumGC:        memStats.NumGC,
		GCPauseTotal: time.Duration(memStats.PauseTotalNs),
	}
}

// Log logs the snapshot to the logger.
func (s *MemorySnapshot) Log(logger interfaces.Logger, label string) {
	logger.Info("%s Memory Snapshot:", label)
	logger.Info("  RSS:          %s (%.2f GB)", helpers.FormatBytes(s.RSS), float64(s.RSS)/float64(types.GB))
	logger.Info("  Heap Alloc:   %s", helpers.FormatBytes(int64(s.HeapAlloc)))
	logger.Info("  Heap Sys:     %s", helpers.FormatBytes(int64(s.HeapSys)))
	logger.Info("  Heap InUse:   %s", helpers.FormatBytes(int64(s.HeapInuse)))
	logger.Info("  Stack InUse:  %s", helpers.FormatBytes(int64(s.StackInuse)))
	logger.Info("  GC Cycles:    %d", s.NumGC)
	logger.Info("  GC Pause:     %v total", s.GCPauseTotal)
}

// RSSGB returns RSS in gigabytes.
func (s *MemorySnapshot) RSSGB() float64 {
	return float64(s.RSS) / float64(types.GB)
}

// =============================================================================
// Platform-Specific RSS Reading
// =============================================================================

// GetRSSBytes returns the current Resident Set Size in bytes.
//
// PLATFORM BEHAVIOR:
//   - Darwin/Linux: Uses syscall.Getrusage for accurate RSS
//   - Other: Falls back to runtime.MemStats (less accurate)
//
// NOTE:
//
//	On macOS, Getrusage returns RSS in bytes.
//	On Linux, Getrusage returns RSS in kilobytes (we multiply by 1024).
func GetRSSBytes() int64 {
	var rusage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		// Fallback to Go's memory stats
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		return int64(memStats.Sys)
	}

	// rusage.Maxrss is in bytes on macOS, kilobytes on Linux
	rss := rusage.Maxrss
	if runtime.GOOS == "linux" {
		rss *= 1024
	}

	return rss
}

// =============================================================================
// Utility Functions
// =============================================================================

// ForceGC triggers a garbage collection.
//
// Call this before taking memory snapshots to get accurate heap usage.
// Note: GC has overhead, so don't call frequently.
func ForceGC() {
	runtime.GC()
}

// GetGoMemStats returns current Go memory statistics.
func GetGoMemStats() runtime.MemStats {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	return memStats
}

// LogGoMemStats logs Go memory statistics to the logger.
func LogGoMemStats(logger interfaces.Logger) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	logger.Info("Go Memory Stats:")
	logger.Info("  Alloc:       %s", helpers.FormatBytes(int64(memStats.Alloc)))
	logger.Info("  TotalAlloc:  %s", helpers.FormatBytes(int64(memStats.TotalAlloc)))
	logger.Info("  Sys:         %s", helpers.FormatBytes(int64(memStats.Sys)))
	logger.Info("  NumGC:       %d", memStats.NumGC)
	logger.Info("  NumGoroutine: %d", runtime.NumGoroutine())
}

// EstimateRecSplitMemory estimates memory needed for RecSplit construction.
//
// PARAMETERS:
//   - keyCount: Number of keys to index
//
// RETURNS:
//   - Memory estimate in bytes
//
// FORMULA:
//
//	~40 bytes per key during construction
//	This is an approximation; actual usage depends on key distribution
func EstimateRecSplitMemory(keyCount uint64) int64 {
	// ~40 bytes per key during construction
	const bytesPerKey = 40
	return int64(keyCount * bytesPerKey)
}

// EstimateRecSplitMemoryAllCFs estimates memory for parallel RecSplit.
//
// PARAMETERS:
//   - cfCounts: Map of CF name to key count
//
// RETURNS:
//   - Total memory estimate in bytes for all CFs building in parallel
func EstimateRecSplitMemoryAllCFs(cfCounts map[string]uint64) int64 {
	var total int64
	for _, count := range cfCounts {
		total += EstimateRecSplitMemory(count)
	}
	return total
}

// LogRecSplitMemoryEstimate logs the memory estimate for RecSplit.
func LogRecSplitMemoryEstimate(logger interfaces.Logger, cfCounts map[string]uint64, parallel bool) {
	logger.Info("")
	logger.Info("RECSPLIT MEMORY ESTIMATE:")

	if parallel {
		total := EstimateRecSplitMemoryAllCFs(cfCounts)
		logger.Info("  Mode:             Parallel (16 CFs simultaneously)")
		logger.Info("  Estimated Memory: %s (%.2f GB)", helpers.FormatBytes(total), float64(total)/float64(types.GB))
		logger.Info("")
		logger.Info("  Per-CF estimates:")
		for _, cfName := range cf.Names {
			count := cfCounts[cfName]
			mem := EstimateRecSplitMemory(count)
			logger.Info("    CF %s: %s keys → %s",
				cfName, helpers.FormatNumber(int64(count)), helpers.FormatBytes(mem))
		}
	} else {
		var maxMem int64
		var maxCF string
		for cfName, count := range cfCounts {
			mem := EstimateRecSplitMemory(count)
			if mem > maxMem {
				maxMem = mem
				maxCF = cfName
			}
		}
		logger.Info("  Mode:             Sequential (one CF at a time)")
		logger.Info("  Peak Memory:      %s (%.2f GB) for CF %s",
			helpers.FormatBytes(maxMem), float64(maxMem)/float64(types.GB), maxCF)
	}
	logger.Info("")
}
