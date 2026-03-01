package backfill

import (
	"fmt"
	"sync"
	"syscall"
)

// =============================================================================
// Memory Monitor Implementation
// =============================================================================
//
// MemoryMonitor tracks process RSS (Resident Set Size) using syscall.Getrusage.
// It maintains peak RSS and logs warnings when a configurable threshold is exceeded.
//
// The warning is logged once per breach (when RSS crosses above the threshold)
// rather than every check, to avoid log spam during sustained high memory usage.

// MemoryMonitorConfig holds the configuration for creating a MemoryMonitor.
type MemoryMonitorConfig struct {
	// WarningThresholdGB is the RSS threshold in GB above which a warning is logged.
	// Default: 100.0 GB (appropriate for large-scale backfill machines).
	WarningThresholdGB float64

	// Logger is the scoped logger for memory warnings.
	Logger Logger
}

// memoryMonitor implements MemoryMonitor with RSS tracking via Getrusage.
type memoryMonitor struct {
	mu                 sync.Mutex
	thresholdBytes     int64
	peakRSSBytes       int64
	warningLogged      bool // true after first threshold breach warning
	log                Logger
}

// NewMemoryMonitor creates a MemoryMonitor that tracks RSS and warns when
// usage exceeds the threshold.
func NewMemoryMonitor(cfg MemoryMonitorConfig) MemoryMonitor {
	threshold := cfg.WarningThresholdGB
	if threshold <= 0 {
		threshold = 100.0
	}
	log := cfg.Logger
	if log == nil {
		log = NewNopLogger()
	}
	return &memoryMonitor{
		thresholdBytes: int64(threshold * 1024 * 1024 * 1024),
		log:            log,
	}
}

// getRSSBytes returns the current process RSS in bytes via syscall.Getrusage.
// On macOS, ru_maxrss is in bytes. On Linux, it's in kilobytes.
func getRSSBytes() int64 {
	var rusage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		return 0
	}
	// On macOS, MaxRss is in bytes. On Linux, it's in KB.
	// We use a build-tag-free approach: if the value seems too small to be bytes
	// (< 1 MB), treat it as KB. In practice, macOS returns bytes and Linux returns KB.
	rss := rusage.Maxrss
	if rss < 1024*1024 {
		// Likely Linux (value is in KB)
		rss *= 1024
	}
	return rss
}

func (m *memoryMonitor) Check() int64 {
	rss := getRSSBytes()

	m.mu.Lock()
	defer m.mu.Unlock()

	if rss > m.peakRSSBytes {
		m.peakRSSBytes = rss
	}

	if rss > m.thresholdBytes && !m.warningLogged {
		m.warningLogged = true
		m.log.Error("WARNING: RSS %.1f GB exceeds threshold %.1f GB",
			float64(rss)/(1024*1024*1024),
			float64(m.thresholdBytes)/(1024*1024*1024))
	}

	return rss
}

func (m *memoryMonitor) CurrentRSSGB() float64 {
	rss := getRSSBytes()
	m.mu.Lock()
	if rss > m.peakRSSBytes {
		m.peakRSSBytes = rss
	}
	m.mu.Unlock()
	return float64(rss) / (1024 * 1024 * 1024)
}

func (m *memoryMonitor) PeakRSSGB() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return float64(m.peakRSSBytes) / (1024 * 1024 * 1024)
}

func (m *memoryMonitor) LogSummary(log Logger) {
	m.mu.Lock()
	peak := m.peakRSSBytes
	m.mu.Unlock()

	log.Info("Memory summary: peak RSS = %.1f GB", float64(peak)/(1024*1024*1024))
}

func (m *memoryMonitor) Stop() {
	// No background goroutine to stop in this implementation.
}

// =============================================================================
// Nop Memory Monitor (for tests)
// =============================================================================

// nopMemoryMonitor is a no-op MemoryMonitor for testing.
type nopMemoryMonitor struct {
	rssGB float64
}

// NewNopMemoryMonitor returns a MemoryMonitor that reports a fixed RSS value.
func NewNopMemoryMonitor(rssGB float64) MemoryMonitor {
	return &nopMemoryMonitor{rssGB: rssGB}
}

func (n *nopMemoryMonitor) Check() int64 {
	return int64(n.rssGB * 1024 * 1024 * 1024)
}

func (n *nopMemoryMonitor) CurrentRSSGB() float64      { return n.rssGB }
func (n *nopMemoryMonitor) PeakRSSGB() float64          { return n.rssGB }
func (n *nopMemoryMonitor) LogSummary(log Logger)        { log.Info(fmt.Sprintf("Memory: %.1f GB (mock)", n.rssGB)) }
func (n *nopMemoryMonitor) Stop()                        {}
