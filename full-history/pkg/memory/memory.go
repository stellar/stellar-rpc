package memory

import (
	"runtime"
	"sync"
	"syscall"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/format"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/logging"
)

// Monitor tracks process memory and logs warnings when usage exceeds a
// configured threshold. Checked at key points: chunk completion, RecSplit
// phase boundaries, and the 1-minute progress ticker.
type Monitor interface {
	// Check reads current RSS and logs a warning if it exceeds the threshold.
	// Returns current RSS in bytes.
	Check() int64

	// Snapshot returns a point-in-time memory snapshot with RSS, heap, and
	// goroutine stats. More expensive than Check() due to runtime.ReadMemStats.
	// Use for periodic reporting (1-minute ticker, final summary), not per-chunk.
	Snapshot() MemSnapshot

	// PeakRSSGB returns the peak RSS observed since monitor creation.
	PeakRSSGB() float64

	// LogSummary logs a final memory usage summary.
	LogSummary(log logging.Logger)

	// Stop halts any background monitoring goroutines.
	Stop()
}

// MemSnapshot holds a point-in-time memory snapshot for logging.
type MemSnapshot struct {
	// CurrentRSS is the current resident set size in bytes.
	// Linux: from /proc/self/statm (true current RSS).
	// macOS: from runtime.MemStats.Sys (Go-only approximation; misses CGO).
	CurrentRSS int64

	// PeakRSS is the peak RSS over the process lifetime.
	// From syscall.Getrusage.Maxrss (both platforms).
	PeakRSS int64

	// HeapAlloc is bytes of live heap objects.
	HeapAlloc uint64

	// HeapSys is bytes of heap memory obtained from the OS.
	HeapSys uint64

	// NumGC is the number of completed GC cycles.
	NumGC uint32

	// NumGoroutine is the current goroutine count.
	NumGoroutine int
}

// MonitorConfig holds the configuration for creating a Monitor.
type MonitorConfig struct {
	// WarningThresholdGB is the RSS threshold in GB above which a warning is logged.
	// Default: 100.0 GB (appropriate for large-scale backfill machines).
	WarningThresholdGB float64

	// Logger is the scoped logger for memory warnings.
	Logger logging.Logger
}

// monitor implements Monitor with platform-aware RSS tracking.
type monitor struct {
	mu             sync.Mutex
	thresholdBytes int64
	peakRSSBytes   int64
	warningLogged  bool
	log            logging.Logger
}

// NewMonitor creates a Monitor that tracks RSS and warns when
// usage exceeds the threshold.
func NewMonitor(cfg MonitorConfig) Monitor {
	threshold := cfg.WarningThresholdGB
	if threshold <= 0 {
		threshold = 100.0
	}
	log := cfg.Logger
	if log == nil {
		log = logging.NewNopLogger()
	}
	return &monitor{
		thresholdBytes: int64(threshold * 1024 * 1024 * 1024),
		log:            log,
	}
}

// getPeakRSSBytes returns peak process RSS in bytes via syscall.Getrusage.
// On macOS, ru_maxrss is in bytes. On Linux, it's in kilobytes.
func getPeakRSSBytes() int64 {
	var rusage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		return 0
	}
	rss := rusage.Maxrss
	if runtime.GOOS == "linux" {
		rss *= 1024 // Linux ru_maxrss is in KB; convert to bytes
	}
	return rss
}

// getCurrentRSSBytes returns current process RSS in bytes.
// On Linux: reads /proc/self/statm for true current RSS.
// On macOS: falls back to runtime.MemStats.Sys (Go-only, misses CGO).
func getCurrentRSSBytes() int64 {
	if runtime.GOOS == "linux" {
		if rss := readProcStatmRSS(); rss > 0 {
			return rss
		}
	}
	// macOS fallback (or Linux /proc read failure): use Go stats.
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return int64(ms.Sys)
}

func (m *monitor) Check() int64 {
	rss := getCurrentRSSBytes()

	m.mu.Lock()
	defer m.mu.Unlock()

	if rss > m.peakRSSBytes {
		m.peakRSSBytes = rss
	}
	// Also check OS-reported peak in case CGO allocations push it higher.
	if osPeak := getPeakRSSBytes(); osPeak > m.peakRSSBytes {
		m.peakRSSBytes = osPeak
	}

	if rss > m.thresholdBytes && !m.warningLogged {
		m.warningLogged = true
		m.log.Error("WARNING: RSS %s exceeds threshold %s",
			format.FormatBytes(rss),
			format.FormatBytes(m.thresholdBytes))
	}

	return rss
}

func (m *monitor) Snapshot() MemSnapshot {
	currentRSS := getCurrentRSSBytes()

	m.mu.Lock()
	if currentRSS > m.peakRSSBytes {
		m.peakRSSBytes = currentRSS
	}
	if osPeak := getPeakRSSBytes(); osPeak > m.peakRSSBytes {
		m.peakRSSBytes = osPeak
	}
	peakRSS := m.peakRSSBytes
	m.mu.Unlock()

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	return MemSnapshot{
		CurrentRSS:   currentRSS,
		PeakRSS:      peakRSS,
		HeapAlloc:    ms.HeapAlloc,
		HeapSys:      ms.HeapSys,
		NumGC:        ms.NumGC,
		NumGoroutine: runtime.NumGoroutine(),
	}
}

func (m *monitor) PeakRSSGB() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if osPeak := getPeakRSSBytes(); osPeak > m.peakRSSBytes {
		m.peakRSSBytes = osPeak
	}
	return float64(m.peakRSSBytes) / (1024 * 1024 * 1024)
}

func (m *monitor) LogSummary(log logging.Logger) {
	snap := m.Snapshot()
	log.Info("  Memory summary:")
	log.Info("    Peak RSS:        %s", format.FormatBytes(snap.PeakRSS))
	log.Info("    Go heap alloc:   %s", format.FormatBytes(int64(snap.HeapAlloc)))
	log.Info("    Go heap sys:     %s", format.FormatBytes(int64(snap.HeapSys)))
	log.Info("    GC cycles:       %s", format.FormatNumber(int64(snap.NumGC)))
	log.Info("    Goroutines:      %d", snap.NumGoroutine)
}

func (m *monitor) Stop() {}

// =============================================================================
// Nop Monitor (for tests)
// =============================================================================

type nopMonitor struct {
	rssGB float64
}

// NewNopMonitor returns a Monitor that reports a fixed RSS value.
func NewNopMonitor(rssGB float64) Monitor {
	return &nopMonitor{rssGB: rssGB}
}

func (n *nopMonitor) Check() int64 {
	return int64(n.rssGB * 1024 * 1024 * 1024)
}

func (n *nopMonitor) Snapshot() MemSnapshot {
	rssBytes := int64(n.rssGB * 1024 * 1024 * 1024)
	return MemSnapshot{
		CurrentRSS:   rssBytes,
		PeakRSS:      rssBytes,
		NumGoroutine: 1,
	}
}

func (n *nopMonitor) PeakRSSGB() float64 { return n.rssGB }
func (n *nopMonitor) LogSummary(log logging.Logger) {
	log.Info("Memory: %s (mock)", format.FormatBytes(int64(n.rssGB*1024*1024*1024)))
}
func (n *nopMonitor) Stop() {}
