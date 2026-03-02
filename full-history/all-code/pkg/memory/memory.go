package memory

import (
	"runtime"
	"sync"
	"syscall"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/format"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/logging"
)

// Monitor tracks process RSS (Resident Set Size) and logs warnings when
// memory usage exceeds a configured threshold. It is checked at key points:
//   - After each chunk completion (every 10K ledgers)
//   - After each BSB instance completes its chunk slice
//   - During RecSplit build (after each CF index)
//   - In the 1-minute progress ticker
type Monitor interface {
	// Check reads current RSS and logs a warning if it exceeds the threshold.
	// Returns current RSS in bytes.
	Check() int64

	// CurrentRSSGB returns the current RSS in gigabytes.
	CurrentRSSGB() float64

	// PeakRSSGB returns the peak RSS observed since monitor creation.
	PeakRSSGB() float64

	// LogSummary logs a final memory usage summary.
	LogSummary(log logging.Logger)

	// Stop halts any background monitoring goroutines.
	Stop()
}

// MonitorConfig holds the configuration for creating a Monitor.
type MonitorConfig struct {
	// WarningThresholdGB is the RSS threshold in GB above which a warning is logged.
	// Default: 100.0 GB (appropriate for large-scale backfill machines).
	WarningThresholdGB float64

	// Logger is the scoped logger for memory warnings.
	Logger logging.Logger
}

// monitor implements Monitor with RSS tracking via Getrusage.
type monitor struct {
	mu             sync.Mutex
	thresholdBytes int64
	peakRSSBytes   int64
	warningLogged  bool // true after first threshold breach warning
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

// getRSSBytes returns the current process RSS in bytes via syscall.Getrusage.
// On macOS, ru_maxrss is in bytes. On Linux, it's in kilobytes.
func getRSSBytes() int64 {
	var rusage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		return 0
	}
	rss := rusage.Maxrss
	if runtime.GOOS == "linux" {
		rss *= 1024 // Linux ru_maxrss is in KB; convert to bytes
	}
	// macOS ru_maxrss is already in bytes — no conversion needed.
	return rss
}

func (m *monitor) Check() int64 {
	rss := getRSSBytes()

	m.mu.Lock()
	defer m.mu.Unlock()

	if rss > m.peakRSSBytes {
		m.peakRSSBytes = rss
	}

	if rss > m.thresholdBytes && !m.warningLogged {
		m.warningLogged = true
		m.log.Error("WARNING: RSS %s exceeds threshold %s",
			format.FormatBytes(rss),
			format.FormatBytes(m.thresholdBytes))
	}

	return rss
}

func (m *monitor) CurrentRSSGB() float64 {
	rss := getRSSBytes()
	m.mu.Lock()
	if rss > m.peakRSSBytes {
		m.peakRSSBytes = rss
	}
	m.mu.Unlock()
	return float64(rss) / (1024 * 1024 * 1024)
}

func (m *monitor) PeakRSSGB() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return float64(m.peakRSSBytes) / (1024 * 1024 * 1024)
}

func (m *monitor) LogSummary(log logging.Logger) {
	m.mu.Lock()
	peak := m.peakRSSBytes
	m.mu.Unlock()

	log.Info("Memory summary: peak RSS = %s", format.FormatBytes(peak))
}

func (m *monitor) Stop() {
	// No background goroutine to stop in this implementation.
}

// =============================================================================
// Nop Monitor (for tests)
// =============================================================================

// nopMonitor is a no-op Monitor for testing.
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

func (n *nopMonitor) CurrentRSSGB() float64 { return n.rssGB }
func (n *nopMonitor) PeakRSSGB() float64    { return n.rssGB }
func (n *nopMonitor) LogSummary(log logging.Logger) {
	log.Info("Memory: %s (mock)", format.FormatBytes(int64(n.rssGB*1024*1024*1024)))
}
func (n *nopMonitor) Stop() {}
