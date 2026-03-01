package backfill

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
)

// =============================================================================
// LatencyStats — Thread-Safe Percentile Tracking
// =============================================================================
//
// LatencyStats collects duration samples from concurrent goroutines and computes
// percentiles (p50, p90, p95, p99). It uses a mutex-protected slice for storage.
//
// For the backfill pipeline, this is used to track:
//   - LFS write latency (per-ledger append)
//   - TxHash write latency (per-ledger append)
//   - BSB GetLedger latency (per-ledger GCS fetch)
//   - Chunk fsync latency (per-chunk)
//   - RecSplit build latency (per-CF)

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
		helpers.FormatDuration(lp.P50),
		helpers.FormatDuration(lp.P90),
		helpers.FormatDuration(lp.P95),
		helpers.FormatDuration(lp.P99))
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

// =============================================================================
// Per-Component Stats Structs
// =============================================================================

// ChunkWriteStats holds timing and count data for a single chunk's write operation.
type ChunkWriteStats struct {
	ChunkID           uint32
	LedgersProcessed  int
	TxCount           int64
	LFSWriteTime     time.Duration
	TxHashWriteTime  time.Duration
	FsyncTime        time.Duration
	TotalTime        time.Duration
	LFSBytesWritten  int64
	TxHashBytesWritten int64
}

// BSBInstanceStats holds aggregate stats for a single BSB instance (50 chunks).
type BSBInstanceStats struct {
	InstanceID      int
	ChunksProcessed int
	ChunksSkipped   int
	TotalLedgers    int64
	TotalTx         int64
	TotalTime       time.Duration
}

// RangeStats holds aggregate stats for a full range (1000 chunks + RecSplit).
type RangeStats struct {
	RangeID          uint32
	ChunksCompleted  int
	ChunksSkipped    int
	TotalLedgers     int64
	TotalTx          int64
	LFSTotalBytes    int64
	TxHashTotalBytes int64
	IngestionTime    time.Duration
	RecSplitTime     time.Duration
	TotalTime        time.Duration
}

// RecSplitCFStats holds stats for building a single CF's RecSplit index.
type RecSplitCFStats struct {
	CFIndex   int
	CFName    string
	KeyCount  uint64
	IndexSize int64
	BuildTime time.Duration
	Skipped   bool
}

// RecSplitBuildStats holds aggregate stats for building all 16 CF indexes.
type RecSplitBuildStats struct {
	RangeID        uint32
	CFStats        [CFCount]RecSplitCFStats
	TotalKeys      uint64
	TotalIndexSize int64
	CFsSkipped     int
	TotalTime      time.Duration
}

// =============================================================================
// ProgressTracker — Aggregate Throughput Tracking
// =============================================================================
//
// ProgressTracker maintains global atomic counters that are incremented by all
// concurrent BSB instances. It produces the 1-minute progress log line showing
// aggregate throughput across all workers.
//
// All counters are atomic for lock-free concurrent access from 20+ goroutines.

// ProgressTracker tracks global backfill progress across all concurrent workers.
type ProgressTracker struct {
	startTime time.Time
	totalChunks int

	// Atomic counters — incremented by all BSB instances concurrently.
	completedChunks atomic.Int64
	totalLedgers    atomic.Int64  // +ChunkSize per chunk
	totalTx         atomic.Int64  // actual tx count per chunk

	// Per-operation latency tracking — thread-safe.
	LFSWriteLatency     *LatencyStats
	TxHashWriteLatency   *LatencyStats
	BSBGetLedgerLatency  *LatencyStats
	ChunkFsyncLatency    *LatencyStats
}

// NewProgressTracker creates a ProgressTracker for tracking throughput across
// totalChunks expected chunks.
func NewProgressTracker(totalChunks int) *ProgressTracker {
	return &ProgressTracker{
		startTime:           time.Now(),
		totalChunks:         totalChunks,
		LFSWriteLatency:     NewLatencyStats(),
		TxHashWriteLatency:  NewLatencyStats(),
		BSBGetLedgerLatency: NewLatencyStats(),
		ChunkFsyncLatency:   NewLatencyStats(),
	}
}

// RecordChunkComplete records the completion of a single chunk.
// Called by each BSB instance after a chunk is fsynced and flagged.
func (p *ProgressTracker) RecordChunkComplete(stats ChunkWriteStats) {
	p.completedChunks.Add(1)
	p.totalLedgers.Add(int64(stats.LedgersProcessed))
	p.totalTx.Add(stats.TxCount)
	p.LFSWriteLatency.Add(stats.LFSWriteTime)
	p.TxHashWriteLatency.Add(stats.TxHashWriteTime)
	p.ChunkFsyncLatency.Add(stats.FsyncTime)
}

// RecordBSBGetLedger records a single BSB GetLedger call latency.
func (p *ProgressTracker) RecordBSBGetLedger(d time.Duration) {
	p.BSBGetLedgerLatency.Add(d)
}

// CompletedChunks returns the number of chunks completed so far.
func (p *ProgressTracker) CompletedChunks() int64 {
	return p.completedChunks.Load()
}

// LogProgress formats and logs the 1-minute progress block.
//
// Format:
//
//	── Progress ──────────────────────
//	  Chunks: 1,247/3,000 complete (41.6%)
//	  THROUGHPUT: 2,048 ledgers/s | 5,120 tx/s | 12.3 chunks/min
//	  Elapsed: 1h 41m 12s | ETA: 2h 23m
//	  LFS write latency — p50: 1.2ms  p90: 3.4ms  p95: 5.1ms  p99: 12.7ms
//	  TxHash write latency — p50: 0.8ms  p90: 2.1ms  p95: 3.3ms  p99: 8.9ms
//	  BSB GetLedger latency — p50: 45ms  p90: 120ms  p95: 180ms  p99: 350ms
//	  Memory: 24.3 GB current, 26.1 GB peak
//	───────────────────────────────────
func (p *ProgressTracker) LogProgress(log Logger, mem MemoryMonitor) {
	completed := p.completedChunks.Load()
	total := int64(p.totalChunks)
	ledgers := p.totalLedgers.Load()
	txs := p.totalTx.Load()
	elapsed := time.Since(p.startTime)
	elapsedSec := elapsed.Seconds()

	pct := float64(0)
	if total > 0 {
		pct = float64(completed) / float64(total) * 100
	}

	// Throughput calculations
	var ledgersPerSec, txPerSec, chunksPerMin float64
	if elapsedSec > 0 {
		ledgersPerSec = float64(ledgers) / elapsedSec
		txPerSec = float64(txs) / elapsedSec
		chunksPerMin = float64(completed) / (elapsedSec / 60.0)
	}

	// ETA
	eta := "N/A"
	if completed > 0 && completed < total {
		remaining := total - completed
		perChunkSec := elapsedSec / float64(completed)
		etaDur := time.Duration(float64(remaining) * perChunkSec * float64(time.Second))
		eta = helpers.FormatDuration(etaDur)
	}

	log.Info("── Progress ──────────────────────────────────────")
	log.Info("  Chunks: %s/%s complete (%s)",
		helpers.FormatNumber(completed), helpers.FormatNumber(total),
		helpers.FormatPercent(pct, 1))
	log.Info("  THROUGHPUT: %s ledgers/s | %s tx/s | %.1f chunks/min",
		helpers.FormatNumber(int64(ledgersPerSec)),
		helpers.FormatNumber(int64(txPerSec)),
		chunksPerMin)
	log.Info("  Elapsed: %s | ETA: %s", helpers.FormatDuration(elapsed), eta)

	if lfs := p.LFSWriteLatency.Summary(); p.LFSWriteLatency.Count() > 0 {
		log.Info("  LFS write latency — %s", lfs.String())
	}
	if txh := p.TxHashWriteLatency.Summary(); p.TxHashWriteLatency.Count() > 0 {
		log.Info("  TxHash write latency — %s", txh.String())
	}
	if bsb := p.BSBGetLedgerLatency.Summary(); p.BSBGetLedgerLatency.Count() > 0 {
		log.Info("  BSB GetLedger latency — %s", bsb.String())
	}

	rssBytes := mem.Check()
	log.Info("  Memory: %s current, %s peak",
		helpers.FormatBytes(rssBytes),
		helpers.FormatBytes(int64(mem.PeakRSSGB()*1024*1024*1024)))
	log.Info("───────────────────────────────────────────────────")
}
