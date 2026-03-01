package backfill

import (
	"sync/atomic"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/cf"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/format"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/memory"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/stats"
)

// =============================================================================
// Per-Component Stats Structs
// =============================================================================

// ChunkWriteStats holds timing and count data for a single chunk's write operation.
type ChunkWriteStats struct {
	ChunkID            uint32
	LedgersProcessed   int
	TxCount            int64
	LFSWriteTime       time.Duration
	TxHashWriteTime    time.Duration
	FsyncTime          time.Duration
	TotalTime          time.Duration
	LFSBytesWritten    int64
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
	CFStats        [cf.Count]RecSplitCFStats
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
	startTime   time.Time
	totalChunks int

	// Atomic counters — incremented by all BSB instances concurrently.
	completedChunks atomic.Int64
	totalLedgers    atomic.Int64 // +ChunkSize per chunk
	totalTx         atomic.Int64 // actual tx count per chunk

	// Per-operation latency tracking — thread-safe.
	LFSWriteLatency     *stats.LatencyStats
	TxHashWriteLatency  *stats.LatencyStats
	BSBGetLedgerLatency *stats.LatencyStats
	ChunkFsyncLatency   *stats.LatencyStats
}

// NewProgressTracker creates a ProgressTracker for tracking throughput across
// totalChunks expected chunks.
func NewProgressTracker(totalChunks int) *ProgressTracker {
	return &ProgressTracker{
		startTime:           time.Now(),
		totalChunks:         totalChunks,
		LFSWriteLatency:     stats.NewLatencyStats(),
		TxHashWriteLatency:  stats.NewLatencyStats(),
		BSBGetLedgerLatency: stats.NewLatencyStats(),
		ChunkFsyncLatency:   stats.NewLatencyStats(),
	}
}

// RecordChunkComplete records the completion of a single chunk.
// Called by each BSB instance after a chunk is fsynced and flagged.
func (p *ProgressTracker) RecordChunkComplete(s ChunkWriteStats) {
	p.completedChunks.Add(1)
	p.totalLedgers.Add(int64(s.LedgersProcessed))
	p.totalTx.Add(s.TxCount)
	p.LFSWriteLatency.Add(s.LFSWriteTime)
	p.TxHashWriteLatency.Add(s.TxHashWriteTime)
	p.ChunkFsyncLatency.Add(s.FsyncTime)
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
func (p *ProgressTracker) LogProgress(log logging.Logger, mem memory.Monitor) {
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
		eta = format.FormatDuration(etaDur)
	}

	log.Info("── Progress ──────────────────────────────────────")
	log.Info("  Chunks: %s/%s complete (%s)",
		format.FormatNumber(completed), format.FormatNumber(total),
		format.FormatPercent(pct, 1))
	log.Info("  THROUGHPUT: %s ledgers/s | %s tx/s | %.1f chunks/min",
		format.FormatNumber(int64(ledgersPerSec)),
		format.FormatNumber(int64(txPerSec)),
		chunksPerMin)
	log.Info("  Elapsed: %s | ETA: %s", format.FormatDuration(elapsed), eta)

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
		format.FormatBytes(rssBytes),
		format.FormatBytes(int64(mem.PeakRSSGB()*1024*1024*1024)))
	log.Info("───────────────────────────────────────────────────")
}
