package backfill

import (
	"fmt"
	"sync"
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
// Progress Tracking — Per-Range Throughput
// =============================================================================
//
// ProgressTracker is a registry of per-range RangeProgress instances. Each
// range worker registers when it starts and deregisters when it completes.
// The 1-minute ticker iterates active ranges and logs per-range progress.
//
// RangeProgress holds the atomic counters and latency stats for a single range.
// All counters are atomic for lock-free concurrent access from 20+ goroutines.

// Range processing phases.
const (
	PhaseIngesting int32 = 0
	PhaseRecSplit  int32 = 1
	PhaseComplete  int32 = 2
)

// RangeProgress tracks progress for a single range. Thread-safe.
type RangeProgress struct {
	rangeID     uint32
	startTime   time.Time
	totalChunks int

	phase atomic.Int32 // PhaseIngesting, PhaseRecSplit, PhaseComplete

	// Ingestion counters — incremented by all BSB instances concurrently.
	completedChunks atomic.Int64
	totalLedgers    atomic.Int64
	totalTx         atomic.Int64

	// Per-operation latency tracking — thread-safe.
	LFSWriteLatency     *stats.LatencyStats
	TxHashWriteLatency  *stats.LatencyStats
	BSBGetLedgerLatency *stats.LatencyStats
	ChunkFsyncLatency   *stats.LatencyStats

	// RecSplit progress — incremented as each CF index completes.
	recsplitCFsDone atomic.Int32
}

// SeedCompleted sets the initial completed chunk count for resumed ranges.
// Called once before ingestion starts, with the number of chunks already done
// from a prior run. This ensures progress percentages reflect total progress,
// not just the current session.
func (r *RangeProgress) SeedCompleted(chunks int) {
	r.completedChunks.Store(int64(chunks))
}

// RecordChunkComplete records the completion of a single chunk.
// Called by each BSB instance after a chunk is fsynced and flagged.
func (r *RangeProgress) RecordChunkComplete(s ChunkWriteStats) {
	r.completedChunks.Add(1)
	r.totalLedgers.Add(int64(s.LedgersProcessed))
	r.totalTx.Add(s.TxCount)
	r.LFSWriteLatency.Add(s.LFSWriteTime)
	r.TxHashWriteLatency.Add(s.TxHashWriteTime)
	r.ChunkFsyncLatency.Add(s.FsyncTime)
}

// RecordBSBGetLedger records a single BSB GetLedger call latency.
func (r *RangeProgress) RecordBSBGetLedger(d time.Duration) {
	r.BSBGetLedgerLatency.Add(d)
}

// CompletedChunks returns the number of chunks completed so far.
func (r *RangeProgress) CompletedChunks() int64 {
	return r.completedChunks.Load()
}

// SetPhase transitions the range to a new processing phase.
func (r *RangeProgress) SetPhase(phase int32) {
	r.phase.Store(phase)
}

// RecordRecSplitCFDone increments the count of completed RecSplit CFs.
func (r *RangeProgress) RecordRecSplitCFDone() {
	r.recsplitCFsDone.Add(1)
}

// ProgressTracker is a registry of active per-range progress trackers.
// The orchestrator creates one and passes it to all range workers.
type ProgressTracker struct {
	startTime time.Time
	mu        sync.RWMutex
	ranges    map[uint32]*RangeProgress
}

// NewProgressTracker creates a ProgressTracker registry.
func NewProgressTracker() *ProgressTracker {
	return &ProgressTracker{
		startTime: time.Now(),
		ranges:    make(map[uint32]*RangeProgress),
	}
}

// RegisterRange creates and registers a RangeProgress for the given range.
// Returns the per-range tracker that should be passed to BSB instances.
func (p *ProgressTracker) RegisterRange(rangeID uint32, totalChunks int) *RangeProgress {
	rp := &RangeProgress{
		rangeID:             rangeID,
		startTime:           time.Now(),
		totalChunks:         totalChunks,
		LFSWriteLatency:     stats.NewLatencyStats(),
		TxHashWriteLatency:  stats.NewLatencyStats(),
		BSBGetLedgerLatency: stats.NewLatencyStats(),
		ChunkFsyncLatency:   stats.NewLatencyStats(),
	}
	p.mu.Lock()
	p.ranges[rangeID] = rp
	p.mu.Unlock()
	return rp
}

// DeregisterRange removes a range from the active set.
func (p *ProgressTracker) DeregisterRange(rangeID uint32) {
	p.mu.Lock()
	delete(p.ranges, rangeID)
	p.mu.Unlock()
}

// CompletedChunks returns the total completed chunks across all active ranges.
func (p *ProgressTracker) CompletedChunks() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var total int64
	for _, rp := range p.ranges {
		total += rp.completedChunks.Load()
	}
	return total
}

// TotalLedgers returns the total ledgers processed across all active ranges.
func (p *ProgressTracker) TotalLedgers() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var total int64
	for _, rp := range p.ranges {
		total += rp.totalLedgers.Load()
	}
	return total
}

// TotalTx returns the total transactions processed across all active ranges.
func (p *ProgressTracker) TotalTx() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var total int64
	for _, rp := range p.ranges {
		total += rp.totalTx.Load()
	}
	return total
}

// AggregateLatency returns combined latency stats across all active ranges
// for the final summary. Returns LFS, TxHash, BSBGetLedger, ChunkFsync.
func (p *ProgressTracker) AggregateLatency() (lfs, txh, bsb, fsync *stats.LatencyStats) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	// Return from any range — the stats objects accumulate across all workers
	// that used them. Since each range has its own stats, merge them.
	lfs = stats.NewLatencyStats()
	txh = stats.NewLatencyStats()
	bsb = stats.NewLatencyStats()
	fsync = stats.NewLatencyStats()
	for _, rp := range p.ranges {
		lfs.Merge(rp.LFSWriteLatency)
		txh.Merge(rp.TxHashWriteLatency)
		bsb.Merge(rp.BSBGetLedgerLatency)
		fsync.Merge(rp.ChunkFsyncLatency)
	}
	return
}

// LogProgress formats and logs per-range progress blocks.
func (p *ProgressTracker) LogProgress(log logging.Logger, mem memory.Monitor) {
	elapsed := time.Since(p.startTime)

	p.mu.RLock()
	// Collect range IDs and sort for deterministic output.
	rangeIDs := make([]uint32, 0, len(p.ranges))
	for id := range p.ranges {
		rangeIDs = append(rangeIDs, id)
	}
	p.mu.RUnlock()

	sortUint32s(rangeIDs)

	log.Info("── Progress (%s elapsed) ────────────────────────────", format.FormatDuration(elapsed))

	for _, id := range rangeIDs {
		p.mu.RLock()
		rp, ok := p.ranges[id]
		p.mu.RUnlock()
		if !ok {
			continue
		}
		logRangeProgress(log, rp)
	}

	rssBytes := mem.Check()
	log.Info("  Memory: %s current, %s peak",
		format.FormatBytes(rssBytes),
		format.FormatBytes(int64(mem.PeakRSSGB()*1024*1024*1024)))
	log.Info("──────────────────────────────────────────────────────")
}

// logRangeProgress logs a single range's progress block.
func logRangeProgress(log logging.Logger, rp *RangeProgress) {
	phase := rp.phase.Load()

	switch phase {
	case PhaseRecSplit:
		cfsDone := rp.recsplitCFsDone.Load()
		log.Info("  Range %04d [RECSPLIT]: %d/%d CFs built", rp.rangeID, cfsDone, cf.Count)

	case PhaseComplete:
		log.Info("  Range %04d [COMPLETE]", rp.rangeID)

	default: // PhaseIngesting
		completed := rp.completedChunks.Load()
		total := int64(rp.totalChunks)
		ledgers := rp.totalLedgers.Load()
		txs := rp.totalTx.Load()
		elapsedSec := time.Since(rp.startTime).Seconds()

		pct := float64(0)
		if total > 0 {
			pct = float64(completed) / float64(total) * 100
		}

		var ledgersPerSec, txPerSec, chunksPerMin float64
		if elapsedSec > 0 {
			ledgersPerSec = float64(ledgers) / elapsedSec
			txPerSec = float64(txs) / elapsedSec
			chunksPerMin = float64(completed) / (elapsedSec / 60.0)
		}

		eta := "N/A"
		if completed > 0 && completed < total {
			remaining := total - completed
			perChunkSec := elapsedSec / float64(completed)
			etaDur := time.Duration(float64(remaining) * perChunkSec * float64(time.Second))
			eta = format.FormatDuration(etaDur)
		}

		log.Info("  Range %04d [INGESTING]: %s/%s chunks (%s) — ETA %s",
			rp.rangeID,
			format.FormatNumber(completed), format.FormatNumber(total),
			format.FormatPercent(pct, 1), eta)
		log.Info("    %s ledgers/s | %s tx/s | %.1f chunks/min",
			format.FormatNumber(int64(ledgersPerSec)),
			format.FormatNumber(int64(txPerSec)),
			chunksPerMin)

		// Compact latency line
		var latencyParts []string
		if rp.LFSWriteLatency.Count() > 0 {
			s := rp.LFSWriteLatency.Summary()
			latencyParts = append(latencyParts, fmt.Sprintf("LFS p50=%v p90=%v", s.P50, s.P90))
		}
		if rp.BSBGetLedgerLatency.Count() > 0 {
			s := rp.BSBGetLedgerLatency.Summary()
			latencyParts = append(latencyParts, fmt.Sprintf("BSB p50=%v p90=%v", s.P50, s.P90))
		}
		if len(latencyParts) > 0 {
			log.Info("    %s", joinStrings(latencyParts, " — "))
		}
	}
}

// sortUint32s sorts a slice of uint32 in ascending order.
func sortUint32s(s []uint32) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// joinStrings joins strings with a separator.
func joinStrings(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	result := parts[0]
	for _, p := range parts[1:] {
		result += sep + p
	}
	return result
}
