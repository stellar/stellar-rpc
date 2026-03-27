// =============================================================================
// parallel_ingest.go - Parallel LFS to RocksDB Ingestion
// =============================================================================
//
// This file implements parallel ingestion that uses multiple goroutines to
// read, decompress, unmarshal, and extract transaction hashes concurrently.
//
// USAGE:
//
//	ingester := NewParallelIngester(ParallelIngestionConfig{
//	    LFSStorePath:    "/path/to/lfs",
//	    StartLedger:     1,
//	    EndLedger:       1000000,
//	    BatchSize:       5000,
//	    ParallelWorkers: 16,
//	    ParallelReaders: 4,
//	    Store:           txHashStore,
//	    Meta:            metaStore,
//	    Logger:          logger,
//	    Memory:          memoryMonitor,
//	})
//	err := ingester.Run()
//
// ARCHITECTURE:
//
//   ┌─────────────────┐    ┌─────────────────────┐    ┌──────────────┐
//   │   LFS READERS   │───>│    WORKER POOL      │───>│   COLLECTOR  │
//   │  (4 goroutines) │    │   (16 goroutines)   │    │ (1 goroutine)│
//   └─────────────────┘    └─────────────────────┘    └──────────────┘
//          │                        │                        │
//     Read compressed      Decompress+Unmarshal        Accumulate by CF
//     data from LFS        Extract txHashes            Track completion
//
// PARALLEL PIPELINE:
//
//   1. LFS Readers (4 goroutines):
//      - Read compressed data from LFS store
//      - Send LedgerWork items to workChan
//
//   2. Worker Pool (16 goroutines):
//      - Receive LedgerWork from workChan
//      - Decompress zstd data
//      - Unmarshal XDR to LedgerCloseMeta
//      - Extract txHashes and create entries
//      - Send LedgerEntries to entryChan
//
//   3. Collector (1 goroutine):
//      - Receive LedgerEntries from entryChan
//      - Accumulate entries by column family
//      - Track completion count for batch
//
// BATCH PROCESSING:
//
//   Each batch contains 5000 ledgers (configurable).
//   Within a batch:
//     1. All readers/workers process in parallel
//     2. Collector accumulates until batch complete
//     3. Write batch to RocksDB
//     4. Checkpoint to meta store
//     5. Log batch stats (every 10 batches)
//
// METRICS HANDLING:
//
//   - Wall-clock time: Tracked per batch for actual elapsed time
//   - Worker time: Summed across workers, divided by parallelism for "effective" time
//   - Per-ledger latency: Based on wall-clock time / ledger count
//   - Throughput: Ledgers per second based on wall-clock time
//
// CRASH RECOVERY:
//
//   Same as sequential mode:
//   - Resume from last_committed_ledger + 1
//   - Up to batch_size-1 ledgers may be re-ingested
//   - Duplicates handled by RocksDB compaction
//
// The ParallelIngester uses a scoped logger with [PARALLEL-INGEST] prefix.
//
// =============================================================================

package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
	"github.com/stellar/stellar-rpc/full-history/all-code/helpers/lfs"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/cf"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/interfaces"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/memory"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/types"
	"github.com/klauspost/compress/zstd"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// =============================================================================
// Constants
// =============================================================================

const (
	// DefaultParallelBatchSize is the number of ledgers per batch in parallel mode
	DefaultParallelBatchSize = 5000

	// DefaultParallelWorkers is the number of decompress/unmarshal/extract workers
	DefaultParallelWorkers = 16

	// DefaultParallelReaders is the number of LFS reader goroutines
	DefaultParallelReaders = 4

	// WorkChanBuffer is the buffer size for the work channel (compressed data)
	// ~200 items × ~150KB average = ~30 MB buffer
	WorkChanBuffer = 200

	// EntryChanBuffer is the buffer size for the entry channel
	// ~100 items × ~20KB average = ~2 MB buffer
	EntryChanBuffer = 100

	// ProgressLogInterval is how often to log progress (in batches)
	ProgressLogInterval = 10
)

// =============================================================================
// Types
// =============================================================================

// LedgerWork represents compressed ledger data ready for processing
type LedgerWork struct {
	LedgerSeq      uint32
	CompressedData []byte
	ReadTime       time.Duration // Time spent reading from LFS
}

// LedgerEntries represents extracted entries from a single ledger
type LedgerEntries struct {
	LedgerSeq   uint32
	EntriesByCF map[string][]types.Entry
	TxCount     int
	Timing      WorkerTiming
}

// WorkerTiming tracks timing for a single ledger's processing
type WorkerTiming struct {
	DecompressTime time.Duration
	UnmarshalTime  time.Duration
	ExtractTime    time.Duration
	TotalTime      time.Duration
}

// BatchTiming aggregates timing for an entire batch
type BatchTiming struct {
	// Wall-clock times (actual elapsed time)
	WallClockStart time.Time
	WallClockEnd   time.Time

	// Cumulative worker times (sum across all workers)
	TotalReadTime       time.Duration
	TotalDecompressTime time.Duration
	TotalUnmarshalTime  time.Duration
	TotalExtractTime    time.Duration
	TotalWriteTime      time.Duration
	TotalCheckpointTime time.Duration

	// Counts
	LedgerCount int
	TxCount     int
}

// WallClockDuration returns the wall-clock duration of the batch
func (bt *BatchTiming) WallClockDuration() time.Duration {
	return bt.WallClockEnd.Sub(bt.WallClockStart)
}

// LedgersPerSecond returns the throughput based on wall-clock time
func (bt *BatchTiming) LedgersPerSecond() float64 {
	wallSec := bt.WallClockDuration().Seconds()
	if wallSec == 0 {
		return 0
	}
	return float64(bt.LedgerCount) / wallSec
}

// TxPerSecond returns the transaction throughput based on wall-clock time
func (bt *BatchTiming) TxPerSecond() float64 {
	wallSec := bt.WallClockDuration().Seconds()
	if wallSec == 0 {
		return 0
	}
	return float64(bt.TxCount) / wallSec
}

// AvgLedgerLatency returns average wall-clock time per ledger
func (bt *BatchTiming) AvgLedgerLatency() time.Duration {
	if bt.LedgerCount == 0 {
		return 0
	}
	return bt.WallClockDuration() / time.Duration(bt.LedgerCount)
}

// =============================================================================
// ParallelIngester - Main Parallel Ingestion Component
// =============================================================================

// ParallelIngestionConfig holds configuration for creating a ParallelIngester.
type ParallelIngestionConfig struct {
	// LFSStorePath is the path to the LFS ledger store (required)
	LFSStorePath string

	// StartLedger is the first ledger to ingest (required)
	StartLedger uint32

	// EndLedger is the last ledger to ingest (required)
	EndLedger uint32

	// BatchSize is the number of ledgers per batch (default: 5000)
	BatchSize int

	// ParallelWorkers is the number of decompress/unmarshal/extract workers (default: 16)
	ParallelWorkers int

	// ParallelReaders is the number of LFS reader goroutines (default: 4)
	ParallelReaders int

	// Store is the TxHash store to write to (required)
	Store interfaces.TxHashStore

	// Meta is the meta store for checkpointing (required)
	Meta interfaces.MetaStore

	// Logger is the parent logger (required)
	// A scoped logger with [PARALLEL-INGEST] prefix will be created internally
	Logger interfaces.Logger

	// Memory is the memory monitor for tracking RAM usage (optional)
	Memory interfaces.MemoryMonitor
}

// ParallelIngester handles parallel ingestion from LFS to RocksDB.
//
// The ParallelIngester is designed to be composable - it takes its dependencies
// via ParallelIngestionConfig and uses a scoped logger for all output.
type ParallelIngester struct {
	config ParallelIngestionConfig
	store  interfaces.TxHashStore
	meta   interfaces.MetaStore
	log    interfaces.Logger // Scoped logger with [PARALLEL-INGEST] prefix
	memory interfaces.MemoryMonitor

	// Channels
	workChan  chan LedgerWork
	entryChan chan LedgerEntries

	// Statistics
	aggStats *ParallelAggregatedStats
	cfCounts map[string]uint64 // Cumulative counts from meta store

	// Synchronization
	readerWg  sync.WaitGroup
	workerWg  sync.WaitGroup
	closeOnce sync.Once
}

// NewParallelIngester creates a new ParallelIngester with the given configuration.
//
// The ParallelIngester creates a scoped logger with [PARALLEL-INGEST] prefix internally,
// so all log messages are automatically prefixed.
func NewParallelIngester(config ParallelIngestionConfig) *ParallelIngester {
	if config.LFSStorePath == "" {
		panic("NewParallelIngester: LFSStorePath is required")
	}
	if config.Store == nil {
		panic("NewParallelIngester: Store is required")
	}
	if config.Meta == nil {
		panic("NewParallelIngester: Meta is required")
	}
	if config.Logger == nil {
		panic("NewParallelIngester: Logger is required")
	}

	// Initialize CF counts from meta store
	cfCounts, err := config.Meta.GetCFCounts()
	if err != nil || cfCounts == nil {
		cfCounts = make(map[string]uint64)
		for _, cfName := range cf.Names {
			cfCounts[cfName] = 0
		}
	}

	return &ParallelIngester{
		config:    config,
		store:     config.Store,
		meta:      config.Meta,
		log:       config.Logger.WithScope("PARALLEL-INGEST"),
		memory:    config.Memory,
		workChan:  make(chan LedgerWork, WorkChanBuffer),
		entryChan: make(chan LedgerEntries, EntryChanBuffer),
		aggStats:  NewParallelAggregatedStats(config.ParallelWorkers, config.ParallelReaders),
		cfCounts:  cfCounts,
	}
}

// Run executes the parallel ingestion
func (pi *ParallelIngester) Run() error {
	pi.log.Separator()
	pi.log.Info("                    PARALLEL INGESTION PHASE")
	pi.log.Separator()
	pi.log.Info("")
	pi.log.Info("LFS Store:        %s", pi.config.LFSStorePath)
	pi.log.Info("Ledger Range:     %d - %d", pi.config.StartLedger, pi.config.EndLedger)
	pi.log.Info("Total Ledgers:    %s", helpers.FormatNumber(int64(pi.config.EndLedger-pi.config.StartLedger+1)))
	pi.log.Info("Batch Size:       %d ledgers", pi.config.BatchSize)
	pi.log.Info("Parallel Workers: %d (decompress + unmarshal + extract)", pi.config.ParallelWorkers)
	pi.log.Info("Parallel Readers: %d (LFS read)", pi.config.ParallelReaders)
	pi.log.Info("")

	totalLedgers := int(pi.config.EndLedger - pi.config.StartLedger + 1)
	totalBatches := (totalLedgers + pi.config.BatchSize - 1) / pi.config.BatchSize

	pi.log.Info("Total Batches:    %d", totalBatches)
	pi.log.Info("")

	// Process ledgers in batches
	currentLedger := pi.config.StartLedger
	batchNum := 0

	for currentLedger <= pi.config.EndLedger {
		batchNum++

		// Calculate batch range
		batchStart := currentLedger
		batchEnd := currentLedger + uint32(pi.config.BatchSize) - 1
		if batchEnd > pi.config.EndLedger {
			batchEnd = pi.config.EndLedger
		}

		// Process batch
		timing, err := pi.processBatch(batchNum, batchStart, batchEnd)
		if err != nil {
			return fmt.Errorf("failed to process batch %d: %w", batchNum, err)
		}

		// Update aggregate stats
		pi.aggStats.AddBatch(timing)

		// Log progress every N batches
		if batchNum%ProgressLogInterval == 0 || batchEnd == pi.config.EndLedger {
			pi.logProgress(batchNum, totalBatches, batchEnd)
		}

		// Memory check every 10 batches
		if batchNum%10 == 0 {
			pi.memory.Check()
		}

		currentLedger = batchEnd + 1
	}

	// Flush all MemTables to disk
	pi.log.Info("")
	pi.log.Info("Flushing all MemTables to disk...")
	flushStart := time.Now()
	if err := pi.store.FlushAll(); err != nil {
		return fmt.Errorf("failed to flush MemTables: %w", err)
	}
	pi.log.Info("Flush completed in %s", helpers.FormatDuration(time.Since(flushStart)))

	// Log final summary
	pi.aggStats.LogSummary(pi.log)
	pi.logCFSummary()
	pi.memory.LogSummary(pi.log)

	pi.log.Sync()

	return nil
}

// processBatch processes a single batch of ledgers
func (pi *ParallelIngester) processBatch(batchNum int, startLedger, endLedger uint32) (*BatchTiming, error) {
	timing := &BatchTiming{
		WallClockStart: time.Now(),
		LedgerCount:    int(endLedger - startLedger + 1),
	}

	// Prepare collection structures
	entriesByCF := make(map[string][]types.Entry)
	for _, cfName := range cf.Names {
		entriesByCF[cfName] = make([]types.Entry, 0, timing.LedgerCount*25) // ~25 tx per ledger avg
	}
	txCountByCF := make(map[string]int)

	// Error channel for readers/workers
	errChan := make(chan error, pi.config.ParallelReaders+pi.config.ParallelWorkers)

	// Completion tracking
	var completedLedgers int64

	// Start workers (before readers to be ready to consume)
	for w := 0; w < pi.config.ParallelWorkers; w++ {
		pi.workerWg.Add(1)
		go pi.worker(w, errChan)
	}

	// Start readers
	ledgersPerReader := timing.LedgerCount / pi.config.ParallelReaders
	remainder := timing.LedgerCount % pi.config.ParallelReaders

	readerStart := startLedger
	for r := 0; r < pi.config.ParallelReaders; r++ {
		count := ledgersPerReader
		if r < remainder {
			count++
		}
		if count == 0 {
			continue
		}

		readerEnd := readerStart + uint32(count) - 1
		pi.readerWg.Add(1)
		go pi.reader(r, readerStart, readerEnd, errChan)
		readerStart = readerEnd + 1
	}

	// Collector: accumulate entries until batch complete
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)

		for entries := range pi.entryChan {
			// Accumulate entries
			for cf, cfEntries := range entries.EntriesByCF {
				entriesByCF[cf] = append(entriesByCF[cf], cfEntries...)
				txCountByCF[cf] += len(cfEntries)
			}

			// Accumulate timing
			timing.TotalDecompressTime += entries.Timing.DecompressTime
			timing.TotalUnmarshalTime += entries.Timing.UnmarshalTime
			timing.TotalExtractTime += entries.Timing.ExtractTime
			timing.TxCount += entries.TxCount

			atomic.AddInt64(&completedLedgers, 1)
		}
	}()

	// Wait for all readers to finish
	pi.readerWg.Wait()
	close(pi.workChan)

	// Wait for all workers to finish
	pi.workerWg.Wait()
	close(pi.entryChan)

	// Wait for collector to finish
	<-collectorDone

	// Check for errors
	select {
	case err := <-errChan:
		return nil, err
	default:
	}

	// Reset channels for next batch
	pi.workChan = make(chan LedgerWork, WorkChanBuffer)
	pi.entryChan = make(chan LedgerEntries, EntryChanBuffer)

	// Write to RocksDB
	writeStart := time.Now()
	if _, err := pi.store.WriteBatch(entriesByCF); err != nil {
		return nil, fmt.Errorf("failed to write batch to RocksDB: %w", err)
	}
	timing.TotalWriteTime = time.Since(writeStart)

	// Update cumulative CF counts
	for cf, count := range txCountByCF {
		pi.cfCounts[cf] += uint64(count)
	}

	// Checkpoint to meta store
	checkpointStart := time.Now()
	if err := pi.meta.CommitBatchProgress(endLedger, pi.cfCounts); err != nil {
		return nil, fmt.Errorf("failed to checkpoint batch: %w", err)
	}
	timing.TotalCheckpointTime = time.Since(checkpointStart)

	timing.WallClockEnd = time.Now()

	// Log batch summary
	pi.logBatchSummary(batchNum, startLedger, endLedger, timing)

	return timing, nil
}

// reader reads RAW compressed ledger data from LFS (no decompression).
// Workers handle the CPU-intensive decompression and unmarshaling.
func (pi *ParallelIngester) reader(id int, startSeq, endSeq uint32, errChan chan<- error) {
	defer pi.readerWg.Done()

	// Use RawLedgerIterator - only reads compressed bytes, no decompression
	iterator, err := lfs.NewLFSRawLedgerIterator(pi.config.LFSStorePath, startSeq, endSeq)
	if err != nil {
		select {
		case errChan <- fmt.Errorf("reader %d: failed to create iterator: %w", id, err):
		default:
		}
		return
	}
	defer iterator.Close()

	for {
		// Read raw compressed data - NO decompression here
		data, hasMore, err := iterator.Next()
		if err != nil {
			select {
			case errChan <- fmt.Errorf("reader %d: failed to read ledger %d: %w", id, data.LedgerSeq, err):
			default:
			}
			return
		}
		if !hasMore {
			break
		}

		// Send raw compressed bytes to worker for processing
		pi.workChan <- LedgerWork{
			LedgerSeq:      data.LedgerSeq,
			CompressedData: data.CompressedData, // Actually compressed now!
			ReadTime:       data.ReadTime,
		}
	}
}

// worker processes ledger data (decompress, unmarshal, extract)
func (pi *ParallelIngester) worker(id int, errChan chan<- error) {
	defer pi.workerWg.Done()

	// Create a zstd decoder for this worker - workers do actual decompression
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		select {
		case errChan <- fmt.Errorf("worker %d: failed to create zstd decoder: %w", id, err):
		default:
		}
		return
	}
	defer decoder.Close()

	// Reusable ledger sequence bytes
	ledgerSeqBytes := make([]byte, 4)

	for work := range pi.workChan {
		timing := WorkerTiming{}
		totalStart := time.Now()

		// Decompress - workers now do actual decompression (parallelized!)
		decompressStart := time.Now()
		uncompressed, err := decoder.DecodeAll(work.CompressedData, nil)
		if err != nil {
			select {
			case errChan <- fmt.Errorf("worker %d: decompress failed for ledger %d: %w", id, work.LedgerSeq, err):
			default:
			}
			return
		}
		timing.DecompressTime = time.Since(decompressStart)

		// Unmarshal XDR
		unmarshalStart := time.Now()
		var lcm xdr.LedgerCloseMeta
		if err := lcm.UnmarshalBinary(uncompressed); err != nil {
			select {
			case errChan <- fmt.Errorf("worker %d: failed to unmarshal ledger %d: %w", id, work.LedgerSeq, err):
			default:
			}
			return
		}
		timing.UnmarshalTime = time.Since(unmarshalStart)

		// Extract transaction hashes
		extractStart := time.Now()
		entriesByCF := make(map[string][]types.Entry)
		for _, cfName := range cf.Names {
			entriesByCF[cfName] = make([]types.Entry, 0, 32)
		}

		txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
			network.PublicNetworkPassphrase, lcm)
		if err != nil {
			select {
			case errChan <- fmt.Errorf("worker %d: failed to create tx reader for ledger %d: %w", id, work.LedgerSeq, err):
			default:
			}
			return
		}

		binary.BigEndian.PutUint32(ledgerSeqBytes, work.LedgerSeq)

		txCount := 0
		for {
			tx, err := txReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				txReader.Close()
				select {
				case errChan <- fmt.Errorf("worker %d: failed to read tx from ledger %d: %w", id, work.LedgerSeq, err):
				default:
				}
				return
			}

			txHash := tx.Result.TransactionHash[:]
			cfName := cf.GetName(txHash)

			entriesByCF[cfName] = append(entriesByCF[cfName], types.Entry{
				Key:   copyBytes(txHash),
				Value: copyBytes(ledgerSeqBytes),
			})
			txCount++
		}
		txReader.Close()
		timing.ExtractTime = time.Since(extractStart)
		timing.TotalTime = time.Since(totalStart)

		// Send to collector
		pi.entryChan <- LedgerEntries{
			LedgerSeq:   work.LedgerSeq,
			EntriesByCF: entriesByCF,
			TxCount:     txCount,
			Timing:      timing,
		}
	}
}

// logBatchSummary logs a summary of a single batch
func (pi *ParallelIngester) logBatchSummary(batchNum int, startLedger, endLedger uint32, timing *BatchTiming) {
	// Calculate effective per-phase time (cumulative worker time / parallelism)
	// This gives a sense of "if we had 1 worker, how long would each phase take"
	effectiveWorkers := float64(pi.config.ParallelWorkers)

	pi.log.Info("Batch %d: ledgers %d-%d | %s txs | wall=%s | %.0f ledgers/sec | %.0f tx/sec",
		batchNum, startLedger, endLedger, helpers.FormatNumber(int64(timing.TxCount)),
		helpers.FormatDuration(timing.WallClockDuration().Truncate(time.Millisecond)),
		timing.LedgersPerSecond(), timing.TxPerSecond())

	// Only log detailed timing breakdown occasionally (every ProgressLogInterval batches)
	if batchNum%ProgressLogInterval == 0 {
		pi.log.Info("  Timing breakdown (cumulative across %d workers, effective=cumulative/%d):",
			pi.config.ParallelWorkers, pi.config.ParallelWorkers)
		pi.log.Info("    Decompress: %s (eff: %s/ledger)",
			helpers.FormatDuration(timing.TotalDecompressTime),
			helpers.FormatDuration(time.Duration(float64(timing.TotalDecompressTime)/float64(timing.LedgerCount))))
		pi.log.Info("    Unmarshal:  %s (eff: %s/ledger)",
			helpers.FormatDuration(timing.TotalUnmarshalTime),
			helpers.FormatDuration(time.Duration(float64(timing.TotalUnmarshalTime)/float64(timing.LedgerCount))))
		pi.log.Info("    Extract:    %s (eff: %s/ledger)",
			helpers.FormatDuration(timing.TotalExtractTime),
			helpers.FormatDuration(time.Duration(float64(timing.TotalExtractTime)/float64(timing.LedgerCount))))
		pi.log.Info("    Write:      %s (eff: %s/ledger)",
			helpers.FormatDuration(timing.TotalWriteTime),
			helpers.FormatDuration(time.Duration(float64(timing.TotalWriteTime)/float64(timing.LedgerCount))))
		pi.log.Info("    Checkpoint: %s", helpers.FormatDuration(timing.TotalCheckpointTime))
		pi.log.Info("  Parallelism efficiency: %.1f%% (wall=%s vs serial_work=%s)",
			100.0*float64(timing.TotalDecompressTime+timing.TotalUnmarshalTime+timing.TotalExtractTime)/
				(effectiveWorkers*float64(timing.WallClockDuration()-timing.TotalWriteTime-timing.TotalCheckpointTime)),
			helpers.FormatDuration(timing.WallClockDuration()-timing.TotalWriteTime-timing.TotalCheckpointTime),
			helpers.FormatDuration(timing.TotalDecompressTime+timing.TotalUnmarshalTime+timing.TotalExtractTime))
	}
}

// logProgress logs overall progress
func (pi *ParallelIngester) logProgress(batchNum, totalBatches int, currentLedger uint32) {
	stats := pi.aggStats

	pct := float64(batchNum) / float64(totalBatches) * 100
	elapsed := stats.Elapsed()

	// Calculate ETA
	var eta time.Duration
	if batchNum > 0 {
		remainingBatches := totalBatches - batchNum
		avgBatchTime := elapsed / time.Duration(batchNum)
		eta = avgBatchTime * time.Duration(remainingBatches)
	}

	pi.log.Info("")
	pi.log.Separator()
	pi.log.Info("PROGRESS: %d/%d batches (%.1f%%) | elapsed=%s | ETA=%s",
		batchNum, totalBatches, pct,
		helpers.FormatDuration(elapsed.Truncate(time.Second)),
		helpers.FormatDuration(eta.Truncate(time.Second)))
	pi.log.Info("  Ledgers: %s | Transactions: %s",
		helpers.FormatNumber(int64(stats.TotalLedgers)),
		helpers.FormatNumber(int64(stats.TotalTx)))
	pi.log.Info("  Throughput: %.0f ledgers/sec | %.0f tx/sec",
		stats.OverallLedgersPerSecond(), stats.OverallTxPerSecond())
	pi.log.Info("  Avg latency: %s/ledger (wall-clock)",
		helpers.FormatDuration(stats.AvgWallClockPerLedger()))
	pi.log.Separator()
	pi.log.Info("")
}

// logCFSummary logs transaction counts per column family
func (pi *ParallelIngester) logCFSummary() {
	var total uint64
	for _, count := range pi.cfCounts {
		total += count
	}

	pi.log.Info("TRANSACTIONS BY COLUMN FAMILY:")
	for _, cfName := range cf.Names {
		count := pi.cfCounts[cfName]
		pct := float64(count) / float64(total) * 100
		pi.log.Info("  CF %s: %12s (%.2f%%)", cfName, helpers.FormatNumber(int64(count)), pct)
	}
	pi.log.Info("")
}

// GetCFCounts returns the current CF counts
func (pi *ParallelIngester) GetCFCounts() map[string]uint64 {
	counts := make(map[string]uint64)
	for cf, count := range pi.cfCounts {
		counts[cf] = count
	}
	return counts
}

// =============================================================================
// ParallelAggregatedStats - Statistics across all batches
// =============================================================================

// ParallelAggregatedStats accumulates statistics across batches for parallel ingestion
type ParallelAggregatedStats struct {
	mu sync.Mutex

	// Configuration
	ParallelWorkers int
	ParallelReaders int

	// Counts
	TotalBatches int
	TotalLedgers int
	TotalTx      int

	// Wall-clock timing (actual elapsed)
	TotalWallClock time.Duration

	// Cumulative worker times (sum across all workers)
	TotalReadTime       time.Duration
	TotalDecompressTime time.Duration
	TotalUnmarshalTime  time.Duration
	TotalExtractTime    time.Duration
	TotalWriteTime      time.Duration
	TotalCheckpointTime time.Duration

	// Start time
	StartTime time.Time
}

// NewParallelAggregatedStats creates a new stats accumulator
func NewParallelAggregatedStats(workers, readers int) *ParallelAggregatedStats {
	return &ParallelAggregatedStats{
		ParallelWorkers: workers,
		ParallelReaders: readers,
		StartTime:       time.Now(),
	}
}

// AddBatch adds a batch's timing to the aggregate
func (pas *ParallelAggregatedStats) AddBatch(timing *BatchTiming) {
	pas.mu.Lock()
	defer pas.mu.Unlock()

	pas.TotalBatches++
	pas.TotalLedgers += timing.LedgerCount
	pas.TotalTx += timing.TxCount
	pas.TotalWallClock += timing.WallClockDuration()
	pas.TotalReadTime += timing.TotalReadTime
	pas.TotalDecompressTime += timing.TotalDecompressTime
	pas.TotalUnmarshalTime += timing.TotalUnmarshalTime
	pas.TotalExtractTime += timing.TotalExtractTime
	pas.TotalWriteTime += timing.TotalWriteTime
	pas.TotalCheckpointTime += timing.TotalCheckpointTime
}

// Elapsed returns time since start
func (pas *ParallelAggregatedStats) Elapsed() time.Duration {
	pas.mu.Lock()
	defer pas.mu.Unlock()
	return time.Since(pas.StartTime)
}

// OverallLedgersPerSecond returns overall throughput based on wall-clock
func (pas *ParallelAggregatedStats) OverallLedgersPerSecond() float64 {
	pas.mu.Lock()
	defer pas.mu.Unlock()

	elapsed := time.Since(pas.StartTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(pas.TotalLedgers) / elapsed
}

// OverallTxPerSecond returns overall transaction throughput
func (pas *ParallelAggregatedStats) OverallTxPerSecond() float64 {
	pas.mu.Lock()
	defer pas.mu.Unlock()

	elapsed := time.Since(pas.StartTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(pas.TotalTx) / elapsed
}

// AvgWallClockPerLedger returns average wall-clock time per ledger
func (pas *ParallelAggregatedStats) AvgWallClockPerLedger() time.Duration {
	pas.mu.Lock()
	defer pas.mu.Unlock()

	if pas.TotalLedgers == 0 {
		return 0
	}
	return pas.TotalWallClock / time.Duration(pas.TotalLedgers)
}

// LogSummary logs the final summary
func (pas *ParallelAggregatedStats) LogSummary(logger interfaces.Logger) {
	pas.mu.Lock()
	defer pas.mu.Unlock()

	elapsed := time.Since(pas.StartTime)

	logger.Separator()
	logger.Info("                    PARALLEL INGESTION SUMMARY")
	logger.Separator()
	logger.Info("")
	logger.Info("CONFIGURATION:")
	logger.Info("  Parallel Workers:  %d", pas.ParallelWorkers)
	logger.Info("  Parallel Readers:  %d", pas.ParallelReaders)
	logger.Info("")
	logger.Info("TOTALS:")
	logger.Info("  Batches:           %d", pas.TotalBatches)
	logger.Info("  Ledgers:           %s", helpers.FormatNumber(int64(pas.TotalLedgers)))
	logger.Info("  Transactions:      %s", helpers.FormatNumber(int64(pas.TotalTx)))
	logger.Info("")
	logger.Info("WALL-CLOCK TIMING:")
	logger.Info("  Total Elapsed:     %s", helpers.FormatDuration(elapsed.Truncate(time.Second)))
	logger.Info("  Avg per Ledger:    %s", helpers.FormatDuration(pas.TotalWallClock/time.Duration(pas.TotalLedgers)))
	logger.Info("")
	logger.Info("THROUGHPUT (based on wall-clock):")
	logger.Info("  Ledgers/sec:       %.0f", float64(pas.TotalLedgers)/elapsed.Seconds())
	logger.Info("  Transactions/sec:  %.0f", float64(pas.TotalTx)/elapsed.Seconds())
	logger.Info("")

	// Calculate effective times (cumulative / parallelism)
	// This shows how much work was done per component
	logger.Info("CUMULATIVE WORKER TIME (sum across all %d workers):", pas.ParallelWorkers)
	logger.Info("  Decompress:        %s", helpers.FormatDuration(pas.TotalDecompressTime))
	logger.Info("  Unmarshal:         %s", helpers.FormatDuration(pas.TotalUnmarshalTime))
	logger.Info("  Extract:           %s", helpers.FormatDuration(pas.TotalExtractTime))
	logger.Info("  Write (serial):    %s", helpers.FormatDuration(pas.TotalWriteTime))
	logger.Info("  Checkpoint:        %s", helpers.FormatDuration(pas.TotalCheckpointTime))
	logger.Info("")

	// Effective per-ledger times (cumulative / ledger count)
	// This shows average time spent per ledger across all workers
	if pas.TotalLedgers > 0 {
		logger.Info("EFFECTIVE TIME PER LEDGER (cumulative / ledger count):")
		logger.Info("  Decompress:        %s", helpers.FormatDuration(pas.TotalDecompressTime/time.Duration(pas.TotalLedgers)))
		logger.Info("  Unmarshal:         %s", helpers.FormatDuration(pas.TotalUnmarshalTime/time.Duration(pas.TotalLedgers)))
		logger.Info("  Extract:           %s", helpers.FormatDuration(pas.TotalExtractTime/time.Duration(pas.TotalLedgers)))
		logger.Info("  Write:             %s", helpers.FormatDuration(pas.TotalWriteTime/time.Duration(pas.TotalLedgers)))
		logger.Info("")
	}

	// Parallelism efficiency
	totalWork := pas.TotalDecompressTime + pas.TotalUnmarshalTime + pas.TotalExtractTime
	parallelizableElapsed := elapsed - pas.TotalWriteTime - pas.TotalCheckpointTime
	if parallelizableElapsed > 0 {
		efficiency := float64(totalWork) / (float64(pas.ParallelWorkers) * float64(parallelizableElapsed)) * 100
		logger.Info("PARALLELISM:")
		logger.Info("  Theoretical serial time: %s", helpers.FormatDuration(totalWork))
		logger.Info("  Actual parallel time:    %s", helpers.FormatDuration(parallelizableElapsed))
		logger.Info("  Speedup:                 %.1fx", float64(totalWork)/float64(parallelizableElapsed))
		logger.Info("  Efficiency:              %.1f%%", efficiency)
		logger.Info("")
	}
}

// =============================================================================
// Convenience Function
// =============================================================================

// RunParallelIngestion is a convenience function to run parallel ingestion.
//
// Deprecated: Use NewParallelIngester(ParallelIngestionConfig{...}).Run() instead.
// This function is kept for backward compatibility.
//
// PARAMETERS:
//   - config: Main configuration
//   - store: RocksDB data store
//   - meta: Meta store for checkpointing
//   - logger: Logger
//   - memory: Memory monitor
//   - startFromLedger: First ledger to ingest (may be > config.StartLedger if resuming)
//
// RETURNS:
//   - error if ingestion fails
func RunParallelIngestion(
	config *Config,
	store interfaces.TxHashStore,
	meta interfaces.MetaStore,
	logger interfaces.Logger,
	mem *memory.MemoryMonitor,
	startFromLedger uint32,
) error {
	ingester := NewParallelIngester(ParallelIngestionConfig{
		LFSStorePath:    config.LFSStorePath,
		StartLedger:     startFromLedger,
		EndLedger:       config.EndLedger,
		BatchSize:       config.ParallelBatchSize,
		ParallelWorkers: config.ParallelWorkers,
		ParallelReaders: config.ParallelReaders,
		Store:           store,
		Meta:            meta,
		Logger:          logger,
		Memory:          mem,
	})
	return ingester.Run()
}
