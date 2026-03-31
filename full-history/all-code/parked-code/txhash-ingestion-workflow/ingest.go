// =============================================================================
// ingest.go - LFS to RocksDB Ingestion
// =============================================================================
//
// This file implements the ingestion phase that reads ledgers from LFS,
// extracts transaction hashes, and writes txHash→ledgerSeq mappings to RocksDB.
//
// USAGE:
//
//	ingester := NewIngester(ingest.Config{
//	    LFSStorePath: "/path/to/lfs",
//	    StartLedger:  1,
//	    EndLedger:    1000000,
//	    BatchSize:    1000,
//	    Store:        txHashStore,
//	    Meta:         metaStore,
//	    Logger:       logger,
//	    Memory:       memoryMonitor,
//	})
//	err := ingester.Run()
//
// INGESTION WORKFLOW:
//
//	1. Read ledgers from LFS store (using LedgerIterator)
//	2. Extract transaction hashes from each ledger
//	3. Accumulate entries in a batch (1000 ledgers)
//	4. Write batch to RocksDB
//	5. Checkpoint progress to meta store
//	6. Repeat until all ledgers are processed
//
// CRASH RECOVERY:
//
//	On restart, ingestion resumes from last_committed_ledger + 1.
//	Up to 999 ledgers may be re-ingested (duplicates are fine - compaction handles them).
//	CF counts are derived from the meta store, ensuring accuracy.
//
// BATCH SIZE:
//
//	Each batch contains exactly 1000 ledgers (the last batch may be smaller).
//	This provides a good balance between:
//	  - Checkpoint frequency (reasonable recovery time)
//	  - Write efficiency (amortized RocksDB overhead)
//	  - Memory usage (manageable batch size)
//
// ERROR HANDLING:
//
//	- Ledger parse errors: ABORT (fatal - data integrity issue)
//	- RocksDB write errors: ABORT (fatal - storage issue)
//	- Meta store errors: ABORT (fatal - crash recovery compromised)
//
// The Ingester uses a scoped logger with [INGEST] prefix for all log messages.
//
// =============================================================================

package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
	"github.com/stellar/stellar-rpc/full-history/all-code/helpers/lfs"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/cf"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/interfaces"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/memory"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/stats"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/types"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// =============================================================================
// Ingester - Main Ingestion Component
// =============================================================================

// IngestionConfig holds configuration for creating an Ingester.
type IngestionConfig struct {
	// LFSStorePath is the path to the LFS ledger store (required)
	LFSStorePath string

	// StartLedger is the first ledger to ingest (required)
	StartLedger uint32

	// EndLedger is the last ledger to ingest (required)
	EndLedger uint32

	// BatchSize is the number of ledgers per checkpoint (default: 1000)
	BatchSize int

	// Store is the TxHash store to write to (required)
	Store interfaces.TxHashStore

	// Meta is the meta store for checkpointing (required)
	Meta interfaces.MetaStore

	// Logger is the parent logger (required)
	// A scoped logger with [INGEST] prefix will be created internally
	Logger interfaces.Logger

	// Memory is the memory monitor for tracking RAM usage (optional)
	Memory interfaces.MemoryMonitor
}

// Ingester handles reading from LFS and writing to RocksDB.
//
// The Ingester is designed to be composable - it takes its dependencies
// via IngestionConfig and uses a scoped logger for all output.
type Ingester struct {
	config IngestionConfig
	store  interfaces.TxHashStore
	meta   interfaces.MetaStore
	log    interfaces.Logger // Scoped logger with [INGEST] prefix
	memory interfaces.MemoryMonitor

	// Statistics
	aggStats *stats.AggregatedStats

	// Current batch state
	currentBatch   *stats.BatchStats
	entriesByCF    map[string][]types.Entry
	cfCounts       map[string]uint64 // Cumulative counts from meta store
	batchEntrySize int
}

// NewIngester creates a new Ingester with the given configuration.
//
// The Ingester creates a scoped logger with [INGEST] prefix internally,
// so all log messages are automatically prefixed.
func NewIngester(config IngestionConfig) *Ingester {
	if config.LFSStorePath == "" {
		panic("NewIngester: LFSStorePath is required")
	}
	if config.Store == nil {
		panic("NewIngester: Store is required")
	}
	if config.Meta == nil {
		panic("NewIngester: Meta is required")
	}
	if config.Logger == nil {
		panic("NewIngester: Logger is required")
	}

	// Initialize CF counts from meta store
	cfCounts, err := config.Meta.GetCFCounts()
	if err != nil || cfCounts == nil {
		cfCounts = make(map[string]uint64)
		for _, cfName := range cf.Names {
			cfCounts[cfName] = 0
		}
	}

	return &Ingester{
		config:   config,
		store:    config.Store,
		meta:     config.Meta,
		log:      config.Logger.WithScope("INGEST"),
		memory:   config.Memory,
		aggStats: stats.NewAggregatedStats(),
		cfCounts: cfCounts,
	}
}

// Run executes the ingestion phase.
//
// Returns an error if ingestion fails. On success, all ledgers have been
// ingested and progress has been checkpointed to the meta store.
func (i *Ingester) Run() error {
	i.log.Separator()
	i.log.Info("                         INGESTION PHASE")
	i.log.Separator()
	i.log.Info("")
	i.log.Info("LFS Store:     %s", i.config.LFSStorePath)
	i.log.Info("Ledger Range:  %d - %d", i.config.StartLedger, i.config.EndLedger)
	i.log.Info("Total Ledgers: %s", helpers.FormatNumber(int64(i.config.EndLedger-i.config.StartLedger+1)))
	i.log.Info("Batch Size:    %d ledgers", i.config.BatchSize)
	i.log.Info("")

	// Create LFS iterator
	iterator, err := lfs.NewLFSLedgerIterator(
		i.config.LFSStorePath,
		i.config.StartLedger,
		i.config.EndLedger,
	)
	if err != nil {
		return fmt.Errorf("failed to create LFS iterator: %w", err)
	}
	defer iterator.Close()

	// Initialize progress tracking
	totalLedgers := int(i.config.EndLedger - i.config.StartLedger + 1)
	progress := stats.NewProgressTracker(totalLedgers)

	// Initialize first batch
	i.startNewBatch(1, i.config.StartLedger)

	batchCheckInterval := 10 // Log memory every 10 batches

	// Process ledgers
	for {
		lcm, ledgerSeq, timing, hasMore, err := iterator.Next()
		if err != nil {
			return fmt.Errorf("failed to read ledger %d from LFS: %w", ledgerSeq, err)
		}
		if !hasMore {
			break
		}

		// Extract transaction hashes
		parseStart := time.Now()
		txCount, err := i.extractTransactions(lcm, ledgerSeq)
		if err != nil {
			return fmt.Errorf("failed to extract transactions from ledger %d: %w", ledgerSeq, err)
		}
		parseTime := time.Since(parseStart)

		// Update batch stats (combine LFS timing with our parse time)
		i.currentBatch.ParseTime += timing.TotalTime + parseTime
		i.currentBatch.LedgerCount++
		i.currentBatch.TxCount += txCount
		i.currentBatch.EndLedger = ledgerSeq

		// Check if batch is complete
		if i.currentBatch.LedgerCount >= i.config.BatchSize {
			if err := i.commitBatch(); err != nil {
				return err
			}

			// Update progress
			progress.Update(int(ledgerSeq - i.config.StartLedger + 1))

			// Memory check
			if i.currentBatch.BatchNumber%batchCheckInterval == 0 {
				i.memory.Check()
			}

			// Log progress every 10 batches
			if i.currentBatch.BatchNumber%10 == 0 {
				progress.LogProgress(i.log, "Ingestion")
			}

			// Start new batch
			nextBatchNum := i.currentBatch.BatchNumber + 1
			i.startNewBatch(nextBatchNum, ledgerSeq+1)
		}
	}

	// Commit final partial batch if any
	if i.currentBatch.LedgerCount > 0 {
		if err := i.commitBatch(); err != nil {
			return err
		}
	}

	// Flush all MemTables to disk
	i.log.Info("")
	i.log.Info("Flushing all MemTables to disk...")
	flushStart := time.Now()
	if err := i.store.FlushAll(); err != nil {
		return fmt.Errorf("failed to flush MemTables: %w", err)
	}
	i.log.Info("Flush completed in %s", helpers.FormatDuration(time.Since(flushStart)))

	// Log summary
	i.aggStats.LogSummary(i.log)
	i.aggStats.LogCFSummary(i.log)
	i.memory.LogSummary(i.log)

	i.log.Sync()

	return nil
}

// startNewBatch initializes a new batch.
func (i *Ingester) startNewBatch(batchNum int, startLedger uint32) {
	i.currentBatch = stats.NewBatchStats(batchNum, startLedger, startLedger)
	i.entriesByCF = make(map[string][]types.Entry)
	for _, cfName := range cf.Names {
		i.entriesByCF[cfName] = make([]types.Entry, 0, 1024)
	}
	i.batchEntrySize = 0
}

// extractTransactions extracts transaction hashes from a ledger.
func (i *Ingester) extractTransactions(lcm xdr.LedgerCloseMeta, ledgerSeq uint32) (int, error) {
	// Create transaction reader
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		network.PublicNetworkPassphrase, lcm)
	if err != nil {
		return 0, fmt.Errorf("failed to create transaction reader: %w", err)
	}
	defer txReader.Close()

	// Encode ledger sequence as 4-byte big-endian
	ledgerSeqBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(ledgerSeqBytes, ledgerSeq)

	txCount := 0
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return txCount, fmt.Errorf("failed to read transaction: %w", err)
		}

		// Get transaction hash (32 bytes)
		txHash := tx.Result.TransactionHash[:]

		// Determine column family
		cfName := cf.GetName(txHash)

		// Add to batch
		i.entriesByCF[cfName] = append(i.entriesByCF[cfName], types.Entry{
			Key:   copyBytes(txHash),
			Value: copyBytes(ledgerSeqBytes),
		})

		// Update per-CF counts for this batch
		i.currentBatch.TxCountByCF[cfName]++
		txCount++
	}

	i.batchEntrySize += txCount
	return txCount, nil
}

// commitBatch writes the current batch to RocksDB and checkpoints to meta store.
func (i *Ingester) commitBatch() error {
	batch := i.currentBatch

	// Write to RocksDB
	writeStart := time.Now()
	if _, err := i.store.WriteBatch(i.entriesByCF); err != nil {
		return fmt.Errorf("failed to write batch %d to RocksDB: %w", batch.BatchNumber, err)
	}
	batch.WriteTime = time.Since(writeStart)

	// Update cumulative CF counts
	for cf, count := range batch.TxCountByCF {
		i.cfCounts[cf] += uint64(count)
	}

	// Checkpoint to meta store
	if err := i.meta.CommitBatchProgress(batch.EndLedger, i.cfCounts); err != nil {
		return fmt.Errorf("failed to checkpoint batch %d: %w", batch.BatchNumber, err)
	}

	// Update aggregate stats
	i.aggStats.AddBatch(batch)

	// Log batch summary
	batch.LogSummary(i.log)

	return nil
}

// GetAggregatedStats returns the aggregated statistics.
func (i *Ingester) GetAggregatedStats() *stats.AggregatedStats {
	return i.aggStats
}

// GetCFCounts returns the current CF counts.
func (i *Ingester) GetCFCounts() map[string]uint64 {
	counts := make(map[string]uint64)
	for cf, count := range i.cfCounts {
		counts[cf] = count
	}
	return counts
}

// =============================================================================
// Helper Functions
// =============================================================================

// copyBytes creates a copy of a byte slice.
// This is necessary because the LFS iterator may reuse buffers.
func copyBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

// =============================================================================
// Convenience Function
// =============================================================================

// RunIngestion is a convenience function to run the ingestion phase.
//
// Deprecated: Use NewIngester(IngestionConfig{...}).Run() instead.
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
func RunIngestion(
	config *Config,
	store interfaces.TxHashStore,
	meta interfaces.MetaStore,
	logger interfaces.Logger,
	mem *memory.MemoryMonitor,
	startFromLedger uint32,
) error {
	ingester := NewIngester(IngestionConfig{
		LFSStorePath: config.LFSStorePath,
		StartLedger:  startFromLedger,
		EndLedger:    config.EndLedger,
		BatchSize:    LedgersPerBatch,
		Store:        store,
		Meta:         meta,
		Logger:       logger,
		Memory:       mem,
	})
	return ingester.Run()
}

// =============================================================================
// Ingestion Progress Logging
// =============================================================================

// LogIngestionStart logs the start of ingestion.
func LogIngestionStart(logger interfaces.Logger, config *Config, resuming bool, startFrom uint32) {
	logger.Separator()
	logger.Info("                         INGESTION PHASE")
	logger.Separator()
	logger.Info("")

	if resuming {
		logger.Info("RESUMING from ledger %d", startFrom)
		logger.Info("  Original Start: %d", config.StartLedger)
		logger.Info("  Original End:   %d", config.EndLedger)
		logger.Info("  Remaining:      %s ledgers", helpers.FormatNumber(int64(config.EndLedger-startFrom+1)))
	} else {
		logger.Info("STARTING fresh ingestion")
		logger.Info("  Start:   %d", config.StartLedger)
		logger.Info("  End:     %d", config.EndLedger)
		logger.Info("  Total:   %s ledgers", helpers.FormatNumber(int64(config.EndLedger-config.StartLedger+1)))
	}

	logger.Info("")
	logger.Info("LFS Store: %s", config.LFSStorePath)
	logger.Info("RocksDB:   %s", config.RocksDBPath)
	logger.Info("")
}

// CalculateExpectedTxCount estimates the expected transaction count.
//
// Based on historical data:
//   - Average ~350 transactions per ledger (varies widely)
//   - ~3.5 billion transactions for 10M ledgers
func CalculateExpectedTxCount(ledgerCount uint32) uint64 {
	const avgTxPerLedger = 350
	return uint64(ledgerCount) * avgTxPerLedger
}
