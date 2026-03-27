// =============================================================================
// config.go - Configuration and RocksDB Settings
// =============================================================================
//
// This file defines the configuration for the txhash-ingestion-workflow tool.
// It includes:
//   - Command-line flag definitions
//   - RocksDB configuration with detailed explanations
//   - Path validation and directory structure setup
//   - Ledger range pattern validation
//
// ROCKSDB CONFIGURATION PHILOSOPHY:
//
//   This tool is designed for BULK INGESTION followed by MANUAL COMPACTION.
//   The RocksDB settings are optimized for write throughput during ingestion,
//   with compaction disabled until explicitly triggered after ingestion.
//
//   Key design decisions:
//   1. WAL is ALWAYS enabled (not configurable) for crash recovery
//   2. Auto-compaction is DISABLED during ingestion
//   3. MemTables are flushed immediately when full (MinWriteBufferNumberToMerge=1)
//   4. Bloom filters are enabled for read performance during live queries
//
// MEMORY BUDGET CALCULATION:
//
//   Total RocksDB Memory = MemTables + Block Cache
//
//   MemTables:
//     WriteBufferSizeMB × MaxWriteBufferNumber × 16 CFs
//     64 MB × 2 × 16 = 2,048 MB (2 GB)
//
//   Block Cache:
//     Configurable via --block-cache-mb (default: 8 GB)
//
//   Total Default: ~10 GB for RocksDB
//
//   RecSplit Build (additional, during Phase 3):
//     ~40 bytes per key during construction
//     ~9 GB per CF (for ~220M keys per CF)
//     Parallel mode: up to 144 GB peak (all 16 CFs building simultaneously)
//
// =============================================================================

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
	"github.com/stellar/stellar-rpc/full-history/all-code/helpers/lfs"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/cf"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/interfaces"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/types"
)

// =============================================================================
// Constants
// =============================================================================

// Import constants from pkg/types for convenience
const (
	// LedgersPerBatch is imported from pkg/types
	LedgersPerBatch = types.LedgersPerBatch

	// DefaultBlockCacheMB is imported from pkg/types
	DefaultBlockCacheMB = types.DefaultBlockCacheMB

	// RAMWarningThresholdGB is imported from pkg/types
	RAMWarningThresholdGB = types.RAMWarningThresholdGB
)

// =============================================================================
// RocksDBSettings Helper Functions
// =============================================================================
//
// These settings are optimized for bulk ingestion with the following goals:
//  1. High write throughput during ingestion
//  2. Reasonable read latency for live queries (via SIGHUP)
//  3. Crash recovery support (WAL enabled)
//  4. Deferred compaction (run explicitly after ingestion)
//
// SETTINGS THAT ARE NOT CONFIGURABLE:
//   - WAL is always enabled (critical for crash recovery)
//   - Auto-compaction is always disabled (explicit compaction phase)
//   - Compression is disabled (values are only 4 bytes)
//
// MEMTABLE CONFIGURATION:
//
//	MemTables are in-memory buffers where writes land first.
//	When a MemTable fills up, it's flushed to an SST file on disk.
//
//	MEMORY IMPACT:
//	  Total MemTable RAM = WriteBufferSizeMB × MaxWriteBufferNumber × 16 CFs
//
//	WAL IMPACT:
//	  WAL size ≈ sum of all active MemTable sizes
//	  Larger MemTables = larger WAL = longer crash recovery
//
// SST FILE CONFIGURATION:
//
//	SST files are the on-disk storage format. Larger files mean:
//	  + Fewer files to manage
//	  + Fewer bloom filter checks per lookup
//	  - Longer compaction times per file
//
// BLOOM FILTER CONFIGURATION:
//
//	BITS PER KEY → FALSE POSITIVE RATE:
//	  10 bits → ~1.0% false positives
//	  12 bits → ~0.3% false positives
//	  14 bits → ~0.1% false positives
//
// =============================================================================

// CalculateMemoryUsage returns the estimated memory usage in megabytes.
//
// Returns:
//   - memtables: Total MemTable memory across all CFs
//   - blockCache: Block cache size
//   - total: memtables + blockCache
func CalculateMemoryUsage(s *types.RocksDBSettings) (memtables, blockCache, total int) {
	memtables = s.WriteBufferSizeMB * s.MaxWriteBufferNumber * cf.Count
	blockCache = s.BlockCacheSizeMB
	total = memtables + blockCache
	return
}

// MaxWALSize returns the theoretical maximum WAL size in megabytes.
//
// WAL size = sum of all active MemTables
// Worst case = all CFs have a full MemTable waiting to be flushed
func MaxWALSize(s *types.RocksDBSettings) int {
	return s.WriteBufferSizeMB * cf.Count
}

// EstimatedRecoveryTime returns the estimated WAL recovery time.
//
// Based on ~300 MB/s WAL replay speed (typical for SSDs).
func EstimatedRecoveryTime(s *types.RocksDBSettings) string {
	maxWALMB := MaxWALSize(s)
	seconds := float64(maxWALMB) / 300.0 // ~300 MB/s replay
	if seconds < 60 {
		return fmt.Sprintf("~%.0f seconds", seconds)
	}
	return fmt.Sprintf("~%.1f minutes", seconds/60)
}

// =============================================================================
// Config - Main Configuration
// =============================================================================

// Config holds all configuration for the txhash-ingestion-workflow tool.
type Config struct {
	// =========================================================================
	// Required Flags
	// =========================================================================

	// LFSStorePath is the path to the LFS ledger store.
	LFSStorePath string

	// StartLedger is the first ledger to ingest (inclusive).
	// Must match pattern: X0,000,001 (e.g., 10000001, 20000001)
	StartLedger uint32

	// EndLedger is the last ledger to ingest (inclusive).
	// Must match pattern: Y0,000,000 (e.g., 20000000, 30000000)
	EndLedger uint32

	// OutputDir is the base output directory.
	// Subdirectories will be created:
	//   <OutputDir>/txHash-ledgerSeq/rocksdb/
	//   <OutputDir>/txHash-ledgerSeq/recsplit/index/
	//   <OutputDir>/txHash-ledgerSeq/recsplit/tmp/
	//   <OutputDir>/txHash-ledgerSeq/meta/
	OutputDir string

	// LogFile is the path to the main log file.
	LogFile string

	// ErrorFile is the path to the error log file.
	ErrorFile string

	// QueryFile is the path to the file containing txHashes to query on SIGHUP.
	QueryFile string

	// QueryOutput is the path to the CSV output file for query results.
	QueryOutput string

	// QueryLog is the path to the query statistics log file.
	QueryLog string

	// QueryError is the path to the query errors log file.
	QueryError string

	// =========================================================================
	// Optional Flags
	// =========================================================================

	// MultiIndexEnabled enables building 16 separate RecSplit indexes (one per CF).
	// Default: false (build single combined index file: txhash.idx)
	// When true: Build 16 separate index files (cf-0.idx through cf-f.idx) in parallel
	//
	// MEMORY WARNING:
	//   Multi-index mode requires ~9 GB per CF during build.
	//   With 16 CFs building simultaneously: ~144 GB peak.
	//
	MultiIndexEnabled bool

	// SequentialIngestion uses the old single-threaded ingestion mode.
	// Default: false (use parallel ingestion)
	//
	// Use this flag if you want to compare performance or if parallel
	// mode has issues on your system.
	//
	SequentialIngestion bool

	// ParallelWorkers is the number of worker goroutines for decompress/unmarshal/extract.
	// Default: 16
	//
	// Each worker handles the full pipeline for a ledger:
	// decompress → unmarshal XDR → extract txHashes
	//
	ParallelWorkers int

	// ParallelReaders is the number of LFS reader goroutines.
	// Default: 4
	//
	// Readers fetch compressed data from LFS and send to workers.
	//
	ParallelReaders int

	// ParallelBatchSize is the number of ledgers per batch in parallel mode.
	// Default: 5000
	//
	// Larger batches reduce checkpoint overhead but increase memory usage
	// and potential re-work on crash recovery.
	//
	ParallelBatchSize int

	// DryRun validates configuration without running the workflow.
	// Shows what would happen and exits.
	DryRun bool

	// =========================================================================
	// RocksDB Settings
	// =========================================================================

	// RocksDB contains the RocksDB configuration.
	RocksDB types.RocksDBSettings

	// =========================================================================
	// Derived Paths (Set During Validation)
	// =========================================================================

	// RocksDBPath is the path to the RocksDB store.
	// Set to: <OutputDir>/txHash-ledgerSeq/rocksdb/
	RocksDBPath string

	// RecsplitIndexPath is the path to the RecSplit index directory.
	// Set to: <OutputDir>/txHash-ledgerSeq/recsplit/index/
	RecsplitIndexPath string

	// RecsplitTmpPath is the path to the RecSplit temp directory.
	// Set to: <OutputDir>/txHash-ledgerSeq/recsplit/tmp/
	RecsplitTmpPath string

	// MetaStorePath is the path to the meta store directory.
	// Set to: <OutputDir>/txHash-ledgerSeq/meta/
	MetaStorePath string
}

// =============================================================================
// Validation
// =============================================================================

// ledgerStartPattern matches ledger sequences like 10000001, 20000001, etc.
// Pattern: X0,000,001 where X is any digit(s)
var ledgerStartPattern = regexp.MustCompile(`^\d*0000001$`)

// ledgerEndPattern matches ledger sequences like 20000000, 30000000, etc.
// Pattern: Y0,000,000 where Y is any digit(s)
var ledgerEndPattern = regexp.MustCompile(`^\d*0000000$`)

// Validate validates the configuration.
//
// Checks:
//  1. All required paths are provided
//  2. LFS store exists
//  3. Ledger range follows the required pattern
//  4. Start < End
//  5. Creates output directories
//
// Returns an error if validation fails.
func (c *Config) Validate() error {
	// =========================================================================
	// Check Required Fields
	// =========================================================================

	if c.LFSStorePath == "" {
		return fmt.Errorf("--lfs-store is required")
	}
	if c.StartLedger == 0 {
		return fmt.Errorf("--start-ledger is required")
	}
	if c.EndLedger == 0 {
		return fmt.Errorf("--end-ledger is required")
	}
	if c.OutputDir == "" {
		return fmt.Errorf("--output-dir is required")
	}
	if c.LogFile == "" {
		return fmt.Errorf("--log-file is required")
	}
	if c.ErrorFile == "" {
		return fmt.Errorf("--error-file is required")
	}
	if c.QueryFile == "" {
		return fmt.Errorf("--query-file is required")
	}
	if c.QueryOutput == "" {
		return fmt.Errorf("--query-output is required")
	}
	if c.QueryLog == "" {
		return fmt.Errorf("--query-log is required")
	}
	if c.QueryError == "" {
		return fmt.Errorf("--query-error is required")
	}

	// =========================================================================
	// Validate LFS Store
	// =========================================================================

	absLFSPath, err := filepath.Abs(c.LFSStorePath)
	if err != nil {
		return fmt.Errorf("invalid --lfs-store path: %w", err)
	}
	c.LFSStorePath = absLFSPath

	// Use helpers/lfs validation
	if err := lfs.ValidateLfsStore(absLFSPath); err != nil {
		return err
	}

	// =========================================================================
	// Validate Ledger Range Pattern
	// =========================================================================

	//startStr := fmt.Sprintf("%d", c.StartLedger)
	//if !ledgerStartPattern.MatchString(startStr) {
	//	return fmt.Errorf("--start-ledger must match pattern X0000001 (e.g., 10000001, 20000001), got: %d", c.StartLedger)
	//}
	//
	//endStr := fmt.Sprintf("%d", c.EndLedger)
	//if !ledgerEndPattern.MatchString(endStr) {
	//	return fmt.Errorf("--end-ledger must match pattern Y0000000 (e.g., 20000000, 30000000), got: %d", c.EndLedger)
	//}

	if c.StartLedger >= c.EndLedger {
		return fmt.Errorf("--start-ledger (%d) must be less than --end-ledger (%d)", c.StartLedger, c.EndLedger)
	}

	// =========================================================================
	// Set Derived Paths
	// =========================================================================

	absOutputDir, err := filepath.Abs(c.OutputDir)
	if err != nil {
		return fmt.Errorf("invalid --output-dir path: %w", err)
	}
	c.OutputDir = absOutputDir

	basePath := filepath.Join(absOutputDir, "txHash-ledgerSeq")
	c.RocksDBPath = filepath.Join(basePath, "rocksdb")
	c.RecsplitIndexPath = filepath.Join(basePath, "recsplit", "index")
	c.RecsplitTmpPath = filepath.Join(basePath, "recsplit", "tmp")
	c.MetaStorePath = filepath.Join(basePath, "meta")

	// =========================================================================
	// Create Directories (unless dry-run)
	// =========================================================================

	if !c.DryRun {
		dirs := []string{
			c.RocksDBPath,
			c.RecsplitIndexPath,
			c.RecsplitTmpPath,
			c.MetaStorePath,
		}

		for _, dir := range dirs {
			if err := helpers.EnsureDir(dir); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", dir, err)
			}
		}

		// Ensure log file directories exist
		for _, logPath := range []string{c.LogFile, c.ErrorFile, c.QueryOutput, c.QueryLog, c.QueryError} {
			logDir := filepath.Dir(logPath)
			if err := helpers.EnsureDir(logDir); err != nil {
				return fmt.Errorf("failed to create log directory %s: %w", logDir, err)
			}
		}
	}

	// =========================================================================
	// Validate Query File Exists
	// =========================================================================

	absQueryFile, err := filepath.Abs(c.QueryFile)
	if err != nil {
		return fmt.Errorf("invalid --query-file path: %w", err)
	}
	c.QueryFile = absQueryFile

	if _, err := os.Stat(absQueryFile); os.IsNotExist(err) {
		return fmt.Errorf("query file does not exist: %s", absQueryFile)
	}

	return nil
}

// TotalLedgers returns the total number of ledgers to process.
func (c *Config) TotalLedgers() uint32 {
	return c.EndLedger - c.StartLedger + 1
}

// BatchSize returns the batch size based on ingestion mode.
// Returns ParallelBatchSize for parallel mode, LedgersPerBatch for sequential mode.
func (c *Config) BatchSize() uint32 {
	if c.SequentialIngestion {
		return LedgersPerBatch
	}
	return uint32(c.ParallelBatchSize)
}

// TotalBatches returns the total number of batches based on ingestion mode.
func (c *Config) TotalBatches() uint32 {
	total := c.TotalLedgers()
	batchSize := c.BatchSize()
	return (total + batchSize - 1) / batchSize
}

// =============================================================================
// Display Functions (for dry-run and logging)
// =============================================================================

// PrintConfig prints the configuration to the logger.
func (c *Config) PrintConfig(logger interfaces.Logger) {
	logger.Separator()
	logger.Info("                         CONFIGURATION")
	logger.Separator()
	logger.Info("")
	logger.Info("INPUT:")
	logger.Info("  LFS Store:           %s", c.LFSStorePath)
	logger.Info("  Start Ledger:        %d", c.StartLedger)
	logger.Info("  End Ledger:          %d", c.EndLedger)
	logger.Info("  Total Ledgers:       %d", c.TotalLedgers())
	logger.Info("  Total Batches:       %d (%d ledgers each)", c.TotalBatches(), c.BatchSize())
	logger.Info("")
	logger.Info("OUTPUT:")
	logger.Info("  Output Dir:          %s", c.OutputDir)
	logger.Info("  RocksDB Path:        %s", c.RocksDBPath)
	logger.Info("  RecSplit Index:      %s", c.RecsplitIndexPath)
	logger.Info("  RecSplit Temp:       %s", c.RecsplitTmpPath)
	logger.Info("  Meta Store:          %s", c.MetaStorePath)
	logger.Info("")
	logger.Info("LOGGING:")
	logger.Info("  Log File:            %s", c.LogFile)
	logger.Info("  Error File:          %s", c.ErrorFile)
	logger.Info("")
	logger.Info("QUERY (SIGHUP):")
	logger.Info("  Query File:          %s", c.QueryFile)
	logger.Info("  Query Output:        %s", c.QueryOutput)
	logger.Info("  Query Log:           %s", c.QueryLog)
	logger.Info("  Query Error:         %s", c.QueryError)
	logger.Info("")
	logger.Info("OPTIONS:")
	logger.Info("  Multi-Index Mode:    %v", c.MultiIndexEnabled)
	logger.Info("  Sequential Ingest:   %v", c.SequentialIngestion)
	logger.Info("  Dry Run:             %v", c.DryRun)
	logger.Info("")
	if !c.SequentialIngestion {
		logger.Info("PARALLEL INGESTION SETTINGS:")
		logger.Info("  Parallel Workers:    %d", c.ParallelWorkers)
		logger.Info("  Parallel Readers:    %d", c.ParallelReaders)
		logger.Info("  Batch Size:          %d ledgers", c.ParallelBatchSize)
		logger.Info("")
	}
}

// PrintRocksDBConfig prints the RocksDB configuration to the logger.
func (c *Config) PrintRocksDBConfig(logger interfaces.Logger) {
	s := &c.RocksDB
	memtables, blockCache, total := CalculateMemoryUsage(s)

	logger.Separator()
	logger.Info("                      ROCKSDB CONFIGURATION")
	logger.Separator()
	logger.Info("")
	logger.Info("MEMTABLE SETTINGS:")
	logger.Info("  WriteBufferSizeMB:          %d", s.WriteBufferSizeMB)
	logger.Info("  MaxWriteBufferNumber:       %d", s.MaxWriteBufferNumber)
	logger.Info("  MinWriteBufferNumberToMerge: %d", s.MinWriteBufferNumberToMerge)
	logger.Info("")
	logger.Info("SST FILE SETTINGS:")
	logger.Info("  TargetFileSizeMB:           %d", s.TargetFileSizeMB)
	logger.Info("")
	logger.Info("BACKGROUND OPERATIONS:")
	logger.Info("  MaxBackgroundJobs:          %d", s.MaxBackgroundJobs)
	logger.Info("")
	logger.Info("READ PERFORMANCE:")
	logger.Info("  BloomFilterBitsPerKey:      %d", s.BloomFilterBitsPerKey)
	logger.Info("  BlockCacheSizeMB:           %d", s.BlockCacheSizeMB)
	logger.Info("")
	logger.Info("FILE HANDLING:")
	logger.Info("  MaxOpenFiles:               %d", s.MaxOpenFiles)
	logger.Info("")
	logger.Info("FIXED SETTINGS (NOT CONFIGURABLE):")
	logger.Info("  DisableAutoCompactions:     true (compaction is manual)")
	logger.Info("  WAL:                        ENABLED (always, for crash recovery)")
	logger.Info("  Compression:                DISABLED (values are only 4 bytes)")
	logger.Info("")
	logger.Info("MEMORY BUDGET:")
	logger.Info("  MemTables:           %d MB × %d × %d CFs = %d MB",
		s.WriteBufferSizeMB, s.MaxWriteBufferNumber, cf.Count, memtables)
	logger.Info("  Block Cache:         %d MB", blockCache)
	logger.Info("  Total RocksDB:       %d MB (~%d GB)", total, total/1024)
	logger.Info("")
	logger.Info("MAX WAL SIZE:")
	logger.Info("  %d MB (recovery time: %s)", MaxWALSize(s), EstimatedRecoveryTime(s))
	logger.Info("")
}
