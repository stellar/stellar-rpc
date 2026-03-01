// =============================================================================
// pkg/recsplit/recsplit.go - RecSplit Index Building
// =============================================================================
//
// This package implements the RecSplit building phase that creates perfect hash
// function indexes from the RocksDB data.
//
// USAGE:
//
//	builder := recsplit.New(recsplit.Config{
//	    Store:      txHashStore,
//	    CFCounts:   cfCounts,
//	    IndexPath:  "/path/to/indexes",
//	    TmpPath:    "/path/to/tmp",
//	    MultiIndex: false,
//	    Logger:     logger,
//	    Memory:     memoryMonitor,
//	})
//	stats, err := builder.Run()
//
// TWO MODES:
//
//	1. Combined Index (default, MultiIndex=false):
//	   - Reads all 16 CFs sequentially
//	   - Feeds all keys into ONE RecSplit builder
//	   - Produces single file: txhash.idx
//	   - Lower memory usage (~9 GB for entire build)
//
//	2. Multi-Index (MultiIndex=true):
//	   - Reads all 16 CFs in parallel
//	   - Each CF has its own RecSplit builder
//	   - Produces 16 files: cf-0.idx through cf-f.idx
//	   - Higher memory usage (~144 GB peak)
//
// =============================================================================

package recsplit

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	erigonlog "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/cf"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/interfaces"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/memory"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/types"
)

// =============================================================================
// Constants
// =============================================================================

const (
	// CombinedIndexFileName is the name of the single combined index file
	CombinedIndexFileName = "txhash.idx"
)

// =============================================================================
// Builder - RecSplit Index Builder
// =============================================================================

// Config holds the configuration for creating a Builder.
type Config struct {
	// Store is the TxHash store to read keys from (required)
	Store interfaces.TxHashStore

	// CFCounts is the per-CF key counts from meta store (required)
	CFCounts map[string]uint64

	// IndexPath is the output directory for index files (required)
	IndexPath string

	// TmpPath is the directory for temporary files during build (required)
	TmpPath string

	// MultiIndex enables multi-index mode (16 separate files vs 1 combined)
	MultiIndex bool

	// Logger is the parent logger (required)
	Logger interfaces.Logger

	// Memory is the memory monitor (optional)
	Memory interfaces.MemoryMonitor
}

// Builder handles building RecSplit indexes from RocksDB data.
//
// The Builder is designed to be composable - it takes dependencies via Config
// and uses a scoped logger with [RECSPLIT] prefix for all output.
type Builder struct {
	store      interfaces.TxHashStore
	cfCounts   map[string]uint64
	indexPath  string
	tmpPath    string
	multiIndex bool
	log        interfaces.Logger
	memory     interfaces.MemoryMonitor
	stats      *Stats
}

// New creates a new RecSplit Builder with the given configuration.
func New(cfg Config) *Builder {
	if cfg.Store == nil {
		panic("recsplit.New: Store is required")
	}
	if cfg.CFCounts == nil {
		panic("recsplit.New: CFCounts is required")
	}
	if cfg.IndexPath == "" {
		panic("recsplit.New: IndexPath is required")
	}
	if cfg.TmpPath == "" {
		panic("recsplit.New: TmpPath is required")
	}
	if cfg.Logger == nil {
		panic("recsplit.New: Logger is required")
	}

	return &Builder{
		store:      cfg.Store,
		cfCounts:   cfg.CFCounts,
		indexPath:  cfg.IndexPath,
		tmpPath:    cfg.TmpPath,
		multiIndex: cfg.MultiIndex,
		log:        cfg.Logger.WithScope("RECSPLIT"),
		memory:     cfg.Memory,
		stats:      NewStats(cfg.MultiIndex),
	}
}

// Run executes the RecSplit building phase.
func (b *Builder) Run() (*Stats, error) {
	b.stats.StartTime = time.Now()

	b.log.Separator()
	b.log.Info("                    RECSPLIT BUILDING PHASE")
	b.log.Separator()
	b.log.Info("")

	// Clean up any existing files (for crash recovery)
	if err := b.cleanupDirectories(); err != nil {
		return nil, err
	}

	// Log memory estimate
	memory.LogRecSplitMemoryEstimate(b.log, b.cfCounts, b.multiIndex)

	// Build indexes based on mode
	var err error
	if b.multiIndex {
		err = b.buildMultiIndex()
	} else {
		err = b.buildCombinedIndex()
	}

	if err != nil {
		return nil, err
	}

	b.stats.EndTime = time.Now()
	b.stats.TotalTime = time.Since(b.stats.StartTime)

	// Calculate total keys
	for _, stats := range b.stats.PerCFStats {
		b.stats.TotalKeys += stats.KeyCount
	}

	// Log summary
	b.stats.LogSummary(b.log)
	if b.memory != nil {
		b.memory.LogSummary(b.log)
	}

	b.log.Sync()

	return b.stats, nil
}

// Stats returns the RecSplit build statistics.
func (b *Builder) Stats() *Stats {
	return b.stats
}

// cleanupDirectories removes existing temp and index files.
func (b *Builder) cleanupDirectories() error {
	b.log.Info("Cleaning up existing files...")

	// Clean temp directory
	if err := os.RemoveAll(b.tmpPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to clean temp directory: %w", err)
	}
	if err := helpers.EnsureDir(b.tmpPath); err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}

	// Clean index directory
	if err := os.RemoveAll(b.indexPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to clean index directory: %w", err)
	}
	if err := helpers.EnsureDir(b.indexPath); err != nil {
		return fmt.Errorf("failed to create index directory: %w", err)
	}

	b.log.Info("Directories cleaned")
	b.log.Info("")

	return nil
}

// =============================================================================
// Combined Index Mode (default)
// =============================================================================

// buildCombinedIndex builds a single combined index from all 16 CFs.
func (b *Builder) buildCombinedIndex() error {
	b.log.Info("Building combined index (single file: %s)...", CombinedIndexFileName)
	b.log.Info("")

	// Calculate total key count
	var totalKeys uint64
	for _, count := range b.cfCounts {
		totalKeys += count
	}

	if totalKeys == 0 {
		b.log.Info("No keys to index")
		return nil
	}

	b.log.Info("Total keys to index: %s", helpers.FormatNumber(int64(totalKeys)))
	b.log.Info("")

	// Determine output path
	indexFilePath := filepath.Join(b.indexPath, CombinedIndexFileName)
	b.stats.IndexPath = indexFilePath

	// Create RecSplit builder with total key count
	erigonLogger := erigonlog.New()

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:           int(totalKeys),
		Enums:              false,
		LessFalsePositives: types.RecSplitLessFalsePositivesEnabled,
		BucketSize:         types.RecSplitBucketSize,
		LeafSize:           types.RecSplitLeafSize,
		TmpDir:             b.tmpPath,
		IndexFile:          indexFilePath,
		BaseDataID:         0,
		Version:            types.RecSplitDataVersion,
	}, erigonLogger)
	if err != nil {
		return fmt.Errorf("failed to create RecSplit: %w", err)
	}
	defer rs.Close()

	// Add keys from all CFs sequentially
	b.log.Info("Adding keys from 16 column families...")
	b.log.Info("")

	keyAddStart := time.Now()
	var totalKeysAdded uint64

	for i, cfName := range cf.Names {
		keyCount := b.cfCounts[cfName]
		if keyCount == 0 {
			b.log.Info("[%2d/16] CF [%s]: No keys, skipping", i+1, cfName)
			b.stats.PerCFStats[cfName] = &CFBuildStats{
				CFName:   cfName,
				KeyCount: 0,
			}
			continue
		}

		cfStart := time.Now()

		// Iterate over CF and add keys
		iter := b.store.NewScanIteratorCF(cfName)
		keysAdded := uint64(0)

		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			key := iter.Key()
			value := iter.Value()

			ledgerSeq := types.ParseLedgerSeq(value)

			if err := rs.AddKey(key, uint64(ledgerSeq)); err != nil {
				iter.Close()
				return fmt.Errorf("failed to add key from CF %s: %w", cfName, err)
			}
			keysAdded++
		}

		if err := iter.Error(); err != nil {
			iter.Close()
			return fmt.Errorf("iterator error for CF %s: %w", cfName, err)
		}
		iter.Close()

		// Verify key count matches expected
		if keysAdded != keyCount {
			return fmt.Errorf("CF %s: key count mismatch: expected %d, got %d", cfName, keyCount, keysAdded)
		}

		cfAddTime := time.Since(cfStart)
		totalKeysAdded += keysAdded

		b.stats.PerCFStats[cfName] = &CFBuildStats{
			CFName:     cfName,
			KeyCount:   keysAdded,
			KeyAddTime: cfAddTime,
		}

		b.log.Info("[%2d/16] CF [%s]: Added %s keys in %s",
			i+1, cfName, helpers.FormatNumber(int64(keysAdded)), helpers.FormatDuration(cfAddTime))

		// Check memory after each CF
		if b.memory != nil {
			b.memory.Check()
		}
	}

	b.stats.KeyAddTime = time.Since(keyAddStart)

	b.log.Info("")
	b.log.Info("Key addition complete: %s keys in %s",
		helpers.FormatNumber(int64(totalKeysAdded)), helpers.FormatDuration(b.stats.KeyAddTime))
	b.log.Info("")

	// Verify total key count
	if totalKeysAdded != totalKeys {
		return fmt.Errorf("total key count mismatch: expected %d, got %d", totalKeys, totalKeysAdded)
	}

	// Build the index
	b.log.Info("Building combined index...")
	buildStart := time.Now()

	ctx := context.Background()
	if err := rs.Build(ctx); err != nil {
		if err == recsplit.ErrCollision {
			return fmt.Errorf("hash collision detected (rare, try rebuilding)")
		}
		return fmt.Errorf("failed to build index: %w", err)
	}

	b.stats.IndexBuildTime = time.Since(buildStart)

	// Get index file size
	if info, err := os.Stat(indexFilePath); err == nil {
		b.stats.IndexSize = info.Size()
	}

	b.log.Info("Index built in %s, size: %s",
		helpers.FormatDuration(b.stats.IndexBuildTime), helpers.FormatBytes(b.stats.IndexSize))
	b.log.Info("")

	// Clean up temp directory
	if err := os.RemoveAll(b.tmpPath); err != nil {
		b.log.Info("Warning: Failed to clean up temp directory: %v", err)
	}

	return nil
}

// =============================================================================
// Multi-Index Mode
// =============================================================================

// buildMultiIndex builds 16 separate CF indexes in parallel.
func (b *Builder) buildMultiIndex() error {
	b.log.Info("Building indexes in parallel (16 CFs simultaneously)...")
	b.log.Info("")
	b.log.Info("WARNING: This requires ~144 GB of RAM!")
	b.log.Info("")

	var wg sync.WaitGroup
	var mu sync.Mutex
	errors := make([]error, len(cf.Names))

	for i, cfName := range cf.Names {
		wg.Add(1)
		go func(idx int, cfn string) {
			defer wg.Done()

			keyCount := b.cfCounts[cfn]
			if keyCount == 0 {
				return
			}

			stats, err := b.buildCFIndex(cfn, keyCount)
			if err != nil {
				errors[idx] = fmt.Errorf("CF %s: %w", cfn, err)
				return
			}

			mu.Lock()
			b.stats.PerCFStats[cfn] = stats
			b.log.Info("CF [%s] completed: %s keys in %s",
				cfn, helpers.FormatNumber(int64(stats.KeyCount)), helpers.FormatDuration(stats.BuildTime))
			mu.Unlock()
		}(i, cfName)
	}

	wg.Wait()

	// Check for errors
	for _, err := range errors {
		if err != nil {
			return err
		}
	}

	// Calculate total index size
	for _, stats := range b.stats.PerCFStats {
		b.stats.IndexSize += stats.IndexSize
	}

	// Clean up temp directory
	if err := os.RemoveAll(b.tmpPath); err != nil {
		b.log.Info("Warning: Failed to clean up temp directory: %v", err)
	}

	return nil
}

// buildCFIndex builds a RecSplit index for a single column family.
func (b *Builder) buildCFIndex(cfName string, keyCount uint64) (*CFBuildStats, error) {
	stats := &CFBuildStats{
		CFName:   cfName,
		KeyCount: keyCount,
	}

	startTime := time.Now()

	// Determine output path
	indexFileName := fmt.Sprintf("cf-%s.idx", cfName)
	indexPath := filepath.Join(b.indexPath, indexFileName)
	stats.IndexPath = indexPath

	// Create temp directory for this CF
	cfTmpDir := filepath.Join(b.tmpPath, cfName)
	if err := helpers.EnsureDir(cfTmpDir); err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	// Create RecSplit builder
	erigonLogger := erigonlog.New()

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:           int(keyCount),
		Enums:              false,
		LessFalsePositives: types.RecSplitLessFalsePositivesEnabled,
		BucketSize:         types.RecSplitBucketSize,
		LeafSize:           types.RecSplitLeafSize,
		TmpDir:             cfTmpDir,
		IndexFile:          indexPath,
		BaseDataID:         0,
		Version:            types.RecSplitDataVersion,
	}, erigonLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create RecSplit: %w", err)
	}
	defer rs.Close()

	// Iterate over CF and add keys
	keyAddStart := time.Now()
	iter := b.store.NewScanIteratorCF(cfName)
	defer iter.Close()

	keysAdded := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		ledgerSeq := types.ParseLedgerSeq(value)

		if err := rs.AddKey(key, uint64(ledgerSeq)); err != nil {
			return nil, fmt.Errorf("failed to add key %d: %w", keysAdded, err)
		}
		keysAdded++
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	stats.KeyAddTime = time.Since(keyAddStart)

	// Verify key count
	if uint64(keysAdded) != keyCount {
		return nil, fmt.Errorf("key count mismatch: expected %d, got %d", keyCount, keysAdded)
	}

	// Build the index
	ctx := context.Background()
	if err := rs.Build(ctx); err != nil {
		if err == recsplit.ErrCollision {
			return nil, fmt.Errorf("hash collision detected (rare, try rebuilding)")
		}
		return nil, fmt.Errorf("failed to build index: %w", err)
	}

	stats.BuildTime = time.Since(startTime)

	// Get index file size
	if info, err := os.Stat(indexPath); err == nil {
		stats.IndexSize = info.Size()
	}

	// Clean up temp directory for this CF
	os.RemoveAll(cfTmpDir)

	return stats, nil
}

// =============================================================================
// Stats - RecSplit Build Statistics
// =============================================================================

// Stats holds statistics for RecSplit building.
type Stats struct {
	StartTime      time.Time
	EndTime        time.Time
	TotalTime      time.Duration
	PerCFStats     map[string]*CFBuildStats
	TotalKeys      uint64
	MultiIndex     bool
	IndexBuildTime time.Duration // Combined mode only
	KeyAddTime     time.Duration // Combined mode only
	IndexSize      int64
	IndexPath      string
}

// CFBuildStats holds per-CF RecSplit statistics.
type CFBuildStats struct {
	CFName     string
	KeyCount   uint64
	BuildTime  time.Duration // Full build time (multi-index) or key-add time (combined)
	IndexSize  int64         // Only set in multi-index mode
	IndexPath  string        // Only set in multi-index mode
	KeyAddTime time.Duration // Time to add keys from this CF
}

// NewStats creates a new Stats instance.
func NewStats(multiIndex bool) *Stats {
	return &Stats{
		PerCFStats: make(map[string]*CFBuildStats),
		MultiIndex: multiIndex,
	}
}

// LogSummary logs a summary of RecSplit building.
func (s *Stats) LogSummary(log interfaces.Logger) {
	log.Separator()
	log.Info("                    RECSPLIT BUILD SUMMARY")
	log.Separator()
	log.Info("")

	if s.MultiIndex {
		log.Info("Mode: Multi-Index (16 separate index files)")
	} else {
		log.Info("Mode: Combined (single index file: %s)", CombinedIndexFileName)
	}
	log.Info("")

	if s.MultiIndex {
		log.Info("PER-CF STATISTICS:")
		log.Info("%-4s %15s %12s %12s", "CF", "Keys", "Build Time", "Index Size")
		log.Info("%-4s %15s %12s %12s", "----", "---------------", "------------", "------------")

		var totalKeys uint64
		var totalSize int64

		for _, cfName := range cf.Names {
			stats := s.PerCFStats[cfName]
			if stats != nil {
				log.Info("%-4s %15s %12s %12s",
					cfName,
					helpers.FormatNumber(int64(stats.KeyCount)),
					helpers.FormatDuration(stats.BuildTime),
					helpers.FormatBytes(stats.IndexSize))
				totalKeys += stats.KeyCount
				totalSize += stats.IndexSize
			}
		}

		log.Info("%-4s %15s %12s %12s", "----", "---------------", "------------", "------------")
		log.Info("%-4s %15s %12s %12s", "TOT", helpers.FormatNumber(int64(totalKeys)), helpers.FormatDuration(s.TotalTime), helpers.FormatBytes(totalSize))
	} else {
		log.Info("KEY ADDITION BY CF:")
		log.Info("%-4s %15s %12s", "CF", "Keys Added", "Add Time")
		log.Info("%-4s %15s %12s", "----", "---------------", "------------")

		var totalKeys uint64

		for _, cfName := range cf.Names {
			stats := s.PerCFStats[cfName]
			if stats != nil {
				log.Info("%-4s %15s %12s",
					cfName,
					helpers.FormatNumber(int64(stats.KeyCount)),
					helpers.FormatDuration(stats.KeyAddTime))
				totalKeys += stats.KeyCount
			}
		}

		log.Info("%-4s %15s %12s", "----", "---------------", "------------")
		log.Info("%-4s %15s %12s", "TOT", helpers.FormatNumber(int64(totalKeys)), helpers.FormatDuration(s.KeyAddTime))
		log.Info("")
		log.Info("INDEX BUILD:")
		log.Info("  Build Time:    %s", helpers.FormatDuration(s.IndexBuildTime))
		log.Info("  Index Size:    %s", helpers.FormatBytes(s.IndexSize))
		log.Info("  Index Path:    %s", s.IndexPath)
		log.Info("")
		log.Info("TOTAL TIME:      %s", helpers.FormatDuration(s.TotalTime))
	}

	log.Info("")
}

// =============================================================================
// Convenience Function (Backward Compatibility)
// =============================================================================

// NewBuilder creates a new RecSplit Builder.
// Deprecated: Use recsplit.New(cfg) instead.
func NewBuilder(
	store interfaces.TxHashStore,
	cfCounts map[string]uint64,
	indexPath string,
	tmpPath string,
	multiIndex bool,
	logger interfaces.Logger,
	mem interfaces.MemoryMonitor,
) *Builder {
	return New(Config{
		Store:      store,
		CFCounts:   cfCounts,
		IndexPath:  indexPath,
		TmpPath:    tmpPath,
		MultiIndex: multiIndex,
		Logger:     logger,
		Memory:     mem,
	})
}

// GetStats returns the RecSplit build statistics.
// Deprecated: Use builder.Stats() instead.
func (b *Builder) GetStats() *Stats {
	return b.stats
}

// =============================================================================
// RecSplit Index Utilities
// =============================================================================

// VerifyIndex opens and performs a basic verification of a RecSplit index.
func VerifyIndex(indexPath string) (uint64, error) {
	idx, err := recsplit.OpenIndex(indexPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open index: %w", err)
	}
	defer idx.Close()

	keyCount := idx.KeyCount()
	return keyCount, nil
}

// OpenIndex opens a RecSplit index for reading.
func OpenIndex(indexPath string) (*recsplit.Index, error) {
	return recsplit.OpenIndex(indexPath)
}

// NewIndexReader creates a new IndexReader for the given index.
func NewIndexReader(idx *recsplit.Index) *recsplit.IndexReader {
	return recsplit.NewIndexReader(idx)
}

// =============================================================================
// Type Aliases for Backward Compatibility
// =============================================================================

// CFStats is an alias for CFBuildStats (backward compatibility).
// Deprecated: Use CFBuildStats instead.
type CFStats = CFBuildStats
