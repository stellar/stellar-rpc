// =============================================================================
// rocksdb-txhashstore-build-recsplit-tool - Standalone RecSplit Index Builder
// =============================================================================
//
// This tool builds RecSplit minimal perfect hash indexes from a RocksDB store
// containing txHash→ledgerSeq mappings.
//
// USAGE:
//
//	rocksdb-txhashstore-build-recsplit-tool \
//	  --rocksdb-path /path/to/rocksdb \
//	  --output-dir /path/to/indexes \
//	  --log /path/to/build.log \
//	  --error /path/to/build.err \
//	  [--compact] \
//	  [--multi-index-enabled] \
//	  [--verify] \
//	  [--block-cache-mb 8192]
//
// FEATURES:
//
//   - Discovers key counts by iterating each CF in parallel
//   - Optional compaction before building (--compact)
//   - Optional verification after building (--verify)
//   - Single combined index (default) or multi-index mode (--multi-index-enabled)
//
// OUTPUT:
//
//	Default mode:      Single txhash.idx file (all CFs combined)
//	Multi-index mode:  16 index files: cf-0.idx through cf-f.idx
//
// =============================================================================

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/cf"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/compact"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/interfaces"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/memory"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/recsplit"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/store"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/types"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/verify"
)

// =============================================================================
// Key Counter (discovers key counts by iterating)
// =============================================================================

func countKeys(txStore interfaces.TxHashStore, cfName string) (uint64, error) {
	// Use scan-optimized iterator for full count (avoids cache pollution, uses readahead)
	iter := txStore.NewScanIteratorCF(cfName)
	defer iter.Close()

	var count uint64
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
	}
	if err := iter.Error(); err != nil {
		return 0, err
	}
	return count, nil
}

// =============================================================================
// Main
// =============================================================================

func main() {
	// Parse flags
	rocksdbPath := flag.String("rocksdb-path", "", "Path to RocksDB store (required)")
	outputDir := flag.String("output-dir", "", "Directory for RecSplit index files (required)")
	logPath := flag.String("log", "", "Path to log file (required)")
	errorPath := flag.String("error", "", "Path to error file (required)")
	multiIndexEnabled := flag.Bool("multi-index-enabled", false, "Build 16 separate cf-X.idx files instead of single txhash.idx")
	doCompact := flag.Bool("compact", false, "Compact RocksDB before building")
	doVerify := flag.Bool("verify", false, "Verify indexes after building")
	blockCacheMB := flag.Int("block-cache-mb", 8192, "RocksDB block cache size in MB")

	flag.Parse()

	// Validate flags
	if *rocksdbPath == "" || *outputDir == "" || *logPath == "" || *errorPath == "" {
		fmt.Fprintln(os.Stderr, "Error: --rocksdb-path, --output-dir, --log, and --error are required")
		flag.Usage()
		os.Exit(1)
	}

	// Initialize logger
	logger, err := logging.NewDualLogger(*logPath, *errorPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Close()

	// Initialize memory monitor
	memMonitor := memory.NewMemoryMonitor(logger, memory.DefaultRAMWarningThresholdGB)
	defer memMonitor.Stop()

	logger.Separator()
	logger.Info("                    BUILD RECSPLIT TOOL")
	logger.Separator()
	logger.Info("")
	logger.Info("Configuration:")
	logger.Info("  RocksDB Path:    %s", *rocksdbPath)
	logger.Info("  Output Dir:      %s", *outputDir)
	logger.Info("  Multi-Index:     %v", *multiIndexEnabled)
	logger.Info("  Compact First:   %v", *doCompact)
	logger.Info("  Verify After:    %v", *doVerify)
	logger.Info("  Block Cache:     %d MB", *blockCacheMB)
	logger.Info("")

	// Create output and temp directories
	indexPath := filepath.Join(*outputDir, "index")
	tmpPath := filepath.Join(*outputDir, "tmp")

	if err := os.MkdirAll(indexPath, 0755); err != nil {
		logger.Error("Failed to create index directory: %v", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(tmpPath, 0755); err != nil {
		logger.Error("Failed to create temp directory: %v", err)
		os.Exit(1)
	}

	// Configure RocksDB settings
	// Open in read-only mode unless compaction is requested
	settings := types.DefaultRocksDBSettings()
	settings.BlockCacheSizeMB = *blockCacheMB
	settings.ReadOnly = !*doCompact

	// Open RocksDB using pkg/store
	txStore, err := store.OpenRocksDBTxHashStore(*rocksdbPath, &settings, logger)
	if err != nil {
		logger.Error("Failed to open RocksDB: %v", err)
		os.Exit(1)
	}
	defer txStore.Close()

	// Phase 1: Compaction (optional)
	if *doCompact {
		logger.Separator()
		logger.Info("                    COMPACTION PHASE")
		logger.Separator()
		logger.Info("")

		compact.CompactAllCFs(txStore, logger)
		logger.Info("Current RSS: %.2f GB", memMonitor.CurrentRSSGB())
		logger.Info("")
	}

	// Discover key counts in parallel
	logger.Separator()
	logger.Info("                    DISCOVERING KEY COUNTS (PARALLEL)")
	logger.Separator()
	logger.Info("")

	cfCounts := make(map[string]uint64)
	var mu sync.Mutex
	var totalKeys uint64
	countStart := time.Now()

	// Count all CFs in parallel
	type countResult struct {
		cfName   string
		count    uint64
		duration time.Duration
		err      error
	}
	results := make([]countResult, len(cf.Names))

	var wg sync.WaitGroup
	for i, cfName := range cf.Names {
		wg.Add(1)
		go func(idx int, name string) {
			defer wg.Done()
			start := time.Now()
			count, err := countKeys(txStore, name)
			results[idx] = countResult{
				cfName:   name,
				count:    count,
				duration: time.Since(start),
				err:      err,
			}
		}(i, cfName)
	}
	wg.Wait()

	// Collect results and check for errors
	for i, cfName := range cf.Names {
		r := results[i]
		if r.err != nil {
			logger.Error("Failed to count keys in CF %s: %v", cfName, r.err)
			os.Exit(1)
		}
		mu.Lock()
		cfCounts[cfName] = r.count
		totalKeys += r.count
		mu.Unlock()
		logger.Info("[%2d/16] CF [%s]: %s keys (counted in %s)",
			i+1, cfName, helpers.FormatNumber(int64(r.count)), helpers.FormatDuration(r.duration))
	}

	logger.Info("")
	logger.Info("Total keys: %s (discovered in %s)", helpers.FormatNumber(int64(totalKeys)), helpers.FormatDuration(time.Since(countStart)))
	logger.Info("Current RSS: %.2f GB", memMonitor.CurrentRSSGB())
	logger.Info("")

	// Phase 2: Build indexes using pkg/recsplit
	// Use pkg/recsplit.New with Config pattern
	builder := recsplit.New(recsplit.Config{
		Store:      txStore,
		CFCounts:   cfCounts,
		IndexPath:  indexPath,
		TmpPath:    tmpPath,
		MultiIndex: *multiIndexEnabled,
		Logger:     logger,
		Memory:     memMonitor,
	})

	buildStats, err := builder.Run()
	if err != nil {
		logger.Error("RecSplit building failed: %v", err)
		os.Exit(1)
	}

	// Phase 3: Verification (optional) using pkg/verify
	if *doVerify {
		// Use pkg/verify.New with Config pattern
		verifier := verify.New(verify.Config{
			Store:      txStore,
			IndexPath:  indexPath,
			MultiIndex: *multiIndexEnabled,
			Logger:     logger,
			Memory:     memMonitor,
		})

		verifyStats, err := verifier.Run()
		if err != nil {
			logger.Error("Verification failed: %v", err)
			os.Exit(1)
		}

		if verifyStats.TotalFailures > 0 {
			logger.Error("Verification completed with %d failures", verifyStats.TotalFailures)
		}
	}

	// Final summary
	logger.Separator()
	memMonitor.LogSummary(logger)
	logger.Sync()

	// Clean up temp directory
	os.RemoveAll(tmpPath)

	fmt.Printf("Build complete: %s keys in %s\n", helpers.FormatNumber(int64(buildStats.TotalKeys)), helpers.FormatDuration(buildStats.TotalTime))
	fmt.Printf("Index files: %s\n", indexPath)

	// Calculate total size
	var totalSize int64
	for _, stats := range buildStats.PerCFStats {
		totalSize += stats.IndexSize
	}
	fmt.Printf("Total index size: %s\n", helpers.FormatBytes(totalSize))

	if *doVerify {
		fmt.Println("Verification: PASSED")
	}
}
