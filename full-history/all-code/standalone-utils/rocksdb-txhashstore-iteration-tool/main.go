// =============================================================================
// rocksdb-txhashstore-iteration-tool - Parallel Iterator Benchmark Tool
// =============================================================================
//
// This tool opens a RocksDB TxHash store in read-only mode and iterates all
// 16 column families in parallel using scan-optimized iterators.
//
// USAGE:
//
//	rocksdb-txhashstore-iteration-tool \
//	  --rocksdb-path /path/to/rocksdb \
//	  --log-file /path/to/iteration.log \
//	  --error-file /path/to/iteration.err
//
// OUTPUT:
//
//	Per-CF statistics:
//	  - Entry count for each column family
//	  - Iteration time per CF
//	  - Throughput (entries/second) per CF
//
//	Summary:
//	  - Total entries across all CFs
//	  - Total iteration time
//	  - Overall throughput
//
// =============================================================================

package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/cf"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/store"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/types"
)

// CFResult holds iteration results for a single column family.
type CFResult struct {
	Name     string
	Count    int64
	Duration time.Duration
}

func main() {
	// Parse command-line flags
	rocksdbPath := flag.String("rocksdb-path", "", "Path to RocksDB store (required)")
	logFile := flag.String("log-file", "", "Path to log file (required)")
	errorFile := flag.String("error-file", "", "Path to error file (required)")

	flag.Parse()

	// Validate required flags
	if *rocksdbPath == "" || *logFile == "" || *errorFile == "" {
		fmt.Fprintf(os.Stderr, "Error: All flags are required\n")
		fmt.Fprintf(os.Stderr, "\nUsage:\n")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Create logger
	logger, err := logging.NewDualLogger(*logFile, *errorFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Close()

	// Log startup
	logger.Separator()
	logger.Info("RocksDB TxHash Store Iteration Tool")
	logger.Separator()
	logger.Info("RocksDB Path: %s", *rocksdbPath)
	logger.Info("")

	// Open RocksDB store in read-only mode
	settings := types.DefaultRocksDBSettings()
	settings.ReadOnly = true

	txStore, err := store.OpenRocksDBTxHashStore(*rocksdbPath, &settings, logger)
	if err != nil {
		logger.Error("Failed to open RocksDB store: %v", err)
		logger.Sync()
		os.Exit(1)
	}

	// Start iteration
	logger.Info("Starting parallel iteration of 16 column families...")
	logger.Info("")

	totalStart := time.Now()

	// Create indexed results slice for consistent ordering (not a map)
	results := make([]CFResult, len(cf.Names))

	// Parallel iteration using WaitGroup
	var wg sync.WaitGroup

	for i, cfName := range cf.Names {
		wg.Add(1)

		// Pass loop variables as parameters to avoid capture bug
		go func(idx int, name string) {
			defer wg.Done()

			// Measure iteration time
			start := time.Now()

			// Create scan-optimized iterator
			iter := txStore.NewScanIteratorCF(name)
			defer iter.Close()

			// Count entries in this CF
			count := int64(0)
			for iter.SeekToFirst(); iter.Valid(); iter.Next() {
				count++
			}

			// Record results at the correct index
			results[idx] = CFResult{
				Name:     name,
				Count:    count,
				Duration: time.Since(start),
			}
		}(i, cfName)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	totalDuration := time.Since(totalStart)

	// Log per-CF statistics
	logger.Separator()
	logger.Info("PER-COLUMN-FAMILY STATISTICS")
	logger.Separator()

	totalEntries := int64(0)

	logger.Info("")
	logger.Info("CF          Count       Duration       Rate (entries/sec)")
	logger.Info("--  ---------------  ---------------  -------------------")

	for _, result := range results {
		totalEntries += result.Count
		rateStr := helpers.FormatRate(result.Count, result.Duration)
		logger.Info("%-2s  %15s  %15s  %s",
			result.Name,
			helpers.FormatNumber(result.Count),
			helpers.FormatDuration(result.Duration),
			rateStr)
	}

	logger.Info("--  ---------------  ---------------  -------------------")

	// Log summary
	logger.Info("")
	logger.Separator()
	logger.Info("SUMMARY")
	logger.Separator()

	logger.Info("Total entries:    %s", helpers.FormatNumber(totalEntries))
	logger.Info("Total time:       %s", helpers.FormatDuration(totalDuration))
	logger.Info("Overall rate:     %s", helpers.FormatRate(totalEntries, totalDuration))

	// Handle empty store gracefully
	if totalEntries == 0 {
		logger.Info("")
		logger.Info("Store is empty: 0 entries")
	}

	logger.Info("")
	logger.Separator()
	logger.Info("Iteration complete")
	logger.Separator()

	logger.Sync()
}
