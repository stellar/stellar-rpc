// =============================================================================
// rocksdb-txhashstore-compaction-tool/main.go
// =============================================================================
//
// Standalone tool that opens a RocksDB TxHash store in read-write mode and
// compacts all 16 column families in parallel, reporting timing and before/after
// statistics.
//
// Usage:
//
//	./rocksdb-txhashstore-compaction-tool \
//	  --rocksdb-path /path/to/rocksdb \
//	  --log-file /path/to/compaction.log \
//	  --error-file /path/to/compaction.err
//
// =============================================================================

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/compact"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/store"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/types"
)

func main() {
	rocksdbPath := flag.String("rocksdb-path", "", "Path to RocksDB store (required)")
	logFile := flag.String("log-file", "", "Path to log file (required)")
	errorFile := flag.String("error-file", "", "Path to error file (required)")

	flag.Parse()

	if *rocksdbPath == "" {
		fmt.Fprintf(os.Stderr, "ERROR: --rocksdb-path is required\n")
		flag.Usage()
		os.Exit(1)
	}
	if *logFile == "" {
		fmt.Fprintf(os.Stderr, "ERROR: --log-file is required\n")
		flag.Usage()
		os.Exit(1)
	}
	if *errorFile == "" {
		fmt.Fprintf(os.Stderr, "ERROR: --error-file is required\n")
		flag.Usage()
		os.Exit(1)
	}

	// Ensure parent directories exist for log files
	if err := helpers.EnsureDir(filepath.Dir(*logFile)); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Failed to create log directory: %v\n", err)
		os.Exit(1)
	}
	if err := helpers.EnsureDir(filepath.Dir(*errorFile)); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Failed to create error directory: %v\n", err)
		os.Exit(1)
	}

	logger, err := logging.NewDualLogger(*logFile, *errorFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Close()

	logger.Info("===============================================================================")
	logger.Info("RocksDB TxHash Store Compaction Tool")
	logger.Info("===============================================================================")
	logger.Info("")
	logger.Info("Configuration:")
	logger.Info("  RocksDB path:  %s", *rocksdbPath)
	logger.Info("  Log file:      %s", *logFile)
	logger.Info("  Error file:    %s", *errorFile)
	logger.Info("")

	settings := types.DefaultRocksDBSettings()
	settings.ReadOnly = false

	logger.Info("Opening RocksDB store in read-write mode...")
	txStore, err := store.OpenRocksDBTxHashStore(*rocksdbPath, &settings, logger)
	if err != nil {
		logger.Error("Failed to open RocksDB store: %v", err)
		logger.Sync()
		os.Exit(1)
	}
	defer txStore.Close()

	logger.Info("Collecting pre-compaction statistics...")
	logger.Info("")
	store.LogAllCFStats(txStore, logger, "PRE-COMPACTION STATISTICS")
	logger.Info("")

	logger.Info("Starting parallel compaction of all 16 column families...")
	logger.Info("")
	totalTime := compact.CompactAllCFs(txStore, logger)
	logger.Info("")

	logger.Info("Collecting post-compaction statistics...")
	logger.Info("")
	store.LogAllCFStats(txStore, logger, "POST-COMPACTION STATISTICS")
	logger.Info("")

	logger.Info("===============================================================================")
	logger.Info("Compaction Complete")
	logger.Info("===============================================================================")
	logger.Info("Total compaction time: %s", helpers.FormatDuration(totalTime))
	logger.Info("")

	logger.Sync()
}
