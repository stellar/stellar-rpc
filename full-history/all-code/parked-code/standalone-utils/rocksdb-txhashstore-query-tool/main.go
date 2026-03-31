// =============================================================================
// rocksdb-txhashstore-query-tool - Standalone Query Performance Testing Tool
// =============================================================================
//
// This tool benchmarks query performance against a compacted RocksDB store
// containing txHash→ledgerSeq mappings.
//
// USAGE:
//
//	rocksdb-txhashstore-query-tool \
//	  --rocksdb-path /path/to/rocksdb \
//	  --query-file /path/to/queries.txt \
//	  --output /path/to/results.csv \
//	  --log /path/to/benchmark.log \
//	  --block-cache-mb 8192
//
// INPUT FORMAT:
//
//	Query file contains one txHash per line (64-character hex string).
//
// OUTPUT FORMAT:
//
//	CSV with columns: txHash,ledgerSeq,queryTimeUs
//	ledgerSeq = -1 if not found
//
// =============================================================================

package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/cf"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/stats"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/types"
	"github.com/linxGnu/grocksdb"
)

// =============================================================================
// Logging
// =============================================================================

// Logger is a simple file-based logger for rocksdb-txhashstore-query-tool.
// This is intentionally minimal - the tool doesn't need dual logging.
type Logger struct {
	mu      sync.Mutex
	logFile *os.File
}

func NewLogger(path string) (*Logger, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	return &Logger{logFile: f}, nil
}

func (l *Logger) Info(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(l.logFile, "[%s] %s\n", timestamp, msg)
}

func (l *Logger) Separator() {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintln(l.logFile, "=========================================================================")
}

func (l *Logger) Sync() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logFile.Sync()
}

func (l *Logger) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logFile.Sync()
	l.logFile.Close()
}

// =============================================================================
// RocksDB Store (Read-Only)
// =============================================================================

type Store struct {
	db         *grocksdb.DB
	opts       *grocksdb.Options
	cfHandles  []*grocksdb.ColumnFamilyHandle
	cfOpts     []*grocksdb.Options
	readOpts   *grocksdb.ReadOptions
	blockCache *grocksdb.Cache
	cfIndexMap map[string]int
}

func OpenStore(path string, blockCacheMB int) (*Store, error) {
	// Create shared block cache
	var blockCache *grocksdb.Cache
	if blockCacheMB > 0 {
		blockCache = grocksdb.NewLRUCache(uint64(blockCacheMB * types.MB))
	}

	// Create database options
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)

	// Prepare column family names
	cfNames := []string{"default"}
	cfNames = append(cfNames, cf.Names...)

	// Create options for each CF
	cfOptsList := make([]*grocksdb.Options, len(cfNames))
	for i := range cfNames {
		cfOpts := grocksdb.NewDefaultOptions()

		// Block-based table options with bloom filter
		bbto := grocksdb.NewDefaultBlockBasedTableOptions()
		bbto.SetFilterPolicy(grocksdb.NewBloomFilter(12))
		if blockCache != nil {
			bbto.SetBlockCache(blockCache)
		}
		cfOpts.SetBlockBasedTableFactory(bbto)

		cfOptsList[i] = cfOpts
	}

	// Open database
	db, cfHandles, err := grocksdb.OpenDbForReadOnlyColumnFamilies(opts, path, cfNames, cfOptsList, false)
	if err != nil {
		opts.Destroy()
		for _, cfOpt := range cfOptsList {
			cfOpt.Destroy()
		}
		if blockCache != nil {
			blockCache.Destroy()
		}
		return nil, fmt.Errorf("failed to open RocksDB: %w", err)
	}

	// Build CF index map
	cfIndexMap := make(map[string]int)
	for i, name := range cfNames {
		cfIndexMap[name] = i
	}

	// Read options
	readOpts := grocksdb.NewDefaultReadOptions()

	return &Store{
		db:         db,
		opts:       opts,
		cfHandles:  cfHandles,
		cfOpts:     cfOptsList,
		readOpts:   readOpts,
		blockCache: blockCache,
		cfIndexMap: cfIndexMap,
	}, nil
}

func (s *Store) Get(txHash []byte) (value []byte, found bool, err error) {
	cfName := cf.GetName(txHash)
	cfHandle := s.cfHandles[s.cfIndexMap[cfName]]

	slice, err := s.db.GetCF(s.readOpts, cfHandle, txHash)
	if err != nil {
		return nil, false, err
	}
	defer slice.Free()

	if !slice.Exists() {
		return nil, false, nil
	}

	value = make([]byte, slice.Size())
	copy(value, slice.Data())
	return value, true, nil
}

func (s *Store) Close() {
	if s.readOpts != nil {
		s.readOpts.Destroy()
	}
	for _, cfHandle := range s.cfHandles {
		if cfHandle != nil {
			cfHandle.Destroy()
		}
	}
	if s.db != nil {
		s.db.Close()
	}
	if s.opts != nil {
		s.opts.Destroy()
	}
	for _, cfOpt := range s.cfOpts {
		if cfOpt != nil {
			cfOpt.Destroy()
		}
	}
	if s.blockCache != nil {
		s.blockCache.Destroy()
	}
}

// =============================================================================
// Main
// =============================================================================

func main() {
	// Parse flags
	rocksdbPath := flag.String("rocksdb-path", "", "Path to RocksDB store (required)")
	queryFile := flag.String("query-file", "", "Path to query file (required)")
	outputPath := flag.String("output", "query-results.csv", "Path to CSV output")
	logPath := flag.String("log", "rocksdb-txhashstore-query-tool.log", "Path to log file")
	blockCacheMB := flag.Int("block-cache-mb", 8192, "RocksDB block cache size in MB")

	flag.Parse()

	// Validate flags
	if *rocksdbPath == "" {
		fmt.Fprintln(os.Stderr, "Error: --rocksdb-path is required")
		flag.Usage()
		os.Exit(1)
	}
	if *queryFile == "" {
		fmt.Fprintln(os.Stderr, "Error: --query-file is required")
		flag.Usage()
		os.Exit(1)
	}

	// Initialize logger
	logger, err := NewLogger(*logPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating log file: %v\n", err)
		os.Exit(1)
	}
	defer logger.Close()

	logger.Separator()
	logger.Info("                    QUERY BENCHMARK TOOL")
	logger.Separator()
	logger.Info("")
	logger.Info("Configuration:")
	logger.Info("  RocksDB Path:    %s", *rocksdbPath)
	logger.Info("  Query File:      %s", *queryFile)
	logger.Info("  Output File:     %s", *outputPath)
	logger.Info("  Block Cache:     %d MB", *blockCacheMB)
	logger.Info("")

	// Open RocksDB
	logger.Info("Opening RocksDB store...")
	store, err := OpenStore(*rocksdbPath, *blockCacheMB)
	if err != nil {
		logger.Info("ERROR: Failed to open RocksDB: %v", err)
		fmt.Fprintf(os.Stderr, "Error opening RocksDB: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()
	logger.Info("RocksDB store opened successfully")
	logger.Info("")

	// Open query file
	qf, err := os.Open(*queryFile)
	if err != nil {
		logger.Info("ERROR: Failed to open query file: %v", err)
		fmt.Fprintf(os.Stderr, "Error opening query file: %v\n", err)
		os.Exit(1)
	}
	defer qf.Close()

	// Open output file
	output, err := os.OpenFile(*outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		logger.Info("ERROR: Failed to create output file: %v", err)
		fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer output.Close()

	// Write CSV header
	fmt.Fprintln(output, "txHash,ledgerSeq,queryTimeUs")

	// Initialize statistics using pkg/stats
	foundStats := stats.NewLatencyStats()
	notFoundStats := stats.NewLatencyStats()
	var foundCount, notFoundCount, parseErrors int

	logger.Info("Running queries...")
	startTime := time.Now()

	// Process queries
	scanner := bufio.NewScanner(qf)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// Skip empty lines
		if len(line) == 0 {
			continue
		}

		// Parse txHash using pkg/types
		txHash := types.HexToBytes(line)
		if txHash == nil || len(txHash) != 32 {
			parseErrors++
			logger.Info("Line %d: invalid txHash: %s", lineNum, line)
			continue
		}

		// Query
		queryStart := time.Now()
		value, found, err := store.Get(txHash)
		queryTime := time.Since(queryStart)

		if err != nil {
			logger.Info("Line %d: query error: %v", lineNum, err)
			continue
		}

		if found {
			ledgerSeq := types.ParseLedgerSeq(value)
			foundStats.Add(queryTime)
			foundCount++
			fmt.Fprintf(output, "%s,%d,%d\n", line, ledgerSeq, queryTime.Microseconds())
		} else {
			notFoundStats.Add(queryTime)
			notFoundCount++
			fmt.Fprintf(output, "%s,-1,%d\n", line, queryTime.Microseconds())
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Info("ERROR: Scanner error: %v", err)
	}

	totalTime := time.Since(startTime)
	totalQueries := foundCount + notFoundCount

	// Log summary
	logger.Separator()
	logger.Info("                    BENCHMARK RESULTS")
	logger.Separator()
	logger.Info("")
	logger.Info("SUMMARY:")
	logger.Info("  Total Queries:     %s", helpers.FormatNumber(int64(totalQueries)))
	logger.Info("  Found:             %s (%.2f%%)", helpers.FormatNumber(int64(foundCount)),
		float64(foundCount)/float64(totalQueries)*100)
	logger.Info("  Not Found:         %s (%.2f%%)", helpers.FormatNumber(int64(notFoundCount)),
		float64(notFoundCount)/float64(totalQueries)*100)
	logger.Info("  Parse Errors:      %d", parseErrors)
	logger.Info("  Total Time:        %s", helpers.FormatDuration(totalTime))
	logger.Info("  Queries/sec:       %.2f", float64(totalQueries)/totalTime.Seconds())
	logger.Info("")

	if foundCount > 0 {
		foundSummary := foundStats.Summary()
		logger.Info("FOUND QUERIES LATENCY:")
		logger.Info("  %s", foundSummary.String())
		logger.Info("")
	}

	if notFoundCount > 0 {
		notFoundSummary := notFoundStats.Summary()
		logger.Info("NOT-FOUND QUERIES LATENCY:")
		logger.Info("  %s", notFoundSummary.String())
		logger.Info("")
	}

	logger.Info("Results written to: %s", *outputPath)
	logger.Separator()
	logger.Sync()

	fmt.Printf("Benchmark complete: %s queries in %s (%.2f qps)\n",
		helpers.FormatNumber(int64(totalQueries)), helpers.FormatDuration(totalTime), float64(totalQueries)/totalTime.Seconds())
	fmt.Printf("Results: %s, Log: %s\n", *outputPath, *logPath)
}
