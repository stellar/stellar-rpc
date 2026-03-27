// =============================================================================
// query_handler.go - SIGHUP Query Handling
// =============================================================================
//
// This file implements the query handler for SIGHUP signals during ingestion
// and compaction phases. When a SIGHUP is received, the handler:
//
//   1. Reads txHashes from the query file (one hex string per line)
//   2. Looks up each txHash in RocksDB
//   3. Writes results to a CSV file: txHash,ledgerSeq,queryTimeUs
//   4. Logs detailed statistics (latencies, found/not-found counts)
//
// DESIGN PHILOSOPHY:
//
//   The query handler allows operators to benchmark lookup performance
//   during ingestion without stopping the process. This is useful for:
//     - Validating that recently ingested data is queryable
//     - Measuring read performance under write load
//     - Debugging specific transaction lookups
//
// PHASE RESTRICTIONS:
//
//   SIGHUP queries are ONLY processed during:
//     - INGESTING: Data is being written, reads are concurrent
//     - COMPACTING: Reads are concurrent with compaction
//
//   SIGHUP is IGNORED during:
//     - BUILDING_RECSPLIT: RecSplit has exclusive iteration
//     - VERIFYING_RECSPLIT: Parallel verification in progress
//     - COMPLETE: Process exits soon anyway
//
// QUERY FILE FORMAT:
//
//   One txHash per line, as 64-character hex strings:
//
//     a1b2c3d4e5f6...  (64 hex characters = 32 bytes)
//     f0e1d2c3b4a5...
//     ...
//
//   Lines that are not valid 64-character hex are logged as errors.
//
// OUTPUT FILE FORMAT:
//
//   CSV with header:
//
//     txHash,ledgerSeq,queryTimeUs
//     a1b2c3d4e5f6...,12345678,42
//     f0e1d2c3b4a5...,-1,38       (ledgerSeq=-1 means not found)
//
// THREAD SAFETY:
//
//   The query handler runs in a separate goroutine and uses RocksDB's
//   thread-safe Get() method. The main ingestion/compaction continues
//   uninterrupted.
//
// FILE RECREATION:
//
//   Query output files are TRUNCATED on each process restart (not appended).
//   Each SIGHUP triggers a new batch of queries with fresh statistics.
//
// =============================================================================

package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/interfaces"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/stats"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/types"
)

// =============================================================================
// QueryHandler - SIGHUP Query Processing
// =============================================================================

// QueryHandler processes SIGHUP query requests during ingestion/compaction.
//
// LIFECYCLE:
//
//	handler := NewQueryHandler(config, store, logger)
//	handler.Start() // Begins listening for query requests
//	...
//	handler.TriggerQuery() // Called when SIGHUP received
//	...
//	handler.Stop() // Stop handler, wait for pending queries
//
// IMPORTANT:
//   - TriggerQuery() is non-blocking; queries run in background
//   - Multiple rapid SIGHUPs will queue up (but only one runs at a time)
//   - Stop() waits for any in-progress query to complete
type QueryHandler struct {
	// config holds all file paths and settings
	config *Config

	// store is the RocksDB store to query
	store interfaces.TxHashStore

	// mainLogger is the primary workflow logger (for phase-level logging)
	mainLogger interfaces.Logger

	// queryLogger is the specialized logger for query results/stats
	queryLogger *logging.QueryLogger

	// mu protects currentPhase and isRunning
	mu sync.Mutex

	// currentPhase is the workflow phase (determines if queries are allowed)
	currentPhase types.Phase

	// isRunning indicates if the handler is active
	isRunning bool

	// queryInProgress is set while a query batch is executing
	// Prevents concurrent query batches
	queryInProgress atomic.Bool

	// triggerChan receives signals to run a query batch
	triggerChan chan struct{}

	// stopChan signals the handler to stop
	stopChan chan struct{}

	// doneChan is closed when the handler goroutine exits
	doneChan chan struct{}

	// batchCount tracks the number of query batches processed
	batchCount int

	// totalQueries tracks total queries across all batches
	totalQueries int64

	// totalFound tracks total found queries across all batches
	totalFound int64

	// totalNotFound tracks total not-found queries across all batches
	totalNotFound int64
}

// NewQueryHandler creates a new QueryHandler.
//
// PARAMETERS:
//   - config: Configuration with file paths
//   - store: RocksDB store to query
//   - mainLogger: Main workflow logger
//
// RETURNS:
//   - A new QueryHandler (not yet started)
//   - An error if query logger creation fails
func NewQueryHandler(config *Config, store interfaces.TxHashStore, mainLogger interfaces.Logger) (*QueryHandler, error) {
	// Create query logger with the configured paths
	queryLogger, err := logging.NewQueryLogger(
		config.QueryOutput,
		config.QueryLog,
		config.QueryError,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create query logger: %w", err)
	}

	return &QueryHandler{
		config:       config,
		store:        store,
		mainLogger:   mainLogger,
		queryLogger:  queryLogger,
		currentPhase: types.PhaseIngesting,
		triggerChan:  make(chan struct{}, 10), // Buffer to prevent blocking
		stopChan:     make(chan struct{}),
		doneChan:     make(chan struct{}),
	}, nil
}

// Start begins the query handler goroutine.
//
// The goroutine listens for query triggers and processes them.
// Call Stop() to terminate the handler.
func (qh *QueryHandler) Start() {
	qh.mu.Lock()
	if qh.isRunning {
		qh.mu.Unlock()
		return
	}
	qh.isRunning = true
	qh.mu.Unlock()

	qh.mainLogger.Info("QueryHandler started - listening for SIGHUP queries")
	qh.queryLogger.Stats("QueryHandler started")

	go qh.runLoop()
}

// Stop signals the handler to stop and waits for completion.
//
// Any in-progress query batch will complete before Stop returns.
// Safe to call multiple times.
func (qh *QueryHandler) Stop() {
	qh.mu.Lock()
	if !qh.isRunning {
		qh.mu.Unlock()
		return
	}
	qh.isRunning = false
	qh.mu.Unlock()

	// Signal stop
	close(qh.stopChan)

	// Wait for handler goroutine to exit
	<-qh.doneChan

	// Log final statistics
	qh.logFinalStats()

	// Close query logger
	qh.queryLogger.Close()

	qh.mainLogger.Info("QueryHandler stopped")
}

// SetPhase updates the current workflow phase.
//
// Queries are only processed during INGESTING and COMPACTING phases.
func (qh *QueryHandler) SetPhase(phase types.Phase) {
	qh.mu.Lock()
	defer qh.mu.Unlock()
	qh.currentPhase = phase

	qh.queryLogger.Stats("Phase changed to: %s", phase)
}

// TriggerQuery signals the handler to process the query file.
//
// This method is NON-BLOCKING. The actual query processing happens
// in the background goroutine.
//
// If a query is already in progress, this trigger is queued.
// If the phase doesn't allow queries, the trigger is logged and ignored.
func (qh *QueryHandler) TriggerQuery() {
	qh.mu.Lock()
	phase := qh.currentPhase
	running := qh.isRunning
	qh.mu.Unlock()

	if !running {
		qh.mainLogger.Info("SIGHUP received but QueryHandler not running")
		return
	}

	// Check if phase allows queries
	if !qh.isQueryPhase(phase) {
		qh.mainLogger.Info("SIGHUP received but ignored during %s phase", phase)
		qh.queryLogger.Stats("SIGHUP ignored during %s phase", phase)
		return
	}

	// Send trigger (non-blocking due to buffer)
	select {
	case qh.triggerChan <- struct{}{}:
		qh.mainLogger.Info("SIGHUP received - query triggered (phase=%s)", phase)
	default:
		qh.mainLogger.Info("SIGHUP received but query queue full - skipping")
	}
}

// isQueryPhase returns true if queries are allowed in the given phase.
func (qh *QueryHandler) isQueryPhase(phase types.Phase) bool {
	return phase == types.PhaseIngesting || phase == types.PhaseCompacting
}

// runLoop is the main handler goroutine.
func (qh *QueryHandler) runLoop() {
	defer close(qh.doneChan)

	for {
		select {
		case <-qh.stopChan:
			return

		case <-qh.triggerChan:
			qh.processQueryBatch()
		}
	}
}

// processQueryBatch reads the query file and executes all queries.
//
// DESIGN NOTES:
//
//   - Only one batch runs at a time (queryInProgress guard)
//   - Each query is timed individually
//   - Results are written immediately (no batching for output)
//   - Statistics are collected and logged at the end
//
// ERROR HANDLING:
//
//   - File open errors are logged to query error log
//   - Invalid hex lines are logged and counted as parse errors
//   - RocksDB errors are logged per-query (don't abort batch)
func (qh *QueryHandler) processQueryBatch() {
	// Prevent concurrent batches
	if !qh.queryInProgress.CompareAndSwap(false, true) {
		qh.queryLogger.Stats("Query batch skipped - another batch in progress")
		return
	}
	defer qh.queryInProgress.Store(false)

	// Get current phase for logging
	qh.mu.Lock()
	phase := qh.currentPhase
	qh.mu.Unlock()

	qh.batchCount++
	batchNum := qh.batchCount

	// Log batch start
	startTime := time.Now()
	qh.queryLogger.Stats("")
	qh.queryLogger.Stats("=== QUERY BATCH %d START (phase=%s) ===", batchNum, phase)

	// Open query file
	file, err := os.Open(qh.config.QueryFile)
	if err != nil {
		qh.queryLogger.Error("Failed to open query file %s: %v", qh.config.QueryFile, err)
		qh.queryLogger.Stats("=== QUERY BATCH %d FAILED: could not open file ===", batchNum)
		return
	}
	defer file.Close()

	// Create statistics collector
	qStats := stats.NewQueryStats()

	// Process each line
	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Parse hex string
		txHashBytes := types.HexToBytes(line)
		if txHashBytes == nil || len(txHashBytes) != 32 {
			qh.queryLogger.Error("Line %d: invalid txHash hex: %s", lineNum, truncateForLog(line, 70))
			qStats.AddParseError()
			continue
		}

		// Execute query
		qh.executeQuery(line, txHashBytes, qStats)
	}

	if err := scanner.Err(); err != nil {
		qh.queryLogger.Error("Error reading query file: %v", err)
	}

	// Mark batch complete
	qStats.Finish()
	batchDuration := time.Since(startTime)

	// Update cumulative statistics
	totalQueries := qStats.TotalQueries()
	atomic.AddInt64(&qh.totalQueries, int64(totalQueries))
	atomic.AddInt64(&qh.totalFound, int64(qStats.FoundCount))
	atomic.AddInt64(&qh.totalNotFound, int64(qStats.NotFoundCount))

	// Log batch statistics
	qh.logBatchStats(batchNum, qStats, batchDuration)

	// Flush logs
	qh.queryLogger.Sync()
}

// executeQuery performs a single query and records the result.
//
// TIMING:
//   - Query time includes only the RocksDB Get() call
//   - Result writing is NOT included in query time
//
// RESULT FORMAT:
//   - Found: txHash,ledgerSeq,queryTimeUs
//   - Not found: txHash,-1,queryTimeUs
func (qh *QueryHandler) executeQuery(txHashHex string, txHashBytes []byte, qStats *stats.QueryStats) {
	// Time the query
	queryStart := time.Now()
	value, found, err := qh.store.Get(txHashBytes)
	queryTime := time.Since(queryStart)

	if err != nil {
		// RocksDB error - log and count as not found
		qh.queryLogger.Error("Query error for %s: %v", txHashHex, err)
		qStats.AddNotFound(queryTime)
		qh.queryLogger.NotFound(txHashHex, queryTime.Microseconds())
		return
	}

	if found {
		ledgerSeq := types.ParseLedgerSeq(value)
		qStats.AddFound(queryTime)
		qh.queryLogger.Result(txHashHex, ledgerSeq, queryTime.Microseconds())
	} else {
		qStats.AddNotFound(queryTime)
		qh.queryLogger.NotFound(txHashHex, queryTime.Microseconds())
	}
}

// logBatchStats logs detailed statistics for a query batch.
func (qh *QueryHandler) logBatchStats(batchNum int, qStats *stats.QueryStats, duration time.Duration) {
	qh.queryLogger.Stats("")
	qh.queryLogger.Stats("--- QUERY BATCH %d COMPLETE ---", batchNum)
	qh.queryLogger.Stats("")
	qh.queryLogger.Stats("SUMMARY:")
	qh.queryLogger.Stats("  Total Queries:    %d", qStats.TotalQueries())
	qh.queryLogger.Stats("  Found:            %d (%.2f%%)",
		qStats.FoundCount,
		safePercent(qStats.FoundCount, qStats.TotalQueries()))
	qh.queryLogger.Stats("  Not Found:        %d (%.2f%%)",
		qStats.NotFoundCount,
		safePercent(qStats.NotFoundCount, qStats.TotalQueries()))
	qh.queryLogger.Stats("  Parse Errors:     %d", qStats.ParseErrorCount)
	qh.queryLogger.Stats("  Batch Duration:   %v", duration)

	// Calculate throughput
	if duration.Seconds() > 0 {
		qps := float64(qStats.TotalQueries()) / duration.Seconds()
		qh.queryLogger.Stats("  Throughput:       %.1f queries/sec", qps)
	}

	// Log latency statistics for found queries
	if qStats.FoundCount > 0 {
		qh.queryLogger.Stats("")
		qh.queryLogger.Stats("FOUND QUERY LATENCIES:")
		summary := qStats.FoundLatenciesSummary()
		qh.queryLogger.Stats("  Count:   %d", summary.Count)
		qh.queryLogger.Stats("  Min:     %v", summary.Min)
		qh.queryLogger.Stats("  Max:     %v", summary.Max)
		qh.queryLogger.Stats("  Avg:     %v (+/- %v)", summary.Avg, summary.StdDev)
		qh.queryLogger.Stats("  P50:     %v", summary.P50)
		qh.queryLogger.Stats("  P90:     %v", summary.P90)
		qh.queryLogger.Stats("  P95:     %v", summary.P95)
		qh.queryLogger.Stats("  P99:     %v", summary.P99)
	}

	// Log latency statistics for not-found queries
	if qStats.NotFoundCount > 0 {
		qh.queryLogger.Stats("")
		qh.queryLogger.Stats("NOT-FOUND QUERY LATENCIES:")
		summary := qStats.NotFoundLatenciesSummary()
		qh.queryLogger.Stats("  Count:   %d", summary.Count)
		qh.queryLogger.Stats("  Min:     %v", summary.Min)
		qh.queryLogger.Stats("  Max:     %v", summary.Max)
		qh.queryLogger.Stats("  Avg:     %v (+/- %v)", summary.Avg, summary.StdDev)
		qh.queryLogger.Stats("  P50:     %v", summary.P50)
		qh.queryLogger.Stats("  P90:     %v", summary.P90)
		qh.queryLogger.Stats("  P95:     %v", summary.P95)
		qh.queryLogger.Stats("  P99:     %v", summary.P99)
	}

	qh.queryLogger.Stats("")
	qh.queryLogger.Stats("=== QUERY BATCH %d END ===", batchNum)
	qh.queryLogger.Stats("")
}

// logFinalStats logs cumulative statistics across all batches.
func (qh *QueryHandler) logFinalStats() {
	totalQueries := atomic.LoadInt64(&qh.totalQueries)
	totalFound := atomic.LoadInt64(&qh.totalFound)
	totalNotFound := atomic.LoadInt64(&qh.totalNotFound)

	qh.queryLogger.Stats("")
	qh.queryLogger.Stats("=========================================================================")
	qh.queryLogger.Stats("                    QUERY HANDLER FINAL STATISTICS")
	qh.queryLogger.Stats("=========================================================================")
	qh.queryLogger.Stats("")
	qh.queryLogger.Stats("CUMULATIVE STATISTICS:")
	qh.queryLogger.Stats("  Total Batches:    %d", qh.batchCount)
	qh.queryLogger.Stats("  Total Queries:    %d", totalQueries)
	qh.queryLogger.Stats("  Total Found:      %d (%.2f%%)",
		totalFound, safePercentInt64(totalFound, totalQueries))
	qh.queryLogger.Stats("  Total Not Found:  %d (%.2f%%)",
		totalNotFound, safePercentInt64(totalNotFound, totalQueries))
	qh.queryLogger.Stats("")
	qh.queryLogger.Stats("QueryHandler shutdown complete")

	// Also log to main logger
	qh.mainLogger.Info("QueryHandler: processed %d batches, %d queries (%d found, %d not found)",
		qh.batchCount, totalQueries, totalFound, totalNotFound)
}

// =============================================================================
// Helper Functions
// =============================================================================

// safePercent calculates percentage without division by zero.
func safePercent(numerator, denominator int) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator) * 100
}

// safePercentInt64 calculates percentage for int64 values.
func safePercentInt64(numerator, denominator int64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator) * 100
}

// truncateForLog truncates a string for safe logging.
func truncateForLog(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

// =============================================================================
// QueryHandlerStats - Read-only Statistics Access
// =============================================================================

// GetStats returns current query handler statistics.
//
// Returns:
//   - batchCount: Number of query batches processed
//   - totalQueries: Total queries across all batches
//   - totalFound: Total found queries
//   - totalNotFound: Total not-found queries
func (qh *QueryHandler) GetStats() (batchCount int, totalQueries, totalFound, totalNotFound int64) {
	qh.mu.Lock()
	batchCount = qh.batchCount
	qh.mu.Unlock()

	totalQueries = atomic.LoadInt64(&qh.totalQueries)
	totalFound = atomic.LoadInt64(&qh.totalFound)
	totalNotFound = atomic.LoadInt64(&qh.totalNotFound)
	return
}

// IsQueryInProgress returns true if a query batch is currently running.
func (qh *QueryHandler) IsQueryInProgress() bool {
	return qh.queryInProgress.Load()
}
