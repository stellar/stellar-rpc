// =============================================================================
// pkg/compact/compact.go - RocksDB Compaction Phase
// =============================================================================
//
// This package implements the compaction phase that runs after ingestion completes.
//
// USAGE:
//
//	compactor := compact.New(compact.Config{
//	    Store:  txHashStore,
//	    Logger: logger,
//	    Memory: memoryMonitor,
//	})
//	stats, err := compactor.Run()
//
// The Compactor uses a scoped logger with [COMPACT] prefix for all log messages.
//
// =============================================================================

package compact

import (
	"fmt"
	"sync"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/cf"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/interfaces"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/memory"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/store"
	"github.com/stellar/stellar-rpc/full-history/all-code/parked-code/txhash-ingestion-workflow/pkg/types"
)

// =============================================================================
// Compactor - Main Compaction Component
// =============================================================================

// Config holds the configuration for creating a Compactor.
type Config struct {
	// Store is the TxHash store to compact (required)
	Store interfaces.TxHashStore

	// Logger is the parent logger (required)
	// A scoped logger with [COMPACT] prefix will be created internally
	Logger interfaces.Logger

	// Memory is the memory monitor for tracking RAM usage (optional)
	Memory interfaces.MemoryMonitor
}

// Compactor handles the compaction phase of the workflow.
//
// Compaction runs after ingestion completes and:
//  1. Collects pre-compaction statistics
//  2. Compacts all 16 column families in parallel
//  3. Collects post-compaction statistics
//  4. Logs before/after comparison
//
// The Compactor is designed to be composable - it takes its dependencies
// via Config and uses a scoped logger for all output.
type Compactor struct {
	store  interfaces.TxHashStore
	log    interfaces.Logger // Scoped logger with [COMPACT] prefix
	memory interfaces.MemoryMonitor
	stats  *Stats
}

// New creates a new Compactor with the given configuration.
//
// The Compactor creates a scoped logger with [COMPACT] prefix internally,
// so all log messages are automatically prefixed.
func New(cfg Config) *Compactor {
	if cfg.Store == nil {
		panic("compact.New: Store is required")
	}
	if cfg.Logger == nil {
		panic("compact.New: Logger is required")
	}

	return &Compactor{
		store:  cfg.Store,
		log:    cfg.Logger.WithScope("COMPACT"),
		memory: cfg.Memory,
		stats:  NewStats(),
	}
}

// Run executes the compaction phase.
//
// Compacts all 16 column families in parallel and collects before/after statistics.
func (c *Compactor) Run() (*Stats, error) {
	c.stats.StartTime = time.Now()

	c.log.Separator()
	c.log.Info("                         COMPACTION PHASE")
	c.log.Separator()
	c.log.Info("")

	// Collect before stats
	c.log.Info("Collecting pre-compaction statistics...")
	c.stats.BeforeStats = c.store.GetAllCFStats()
	store.LogAllCFStats(c.store, c.log, "PRE-COMPACTION STATISTICS")

	// Log level distribution before
	c.log.Info("Level distribution BEFORE compaction:")
	store.LogCFLevelStats(c.store, c.log)

	// Take memory snapshot
	beforeMem := memory.TakeMemorySnapshot()
	beforeMem.Log(c.log, "Pre-Compaction")

	// Compact each CF in parallel
	c.log.Separator()
	c.log.Info("                    COMPACTING COLUMN FAMILIES (PARALLEL)")
	c.log.Separator()
	c.log.Info("")
	c.log.Info("Starting parallel compaction of all 16 column families...")

	totalStart := time.Now()

	// Use a mutex to protect stats map writes (Go maps are not thread-safe)
	var statsMu sync.Mutex
	var wg sync.WaitGroup

	for _, cfName := range cf.Names {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()

			cfStart := time.Now()
			c.store.CompactCF(name)
			cfElapsed := time.Since(cfStart)

			statsMu.Lock()
			c.stats.PerCFTime[name] = cfElapsed
			statsMu.Unlock()

			c.log.Info("  CF [%s] compacted in %s", name, helpers.FormatDuration(cfElapsed))
		}(cfName)
	}
	wg.Wait()

	// Memory check after all compactions complete
	if c.memory != nil {
		c.memory.Check()
	}

	c.stats.TotalTime = time.Since(totalStart)
	c.stats.EndTime = time.Now()

	c.log.Info("")
	c.log.Info("All column families compacted in %s (parallel)", helpers.FormatDuration(c.stats.TotalTime))
	c.log.Info("")

	// Collect after stats
	c.log.Info("Collecting post-compaction statistics...")
	c.stats.AfterStats = c.store.GetAllCFStats()
	store.LogAllCFStats(c.store, c.log, "POST-COMPACTION STATISTICS")

	// Log level distribution after
	c.log.Info("Level distribution AFTER compaction:")
	store.LogCFLevelStats(c.store, c.log)

	// Log comparison
	c.stats.LogBeforeAfterComparison(c.log)

	// Take memory snapshot
	afterMem := memory.TakeMemorySnapshot()
	afterMem.Log(c.log, "Post-Compaction")

	// Log summary
	c.stats.LogSummary(c.log)

	c.log.Sync()

	return c.stats, nil
}

// Stats returns the compaction statistics.
func (c *Compactor) Stats() *Stats {
	return c.stats
}

// =============================================================================
// Stats - Compaction Statistics
// =============================================================================

// Stats holds statistics from the compaction phase.
type Stats struct {
	StartTime   time.Time
	EndTime     time.Time
	TotalTime   time.Duration
	PerCFTime   map[string]time.Duration
	BeforeStats []types.CFStats
	AfterStats  []types.CFStats
}

// NewStats creates a new Stats instance.
func NewStats() *Stats {
	return &Stats{
		PerCFTime: make(map[string]time.Duration),
	}
}

// LogSummary logs a summary of compaction statistics.
func (s *Stats) LogSummary(log interfaces.Logger) {
	log.Separator()
	log.Info("                    COMPACTION SUMMARY")
	log.Separator()
	log.Info("")

	log.Info("COMPACTION TIME BY COLUMN FAMILY:")
	for _, cfName := range cf.Names {
		elapsed := s.PerCFTime[cfName]
		log.Info("  CF %s: %s", cfName, helpers.FormatDuration(elapsed))
	}
	log.Info("")
	log.Info("Total Compaction Time: %s", helpers.FormatDuration(s.TotalTime))
	log.Info("")
}

// LogBeforeAfterComparison logs a before/after comparison.
func (s *Stats) LogBeforeAfterComparison(log interfaces.Logger) {
	log.Info("BEFORE/AFTER COMPARISON:")
	log.Info("")

	beforeMap := make(map[string]types.CFStats)
	afterMap := make(map[string]types.CFStats)
	for _, stat := range s.BeforeStats {
		beforeMap[stat.Name] = stat
	}
	for _, stat := range s.AfterStats {
		afterMap[stat.Name] = stat
	}

	log.Info("%-4s %12s %12s %12s %12s",
		"CF", "Files Before", "Files After", "Size Before", "Size After")
	log.Info("%-4s %12s %12s %12s %12s",
		"----", "------------", "-----------", "-----------", "----------")

	var totalFilesBefore, totalFilesAfter, totalSizeBefore, totalSizeAfter int64

	for _, cfName := range cf.Names {
		before := beforeMap[cfName]
		after := afterMap[cfName]

		log.Info("%-4s %12d %12d %12s %12s",
			cfName,
			before.TotalFiles,
			after.TotalFiles,
			helpers.FormatBytes(before.TotalSize),
			helpers.FormatBytes(after.TotalSize))

		totalFilesBefore += before.TotalFiles
		totalFilesAfter += after.TotalFiles
		totalSizeBefore += before.TotalSize
		totalSizeAfter += after.TotalSize
	}

	log.Info("%-4s %12s %12s %12s %12s",
		"----", "------------", "-----------", "-----------", "----------")
	log.Info("%-4s %12d %12d %12s %12s",
		"TOT",
		totalFilesBefore,
		totalFilesAfter,
		helpers.FormatBytes(totalSizeBefore),
		helpers.FormatBytes(totalSizeAfter))

	if totalFilesBefore > 0 {
		fileReduction := 100.0 * float64(totalFilesBefore-totalFilesAfter) / float64(totalFilesBefore)
		log.Info("")
		log.Info("File count reduction: %.1f%%", fileReduction)
	}

	if totalSizeBefore > 0 && totalSizeAfter != totalSizeBefore {
		sizeChange := 100.0 * float64(totalSizeAfter-totalSizeBefore) / float64(totalSizeBefore)
		if sizeChange > 0 {
			log.Info("Size increase: %.1f%% (expected due to metadata)", sizeChange)
		} else {
			log.Info("Size reduction: %.1f%%", -sizeChange)
		}
	}

	log.Info("")
}

// =============================================================================
// CountVerifier - Post-Compaction Count Verification
// =============================================================================

// CountVerifierConfig holds the configuration for count verification.
type CountVerifierConfig struct {
	// Store is the TxHash store to verify (required)
	Store interfaces.TxHashStore

	// Meta is the meta store with expected counts (required)
	Meta interfaces.MetaStore

	// Logger is the parent logger (required)
	Logger interfaces.Logger
}

// CountVerifier verifies that RocksDB entry counts match the expected counts
// from the meta store after compaction.
type CountVerifier struct {
	store interfaces.TxHashStore
	meta  interfaces.MetaStore
	log   interfaces.Logger
}

// NewCountVerifier creates a new CountVerifier.
func NewCountVerifier(cfg CountVerifierConfig) *CountVerifier {
	if cfg.Store == nil {
		panic("NewCountVerifier: Store is required")
	}
	if cfg.Meta == nil {
		panic("NewCountVerifier: Meta is required")
	}
	if cfg.Logger == nil {
		panic("NewCountVerifier: Logger is required")
	}

	return &CountVerifier{
		store: cfg.Store,
		meta:  cfg.Meta,
		log:   cfg.Logger.WithScope("COUNT-VERIFY"),
	}
}

// CountVerificationResult holds the result for one CF.
type CountVerificationResult struct {
	CFName        string
	ExpectedCount uint64
	ActualCount   uint64
	Match         bool
	Duration      time.Duration
}

// CountVerificationStats holds overall verification statistics.
type CountVerificationStats struct {
	StartTime  time.Time
	EndTime    time.Time
	TotalTime  time.Duration
	Results    []CountVerificationResult
	AllMatched bool
	Mismatches int
}

// Run executes the count verification.
func (v *CountVerifier) Run() (*CountVerificationStats, error) {
	stats := &CountVerificationStats{
		StartTime:  time.Now(),
		Results:    make([]CountVerificationResult, 16),
		AllMatched: true,
	}

	v.log.Separator()
	v.log.Info("                POST-COMPACTION COUNT VERIFICATION (PARALLEL)")
	v.log.Separator()
	v.log.Info("")
	v.log.Info("Verifying RocksDB entry counts match checkpointed counts...")
	v.log.Info("")

	expectedCounts, err := v.meta.GetCFCounts()
	if err != nil {
		return nil, fmt.Errorf("failed to get CF counts from meta store: %w", err)
	}

	var totalExpected uint64
	for _, count := range expectedCounts {
		totalExpected += count
	}
	v.log.Info("Expected total entries: %s", helpers.FormatNumber(int64(totalExpected)))
	v.log.Info("")
	v.log.Info("Starting parallel count verification of all 16 column families...")
	v.log.Info("")

	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error

	for i, cfName := range cf.Names {
		wg.Add(1)
		go func(idx int, name string) {
			defer wg.Done()

			cfStart := time.Now()
			expectedCount := expectedCounts[name]

			actualCount := uint64(0)
			iter := v.store.NewScanIteratorCF(name)
			for iter.SeekToFirst(); iter.Valid(); iter.Next() {
				actualCount++
			}
			iterErr := iter.Error()
			iter.Close()

			if iterErr != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("iterator error for CF %s: %w", name, iterErr)
				}
				errMu.Unlock()
				return
			}

			cfDuration := time.Since(cfStart)
			match := expectedCount == actualCount

			stats.Results[idx] = CountVerificationResult{
				CFName:        name,
				ExpectedCount: expectedCount,
				ActualCount:   actualCount,
				Match:         match,
				Duration:      cfDuration,
			}

			matchStr := "OK"
			if !match {
				matchStr = "MISMATCH"
			}

			v.log.Info("  CF [%s] verified: expected=%s, actual=%s, %s, %s",
				name,
				helpers.FormatNumber(int64(expectedCount)),
				helpers.FormatNumber(int64(actualCount)),
				matchStr,
				helpers.FormatDuration(cfDuration))
		}(i, cfName)
	}
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	var totalActual uint64
	for _, r := range stats.Results {
		totalActual += r.ActualCount
		if !r.Match {
			stats.AllMatched = false
			stats.Mismatches++
		}
	}

	stats.EndTime = time.Now()
	stats.TotalTime = time.Since(stats.StartTime)

	v.log.Info("")
	v.log.Info("%-4s %15s %15s %8s %12s", "CF", "Expected", "Actual", "Match", "Time")
	v.log.Info("%-4s %15s %15s %8s %12s", "----", "---------------", "---------------", "--------", "------------")

	for _, r := range stats.Results {
		matchStr := "OK"
		if !r.Match {
			matchStr = "MISMATCH"
		}
		v.log.Info("%-4s %15s %15s %8s %12s",
			r.CFName,
			helpers.FormatNumber(int64(r.ExpectedCount)),
			helpers.FormatNumber(int64(r.ActualCount)),
			matchStr,
			helpers.FormatDuration(r.Duration))
	}

	v.log.Info("%-4s %15s %15s %8s %12s", "----", "---------------", "---------------", "--------", "------------")

	totalMatch := totalExpected == totalActual
	totalMatchStr := "OK"
	if !totalMatch {
		totalMatchStr = "MISMATCH"
	}

	v.log.Info("%-4s %15s %15s %8s %12s",
		"TOT",
		helpers.FormatNumber(int64(totalExpected)),
		helpers.FormatNumber(int64(totalActual)),
		totalMatchStr,
		helpers.FormatDuration(stats.TotalTime))
	v.log.Info("")

	if stats.AllMatched {
		v.log.Info("Count verification PASSED: All %d CFs match expected counts (parallel, %s)", len(cf.Names), helpers.FormatDuration(stats.TotalTime))
	} else {
		v.log.Error("Count verification FAILED: %d CF(s) have mismatched counts", stats.Mismatches)
		v.log.Error("")
		v.log.Error("MISMATCH DETAILS:")
		for _, r := range stats.Results {
			if !r.Match {
				diff := int64(r.ActualCount) - int64(r.ExpectedCount)
				v.log.Error("  CF %s: expected %d, got %d (diff: %+d)",
					r.CFName, r.ExpectedCount, r.ActualCount, diff)
			}
		}
		v.log.Error("")
		v.log.Error("This may indicate:")
		v.log.Error("  1. Duplicate entries not properly deduplicated during compaction")
		v.log.Error("  2. Data corruption in RocksDB")
		v.log.Error("  3. A bug in the ingestion counting logic")
		v.log.Error("")
		v.log.Error("The RecSplit build phase will fail if counts don't match.")
	}
	v.log.Info("")

	v.log.Sync()

	return stats, nil
}

// =============================================================================
// Convenience Functions (Backward Compatibility)
// =============================================================================

// RunCompaction is a convenience function to run the compaction phase.
// Deprecated: Use compact.New(cfg).Run() instead.
func RunCompaction(s interfaces.TxHashStore, logger interfaces.Logger, mem interfaces.MemoryMonitor) (*Stats, error) {
	c := New(Config{
		Store:  s,
		Logger: logger,
		Memory: mem,
	})
	return c.Run()
}

// VerifyCountsAfterCompaction is a convenience function for count verification.
// Deprecated: Use compact.NewCountVerifier(cfg).Run() instead.
func VerifyCountsAfterCompaction(
	s interfaces.TxHashStore,
	meta interfaces.MetaStore,
	logger interfaces.Logger,
) (*CountVerificationStats, error) {
	v := NewCountVerifier(CountVerifierConfig{
		Store:  s,
		Meta:   meta,
		Logger: logger,
	})
	return v.Run()
}

// =============================================================================
// Compactable - Minimal Interface for External Use
// =============================================================================

// Compactable is a minimal interface for any store that supports compaction.
type Compactable interface {
	CompactCF(cfName string) time.Duration
}

// CompactAllCFs compacts all 16 column families in parallel.
// This is a lightweight helper for tools that don't need full statistics.
func CompactAllCFs(store Compactable, logger interfaces.Logger) time.Duration {
	log := logger.WithScope("COMPACT")
	log.Info("Compacting all 16 column families in parallel...")
	log.Info("")

	totalStart := time.Now()

	var wg sync.WaitGroup
	for _, cfName := range cf.Names {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			cfStart := time.Now()
			store.CompactCF(name)
			log.Info("  CF [%s] compacted in %s", name, helpers.FormatDuration(time.Since(cfStart)))
		}(cfName)
	}
	wg.Wait()

	totalTime := time.Since(totalStart)
	log.Info("")
	log.Info("All column families compacted in %s (parallel)", helpers.FormatDuration(totalTime))

	return totalTime
}

// EstimateCompactionTime provides a rough estimate of compaction time.
func EstimateCompactionTime(totalSizeBytes int64) time.Duration {
	sizeGB := float64(totalSizeBytes) / float64(types.GB)
	estimatedSeconds := sizeGB * 20
	return time.Duration(estimatedSeconds) * time.Second
}

// =============================================================================
// Type Aliases for Backward Compatibility
// =============================================================================

// CompactionStats is an alias for Stats (backward compatibility).
// Deprecated: Use Stats instead.
type CompactionStats = Stats
