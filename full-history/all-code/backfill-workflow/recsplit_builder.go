package backfill

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	erigonlog "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/cf"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/format"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/fsutil"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/memory"
)

// =============================================================================
// RecSplit Builder
// =============================================================================
//
// RecSplitBuilder creates perfect hash function indexes from raw .bin files.
// It is the second phase of the backfill pipeline, running AFTER all 1000 chunks
// in a range have been ingested.
//
// Architecture:
//   - Reads all 1000 .bin files using RangeBinScanner (from bin_reader.go)
//   - Spawns 16 parallel goroutines, one per CF (column family / hash nibble)
//   - Each goroutine filters entries by txhash[0]>>4 == cfIndex
//   - Uses erigon's RecSplit library to build a perfect hash function index
//   - Produces 16 files: cf-0.idx through cf-f.idx
//
// Crash recovery (scenarios B3/B4 from doc 07):
//   - Each CF has an independent done flag in the meta store
//   - If a CF's done flag is set, its .idx is complete → skip on restart
//   - If a CF's done flag is absent, its partial .idx is deleted and rebuilt
//   - After all 16 CFs are done, range state → COMPLETE and raw/ is deleted
//
// The done flag for each CF is set AFTER the .idx file is fsynced. This ensures
// that a flag being present implies the index file is durable on disk.

// RecSplit index building constants. These match the values used in the
// the RecSplit builder, ensuring index format compatibility.
const (
	// RecSplitBucketSize is the bucket size for RecSplit construction.
	// Larger values reduce index size but increase build time.
	RecSplitBucketSize = 2000

	// RecSplitLeafSize is the leaf size for RecSplit construction.
	RecSplitLeafSize = 8

	// RecSplitDataVersion is the data format version stored in the index header.
	// Must match across all tools that read/write these indexes.
	RecSplitDataVersion = 1

	// RecSplitLessFalsePositives enables the less-false-positives mode in erigon's
	// RecSplit. This adds a small overhead but reduces lookup false positive rate.
	RecSplitLessFalsePositives = true
)

// RecSplitBuilderConfig holds the configuration for the RecSplit builder.
type RecSplitBuilderConfig struct {
	// TxHashBase is the base directory for txhash files.
	// Contains raw/ (input .bin files) and index/ (output .idx files).
	TxHashBase string

	// RangeID is the range being processed.
	RangeID uint32

	// FirstChunkID is the first chunk in the range (inclusive).
	FirstChunkID uint32

	// LastChunkID is the last chunk in the range (inclusive).
	LastChunkID uint32

	// Meta is the meta store for checking/setting CF done flags.
	Meta BackfillMetaStore

	// Memory is the memory monitor (checked after each CF build).
	Memory memory.Monitor

	// Logger is the scoped logger.
	Logger logging.Logger

	// Progress is the per-range progress tracker (optional).
	Progress *RangeProgress
}

// recSplitBuilder builds RecSplit indexes for a single range.
type recSplitBuilder struct {
	cfg RecSplitBuilderConfig
	log logging.Logger
}

// NewRecSplitBuilder creates a RecSplit builder for the given range.
// Panics if required dependencies (Meta, Logger) are nil.
func NewRecSplitBuilder(cfg RecSplitBuilderConfig) *recSplitBuilder {
	if cfg.Meta == nil {
		panic("RecSplitBuilder: Meta required")
	}
	if cfg.Logger == nil {
		panic("RecSplitBuilder: Logger required")
	}
	return &recSplitBuilder{
		cfg: cfg,
		log: cfg.Logger.WithScope("RECSPLIT"),
	}
}

// Run executes the RecSplit building phase for all 16 CFs.
//
// For each CF:
//  1. Check done flag → skip if already built (crash recovery)
//  2. Delete partial .idx if it exists (may be corrupt from crash)
//  3. Pre-scan all .bin files to count entries for this CF
//  4. Build RecSplit index: read .bin files → AddKey → Build
//  5. Fsync the .idx file
//  6. Set done flag in meta store
//
// All 16 CFs run in parallel. After all complete:
//   - Update range state to COMPLETE
//   - Delete raw/ directory to free disk space
//
// Returns aggregate stats and any error from the first failing CF.
func (b *recSplitBuilder) Run(ctx context.Context) (*RecSplitBuildStats, error) {
	startTime := time.Now()

	b.log.Separator()
	b.log.Info("                    RECSPLIT BUILDING PHASE")
	b.log.Separator()
	b.log.Info("")

	// Ensure index directory exists
	indexDir := RecSplitIndexDir(b.cfg.TxHashBase, b.cfg.RangeID)
	if err := fsutil.EnsureDir(indexDir); err != nil {
		return nil, fmt.Errorf("create index directory: %w", err)
	}

	// Pre-scan CF counts from all .bin files.
	// This is O(total_entries) sequential I/O — fast, no decompression needed.
	b.log.Info("Pre-scanning .bin files for CF entry counts...")
	scanStart := time.Now()
	cfCounts, err := PreScanCFCounts(b.cfg.TxHashBase, b.cfg.RangeID, b.cfg.FirstChunkID, b.cfg.LastChunkID)
	if err != nil {
		return nil, fmt.Errorf("pre-scan CF counts: %w", err)
	}
	scanDuration := time.Since(scanStart)

	var totalKeys uint64
	for i := 0; i < cf.Count; i++ {
		totalKeys += cfCounts[i]
	}
	b.log.Info("Pre-scan complete in %s: %s total entries across 16 CFs",
		format.FormatDuration(scanDuration), format.FormatNumber(int64(totalKeys)))
	b.log.Info("")

	// Log per-CF counts
	for i := 0; i < cf.Count; i++ {
		b.log.Info("  CF [%s]: %s entries", cf.Names[i], format.FormatNumber(int64(cfCounts[i])))
	}
	b.log.Info("")

	// Build all 16 CFs in parallel.
	// Each goroutine independently checks its done flag, builds if needed,
	// and sets the done flag after fsync. Errors are collected per-CF.
	stats := &RecSplitBuildStats{
		RangeID: b.cfg.RangeID,
	}

	var wg sync.WaitGroup
	cfErrors := make([]error, cf.Count)

	for i := 0; i < cf.Count; i++ {
		wg.Add(1)
		go func(cfIndex int) {
			defer wg.Done()

			cfStats, err := b.buildCF(ctx, cfIndex, cfCounts[cfIndex])
			if err != nil {
				cfErrors[cfIndex] = fmt.Errorf("CF %s: %w", cf.Names[cfIndex], err)
				return
			}

			stats.CFStats[cfIndex] = *cfStats
		}(i)
	}

	wg.Wait()

	// Check for errors — return the first one found
	for _, err := range cfErrors {
		if err != nil {
			return nil, err
		}
	}

	// Aggregate stats
	for i := 0; i < cf.Count; i++ {
		s := &stats.CFStats[i]
		stats.TotalKeys += s.KeyCount
		stats.TotalIndexSize += s.IndexSize
		if s.Skipped {
			stats.CFsSkipped++
		}
	}
	stats.TotalTime = time.Since(startTime)

	// All 16 CFs are done — update range state to COMPLETE.
	b.log.Info("")
	b.log.Info("All 16 CFs complete — updating range state to COMPLETE")
	if err := b.cfg.Meta.SetRangeState(b.cfg.RangeID, RangeStateComplete); err != nil {
		return nil, fmt.Errorf("set range state complete: %w", err)
	}

	// Delete raw/ directory to free disk space.
	// The .bin files are no longer needed after all indexes are built.
	rawDir := RawTxHashDir(b.cfg.TxHashBase, b.cfg.RangeID)
	if fsutil.IsDir(rawDir) {
		b.log.Info("Deleting raw/ directory to free disk space...")
		dirSize := fsutil.GetDirSize(rawDir)
		if err := os.RemoveAll(rawDir); err != nil {
			b.log.Error("Failed to delete raw/ directory: %v", err)
		} else {
			b.log.Info("Deleted raw/ — freed %s", format.FormatBytes(dirSize))
		}
	}

	// Clean up tmp/ directory left over from per-CF RecSplit builds.
	os.RemoveAll(RecSplitTmpDir(b.cfg.TxHashBase, b.cfg.RangeID))

	// Log summary
	b.logSummary(stats)

	return stats, nil
}

// buildCF builds the RecSplit index for a single CF.
//
// Crash recovery logic:
//   - If the CF's done flag is already set, skip entirely (the .idx is durable).
//   - If the done flag is absent but a partial .idx exists, delete it first.
//   - Build the index from scratch by reading all .bin files with CF filter.
//   - After Build() + fsync, set the done flag.
func (b *recSplitBuilder) buildCF(ctx context.Context, cfIndex int, keyCount uint64) (*RecSplitCFStats, error) {
	cfName := cf.Names[cfIndex]
	cfLog := b.log.WithScope(fmt.Sprintf("CF:%s", cfName))

	stats := &RecSplitCFStats{
		CFIndex: cfIndex,
		CFName:  cfName,
	}

	// Scenario B3: Check if this CF is already done (crash recovery).
	// A done flag means the .idx file was fsynced and is durable on disk.
	done, err := b.cfg.Meta.IsRecSplitCFDone(b.cfg.RangeID, cfIndex)
	if err != nil {
		return nil, fmt.Errorf("check done flag: %w", err)
	}
	if done {
		cfLog.Info("Already complete — skipping")
		stats.Skipped = true
		if b.cfg.Progress != nil {
			b.cfg.Progress.RecordRecSplitCFDone()
		}
		// Read existing index size for stats
		idxPath := RecSplitIndexPath(b.cfg.TxHashBase, b.cfg.RangeID, cfName)
		if info, err := os.Stat(idxPath); err == nil {
			stats.IndexSize = info.Size()
		}
		return stats, nil
	}

	// Delete any partial .idx file from a previous crash.
	// A partial file without a done flag may be corrupt/incomplete.
	idxPath := RecSplitIndexPath(b.cfg.TxHashBase, b.cfg.RangeID, cfName)
	if fsutil.FileExists(idxPath) {
		cfLog.Info("Deleting partial index (no done flag)")
		os.Remove(idxPath)
	}

	// Handle empty CF — no keys to index
	if keyCount == 0 {
		cfLog.Info("No entries — skipping build")
		stats.KeyCount = 0
		// Set done flag even for empty CFs so we don't re-scan on restart
		if err := b.cfg.Meta.SetRecSplitCFDone(b.cfg.RangeID, cfIndex); err != nil {
			return nil, fmt.Errorf("set done flag for empty CF: %w", err)
		}
		if b.cfg.Progress != nil {
			b.cfg.Progress.RecordRecSplitCFDone()
		}
		return stats, nil
	}

	cfLog.Info("Building index: %s keys", format.FormatNumber(int64(keyCount)))
	buildStart := time.Now()

	// Create temp directory for this CF's RecSplit build
	tmpDir := RecSplitCFTmpDir(b.cfg.TxHashBase, b.cfg.RangeID, cfName)
	if err := fsutil.EnsureDir(tmpDir); err != nil {
		return nil, fmt.Errorf("create tmp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create the RecSplit builder via erigon library.
	// RecSplitArgs configures the perfect hash function construction:
	//   - KeyCount: pre-scanned count for this CF (must be exact)
	//   - BucketSize/LeafSize: trade space vs build time
	//   - LessFalsePositives: reduces lookup FP rate at small overhead
	//   - IndexFile: final output path for the .idx file
	//   - TmpDir: scratch space for intermediate data during build
	erigonLogger := erigonlog.New()

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:           int(keyCount),
		Enums:              false,
		LessFalsePositives: RecSplitLessFalsePositives,
		BucketSize:         RecSplitBucketSize,
		LeafSize:           RecSplitLeafSize,
		TmpDir:             tmpDir,
		IndexFile:          idxPath,
		BaseDataID:         0,
		Version:            RecSplitDataVersion,
	}, erigonLogger)
	if err != nil {
		return nil, fmt.Errorf("create RecSplit: %w", err)
	}
	defer rs.Close()

	// Read all .bin files filtered by this CF and add keys to the RecSplit builder.
	// Each .bin file contains 36-byte entries [txhash:32][ledgerSeq:4 BE].
	// The RangeBinScanner filters by txhash[0]>>4 == cfIndex during iteration.
	scanner := NewRangeBinScanner(RangeBinScannerConfig{
		TxHashBase:   b.cfg.TxHashBase,
		RangeID:      b.cfg.RangeID,
		FirstChunkID: b.cfg.FirstChunkID,
		LastChunkID:  b.cfg.LastChunkID,
		CFFilter:     cfIndex,
	})
	defer scanner.Close()

	keysAdded := uint64(0)
	for {
		entry, hasMore, err := scanner.Next()
		if err != nil {
			return nil, fmt.Errorf("scan .bin files: %w", err)
		}
		if !hasMore {
			break
		}

		// AddKey takes the txhash as key and ledger sequence as the associated value.
		// The ledger sequence is stored in the index so lookups can return it directly.
		if err := rs.AddKey(entry.TxHash[:], uint64(entry.LedgerSeq)); err != nil {
			return nil, fmt.Errorf("add key %d: %w", keysAdded, err)
		}
		keysAdded++
	}

	// Verify key count matches the pre-scan. A mismatch indicates data corruption
	// or a bug in CF filtering/counting.
	if keysAdded != keyCount {
		return nil, fmt.Errorf("key count mismatch: pre-scan=%d, added=%d", keyCount, keysAdded)
	}

	cfLog.Info("Added %s keys — building index...", format.FormatNumber(int64(keysAdded)))

	// Build the RecSplit index. This is the compute-intensive step that
	// constructs the perfect hash function from all added keys.
	if err := rs.Build(ctx); err != nil {
		if err == recsplit.ErrCollision {
			return nil, fmt.Errorf("hash collision detected (extremely rare — try rebuilding)")
		}
		return nil, fmt.Errorf("build index: %w", err)
	}

	// Fsync the .idx file to ensure it's durable before setting the done flag.
	// This is critical for crash safety — the done flag must only be set after
	// the index file is confirmed on durable storage.
	if err := fsyncFile(idxPath); err != nil {
		return nil, fmt.Errorf("fsync index: %w", err)
	}

	// Set the done flag AFTER fsync. If we crash between fsync and this point,
	// the done flag won't be set and the index will be rebuilt on restart (safe).
	if err := b.cfg.Meta.SetRecSplitCFDone(b.cfg.RangeID, cfIndex); err != nil {
		return nil, fmt.Errorf("set done flag: %w", err)
	}
	if b.cfg.Progress != nil {
		b.cfg.Progress.RecordRecSplitCFDone()
	}

	buildTime := time.Since(buildStart)

	// Read index file size for stats
	if info, err := os.Stat(idxPath); err == nil {
		stats.IndexSize = info.Size()
	}

	stats.KeyCount = keysAdded
	stats.BuildTime = buildTime

	cfLog.Info("Complete: %s keys, %s index, %s",
		format.FormatNumber(int64(keysAdded)),
		format.FormatBytes(stats.IndexSize),
		format.FormatDuration(buildTime))

	// Check memory after each CF build
	if b.cfg.Memory != nil {
		b.cfg.Memory.Check()
	}

	return stats, nil
}

// fsyncFile opens a file, fsyncs it, and closes it.
// Used to ensure a file is durable on disk before setting done flags.
func fsyncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open for fsync: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync: %w", err)
	}
	return f.Close()
}

// logSummary logs the RecSplit build summary in the format specified by the
// metrics-and-logging skill.
func (b *recSplitBuilder) logSummary(stats *RecSplitBuildStats) {
	b.log.Separator()
	b.log.Info("                    RECSPLIT BUILD SUMMARY")
	b.log.Separator()
	b.log.Info("")
	b.log.Info("  Total txhashes indexed:  %s", format.FormatNumber(int64(stats.TotalKeys)))
	b.log.Info("  Per-CF breakdown:")
	b.log.Info("    %-4s %-15s %-12s %-12s", "CF", "Keys", "Index Size", "Build Time")

	for i := 0; i < cf.Count; i++ {
		s := &stats.CFStats[i]
		status := ""
		if s.Skipped {
			status = " (skipped)"
		}
		b.log.Info("    %-4s %-15s %-12s %-12s%s",
			s.CFName,
			format.FormatNumber(int64(s.KeyCount)),
			format.FormatBytes(s.IndexSize),
			format.FormatDuration(s.BuildTime),
			status)
	}

	b.log.Info("  CFs skipped (resumed):   %d", stats.CFsSkipped)
	b.log.Info("  Total index size:        %s", format.FormatBytes(stats.TotalIndexSize))
	b.log.Info("  Duration:                %s", format.FormatDuration(stats.TotalTime))
	b.log.Separator()
}
