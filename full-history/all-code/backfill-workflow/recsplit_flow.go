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
// RecSplit Flow — Parallelized 4-Phase Pipeline
// =============================================================================
//
// RecSplitFlow builds perfect hash function indexes from raw .bin files using
// a 4-phase parallel pipeline:
//
//   Phase 1: Count (100 goroutines) — count entries per CF across all .bin files
//   Phase 2: Add   (100 goroutines) — add keys to 16 RecSplit instances (mutex per CF)
//   Phase 3: Build (16 goroutines)  — build perfect hash indexes in parallel
//   Phase 4: Verify (100 goroutines, optional) — verify all lookups match
//
// This replaces the prior RecSplitBuilder which had two bottlenecks:
//   1. Sequential pre-scan (single goroutine reading all 1000 files)
//   2. 16x redundant I/O (each CF goroutine re-read ALL files)
//
// Crash recovery: all-or-nothing. On crash during any phase, the entire
// flow reruns from scratch (delete all partial .idx files, tmp/, and
// per-CF done flags). Per-CF done flags are re-set during the rebuild
// as bookkeeping records — they do not drive the recovery decision.

// RecSplit index building constants.
const (
	RecSplitBucketSize        = 2000
	RecSplitLeafSize          = 8
	RecSplitDataVersion       = 1
	RecSplitLessFalsePositives = true

	// recSplitWorkers is the number of parallel goroutines for count/add/verify.
	recSplitWorkers = 100
)

// RecSplitFlowConfig holds configuration for the 4-phase RecSplit pipeline.
type RecSplitFlowConfig struct {
	TxHashBase   string
	RangeID      uint32
	FirstChunkID uint32
	LastChunkID  uint32
	Meta         BackfillMetaStore
	Memory       memory.Monitor
	Logger       logging.Logger
	Progress     *RangeProgress
	Verify       bool // true = run Phase 4
}

// recSplitFlow runs the 4-phase RecSplit pipeline for a single range.
type recSplitFlow struct {
	cfg RecSplitFlowConfig
	log logging.Logger
}

// NewRecSplitFlow creates a new 4-phase RecSplit pipeline for the given range.
func NewRecSplitFlow(cfg RecSplitFlowConfig) *recSplitFlow {
	if cfg.Meta == nil {
		panic("RecSplitFlow: Meta required")
	}
	if cfg.Logger == nil {
		panic("RecSplitFlow: Logger required")
	}
	return &recSplitFlow{
		cfg: cfg,
		log: cfg.Logger.WithScope("RECSPLIT"),
	}
}

// Run executes all 4 phases of the RecSplit pipeline.
//
// On entry, cleans up any partial state from a prior crash (all-or-nothing).
// On success, sets range state to COMPLETE and deletes raw/.
func (f *recSplitFlow) Run(ctx context.Context) (*RecSplitFlowStats, error) {
	totalStart := time.Now()
	stats := &RecSplitFlowStats{VerifyEnabled: f.cfg.Verify}

	f.log.Separator()
	f.log.Info("                    RECSPLIT BUILDING PHASE")
	f.log.Separator()
	f.log.Info("")

	// All-or-nothing crash recovery: delete any partial indexes and tmp
	// from a prior crashed run.
	indexDir := RecSplitIndexDir(f.cfg.TxHashBase, f.cfg.RangeID)
	tmpDir := RecSplitTmpDir(f.cfg.TxHashBase, f.cfg.RangeID)
	os.RemoveAll(indexDir)
	os.RemoveAll(tmpDir)

	if err := fsutil.EnsureDir(indexDir); err != nil {
		return nil, fmt.Errorf("create index directory: %w", err)
	}

	totalChunks := f.cfg.LastChunkID - f.cfg.FirstChunkID + 1

	// ── Phase 1: Count ──────────────────────────────────────────────────
	if f.cfg.Progress != nil {
		f.cfg.Progress.SetRecSplitSubPhase(RecSplitSubPhaseCounting)
	}
	f.log.Info("Phase 1/4: Counting entries (%d workers, %d chunks)...", recSplitWorkers, totalChunks)

	countStart := time.Now()
	cfCounts, err := f.phaseCount()
	if err != nil {
		return nil, fmt.Errorf("phase count: %w", err)
	}
	stats.CountPhaseTime = time.Since(countStart)

	var totalKeys uint64
	for i := 0; i < cf.Count; i++ {
		stats.PerCFKeyCount[i] = cfCounts[i]
		totalKeys += cfCounts[i]
	}
	stats.TotalKeys = totalKeys

	f.log.Info("Count complete: %s entries across 16 CFs in %s",
		format.FormatNumber(int64(totalKeys)), format.FormatDuration(stats.CountPhaseTime))

	// Log per-CF counts compactly (4 per line)
	for row := 0; row < 4; row++ {
		base := row * 4
		f.log.Info("  CF %s: %s | CF %s: %s | CF %s: %s | CF %s: %s",
			cf.Names[base], format.FormatNumber(int64(cfCounts[base])),
			cf.Names[base+1], format.FormatNumber(int64(cfCounts[base+1])),
			cf.Names[base+2], format.FormatNumber(int64(cfCounts[base+2])),
			cf.Names[base+3], format.FormatNumber(int64(cfCounts[base+3])))
	}
	f.log.Info("")

	if f.cfg.Memory != nil {
		f.cfg.Memory.Check()
	}

	// ── Phase 2: Add ────────────────────────────────────────────────────
	if f.cfg.Progress != nil {
		f.cfg.Progress.SetRecSplitSubPhase(RecSplitSubPhaseAdding)
	}
	f.log.Info("Phase 2/4: Adding keys to 16 indexes (%d workers)...", recSplitWorkers)

	addStart := time.Now()
	rsInstances, err := f.phaseAdd(cfCounts)
	if err != nil {
		return nil, fmt.Errorf("phase add: %w", err)
	}
	stats.AddPhaseTime = time.Since(addStart)

	f.log.Info("Add complete: %s keys added in %s",
		format.FormatNumber(int64(totalKeys)), format.FormatDuration(stats.AddPhaseTime))
	f.log.Info("")

	if f.cfg.Memory != nil {
		f.cfg.Memory.Check()
	}

	// ── Phase 3: Build ──────────────────────────────────────────────────
	if f.cfg.Progress != nil {
		f.cfg.Progress.SetRecSplitSubPhase(RecSplitSubPhaseBuilding)
		f.cfg.Progress.recsplitCFsDone.Store(0)
	}
	f.log.Info("Phase 3/4: Building 16 indexes...")

	buildStart := time.Now()
	if err := f.phaseBuild(ctx, rsInstances, stats); err != nil {
		return nil, fmt.Errorf("phase build: %w", err)
	}
	stats.BuildPhaseTime = time.Since(buildStart)

	// Find slowest CF
	var slowestCF int
	var slowestTime time.Duration
	for i := 0; i < cf.Count; i++ {
		if stats.PerCFBuildTime[i] > slowestTime {
			slowestTime = stats.PerCFBuildTime[i]
			slowestCF = i
		}
	}

	f.log.Info("Build complete: 16 indexes in %s (slowest: CF %s)",
		format.FormatDuration(stats.BuildPhaseTime), cf.Names[slowestCF])
	f.log.Info("")

	if f.cfg.Memory != nil {
		f.cfg.Memory.Check()
	}

	// ── Phase 4: Verify (optional) ──────────────────────────────────────
	if f.cfg.Verify {
		if f.cfg.Progress != nil {
			f.cfg.Progress.SetRecSplitSubPhase(RecSplitSubPhaseVerifying)
		}
		f.log.Info("Phase 4/4: Verifying all keys (%d workers)...", recSplitWorkers)

		verifyStart := time.Now()
		if err := f.phaseVerify(); err != nil {
			return nil, fmt.Errorf("phase verify: %w", err)
		}
		stats.VerifyPhaseTime = time.Since(verifyStart)

		f.log.Info("Verify complete: %s lookups, 0 failures, %s",
			format.FormatNumber(int64(totalKeys)), format.FormatDuration(stats.VerifyPhaseTime))
		f.log.Info("")
	} else {
		f.log.Info("Phase 4/4: Verify SKIPPED (verify_recsplit = false)")
		f.log.Info("")
	}

	if f.cfg.Memory != nil {
		f.cfg.Memory.Check()
	}

	// ── Post: update state, cleanup ─────────────────────────────────────
	f.log.Info("All phases complete — setting index txhashindex flag")
	if err := f.cfg.Meta.SetIndexTxHashIndex(f.cfg.RangeID); err != nil {
		return nil, fmt.Errorf("set index txhashindex: %w", err)
	}

	// Delete raw/ to free disk space
	rawDir := RawTxHashDir(f.cfg.TxHashBase, f.cfg.RangeID)
	if fsutil.IsDir(rawDir) {
		dirSize := fsutil.GetDirSize(rawDir)
		if err := os.RemoveAll(rawDir); err != nil {
			f.log.Error("Failed to delete raw/ directory: %v", err)
		} else {
			f.log.Info("Deleted raw/ — freed %s", format.FormatBytes(dirSize))
		}
	}

	// Clean up tmp/
	os.RemoveAll(tmpDir)

	// Collect index sizes
	for i := 0; i < cf.Count; i++ {
		idxPath := RecSplitIndexPath(f.cfg.TxHashBase, f.cfg.RangeID, cf.Names[i])
		if info, err := os.Stat(idxPath); err == nil {
			stats.PerCFIndexSize[i] = info.Size()
			stats.TotalIndexSize += info.Size()
		}
	}

	stats.TotalTime = time.Since(totalStart)
	f.logSummary(stats)

	return stats, nil
}

// =============================================================================
// Phase 1: Count
// =============================================================================

// phaseCount launches recSplitWorkers goroutines to count entries per CF.
// Goroutine i reads chunks where (chunkID - firstChunkID) % workers == i.
func (f *recSplitFlow) phaseCount() ([cf.Count]uint64, error) {
	var globalCounts [cf.Count]uint64

	type localResult struct {
		counts [cf.Count]uint64
		err    error
	}
	results := make([]localResult, recSplitWorkers)

	var wg sync.WaitGroup
	for w := 0; w < recSplitWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			var local [cf.Count]uint64

			for chunkID := f.cfg.FirstChunkID + uint32(workerID); chunkID <= f.cfg.LastChunkID; chunkID += recSplitWorkers {
				path := RawTxHashPath(f.cfg.TxHashBase, f.cfg.RangeID, chunkID)
				reader, err := NewBinFileReader(path)
				if err != nil {
					results[workerID].err = fmt.Errorf("count: open chunk %d: %w", chunkID, err)
					return
				}

				for {
					entry, hasMore, err := reader.Next()
					if err != nil {
						reader.Close()
						results[workerID].err = fmt.Errorf("count: read chunk %d: %w", chunkID, err)
						return
					}
					if !hasMore {
						break
					}
					cfIdx := cf.Index(entry.TxHash[:])
					local[cfIdx]++
				}
				reader.Close()
			}

			results[workerID].counts = local
		}(w)
	}

	wg.Wait()

	// Check errors and merge counts.
	for w := 0; w < recSplitWorkers; w++ {
		if results[w].err != nil {
			return globalCounts, results[w].err
		}
		for i := 0; i < cf.Count; i++ {
			globalCounts[i] += results[w].counts[i]
		}
	}

	return globalCounts, nil
}

// =============================================================================
// Phase 2: Add
// =============================================================================

// phaseAdd creates 16 RecSplit instances and launches recSplitWorkers goroutines
// to re-read .bin files and add keys to the appropriate CF instance.
// Returns the 16 RecSplit instances (caller must close after build).
func (f *recSplitFlow) phaseAdd(cfCounts [cf.Count]uint64) ([cf.Count]*recsplit.RecSplit, error) {
	var rsInstances [cf.Count]*recsplit.RecSplit
	var cfMutexes [cf.Count]sync.Mutex

	erigonLogger := erigonlog.New()

	// Create RecSplit instances for CFs that have entries. CFs with 0 entries
	// are left as nil — no index is built for them.
	for i := 0; i < cf.Count; i++ {
		if cfCounts[i] == 0 {
			continue
		}

		cfName := cf.Names[i]
		idxPath := RecSplitIndexPath(f.cfg.TxHashBase, f.cfg.RangeID, cfName)
		tmpDir := RecSplitCFTmpDir(f.cfg.TxHashBase, f.cfg.RangeID, cfName)

		if err := fsutil.EnsureDir(tmpDir); err != nil {
			// Close already-created instances on error.
			for j := 0; j < i; j++ {
				if rsInstances[j] != nil {
					rsInstances[j].Close()
				}
			}
			return rsInstances, fmt.Errorf("create tmp dir for CF %s: %w", cfName, err)
		}

		rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:           int(cfCounts[i]),
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
			for j := 0; j < i; j++ {
				if rsInstances[j] != nil {
					rsInstances[j].Close()
				}
			}
			return rsInstances, fmt.Errorf("create RecSplit for CF %s: %w", cfName, err)
		}
		rsInstances[i] = rs
	}

	// Track per-worker added counts for verification.
	type workerResult struct {
		counts [cf.Count]uint64
		err    error
	}
	results := make([]workerResult, recSplitWorkers)

	var wg sync.WaitGroup
	for w := 0; w < recSplitWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			var local [cf.Count]uint64

			for chunkID := f.cfg.FirstChunkID + uint32(workerID); chunkID <= f.cfg.LastChunkID; chunkID += recSplitWorkers {
				path := RawTxHashPath(f.cfg.TxHashBase, f.cfg.RangeID, chunkID)
				reader, err := NewBinFileReader(path)
				if err != nil {
					results[workerID].err = fmt.Errorf("add: open chunk %d: %w", chunkID, err)
					return
				}

				for {
					entry, hasMore, err := reader.Next()
					if err != nil {
						reader.Close()
						results[workerID].err = fmt.Errorf("add: read chunk %d: %w", chunkID, err)
						return
					}
					if !hasMore {
						break
					}

					cfIdx := cf.Index(entry.TxHash[:])
					cfMutexes[cfIdx].Lock()
					err = rsInstances[cfIdx].AddKey(entry.TxHash[:], uint64(entry.LedgerSeq))
					cfMutexes[cfIdx].Unlock()
					if err != nil {
						reader.Close()
						results[workerID].err = fmt.Errorf("add: AddKey chunk %d: %w", chunkID, err)
						return
					}
					local[cfIdx]++
				}
				reader.Close()
			}

			results[workerID].counts = local
		}(w)
	}

	wg.Wait()

	// Check errors and verify per-CF counts match Phase 1.
	var addedCounts [cf.Count]uint64
	for w := 0; w < recSplitWorkers; w++ {
		if results[w].err != nil {
			// Close all instances on error.
			for i := 0; i < cf.Count; i++ {
				if rsInstances[i] != nil {
					rsInstances[i].Close()
				}
			}
			return rsInstances, results[w].err
		}
		for i := 0; i < cf.Count; i++ {
			addedCounts[i] += results[w].counts[i]
		}
	}

	for i := 0; i < cf.Count; i++ {
		if addedCounts[i] != cfCounts[i] {
			for j := 0; j < cf.Count; j++ {
				if rsInstances[j] != nil {
					rsInstances[j].Close()
				}
			}
			return rsInstances, fmt.Errorf("CF %s count mismatch: phase1=%d, added=%d",
				cf.Names[i], cfCounts[i], addedCounts[i])
		}
	}

	return rsInstances, nil
}

// =============================================================================
// Phase 3: Build
// =============================================================================

// phaseBuild launches 16 goroutines to build RecSplit indexes in parallel.
// Fsyncs each .idx file after build. Closes all RecSplit instances.
func (f *recSplitFlow) phaseBuild(ctx context.Context, rsInstances [cf.Count]*recsplit.RecSplit, stats *RecSplitFlowStats) error {
	var wg sync.WaitGroup
	cfErrors := make([]error, cf.Count)

	for i := 0; i < cf.Count; i++ {
		if rsInstances[i] == nil {
			// Empty CF — no index to build.
			if f.cfg.Progress != nil {
				f.cfg.Progress.RecordRecSplitCFDone()
			}
			continue
		}

		wg.Add(1)
		go func(cfIdx int) {
			defer wg.Done()
			defer rsInstances[cfIdx].Close()

			cfName := cf.Names[cfIdx]
			cfLog := f.log.WithScope(fmt.Sprintf("CF:%s", cfName))

			buildStart := time.Now()
			if err := rsInstances[cfIdx].Build(ctx); err != nil {
				if err == recsplit.ErrCollision {
					cfErrors[cfIdx] = fmt.Errorf("CF %s: hash collision (extremely rare)", cfName)
				} else {
					cfErrors[cfIdx] = fmt.Errorf("CF %s: build: %w", cfName, err)
				}
				return
			}

			// Fsync the index file.
			idxPath := RecSplitIndexPath(f.cfg.TxHashBase, f.cfg.RangeID, cfName)
			if err := fsyncFile(idxPath); err != nil {
				cfErrors[cfIdx] = fmt.Errorf("CF %s: fsync: %w", cfName, err)
				return
			}

			buildTime := time.Since(buildStart)
			stats.PerCFBuildTime[cfIdx] = buildTime

			// Read index size for logging.
			var sizeStr string
			if info, err := os.Stat(idxPath); err == nil {
				sizeStr = format.FormatBytes(info.Size())
			}

			cfLog.Info("Build complete: %s keys, %s, %s",
				format.FormatNumber(int64(stats.PerCFKeyCount[cfIdx])),
				sizeStr,
				format.FormatDuration(buildTime))

			if f.cfg.Progress != nil {
				f.cfg.Progress.RecordRecSplitCFDone()
			}
		}(i)
	}

	wg.Wait()

	for _, err := range cfErrors {
		if err != nil {
			return err
		}
	}

	return nil
}

// =============================================================================
// Phase 4: Verify
// =============================================================================

// phaseVerify opens all 16 built indexes and launches recSplitWorkers goroutines
// to re-read .bin files and verify every key's lookup returns the correct value.
func (f *recSplitFlow) phaseVerify() error {
	// Open indexes for CFs that have entries.
	var indexes [cf.Count]*recsplit.Index
	var readers [cf.Count]*recsplit.IndexReader

	for i := 0; i < cf.Count; i++ {
		idxPath := RecSplitIndexPath(f.cfg.TxHashBase, f.cfg.RangeID, cf.Names[i])
		if !fsutil.FileExists(idxPath) {
			// Empty CF — no index file.
			continue
		}
		idx, err := recsplit.OpenIndex(idxPath)
		if err != nil {
			// Close already-opened indexes.
			for j := 0; j < i; j++ {
				if indexes[j] != nil {
					indexes[j].Close()
				}
			}
			return fmt.Errorf("open index CF %s: %w", cf.Names[i], err)
		}
		indexes[i] = idx
		readers[i] = recsplit.NewIndexReader(idx)
	}

	defer func() {
		for i := 0; i < cf.Count; i++ {
			if indexes[i] != nil {
				indexes[i].Close()
			}
		}
	}()

	// Launch workers.
	type workerResult struct {
		failures int
		firstErr string
	}
	results := make([]workerResult, recSplitWorkers)

	var wg sync.WaitGroup
	for w := 0; w < recSplitWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for chunkID := f.cfg.FirstChunkID + uint32(workerID); chunkID <= f.cfg.LastChunkID; chunkID += recSplitWorkers {
				path := RawTxHashPath(f.cfg.TxHashBase, f.cfg.RangeID, chunkID)
				reader, err := NewBinFileReader(path)
				if err != nil {
					results[workerID].failures++
					results[workerID].firstErr = fmt.Sprintf("verify: open chunk %d: %v", chunkID, err)
					return
				}

				for {
					entry, hasMore, err := reader.Next()
					if err != nil {
						reader.Close()
						results[workerID].failures++
						results[workerID].firstErr = fmt.Sprintf("verify: read chunk %d: %v", chunkID, err)
						return
					}
					if !hasMore {
						break
					}

					cfIdx := cf.Index(entry.TxHash[:])
					if readers[cfIdx] == nil {
						results[workerID].failures++
						if results[workerID].firstErr == "" {
							results[workerID].firstErr = fmt.Sprintf("CF %s: no index (expected 0 entries but found key, chunk %d)",
								cf.Names[cfIdx], chunkID)
						}
						continue
					}
					offset, found := readers[cfIdx].Lookup(entry.TxHash[:])
					if !found {
						results[workerID].failures++
						if results[workerID].firstErr == "" {
							results[workerID].firstErr = fmt.Sprintf("CF %s: key not found in index (chunk %d)",
								cf.Names[cfIdx], chunkID)
						}
						continue
					}
					if uint32(offset) != entry.LedgerSeq {
						results[workerID].failures++
						if results[workerID].firstErr == "" {
							results[workerID].firstErr = fmt.Sprintf("CF %s: value mismatch: got %d, want %d (chunk %d)",
								cf.Names[cfIdx], uint32(offset), entry.LedgerSeq, chunkID)
						}
						continue
					}
				}
				reader.Close()
			}
		}(w)
	}

	wg.Wait()

	// Collect errors.
	var totalFailures int
	var firstErr string
	for w := 0; w < recSplitWorkers; w++ {
		totalFailures += results[w].failures
		if firstErr == "" && results[w].firstErr != "" {
			firstErr = results[w].firstErr
		}
	}

	if totalFailures > 0 {
		return fmt.Errorf("verify: %d failures, first: %s", totalFailures, firstErr)
	}

	return nil
}

// =============================================================================
// Helpers
// =============================================================================

// fsyncFile opens a file, fsyncs it, and closes it.
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

// logSummary logs the RecSplit flow summary.
func (f *recSplitFlow) logSummary(stats *RecSplitFlowStats) {
	f.log.Separator()
	f.log.Info("                    RECSPLIT BUILD SUMMARY")
	f.log.Separator()
	f.log.Info("  Total keys indexed:   %s", format.FormatNumber(int64(stats.TotalKeys)))
	f.log.Info("  Phase timings:")
	f.log.Info("    Count:    %-10s (%d workers)", format.FormatDuration(stats.CountPhaseTime), recSplitWorkers)
	f.log.Info("    Add:      %-10s (%d workers, 16 indexes)", format.FormatDuration(stats.AddPhaseTime), recSplitWorkers)

	// Find slowest CF for build summary.
	var slowestCF int
	var slowestTime time.Duration
	for i := 0; i < cf.Count; i++ {
		if stats.PerCFBuildTime[i] > slowestTime {
			slowestTime = stats.PerCFBuildTime[i]
			slowestCF = i
		}
	}
	f.log.Info("    Build:    %-10s (16 parallel, slowest: CF %s)", format.FormatDuration(stats.BuildPhaseTime), cf.Names[slowestCF])

	if stats.VerifyEnabled {
		f.log.Info("    Verify:   %-10s (%d workers)", format.FormatDuration(stats.VerifyPhaseTime), recSplitWorkers)
	} else {
		f.log.Info("    Verify:   skipped")
	}

	f.log.Info("  Per-CF breakdown:")
	f.log.Info("    %-4s %-15s %-12s %-12s", "CF", "Keys", "Index Size", "Build Time")
	for i := 0; i < cf.Count; i++ {
		f.log.Info("    %-4s %-15s %-12s %-12s",
			cf.Names[i],
			format.FormatNumber(int64(stats.PerCFKeyCount[i])),
			format.FormatBytes(stats.PerCFIndexSize[i]),
			format.FormatDuration(stats.PerCFBuildTime[i]))
	}
	f.log.Info("  Total index size:     %s", format.FormatBytes(stats.TotalIndexSize))
	f.log.Info("  Total duration:       %s", format.FormatDuration(stats.TotalTime))
	f.log.Separator()
}
