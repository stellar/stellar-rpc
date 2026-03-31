package backfill

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/stellar/stellar-rpc/full-history/pkg/format"
	"github.com/stellar/stellar-rpc/full-history/pkg/fsutil"
	"github.com/stellar/stellar-rpc/full-history/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/pkg/memory"
	"github.com/tamirms/streamhash"
)

// =============================================================================
// StreamHash Flow — Alternative TxHash Index Builder
// =============================================================================
//
// StreamHashFlow builds a single txhash.idx file using the StreamHash MPHF
// library as an alternative to RecSplit's 16 per-CF .idx files.
//
// Architecture differences from RecSplit:
//
//	RecSplit:    16 CF files  (cf-0.idx .. cf-f.idx), sharded by txhash[0]>>4
//	StreamHash: 1 file        (txhash.idx), all keys in one MPHF
//
// StreamHash uses:
//   - PreHash(txhash[:]) to redistribute SHA-256 hashes into uniform 128-bit keys
//   - WithPayload(4) to store ledgerSeq as a 4-byte payload per key
//   - WithFingerprint(2) for non-member rejection (1/65536 false positive rate)
//   - WithWorkers(8) for parallel block solving during build
//   - WithUnsortedInput() since .bin files are not sorted by pre-hash order
//
// Phases:
//
//	Phase 1: COUNT  — scan all .bin files, count total entries
//	Phase 2: BUILD  — create streamhash index from all .bin entries
//	Phase 3: VERIFY — (optional) re-read all .bin files, verify all lookups
//
// Crash recovery: all-or-nothing, same as RecSplit. On crash during any phase,
// the entire flow reruns from scratch (stale txhash.idx is deleted at startup).

// streamHashWorkers is the number of workers for the StreamHash builder.
// 8 is optimal: build throughput scales ~4x from 1→4 workers, diminishing after.
const streamHashWorkers = 8

// streamHashVerifyWorkers is the number of goroutines for the verify phase.
const streamHashVerifyWorkers = 100

// StreamHashFlowConfig holds configuration for the StreamHash pipeline.
type StreamHashFlowConfig struct {
	TxHashRawPath string
	TxHashIdxPath string
	IndexID       uint32
	FirstChunkID  uint32
	LastChunkID   uint32
	Meta          BackfillMetaStore
	Memory        memory.Monitor
	Logger        logging.Logger
	Progress      *IndexProgress
	Verify        bool // true = run Phase 3 (verification)
}

// StreamHashFlowStats holds aggregate stats for the StreamHash pipeline.
type StreamHashFlowStats struct {
	CountPhaseTime  time.Duration
	BuildPhaseTime  time.Duration
	VerifyPhaseTime time.Duration
	TotalKeys       uint64
	IndexSize       int64
	VerifyEnabled   bool
	TotalTime       time.Duration
}

// streamHashFlow runs the 3-phase StreamHash pipeline for a single index.
type streamHashFlow struct {
	cfg StreamHashFlowConfig
	log logging.Logger
}

// NewStreamHashFlow creates a new StreamHash pipeline for the given range.
func NewStreamHashFlow(cfg StreamHashFlowConfig) *streamHashFlow {
	if cfg.Meta == nil {
		panic("StreamHashFlow: Meta required")
	}
	if cfg.Logger == nil {
		panic("StreamHashFlow: Logger required")
	}
	return &streamHashFlow{
		cfg: cfg,
		log: cfg.Logger.WithScope("STREAMHASH"),
	}
}

// Run executes all phases of the StreamHash pipeline.
//
// On entry, cleans up any partial state from a prior crash (all-or-nothing).
// On success, sets index txhash flag and deletes raw/ directory.
func (f *streamHashFlow) Run(ctx context.Context) (*StreamHashFlowStats, error) {
	totalStart := time.Now()
	stats := &StreamHashFlowStats{VerifyEnabled: f.cfg.Verify}

	f.log.Separator()
	f.log.Info("                    STREAMHASH BUILDING PHASE")
	f.log.Separator()
	f.log.Info("")

	// All-or-nothing crash recovery: delete any prior index and tmp files.
	indexDir := RecSplitIndexDir(f.cfg.TxHashIdxPath, f.cfg.IndexID)
	tmpDir := RecSplitTmpDir(f.cfg.TxHashIdxPath, f.cfg.IndexID)
	os.RemoveAll(indexDir)
	os.RemoveAll(tmpDir)

	if err := fsutil.EnsureDir(indexDir); err != nil {
		return nil, fmt.Errorf("create index directory: %w", err)
	}
	if err := fsutil.EnsureDir(tmpDir); err != nil {
		return nil, fmt.Errorf("create tmp directory: %w", err)
	}

	totalChunks := f.cfg.LastChunkID - f.cfg.FirstChunkID + 1

	// ── Phase 1: Count ──────────────────────────────────────────────────
	f.log.Info("Phase 1/3: Counting entries (%d chunks)...", totalChunks)

	countStart := time.Now()
	totalKeys, err := f.phaseCount()
	if err != nil {
		return nil, fmt.Errorf("phase count: %w", err)
	}
	stats.CountPhaseTime = time.Since(countStart)
	stats.TotalKeys = totalKeys

	f.log.Info("Count complete: %s entries in %s",
		format.FormatNumber(int64(totalKeys)), format.FormatDuration(stats.CountPhaseTime))
	f.log.Info("")

	if f.cfg.Memory != nil {
		f.cfg.Memory.Check()
	}

	// ── Phase 2: Build ──────────────────────────────────────────────────
	f.log.Info("Phase 2/3: Building StreamHash index (%d workers, unsorted input)...",
		streamHashWorkers)

	outputPath := StreamHashIndexPath(f.cfg.TxHashIdxPath, f.cfg.IndexID)

	buildStart := time.Now()
	if err := f.phaseBuild(ctx, outputPath, totalKeys); err != nil {
		return nil, fmt.Errorf("phase build: %w", err)
	}
	stats.BuildPhaseTime = time.Since(buildStart)

	// Record index size.
	if info, err := os.Stat(outputPath); err == nil {
		stats.IndexSize = info.Size()
	}

	f.log.Info("Build complete: %s entries, index %s, %s",
		format.FormatNumber(int64(totalKeys)),
		format.FormatBytes(stats.IndexSize),
		format.FormatDuration(stats.BuildPhaseTime))
	f.log.Info("")

	if f.cfg.Memory != nil {
		f.cfg.Memory.Check()
	}

	// ── Phase 3: Verify (optional) ──────────────────────────────────────
	if f.cfg.Verify {
		f.log.Info("Phase 3/3: Verifying all keys (%d workers)...", streamHashVerifyWorkers)

		verifyStart := time.Now()
		if err := f.phaseVerify(outputPath); err != nil {
			return nil, fmt.Errorf("phase verify: %w", err)
		}
		stats.VerifyPhaseTime = time.Since(verifyStart)

		f.log.Info("Verify complete: %s lookups, 0 failures, %s",
			format.FormatNumber(int64(totalKeys)), format.FormatDuration(stats.VerifyPhaseTime))
		f.log.Info("")
	} else {
		f.log.Info("Phase 3/3: Verify SKIPPED (verify_recsplit = false)")
		f.log.Info("")
	}

	if f.cfg.Memory != nil {
		f.cfg.Memory.Check()
	}

	// ── Post: update state, cleanup ─────────────────────────────────────
	f.log.Info("All phases complete — setting index txhash flag")
	if err := f.cfg.Meta.SetIndexTxHash(f.cfg.IndexID); err != nil {
		return nil, fmt.Errorf("set index txhash: %w", err)
	}

	// Delete raw txhash flat files and their meta keys.
	rawDir := f.cfg.TxHashRawPath
	if fsutil.IsDir(rawDir) {
		dirSize := fsutil.GetDirSize(rawDir)
		if err := os.RemoveAll(rawDir); err != nil {
			f.log.Error("Failed to delete raw/ directory: %v", err)
		} else {
			f.log.Info("Deleted raw/ — freed %s", format.FormatBytes(dirSize))
		}
	}

	// Delete chunk:C:txhash meta keys for all chunks in this range.
	for chunkID := f.cfg.FirstChunkID; chunkID <= f.cfg.LastChunkID; chunkID++ {
		if err := f.cfg.Meta.DeleteChunkTxHashKey(chunkID); err != nil {
			f.log.Error("Failed to delete txhash meta key for chunk %d: %v", chunkID, err)
		}
	}

	// Clean up tmp/.
	os.RemoveAll(tmpDir)

	stats.TotalTime = time.Since(totalStart)
	f.logSummary(stats)

	return stats, nil
}

// =============================================================================
// Phase 1: Count
// =============================================================================

// phaseCount scans all .bin files and counts total entries using parallel workers.
// Each .bin file contains 36-byte entries (32-byte txhash + 4-byte ledgerSeq).
func (f *streamHashFlow) phaseCount() (uint64, error) {
	type workerResult struct {
		count uint64
		err   error
	}

	workers := streamHashVerifyWorkers
	results := make([]workerResult, workers)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			var local uint64

			// Stripe chunks across workers for even distribution.
			for chunkID := f.cfg.FirstChunkID + uint32(workerID); chunkID <= f.cfg.LastChunkID; chunkID += uint32(workers) {
				path := RawTxHashPath(f.cfg.TxHashRawPath, chunkID)
				reader, err := NewBinFileReader(path)
				if err != nil {
					results[workerID].err = fmt.Errorf("count: open chunk %d: %w", chunkID, err)
					return
				}

				for {
					_, hasMore, err := reader.Next()
					if err != nil {
						reader.Close()
						results[workerID].err = fmt.Errorf("count: read chunk %d: %w", chunkID, err)
						return
					}
					if !hasMore {
						break
					}
					local++
				}
				reader.Close()
			}

			results[workerID].count = local
		}(w)
	}

	wg.Wait()

	var total uint64
	for w := 0; w < workers; w++ {
		if results[w].err != nil {
			return 0, results[w].err
		}
		total += results[w].count
	}

	return total, nil
}

// =============================================================================
// Phase 2: Build
// =============================================================================

// phaseBuild creates a StreamHash index from all .bin files.
//
// Keys are pre-hashed via streamhash.PreHash to ensure uniform distribution.
// Payload is the 4-byte ledgerSeq (uint32 big-endian in .bin, stored as uint64 payload).
// WithUnsortedInput is used because .bin files are ordered by chunkID/fileOffset,
// not by pre-hashed key order.
func (f *streamHashFlow) phaseBuild(ctx context.Context, outputPath string, totalKeys uint64) error {
	// Create the StreamHash builder with configuration matching our use case:
	//   - PayloadSize=4: stores ledgerSeq as a 4-byte value
	//   - FingerprintSize=2: 1/65536 false positive rate for non-member detection
	//   - Workers=8: parallel block solving (diminishing returns past 4-8)
	//   - UnsortedInput: .bin files aren't sorted by pre-hash order;
	//     StreamHash handles internal partitioning via temp files in tmpDir
	tmpDir := RecSplitTmpDir(f.cfg.TxHashIdxPath, f.cfg.IndexID)
	builder, err := streamhash.NewBuilder(ctx, outputPath, totalKeys,
		streamhash.WithPayload(4),
		streamhash.WithFingerprint(2),
		streamhash.WithWorkers(streamHashWorkers),
		streamhash.WithUnsortedInput(
			streamhash.TempDir(tmpDir),
		),
	)
	if err != nil {
		return fmt.Errorf("create builder: %w", err)
	}
	defer builder.Close()

	// Read all .bin files sequentially and add each entry to the builder.
	// PreHash converts 32-byte SHA-256 txhashes into 16-byte uniformly
	// distributed keys required by the StreamHash MPHF construction.
	var preHashBuf [16]byte
	for chunkID := f.cfg.FirstChunkID; chunkID <= f.cfg.LastChunkID; chunkID++ {
		path := RawTxHashPath(f.cfg.TxHashRawPath, chunkID)
		reader, err := NewBinFileReader(path)
		if err != nil {
			return fmt.Errorf("build: open chunk %d: %w", chunkID, err)
		}

		for {
			entry, hasMore, err := reader.Next()
			if err != nil {
				reader.Close()
				return fmt.Errorf("build: read chunk %d: %w", chunkID, err)
			}
			if !hasMore {
				break
			}

			// PreHash the 32-byte txhash into 16 bytes (zero-alloc variant).
			streamhash.PreHashInPlace(entry.TxHash[:], preHashBuf[:])

			// AddKey takes the pre-hashed key and the ledgerSeq as payload.
			// Payload is stored as uint64; for 4-byte payloads only the low 32 bits matter.
			if err := builder.AddKey(preHashBuf[:], uint64(entry.LedgerSeq)); err != nil {
				reader.Close()
				return fmt.Errorf("build: AddKey chunk %d: %w", chunkID, err)
			}
		}
		reader.Close()
	}

	// Finish builds the MPHF, writes the file, and fsyncs.
	if err := builder.Finish(); err != nil {
		return fmt.Errorf("build: finish: %w", err)
	}

	// Fsync the output file for durability.
	if err := fsyncFile(outputPath); err != nil {
		return fmt.Errorf("build: fsync: %w", err)
	}

	return nil
}

// =============================================================================
// Phase 3: Verify
// =============================================================================

// phaseVerify re-reads all .bin files and queries the built StreamHash index
// to confirm every key returns the correct ledgerSeq payload.
//
// This is the same verification concept as RecSplit's Phase 4 — re-read the
// source data and confirm the index returns correct results for every entry.
func (f *streamHashFlow) phaseVerify(indexPath string) error {
	idx, err := streamhash.Open(indexPath)
	if err != nil {
		return fmt.Errorf("open index for verify: %w", err)
	}
	defer idx.Close()

	// Launch parallel verify workers, striped across chunks.
	type workerResult struct {
		failures int
		firstErr string
	}
	results := make([]workerResult, streamHashVerifyWorkers)

	var wg sync.WaitGroup
	for w := 0; w < streamHashVerifyWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Per-goroutine pre-hash buffer to avoid allocation.
			var preHashBuf [16]byte

			for chunkID := f.cfg.FirstChunkID + uint32(workerID); chunkID <= f.cfg.LastChunkID; chunkID += streamHashVerifyWorkers {
				path := RawTxHashPath(f.cfg.TxHashRawPath, chunkID)
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

					// Pre-hash the txhash and query the payload.
					streamhash.PreHashInPlace(entry.TxHash[:], preHashBuf[:])
					payload, err := idx.QueryPayload(preHashBuf[:])
					if err != nil {
						results[workerID].failures++
						if results[workerID].firstErr == "" {
							results[workerID].firstErr = fmt.Sprintf(
								"verify: QueryPayload failed for chunk %d: %v", chunkID, err)
						}
						continue
					}

					if uint32(payload) != entry.LedgerSeq {
						results[workerID].failures++
						if results[workerID].firstErr == "" {
							results[workerID].firstErr = fmt.Sprintf(
								"verify: payload mismatch chunk %d: got %d, want %d",
								chunkID, uint32(payload), entry.LedgerSeq)
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
	for w := 0; w < streamHashVerifyWorkers; w++ {
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
// Summary
// =============================================================================

// logSummary logs the StreamHash flow summary.
func (f *streamHashFlow) logSummary(stats *StreamHashFlowStats) {
	f.log.Separator()
	f.log.Info("                    STREAMHASH BUILD SUMMARY")
	f.log.Separator()
	f.log.Info("  Total keys indexed:   %s", format.FormatNumber(int64(stats.TotalKeys)))
	f.log.Info("  Index file size:      %s", format.FormatBytes(stats.IndexSize))
	f.log.Info("  Phase timings:")
	f.log.Info("    Count:    %s", format.FormatDuration(stats.CountPhaseTime))
	f.log.Info("    Build:    %-10s (%d workers, unsorted input)", format.FormatDuration(stats.BuildPhaseTime), streamHashWorkers)
	if stats.VerifyEnabled {
		f.log.Info("    Verify:   %-10s (%d workers)", format.FormatDuration(stats.VerifyPhaseTime), streamHashVerifyWorkers)
	} else {
		f.log.Info("    Verify:   skipped")
	}
	f.log.Info("  Total duration:       %s", format.FormatDuration(stats.TotalTime))
	f.log.Separator()
}

// Ensure binary import is used (for payload encoding documentation).
var _ = binary.BigEndian
