package backfill

import (
	"fmt"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/cf"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/geometry"
)

// =============================================================================
// Skip-Set Builder
// =============================================================================
//
// BuildSkipSet scans ALL chunk flag pairs for a range (O(1000) scan) and returns
// a set of chunk IDs that are fully complete (both lfs_done and txhash_done = "1").
//
// Key properties:
//   - Does NOT assume contiguity — chunks may be completed in any order by
//     concurrent BSB instances.
//   - A chunk is only in the skip-set if BOTH flags are "1". If either flag
//     is absent, the chunk is treated as incomplete and will be fully rewritten.
//   - The skip-set is read-only after construction — safe for concurrent reads
//     from multiple BSB instance goroutines.
//
// Example: After a crash with 500 of 1000 chunks done (non-contiguous),
// BuildSkipSet returns a map with 500 entries. The remaining 500 chunks
// are missing from the set and will be re-ingested.

// BuildSkipSet scans all chunk flags for a range and returns the set of
// chunk IDs that are fully complete (safe to skip on restart).
func BuildSkipSet(meta BackfillMetaStore, rangeID uint32) (map[uint32]bool, error) {
	flags, err := meta.ScanChunkFlags(rangeID)
	if err != nil {
		return nil, fmt.Errorf("scan chunk flags for range %d: %w", rangeID, err)
	}

	skipSet := make(map[uint32]bool)
	for chunkID, status := range flags {
		// Scenario B5 (doc 07): Only skip if BOTH flags are set.
		// If either flag is absent, the chunk is treated as incomplete and both
		// files are deleted and rewritten from scratch. No partial reuse.
		if status.IsComplete() {
			skipSet[chunkID] = true
		}
	}

	return skipSet, nil
}

// ResumeAction describes what action to take for a range on restart.
type ResumeAction int

const (
	// ResumeActionIngest means the range needs ingestion (some chunks incomplete).
	ResumeActionIngest ResumeAction = iota

	// ResumeActionRecSplit means ingestion is complete but RecSplit needs to run.
	ResumeActionRecSplit

	// ResumeActionComplete means the range is fully complete (skip entirely).
	ResumeActionComplete

	// ResumeActionNew means the range has no state yet (fresh start).
	ResumeActionNew
)

// ResumeResult holds the resume decision for a single range.
type ResumeResult struct {
	Action       ResumeAction
	SkipSet      map[uint32]bool // Only populated for ResumeActionIngest
	CompletedCFs map[int]bool    // Only populated for ResumeActionRecSplit
}

// ResumeRange determines the resume action for a range based on its meta store state.
//
// Decision tree:
//   - No state → ResumeActionNew (fresh start)
//   - COMPLETE → ResumeActionComplete (skip)
//   - RECSPLIT_BUILDING → ResumeActionRecSplit (resume RecSplit, check per-CF flags)
//   - INGESTING → check skip-set size:
//     - All 1000 chunks done → transition to RecSplit
//     - Otherwise → ResumeActionIngest with skip-set
func ResumeRange(meta BackfillMetaStore, rangeID uint32) (*ResumeResult, error) {
	state, err := meta.GetRangeState(rangeID)
	if err != nil {
		return nil, fmt.Errorf("get range state for %d: %w", rangeID, err)
	}

	switch state {
	case "":
		return &ResumeResult{Action: ResumeActionNew}, nil

	case RangeStateComplete:
		return &ResumeResult{Action: ResumeActionComplete}, nil

	case RangeStateRecSplitBuilding:
		// Scenario B3 (doc 07): Process crashed during RecSplit build.
		// Check which CFs have completed and which need rebuilding.
		completedCFs := make(map[int]bool)
		for i := 0; i < cf.Count; i++ {
			done, err := meta.IsRecSplitCFDone(rangeID, i)
			if err != nil {
				return nil, fmt.Errorf("check recsplit cf %d for range %d: %w", i, rangeID, err)
			}
			if done {
				completedCFs[i] = true
			}
		}

		// Scenario B4 (doc 07): All CFs done but state not updated.
		// If all 16 done flags are set but range state is still RECSPLIT_BUILDING,
		// the process crashed after all CFs completed but before state update.
		if len(completedCFs) == cf.Count {
			return &ResumeResult{Action: ResumeActionComplete, CompletedCFs: completedCFs}, nil
		}

		return &ResumeResult{Action: ResumeActionRecSplit, CompletedCFs: completedCFs}, nil

	case RangeStateIngesting:
		skipSet, err := BuildSkipSet(meta, rangeID)
		if err != nil {
			return nil, err
		}

		// If all chunks are done, transition to RecSplit phase
		if uint32(len(skipSet)) == geometry.ChunksPerRange {
			return &ResumeResult{Action: ResumeActionRecSplit, SkipSet: skipSet}, nil
		}

		return &ResumeResult{Action: ResumeActionIngest, SkipSet: skipSet}, nil

	default:
		return nil, fmt.Errorf("unknown range state %q for range %d", state, rangeID)
	}
}
