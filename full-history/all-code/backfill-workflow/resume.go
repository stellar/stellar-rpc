package backfill

import (
	"fmt"

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
	Action  ResumeAction
	SkipSet map[uint32]bool // Only populated for ResumeActionIngest
}

// ResumeRange determines the resume action for a range based on its meta store state.
// The geo parameter is used to check if all chunks are done (parameterized for tests).
//
// Decision tree:
//   - No state → ResumeActionNew (fresh start)
//   - COMPLETE → ResumeActionComplete (skip)
//   - RECSPLIT_BUILDING → ResumeActionRecSplit (all-or-nothing rerun)
//   - INGESTING → check skip-set size:
//     - All chunks done → transition to RecSplit
//     - Otherwise → ResumeActionIngest with skip-set
func ResumeRange(meta BackfillMetaStore, rangeID uint32, geo geometry.Geometry) (*ResumeResult, error) {
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
		// All-or-nothing: the entire 4-phase flow reruns from scratch on crash.
		// Per-CF done flags are cleared and re-set during the rerun — they are
		// not consulted here. Partial indexes are cleaned up on re-entry.
		return &ResumeResult{Action: ResumeActionRecSplit}, nil

	case RangeStateIngesting:
		skipSet, err := BuildSkipSet(meta, rangeID)
		if err != nil {
			return nil, err
		}

		// If all chunks are done, transition to RecSplit phase
		if uint32(len(skipSet)) == geo.ChunksPerIndex {
			return &ResumeResult{Action: ResumeActionRecSplit, SkipSet: skipSet}, nil
		}

		return &ResumeResult{Action: ResumeActionIngest, SkipSet: skipSet}, nil

	default:
		return nil, fmt.Errorf("unknown range state %q for range %d", state, rangeID)
	}
}
