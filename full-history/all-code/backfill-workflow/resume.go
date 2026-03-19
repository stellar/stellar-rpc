package backfill

import (
	"fmt"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/geometry"
)

// =============================================================================
// Skip-Set Builder
// =============================================================================
//
// BuildSkipSet scans ALL chunk flag pairs for an index (O(ChunksPerTxHashIndex) scan)
// and returns a set of chunk IDs that are fully complete (both lfs and txhash = "1").
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

// BuildSkipSet scans all chunk flags for an index and returns the set of
// chunk IDs that are fully complete (safe to skip on restart).
func BuildSkipSet(meta BackfillMetaStore, indexID uint32, geo geometry.Geometry) (map[uint32]bool, error) {
	flags, err := meta.ScanIndexChunkFlags(indexID, geo)
	if err != nil {
		return nil, fmt.Errorf("scan chunk flags for index %d: %w", indexID, err)
	}

	skipSet := make(map[uint32]bool)
	for chunkID, status := range flags {
		// Only skip if BOTH flags are set.
		// If either flag is absent, the chunk is treated as incomplete and both
		// files are deleted and rewritten from scratch. No partial reuse.
		if status.IsComplete() {
			skipSet[chunkID] = true
		}
	}

	return skipSet, nil
}

// ResumeAction describes what action to take for an index on restart.
type ResumeAction int

const (
	// ResumeActionIngest means the index needs ingestion (some chunks incomplete).
	ResumeActionIngest ResumeAction = iota

	// ResumeActionRecSplit means ingestion is complete but RecSplit needs to run.
	ResumeActionRecSplit

	// ResumeActionComplete means the index is fully complete (skip entirely).
	ResumeActionComplete

	// ResumeActionNew means the index has no state yet (fresh start).
	ResumeActionNew
)

// ResumeResult holds the resume decision for a single index.
type ResumeResult struct {
	Action  ResumeAction
	SkipSet map[uint32]bool // Only populated for ResumeActionIngest
}

// ResumeIndex determines the resume action for an index based on its meta store state.
// The geo parameter is used to check if all chunks are done (parameterized for tests).
//
// Decision tree:
//   - txhash key set → ResumeActionComplete (skip)
//   - All chunks done (lfs+txhash) → ResumeActionRecSplit
//   - Some chunks done → ResumeActionIngest with skip-set
//   - No chunks done → ResumeActionNew (fresh start)
func ResumeIndex(meta BackfillMetaStore, indexID uint32, geo geometry.Geometry) (*ResumeResult, error) {
	// Check if the index is already complete (txhash key present).
	done, err := meta.IsIndexTxHashDone(indexID)
	if err != nil {
		return nil, fmt.Errorf("check index %d txhash: %w", indexID, err)
	}
	if done {
		return &ResumeResult{Action: ResumeActionComplete}, nil
	}

	// Scan chunk flags to determine ingestion progress.
	skipSet, err := BuildSkipSet(meta, indexID, geo)
	if err != nil {
		return nil, err
	}

	if len(skipSet) == 0 {
		return &ResumeResult{Action: ResumeActionNew}, nil
	}

	// If all chunks are done, transition to RecSplit phase.
	if uint32(len(skipSet)) == geo.ChunksPerTxHashIndex {
		return &ResumeResult{Action: ResumeActionRecSplit, SkipSet: skipSet}, nil
	}

	return &ResumeResult{Action: ResumeActionIngest, SkipSet: skipSet}, nil
}
