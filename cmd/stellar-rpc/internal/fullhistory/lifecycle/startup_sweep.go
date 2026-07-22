package lifecycle

import (
	"fmt"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// StartupSweep destroys resources a crashed run left demoted, before the daemon
// resumes serving. The catalog demotions are the durable work list: a "transient"
// hot key or a "pruning" cold key marks a deletion that never finished. No query
// survives the process, so the destroy is immediate — the grace period is only for
// in-flight readers, of which a fresh process has none.
//
// It skips chunks at or above liveChunk: a "transient" key there is a mid-CREATE
// leftover (openHotDBForChunk recreates it), not a mid-discard one. Runs before
// the lifecycle goroutine and before any query is admitted.
func StartupSweep(cat *catalog.Catalog, liveChunk chunk.ID) error {
	// Hot: "transient" chunks below the live chunk are mid-discard leftovers.
	hot, err := cat.HotChunkKeys()
	if err != nil {
		return fmt.Errorf("startup sweep: read hot keys: %w", err)
	}
	for _, c := range hot {
		if c >= liveChunk {
			continue
		}
		st, err := cat.HotState(c)
		if err != nil {
			return fmt.Errorf("startup sweep: read hot state %s: %w", c, err)
		}
		if st == geometry.HotTransient {
			if err := cat.DiscardHotChunk(c); err != nil {
				return fmt.Errorf("startup sweep: discard hot %s: %w", c, err)
			}
		}
	}

	// Cold: "freezing"/"pruning" index debris and "pruning" chunk artifacts.
	idx, err := cat.AllTxHashIndexKeys()
	if err != nil {
		return fmt.Errorf("startup sweep: read index keys: %w", err)
	}
	for _, cov := range idx {
		if cov.State == geometry.StateFreezing || cov.State == geometry.StatePruning {
			if err := cat.SweepTxHashIndexKey(cov); err != nil {
				return fmt.Errorf("startup sweep: sweep index %s: %w", cov.Key, err)
			}
		}
	}

	refs, err := cat.ChunkArtifactKeys()
	if err != nil {
		return fmt.Errorf("startup sweep: read chunk keys: %w", err)
	}
	var pruning []catalog.ArtifactRef
	for _, ref := range refs {
		if ref.State == geometry.StatePruning {
			pruning = append(pruning, ref)
		}
	}
	if err := cat.SweepChunkArtifacts(pruning); err != nil {
		return fmt.Errorf("startup sweep: sweep chunk artifacts: %w", err)
	}
	return nil
}
