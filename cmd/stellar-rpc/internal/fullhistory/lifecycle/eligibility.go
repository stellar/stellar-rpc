package lifecycle

import (
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// The discard and prune eligibility scans. Each returns zero-arg op closures the
// tick calls in order. Both are PURE READS — eligibility comes from durable keys
// alone, so re-running against the same snapshot yields nothing (quiescence).

// eligibleDiscardOps returns a discard closure per hot DB the cold artifacts now
// fully serve (or that fell past retention). Per chunk: below the floor → discard;
// complete (last <= through), nothing pending, and the index covers it → discard;
// otherwise (live, or frozen awaiting coverage) → leave alone.
// catalog.DiscardHotChunk is idempotent, so a crash between freeze and discard
// self-heals next tick.
func eligibleDiscardOps(cfg Config, cat *catalog.Catalog, through uint32) ([]func() error, error) {
	earliest, _, err := cat.EarliestLedger()
	if err != nil {
		return nil, err
	}
	// The "past retention" test shares one definition with the read gate
	// (retention.go), so a hot DB retires on exactly the floor the reader stops
	// admitting at. A shortened retentionChunks raises the floor at once.
	gate := NewRetentionFloor(through, cfg.RetentionChunks, earliest)

	hot, err := cat.HotChunkKeys()
	if err != nil {
		return nil, err
	}

	var ops []func() error
	for _, c := range hot {
		last := c.LastLedger()
		switch {
		case gate.Excludes(c):
			ops = append(ops, func() error { return cat.DiscardHotChunk(c) })
		case last <= through:
			pending, perr := pendingArtifacts(c, cat)
			if perr != nil {
				return nil, perr
			}
			covers, cerr := indexCovers(c, cat)
			if cerr != nil {
				return nil, cerr
			}
			if pending.Empty() && covers {
				ops = append(ops, func() error { return cat.DiscardHotChunk(c) })
			}
			// else: frozen awaiting coverage, or still producing — leave alone.
		}
		// default (last > through): the live chunk or above — ingestion's, not ours.
	}
	return ops, nil
}

// pendingArtifacts lists which outputs chunk still needs: ledgers and events must
// be frozen; txhash/.bin is exempt when the window's index already covers the
// chunk (after finalization the chunk:c:txhash key is demoted/swept, so
// regenerating the .bin would orphan it).
func pendingArtifacts(c chunk.ID, cat *catalog.Catalog) (catalog.ArtifactSet, error) {
	var need catalog.ArtifactSet
	for _, kind := range []geometry.Kind{geometry.KindLedgers, geometry.KindEvents} {
		state, err := cat.State(c, kind)
		if err != nil {
			return need, err
		}
		if state != geometry.StateFrozen {
			need = need.Add(kind)
		}
	}
	txState, err := cat.State(c, geometry.KindTxHash)
	if err != nil {
		return need, err
	}
	if txState != geometry.StateFrozen {
		covers, cerr := indexCovers(c, cat)
		if cerr != nil {
			return need, cerr
		}
		if !covers {
			need = need.Add(geometry.KindTxHash)
		}
	}
	return need, nil
}

// indexCovers reports whether the durable .idx for chunk's window already hashes
// it — the frozen coverage's [Lo, Hi] contains c.
func indexCovers(c chunk.ID, cat *catalog.Catalog) (bool, error) {
	fk, ok, err := cat.FrozenTxHashIndex(cat.TxHashIndexLayout().TxHashIndexID(c))
	if err != nil {
		return false, err
	}
	return ok && fk.Lo <= c && c <= fk.Hi, nil
}

// eligiblePruneOps is the system's only file-deleter, key-driven, covering both
// key families. It returns sweep closures (SweepTxHashIndexKey per index key, one
// batched SweepChunkArtifacts for the chunk family). "Below the floor" is the
// gate predicate shared with the discard scan and read path, so prune deletes
// exactly what the reader has stopped admitting.
// The second return is the total number of artifacts the ops will sweep (one per
// index-key op plus every ref in the single batched chunk sweep), so the caller
// meters Prune in artifacts — the same unit the Phase 1 sweep reports — rather
// than in op closures (the chunk family collapses N artifacts into one op).
func eligiblePruneOps(cfg Config, cat *catalog.Catalog, through uint32) ([]func() error, int, error) {
	earliest, _, err := cat.EarliestLedger()
	if err != nil {
		return nil, 0, err
	}
	gate := NewRetentionFloor(through, cfg.RetentionChunks, earliest)

	var ops []func() error
	artifacts := 0

	// Index family: transient debris from any window, plus frozen keys below the floor.
	idxKeys, err := cat.AllTxHashIndexKeys()
	if err != nil {
		return nil, 0, err
	}
	for _, cov := range idxKeys {
		switch {
		case cov.State == geometry.StateFreezing || cov.State == geometry.StatePruning:
			// Transient debris (a crashed build or unfinished demotion). Safe only
			// because no build is in flight when this scan runs (it follows
			// executePlan's return, and backfill finishes before the loop starts).
			ops = append(ops, func() error { return cat.SweepTxHashIndexKey(cov) })
			artifacts++
		case gate.Excludes(cat.TxHashIndexLayout().LastChunk(cov.Index)):
			// Frozen index key below the floor; the sweep demotes it first.
			ops = append(ops, func() error { return cat.SweepTxHashIndexKey(cov) })
			artifacts++
		}
	}

	// Chunk family: swept in one batch.
	refs, err := cat.ChunkArtifactKeys()
	if err != nil {
		return nil, 0, err
	}
	var sweep []catalog.ArtifactRef
	for _, ref := range refs {
		switch {
		case gate.Excludes(ref.Chunk):
			// Past retention: any state goes.
			sweep = append(sweep, ref)
		case ref.State == geometry.StatePruning:
			// In-retention .bin demoted by its window's terminal commit batch.
			sweep = append(sweep, ref)
		case ref.Kind == geometry.KindTxHash:
			// A frozen/freezing chunk:c:txhash inside a FINALIZED window: re-derived
			// (or left mid-write) by a widening backfill that crashed before its
			// terminal rebuild, then abandoned when retention narrowed. The terminal
			// .idx provably covers the chunk and is never re-materialized, so it's
			// redundant.
			redundant, rerr := txhashRedundantInFinalizedWindow(cat, ref.Chunk)
			if rerr != nil {
				return nil, 0, rerr
			}
			if redundant {
				sweep = append(sweep, ref)
			}
		}
	}
	if len(sweep) > 0 {
		ops = append(ops, func() error { return cat.SweepChunkArtifacts(sweep) })
		artifacts += len(sweep)
	}
	return ops, artifacts, nil
}

// txhashRedundantInFinalizedWindow reports whether c's window has a TERMINAL
// frozen index coverage (Hi == the window's last chunk) — the branch that makes
// INV-2's no-leftover-txhash-keys clause self-healing, not merely auditable.
func txhashRedundantInFinalizedWindow(cat *catalog.Catalog, c chunk.ID) (bool, error) {
	w := cat.TxHashIndexLayout().TxHashIndexID(c)
	fk, ok, err := cat.FrozenTxHashIndex(w)
	if err != nil {
		return false, err
	}
	return ok && cat.TxHashIndexLayout().IsTerminalCoverage(fk), nil
}
