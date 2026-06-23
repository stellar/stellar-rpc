package streaming

import (
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// The discard and prune eligibility scans. Each returns a list of zero-arg
// callables (closures over the op and its arguments); the tick just calls them
// in order. Both are PURE READS of the catalog — they decide eligibility from
// durable keys alone, so re-running against the same snapshot after a tick
// finishes yields nothing (the quiescence postcondition).

// eligibleDiscardOps walks hot:chunk:* keys and returns a discard closure per
// hot DB the cold artifacts now fully serve (or that fell past retention). Per
// chunk:
//
//   - chunkLastLedger < floor (past retention OR below earliest_ledger): discard.
//     Its artifact files, if any, carry their own keys and are picked up by the
//     prune stage on the same tick.
//   - complete (last ledger <= through), nothing pending, and the window's index
//     covers it (cold artifacts fully serve it): discard.
//   - otherwise (live, or frozen and awaiting coverage): leave alone.
//
// discardHotTierForChunk is idempotent and re-derives from durable keys, so a
// crash between freeze and discard self-heals on the next tick.
func eligibleDiscardOps(cfg LifecycleConfig, cat *Catalog, through uint32) ([]func() error, error) {
	earliest, _, err := cat.EarliestLedger()
	if err != nil {
		return nil, err
	}
	// The discard scan's "past retention" test is the reader retention
	// contract's ChunkBelowFloor (retention.go) — one definition shared with the
	// read gate, so a hot DB is retired on exactly the floor the reader stops
	// admitting its seqs at. A shortened retentionChunks raises this floor
	// immediately (the gate is rebuilt from the live `through` each tick).
	gate := NewRetentionGate(through, cfg.RetentionChunks, earliest)

	hot, err := cat.HotChunkKeys()
	if err != nil {
		return nil, err
	}

	var ops []func() error
	for _, c := range hot {
		last := c.LastLedger()
		switch {
		case gate.ChunkBelowFloor(c):
			ops = append(ops, func() error { return discardHotTierForChunk(cat, c) })
		case last <= through:
			pending, perr := pendingArtifacts(c, cfg, cat)
			if perr != nil {
				return nil, perr
			}
			covers, cerr := indexCovers(c, cat)
			if cerr != nil {
				return nil, cerr
			}
			if pending.Empty() && covers {
				ops = append(ops, func() error { return discardHotTierForChunk(cat, c) })
			}
			// else: frozen but awaiting coverage, or still producing — leave alone.
		}
		// default (last > through): the live chunk or above — ingestion's, never
		// the lifecycle's to touch.
	}
	return ops, nil
}

// pendingArtifacts lists which processChunk outputs chunk still needs. It is the
// per-chunk counterpart of backfill's per-window rule: ledgers and events must be
// frozen; txhash/.bin is exempt when the window's index already covers the
// chunk — after finalization the chunk:c:txhash key is legitimately demoted or
// swept, and regenerating the .bin would orphan it.
func pendingArtifacts(c chunk.ID, _ LifecycleConfig, cat *Catalog) (ArtifactSet, error) {
	var need ArtifactSet
	for _, kind := range []Kind{KindLedgers, KindEvents} {
		state, err := cat.State(c, kind)
		if err != nil {
			return need, err
		}
		if state != StateFrozen {
			need = need.Add(kind)
		}
	}
	txState, err := cat.State(c, KindTxHash)
	if err != nil {
		return need, err
	}
	if txState != StateFrozen {
		covers, cerr := indexCovers(c, cat)
		if cerr != nil {
			return need, cerr
		}
		if !covers {
			need = need.Add(KindTxHash)
		}
	}
	return need, nil
}

// indexCovers reports whether the durable .idx for chunk's window already
// hashes that chunk — the unique "frozen" coverage's [Lo, Hi] contains it.
func indexCovers(c chunk.ID, cat *Catalog) (bool, error) {
	fk, ok, err := cat.FrozenCoverage(cat.windows.WindowID(c))
	if err != nil {
		return false, err
	}
	return ok && fk.Lo <= c && c <= fk.Hi, nil
}

// eligiblePruneOps is the system's only file-deleter, driven entirely by keys —
// one stage, both key families. It returns closures wrapping the two sweep
// bodies (SweepIndexKey per index key, one batched SweepChunkArtifacts for the
// chunk family).
//
// "Wholly below the floor" is the RetentionGate's predicate — the same one the
// discard scan and the read path use, so prune deletes exactly what the reader
// has stopped admitting. At a genesis floor the gate matches nothing (the
// design's guard: nothing is below genesis), so no hand-rolled sentinel is needed.
func eligiblePruneOps(cfg LifecycleConfig, cat *Catalog, through uint32) ([]func() error, error) {
	earliest, _, err := cat.EarliestLedger()
	if err != nil {
		return nil, err
	}
	gate := NewRetentionGate(through, cfg.RetentionChunks, earliest)

	var ops []func() error

	// Index family: transient debris from any window, plus frozen keys wholly
	// below the floor.
	idxKeys, err := cat.AllIndexKeys()
	if err != nil {
		return nil, err
	}
	for _, cov := range idxKeys {
		switch {
		case cov.State == StateFreezing || cov.State == StatePruning:
			// Transient debris: a crashed build attempt ("freezing": delete, never
			// salvage) or an unfinished demotion ("pruning"). Safe only because no
			// build is in flight when this scan runs (it follows executePlan's
			// return within the tick, and backfill finishes before the loop starts).
			ops = append(ops, func() error { return cat.SweepIndexKey(cov) })
		case gate.WindowBelowFloor(cov.Window, cat.windows):
			// A frozen index key wholly below the floor; the sweep demotes it first.
			ops = append(ops, func() error { return cat.SweepIndexKey(cov) })
		}
	}

	// Chunk family: swept in one batch.
	refs, err := cat.ChunkArtifactKeys()
	if err != nil {
		return nil, err
	}
	var sweep []ArtifactRef
	for _, ref := range refs {
		switch {
		case gate.ChunkBelowFloor(ref.Chunk):
			// Wholly past retention: any state goes.
			sweep = append(sweep, ref)
		case ref.State == StatePruning:
			// In-retention .bin demoted by its window's terminal commit batch.
			sweep = append(sweep, ref)
		case ref.Kind == KindTxHash:
			// "frozen" OR "freezing" chunk:c:txhash inside a FINALIZED window —
			// re-derived (or left mid-write) by a widening backfill that crashed
			// before its terminal rebuild, then abandoned when retention narrowed
			// back. The terminal .idx provably covers the chunk and the resolver
			// never re-materializes a covered window, so it is redundant.
			redundant, rerr := txhashRedundantInFinalizedWindow(cat, ref.Chunk)
			if rerr != nil {
				return nil, rerr
			}
			if redundant {
				sweep = append(sweep, ref)
			}
		}
	}
	if len(sweep) > 0 {
		ops = append(ops, func() error { return cat.SweepChunkArtifacts(sweep) })
	}
	return ops, nil
}

// txhashRedundantInFinalizedWindow reports whether c's window has a TERMINAL
// frozen index coverage (Hi == the window's last chunk). A frozen-or-freezing
// chunk:c:txhash key in such a window is a redundant input the prune scan sweeps
// — this is the branch that makes INV-2's no-leftover-txhash-keys clause self-
// healing rather than merely auditable.
func txhashRedundantInFinalizedWindow(cat *Catalog, c chunk.ID) (bool, error) {
	w := cat.windows.WindowID(c)
	fk, ok, err := cat.FrozenCoverage(w)
	if err != nil {
		return false, err
	}
	return ok && cat.windows.IsTerminalCoverage(fk), nil
}
