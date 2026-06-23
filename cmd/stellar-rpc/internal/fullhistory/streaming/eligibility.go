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
//   - complete (last ledger <= through) and nothing pending (cold artifacts fully
//     serve it): discard.
//   - otherwise (live, or still producing): leave alone.
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
			pending, perr := pendingArtifacts(c, cat)
			if perr != nil {
				return nil, perr
			}
			if pending.Empty() {
				ops = append(ops, func() error { return discardHotTierForChunk(cat, c) })
			}
			// else: still producing — leave alone.
		}
		// default (last > through): the live chunk or above — ingestion's, never
		// the lifecycle's to touch.
	}
	return ops, nil
}

// pendingArtifacts lists which processChunk outputs chunk still needs: the
// per-chunk kinds (currently just ledgers) that are not yet frozen.
func pendingArtifacts(c chunk.ID, cat *Catalog) (ArtifactSet, error) {
	var need ArtifactSet
	for _, kind := range []Kind{KindLedgers} {
		state, err := cat.State(c, kind)
		if err != nil {
			return need, err
		}
		if state != StateFrozen {
			need = need.Add(kind)
		}
	}
	return need, nil
}

// eligiblePruneOps is the system's only file-deleter, driven entirely by keys.
// It returns one batched SweepChunkArtifacts closure for the chunk family.
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
			// In-retention artifact demoted by a recovery.
			sweep = append(sweep, ref)
		}
	}
	if len(sweep) > 0 {
		ops = append(ops, func() error { return cat.SweepChunkArtifacts(sweep) })
	}
	return ops, nil
}
