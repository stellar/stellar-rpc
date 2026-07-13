package backfill

import (
	"slices"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// ChunkBuild names one per-chunk freeze pass: the chunk plus the subset of kinds
// it still needs (one processChunk pass produces all of Artifacts).
type ChunkBuild struct {
	Chunk     chunk.ID
	Artifacts catalog.ArtifactSet
}

// Plan is the resolver's output: the two strata of work (chunk freezes and index
// rebuilds). Behavior-free — loggable, diffable, testable.
type Plan struct {
	ChunkBuilds []ChunkBuild
	IndexBuilds []IndexBuild
}

// Empty reports whether the plan schedules no work (steady state).
func (p Plan) Empty() bool { return len(p.ChunkBuilds) == 0 && len(p.IndexBuilds) == 0 }

// coverageRange is an inclusive [Lo, Hi] chunk range (the per-window txhash rule's desired coverage).
type coverageRange struct {
	Lo, Hi chunk.ID
}

// resolve diffs the desired state (every artifact of [rangeStart, rangeEnd] durable)
// against the catalog, emitting a Plan. A pure read — recomputes from durable keys
// every run, so a restart re-plans cleanly.
//
// Kind rules:
//   - ledgers/events (per-chunk): needed iff chunk:{c}:{kind} is not frozen.
//   - txhash (per-window): desired = [max(FirstChunk(w), rangeStart), min(LastChunk(w), rangeEnd)];
//     if desired ⊆ stored, nothing; else request a .bin per not-frozen desired chunk + one IndexBuild.
//
// The desired.Hi cap by rangeEnd is load-bearing: a window left mid-coverage at
// shutdown is completed by downtime crossing its boundary, which still needs the
// tail .bins + a rebuild — capping keeps the rule uniform.
func resolve(cfg ExecConfig, rangeStart, rangeEnd chunk.ID) (Plan, error) {
	if rangeEnd < rangeStart {
		return Plan{}, nil // no complete chunk exists yet
	}
	cat := cfg.Catalog
	txLayout := cat.TxHashIndexLayout()

	// Per-chunk work, unioned across kinds; one ChunkBuild per chunk.
	needs := map[chunk.ID]catalog.ArtifactSet{}

	for c := rangeStart; c <= rangeEnd; c++ {
		for _, kind := range []geometry.Kind{geometry.KindLedgers, geometry.KindEvents} {
			state, err := cat.State(c, kind)
			if err != nil {
				return Plan{}, err
			}
			if state != geometry.StateFrozen {
				needs[c] = needs[c].Add(kind)
			}
		}
	}

	// The txhash kind: one rule per overlapping window (resolveTxHashIndex runs the diff).
	var builds []IndexBuild
	for _, w := range indexesOverlapping(txLayout, rangeStart, rangeEnd) {
		build, ok, err := resolveTxHashIndex(cat, txLayout, w, rangeStart, rangeEnd, needs)
		if err != nil {
			return Plan{}, err
		}
		if ok {
			builds = append(builds, build)
		}
	}

	return Plan{ChunkBuilds: chunkBuildsFrom(needs), IndexBuilds: builds}, nil
}

// resolveTxHashIndex diffs one txhash window over [rangeStart, rangeEnd], recording any
// .bin (re)builds into needs and returning the window's IndexBuild (ok=false for none).
func resolveTxHashIndex(
	cat *catalog.Catalog,
	txLayout geometry.TxHashIndexLayout,
	w geometry.TxHashIndexID,
	rangeStart, rangeEnd chunk.ID,
	needs map[chunk.ID]catalog.ArtifactSet,
) (IndexBuild, bool, error) {
	desired := coverageRange{
		Lo: max(txLayout.FirstChunk(w), rangeStart),
		Hi: min(txLayout.LastChunk(w), rangeEnd), // capped by range end
	}

	covered, err := cat.FrozenIndexCoversRange(w, desired.Lo, desired.Hi)
	if err != nil {
		return IndexBuild{}, false, err
	}
	if covered {
		// Frozen coverage already spans desired, so no rebuild is due — steady state, a
		// risen floor, or a finalized window. Any non-frozen leftover a crashed build
		// stranded (a superseded "pruning"/"freezing" coverage or a demoted .bin) is the
		// lifecycle prune stage's job to reclaim (eligiblePruneOps), not resolve's: resolve
		// only diffs desired-vs-catalog into builds.
		return IndexBuild{}, false, nil
	}

	// Desired exceeds stored: request a .bin per not-frozen desired chunk + one IndexBuild.
	// Reuse the txHashStates scanner for the per-chunk txhash-state scan.
	for cs, serr := range txHashStates(cat, desired.Lo, desired.Hi) {
		if serr != nil {
			return IndexBuild{}, false, serr
		}
		if cs.State != geometry.StateFrozen {
			needs[cs.Chunk] = needs[cs.Chunk].Add(geometry.KindTxHash)
		}
	}
	return IndexBuild{Index: w, Lo: desired.Lo, Hi: desired.Hi}, true, nil
}

// chunkBuildsFrom flattens needs into a slice sorted by chunk id (deterministic
// plan); empty sets are omitted.
func chunkBuildsFrom(needs map[chunk.ID]catalog.ArtifactSet) []ChunkBuild {
	ids := make([]chunk.ID, 0, len(needs))
	for c, set := range needs {
		if set.Empty() {
			continue
		}
		ids = append(ids, c)
	}
	if len(ids) == 0 {
		return nil
	}
	slices.Sort(ids)
	builds := make([]ChunkBuild, len(ids))
	for i, c := range ids {
		builds[i] = ChunkBuild{Chunk: c, Artifacts: needs[c]}
	}
	return builds
}

// indexesOverlapping returns the window ids overlapping [rangeStart, rangeEnd] inclusive, ascending.
func indexesOverlapping(txLayout geometry.TxHashIndexLayout, rangeStart, rangeEnd chunk.ID) []geometry.TxHashIndexID {
	if rangeEnd < rangeStart {
		return nil
	}
	first := txLayout.TxHashIndexID(rangeStart)
	last := txLayout.TxHashIndexID(rangeEnd)
	out := make([]geometry.TxHashIndexID, 0, uint32(last)-uint32(first)+1)
	for w := first; w <= last; w++ {
		out = append(out, w)
	}
	return out
}
