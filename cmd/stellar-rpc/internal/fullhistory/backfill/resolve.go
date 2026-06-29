package backfill

import (
	"slices"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
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

// covers reports whether this range fully contains other (other ⊆ this).
func (r coverageRange) covers(other coverageRange) bool {
	return r.Lo <= other.Lo && r.Hi >= other.Hi
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

	for c := rangeStart; ; c++ {
		for _, kind := range []geometry.Kind{geometry.KindLedgers, geometry.KindEvents} {
			state, err := cat.State(c, kind)
			if err != nil {
				return Plan{}, err
			}
			if state != geometry.StateFrozen {
				needs[c] = needs[c].Add(kind)
			}
		}
		if c == rangeEnd { // inclusive bound; guard chunk.ID wraparound
			break
		}
	}

	// The txhash kind: one rule per overlapping window (resolveWindow runs the diff).
	var builds []IndexBuild
	for _, w := range indexesOverlapping(txLayout, rangeStart, rangeEnd) {
		build, ok, err := resolveWindow(cat, txLayout, w, rangeStart, rangeEnd, needs)
		if err != nil {
			return Plan{}, err
		}
		if ok {
			builds = append(builds, build)
		}
	}

	return Plan{ChunkBuilds: chunkBuildsFrom(needs), IndexBuilds: builds}, nil
}

// resolveWindow diffs one txhash window over [rangeStart, rangeEnd], recording any
// .bin (re)builds into needs and returning the window's IndexBuild (ok=false for none).
func resolveWindow(
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

	frozen, hasFrozen, err := cat.FrozenTxHashIndex(w)
	if err != nil {
		return IndexBuild{}, false, err
	}
	stored := coverageRange{Lo: frozen.Lo, Hi: frozen.Hi}
	if hasFrozen && stored.covers(desired) {
		// Frozen coverage already spans desired, so no rebuild is due. But a crash between
		// CommitTxHashIndex and its sweep can strand "pruning" keys (the demoted predecessor
		// coverage or terminal .bin inputs); schedule a sweep-only pass at the frozen coverage
		// — buildTxhashIndex skips the build (already frozen), buildThenSweep finishes the
		// prune. All-or-nothing: only "frozen" is durable, so any leftover is redone.
		pruning, perr := windowHasPruning(cat, w, txLayout)
		if perr != nil {
			return IndexBuild{}, false, perr
		}
		if pruning {
			return IndexBuild{Index: w, Lo: frozen.Lo, Hi: frozen.Hi}, true, nil
		}
		return IndexBuild{}, false, nil // steady-state, risen floor, or finalized window
	}

	// Desired exceeds stored: request a .bin per not-frozen desired chunk + one IndexBuild.
	// Reuse the txHashStates scanner (centralizes the wraparound guard).
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

// windowHasPruning reports whether window w carries leftover "pruning" state — a
// demoted index coverage or a demoted .bin input — that a buildThenSweep which
// crashed after CommitTxHashIndex left behind. The window's frozen coverage may
// already satisfy the range (so no rebuild is due), but the prune must still finish.
func windowHasPruning(
	cat *catalog.Catalog,
	w geometry.TxHashIndexID,
	txLayout geometry.TxHashIndexLayout,
) (bool, error) {
	covs, err := cat.TxHashIndexKeys(w)
	if err != nil {
		return false, err
	}
	for _, cov := range covs {
		if cov.State == geometry.StatePruning {
			return true, nil
		}
	}
	for cs, err := range txHashStates(cat, txLayout.FirstChunk(w), txLayout.LastChunk(w)) {
		if err != nil {
			return false, err
		}
		if cs.State == geometry.StatePruning {
			return true, nil
		}
	}
	return false, nil
}

// indexesOverlapping returns the window ids overlapping [rangeStart, rangeEnd] inclusive, ascending.
func indexesOverlapping(txLayout geometry.TxHashIndexLayout, rangeStart, rangeEnd chunk.ID) []geometry.TxHashIndexID {
	if rangeEnd < rangeStart {
		return nil
	}
	first := txLayout.TxHashIndexID(rangeStart)
	last := txLayout.TxHashIndexID(rangeEnd)
	out := make([]geometry.TxHashIndexID, 0, uint32(last)-uint32(first)+1)
	for w := first; ; w++ {
		out = append(out, w)
		if w == last {
			break
		}
	}
	return out
}
