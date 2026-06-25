package streaming

import (
	"slices"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/geometry"
)

// ChunkBuild names one per-chunk freeze pass: the chunk plus the subset of kinds
// it still needs (one processChunk pass produces all of Artifacts). Pure data the
// executor interprets (design-docs "Postcondition-driven scheduling").
type ChunkBuild struct {
	Chunk     chunk.ID
	Artifacts catalog.ArtifactSet
}

// Plan is the resolver's output: the two strata of work (chunk freezes and index
// rebuilds). It carries no behavior — loggable, diffable, and testable without
// running it. IndexBuild is defined in txindex.go (run via buildThenSweep).
type Plan struct {
	ChunkBuilds []ChunkBuild
	IndexBuilds []IndexBuild
}

// Empty reports whether the plan schedules no work — the steady-state /
// quiescent case.
func (p Plan) Empty() bool { return len(p.ChunkBuilds) == 0 && len(p.IndexBuilds) == 0 }

// coverageRange is a [Lo, Hi] chunk range, inclusive on both ends. It is the
// resolver's local arithmetic type for the per-window txhash rule's "desired"
// coverage; the stored coverage comes from a parsed IndexCoverage key.
type coverageRange struct {
	Lo, Hi chunk.ID
}

// covers reports whether this range fully contains other ("other ⊆ this"): its
// Lo is at or below other's Lo and its Hi is at or above other's Hi. The
// resolver schedules nothing for a window when the stored frozen coverage
// covers the desired range.
func (r coverageRange) covers(other coverageRange) bool {
	return r.Lo <= other.Lo && r.Hi >= other.Hi
}

// resolve diffs the desired state — every artifact derived from [rangeStart,
// rangeEnd] is durable and servable — against the catalog, emitting a Plan. A
// PURE READ: it touches no file, marks no key, and recomputes from durable keys
// every run, so a restart re-plans with nothing to reconcile.
//
// The kind rules:
//
//   - ledgers / events (per-chunk): chunk c is needed iff chunk:{c}:{kind} is not
//     "frozen" (a non-frozen key re-materializes, idempotent inside processChunk).
//   - txhash (per-window): for each overlapping window, compare the stored
//     coverage (Catalog.FrozenCoverage) with the desired
//     [max(windowFirstChunk, rangeStart), min(windowLastChunk, rangeEnd)].
//     Desired ⊆ stored → schedule nothing (steady-state restart, risen floor, or
//     a finalized window the range ends in). Otherwise request a .bin for every
//     not-yet-frozen desired chunk and emit one IndexBuild for [desired.Lo,
//     desired.Hi] (terminal iff desired.Hi is the window's last chunk).
//
// The stored_hi clause is load-bearing: a window CURRENT at shutdown has hi <
// windowLastChunk, and downtime crossing the window boundary completes it while
// it still needs its tail chunks' .bin + a full rebuild — classifying by lo alone
// would strand (stored_hi, windowLastChunk]. The desired.Hi cap makes the rule
// uniform — no special trailing-window case. Inverted range ⇒ empty Plan.
func resolve(cfg ExecConfig, rangeStart, rangeEnd chunk.ID) (Plan, error) {
	if rangeEnd < rangeStart {
		return Plan{}, nil // no complete chunk exists yet
	}
	cat := cfg.Catalog
	wins := cat.TxHashIndexLayout()

	// Per-chunk work, unioned across kinds; one ChunkBuild per chunk regardless
	// of how many kinds it needs (one processChunk pass produces all).
	needs := map[chunk.ID]catalog.ArtifactSet{}

	// Per-chunk kinds: ledgers, events.
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
		if c == rangeEnd { // inclusive upper bound; guard chunk.ID wraparound
			break
		}
	}

	// The txhash kind: one rule per overlapping window.
	var builds []IndexBuild
	for _, w := range windowsOverlapping(wins, rangeStart, rangeEnd) {
		desired := coverageRange{
			Lo: maxChunk(wins.FirstChunk(w), rangeStart),
			Hi: minChunk(wins.LastChunk(w), rangeEnd), // capped by range end ⇒ uniform trailing window
		}

		frozen, hasFrozen, err := cat.FrozenTxHashIndex(w)
		if err != nil {
			return Plan{}, err
		}
		if hasFrozen {
			stored := coverageRange{Lo: frozen.Lo, Hi: frozen.Hi}
			if stored.covers(desired) {
				continue // steady-state restart, risen floor, or finalized window
			}
		}

		// Desired exceeds stored (or no frozen key): request a .bin for every
		// desired chunk not already frozen, and emit one IndexBuild.
		for c := desired.Lo; ; c++ {
			state, err := cat.State(c, geometry.KindTxHash)
			if err != nil {
				return Plan{}, err
			}
			if state != geometry.StateFrozen {
				needs[c] = needs[c].Add(geometry.KindTxHash)
			}
			if c == desired.Hi {
				break
			}
		}
		builds = append(builds, IndexBuild{Index: w, Lo: desired.Lo, Hi: desired.Hi})
	}

	return Plan{ChunkBuilds: chunkBuildsFrom(needs), IndexBuilds: builds}, nil
}

// chunkBuildsFrom flattens the per-chunk needs map into a ChunkBuild slice,
// sorted by chunk id so the plan is deterministic (loggable / diffable /
// testable). Chunks whose set ended up empty (all kinds frozen) are omitted.
func chunkBuildsFrom(needs map[chunk.ID]catalog.ArtifactSet) []ChunkBuild {
	if len(needs) == 0 {
		return nil
	}
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

// windowsOverlapping returns the window ids overlapping [rangeStart, rangeEnd]
// inclusive, ascending. The endpoints' windows bracket the run; the range is
// contiguous so every window between them overlaps.
func windowsOverlapping(wins geometry.TxHashIndexLayout, rangeStart, rangeEnd chunk.ID) []geometry.TxHashIndexID {
	if rangeEnd < rangeStart {
		return nil
	}
	first := wins.TxHashIndexID(rangeStart)
	last := wins.TxHashIndexID(rangeEnd)
	out := make([]geometry.TxHashIndexID, 0, uint32(last)-uint32(first)+1)
	for w := first; ; w++ {
		out = append(out, w)
		if w == last {
			break
		}
	}
	return out
}

func maxChunk(a, b chunk.ID) chunk.ID {
	if a > b {
		return a
	}
	return b
}

func minChunk(a, b chunk.ID) chunk.ID {
	if a < b {
		return a
	}
	return b
}
