package streaming

import (
	"slices"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ChunkBuild names one per-chunk freeze pass: the chunk plus the subset of kinds
// it still needs. One processChunk pass produces all of Artifacts. It is pure
// data — the executor interprets it (design-docs/full-history-streaming-
// workflow.md "Postcondition-driven scheduling").
type ChunkBuild struct {
	Chunk     chunk.ID
	Artifacts ArtifactSet
}

// Plan is the resolver's output: the two strata of work (chunk freezes and
// index rebuilds). It carries no behavior — it can be logged, diffed, and
// tested without running it, which is what makes "the plan is just a value"
// literally true. IndexBuild itself is defined in build.go (the executor runs
// it via buildThenSweep).
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

// resolve computes the diff between the desired state — every artifact derived
// from every ledger in [rangeStart, rangeEnd] is durable and servable — and the
// catalog, emitting the difference as a Plan. It is a PURE READ of the Phase A
// catalog: it touches no file, marks no key, and recomputes from durable keys
// on every run, so a restart re-plans from what is actually on disk with
// nothing to reconcile (design-docs "Postcondition-driven scheduling").
//
// The kind rules:
//
//   - ledgers / events (per-chunk): chunk c is needed iff chunk:{c}:{kind} is not
//     "frozen". A "freezing"/"pruning"/absent key re-materializes (idempotent
//     inside processChunk); a "frozen" key self-skips here.
//   - txhash (per-window): for EACH window overlapping the range, compare the
//     stored coverage (the window's unique "frozen" index key, via the Phase A
//     Catalog.FrozenCoverage) with the desired coverage
//     [max(windowFirstChunk, rangeStart), min(windowLastChunk, rangeEnd)].
//     Desired ⊆ stored → schedule nothing (steady-state restart, a risen floor,
//     or a finalized window the range ends in). Otherwise request a .bin for
//     every desired chunk not already frozen (already-frozen .bins self-skip)
//     and emit one IndexBuild for [desired.Lo, desired.Hi]; the build is
//     terminal — derived later via Windows.IsTerminalCoverage — iff desired.Hi
//     is the window's last chunk.
//
// The stored_hi clause is load-bearing: a window that was CURRENT at shutdown
// carries a frozen key with hi < windowLastChunk, and when downtime crosses the
// window boundary it becomes a complete window still needing its tail chunks'
// .bin and a full rebuild — classifying by lo alone would strand chunks
// (stored_hi, windowLastChunk] permanently. The desired.Hi upper cap
// (min(windowLastChunk, rangeEnd)) makes the rule uniform: no special trailing-
// window case exists.
//
// Inverted range (rangeEnd < rangeStart, a network younger than one complete
// chunk) returns the empty Plan.
func resolve(cfg ExecConfig, rangeStart, rangeEnd chunk.ID) (Plan, error) {
	if rangeEnd < rangeStart {
		return Plan{}, nil // no complete chunk exists yet
	}
	cat := cfg.Catalog
	wins := cat.Windows()

	// Per-chunk work, unioned across kinds; one ChunkBuild per chunk regardless
	// of how many kinds it needs (one processChunk pass produces all).
	needs := map[chunk.ID]ArtifactSet{}

	// Per-chunk kinds: ledgers, events.
	for c := rangeStart; ; c++ {
		for _, kind := range []Kind{KindLedgers, KindEvents} {
			state, err := cat.State(c, kind)
			if err != nil {
				return Plan{}, err
			}
			if state != StateFrozen {
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

		frozen, hasFrozen, err := cat.FrozenCoverage(w)
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
			state, err := cat.State(c, KindTxHash)
			if err != nil {
				return Plan{}, err
			}
			if state != StateFrozen {
				needs[c] = needs[c].Add(KindTxHash)
			}
			if c == desired.Hi {
				break
			}
		}
		builds = append(builds, IndexBuild{Window: w, Lo: desired.Lo, Hi: desired.Hi})
	}

	return Plan{ChunkBuilds: chunkBuildsFrom(needs), IndexBuilds: builds}, nil
}

// chunkBuildsFrom flattens the per-chunk needs map into a ChunkBuild slice,
// sorted by chunk id so the plan is deterministic (loggable / diffable /
// testable). Chunks whose set ended up empty (all kinds frozen) are omitted.
func chunkBuildsFrom(needs map[chunk.ID]ArtifactSet) []ChunkBuild {
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
func windowsOverlapping(wins Windows, rangeStart, rangeEnd chunk.ID) []WindowID {
	if rangeEnd < rangeStart {
		return nil
	}
	first := wins.WindowID(rangeStart)
	last := wins.WindowID(rangeEnd)
	out := make([]WindowID, 0, uint32(last)-uint32(first)+1)
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
