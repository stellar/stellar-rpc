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

// Plan is the resolver's output: the per-chunk freeze work. It carries no
// behavior — it can be logged, diffed, and tested without running it, which is
// what makes "the plan is just a value" literally true.
type Plan struct {
	ChunkBuilds []ChunkBuild
}

// Empty reports whether the plan schedules no work — the steady-state /
// quiescent case.
func (p Plan) Empty() bool { return len(p.ChunkBuilds) == 0 }

// resolve computes the diff between the desired state — every artifact derived
// from every ledger in [rangeStart, rangeEnd] is durable and servable — and the
// catalog, emitting the difference as a Plan. It is a PURE READ of the Phase A
// catalog: it touches no file, marks no key, and recomputes from durable keys
// on every run, so a restart re-plans from what is actually on disk with
// nothing to reconcile (design-docs "Postcondition-driven scheduling").
//
// The kind rule:
//
//   - ledgers / events (per-chunk): chunk c is needed iff chunk:{c}:{kind} is not
//     "frozen". A "freezing"/"pruning"/absent key re-materializes (idempotent
//     inside processChunk); a "frozen" key self-skips here.
//
// Inverted range (rangeEnd < rangeStart, a network younger than one complete
// chunk) returns the empty Plan.
func resolve(cfg ExecConfig, rangeStart, rangeEnd chunk.ID) (Plan, error) {
	if rangeEnd < rangeStart {
		return Plan{}, nil // no complete chunk exists yet
	}
	cat := cfg.Catalog

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

	return Plan{ChunkBuilds: chunkBuildsFrom(needs)}, nil
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
