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
// rebuilds). Behavior-free — loggable, diffable, testable. IndexBuild is in
// txindex.go.
type Plan struct {
	ChunkBuilds []ChunkBuild
	IndexBuilds []IndexBuild
}

// Empty reports whether the plan schedules no work (steady state).
func (p Plan) Empty() bool { return len(p.ChunkBuilds) == 0 && len(p.IndexBuilds) == 0 }

// coverageRange is an inclusive [Lo, Hi] chunk range, the resolver's local type for
// the per-window txhash rule's desired coverage.
type coverageRange struct {
	Lo, Hi chunk.ID
}

// covers reports whether this range fully contains other (other ⊆ this).
func (r coverageRange) covers(other coverageRange) bool {
	return r.Lo <= other.Lo && r.Hi >= other.Hi
}

// resolve diffs the desired state (every artifact of [rangeStart, rangeEnd]
// durable) against the catalog, emitting a Plan. A pure read: marks no key and
// recomputes from durable keys every run, so a restart re-plans cleanly.
//
// Kind rules:
//
//   - ledgers / events (per-chunk): chunk c is needed iff chunk:{c}:{kind} is not
//     frozen.
//   - txhash (per-window): compare stored coverage with desired
//     [max(txLayout.FirstChunk(w), rangeStart), min(txLayout.LastChunk(w), rangeEnd)].
//     Desired ⊆ stored → schedule nothing; else request a .bin for every
//     not-yet-frozen desired chunk and emit one IndexBuild for [desired.Lo, desired.Hi].
//
// The desired.Hi cap is load-bearing: a window current at shutdown has stored hi <
// txLayout.LastChunk(w); downtime crossing the boundary completes it but still needs
// the tail chunks' .bin + a full rebuild. Capping by range end keeps the rule uniform.
func resolve(cfg ExecConfig, rangeStart, rangeEnd chunk.ID) (Plan, error) {
	if rangeEnd < rangeStart {
		return Plan{}, nil // no complete chunk exists yet
	}
	cat := cfg.Catalog
	txLayout := cat.TxHashIndexLayout()

	// Per-chunk work, unioned across kinds; one ChunkBuild per chunk (one
	// processChunk pass produces all).
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
		if c == rangeEnd { // inclusive bound; guard chunk.ID wraparound
			break
		}
	}

	// The txhash kind: one rule per overlapping window.
	var builds []IndexBuild
	for _, w := range indexesOverlapping(txLayout, rangeStart, rangeEnd) {
		desired := coverageRange{
			Lo: max(txLayout.FirstChunk(w), rangeStart),
			Hi: min(txLayout.LastChunk(w), rangeEnd), // capped by range end
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

		// Desired exceeds stored (or no frozen key): request a .bin per not-frozen
		// desired chunk and emit one IndexBuild.
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

// chunkBuildsFrom flattens the needs map into a ChunkBuild slice, sorted by chunk
// id for a deterministic plan. Empty sets (all kinds frozen) are omitted.
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

// indexesOverlapping returns the window ids overlapping [rangeStart, rangeEnd]
// inclusive, ascending (the endpoints' windows bracket the contiguous run).
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
