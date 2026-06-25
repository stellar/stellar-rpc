package catalog

import (
	"strings"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/geometry"
)

// ArtifactSet is the subset of per-chunk artifact Kinds a processChunk pass must
// produce. It is a small immutable set over the three per-chunk kinds (ledgers,
// events, txhash): the resolver builds it from the catalog difference and
// processChunk narrows it by dropping already-frozen kinds (per-kind
// idempotency).
//
// The representation is a fixed-width bitmask over allKinds' canonical order, so
// Kinds() yields kinds in that order (the canonical ledgers→txhash→events order
// the cold ingesters build in) and membership tests are allocation-free.
type ArtifactSet struct {
	mask uint8
}

// kindBit maps a Kind to its bit in ArtifactSet.mask via its index in allKinds.
// An unknown kind returns (0,false) so callers never set a phantom bit.
func kindBit(k geometry.Kind) (uint8, bool) {
	for i, kk := range geometry.AllKinds() {
		if kk == k {
			return uint8(1) << i, true
		}
	}
	return 0, false
}

// NewArtifactSet builds a set from the given kinds. Unknown kinds are ignored
// (the kind registry in the geometry package is the authority); duplicates are
// idempotent.
func NewArtifactSet(kinds ...geometry.Kind) ArtifactSet {
	var s ArtifactSet
	for _, k := range kinds {
		if bit, ok := kindBit(k); ok {
			s.mask |= bit
		}
	}
	return s
}

// AllArtifacts is the full set (ledgers, events, txhash) — what a from-scratch
// chunk freeze requests before per-kind idempotency narrows it.
func AllArtifacts() ArtifactSet { return NewArtifactSet(geometry.AllKinds()...) }

// Has reports whether kind is in the set.
func (s ArtifactSet) Has(kind geometry.Kind) bool {
	bit, ok := kindBit(kind)
	return ok && s.mask&bit != 0
}

// Empty reports whether the set requests no kinds.
func (s ArtifactSet) Empty() bool { return s.mask == 0 }

// Remove returns a copy of the set without kind (idempotent if absent).
func (s ArtifactSet) Remove(kind geometry.Kind) ArtifactSet {
	if bit, ok := kindBit(kind); ok {
		s.mask &^= bit
	}
	return s
}

// Add returns a copy of the set with kind included (idempotent if present).
func (s ArtifactSet) Add(kind geometry.Kind) ArtifactSet {
	if bit, ok := kindBit(kind); ok {
		s.mask |= bit
	}
	return s
}

// Kinds returns the requested kinds in canonical (allKinds) order.
func (s ArtifactSet) Kinds() []geometry.Kind {
	var out []geometry.Kind
	for i, k := range geometry.AllKinds() {
		if s.mask&(uint8(1)<<i) != 0 {
			out = append(out, k)
		}
	}
	return out
}

// String renders the set as a comma-joined kind list (debug/error messages).
func (s ArtifactSet) String() string {
	ks := s.Kinds()
	parts := make([]string, len(ks))
	for i, k := range ks {
		parts[i] = string(k)
	}
	return "{" + strings.Join(parts, ",") + "}"
}
