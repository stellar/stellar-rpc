package streaming

import (
	"strings"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
)

// ArtifactSet is the subset of per-chunk artifact Kinds a processChunk pass must
// produce (design-docs rule 2). It is a small immutable set over the per-chunk
// kinds (currently just ledgers); the resolver builds it from the catalog
// difference and processChunk narrows it further by dropping already-frozen
// kinds (rule 1's per-kind idempotency).
//
// The representation is a fixed-width bitmask over allKinds' canonical order, so
// Kinds() yields kinds in that order (the same order buildColdIngesters uses)
// and membership tests are allocation-free.
type ArtifactSet struct {
	mask uint8
}

// kindBit maps a Kind to its bit in ArtifactSet.mask via its index in allKinds.
// An unknown kind returns (0,false) so callers never set a phantom bit.
func kindBit(k Kind) (uint8, bool) {
	for i, kk := range allKinds {
		if kk == k {
			return uint8(1) << i, true
		}
	}
	return 0, false
}

// NewArtifactSet builds a set from the given kinds. Unknown kinds are ignored
// (the kind registry in keys.go is the authority); duplicates are idempotent.
func NewArtifactSet(kinds ...Kind) ArtifactSet {
	var s ArtifactSet
	for _, k := range kinds {
		if bit, ok := kindBit(k); ok {
			s.mask |= bit
		}
	}
	return s
}

// AllArtifacts is the full set (currently just ledgers) — what a from-scratch
// chunk freeze requests before per-kind idempotency narrows it.
func AllArtifacts() ArtifactSet { return NewArtifactSet(allKinds...) }

// Has reports whether kind is in the set.
func (s ArtifactSet) Has(kind Kind) bool {
	bit, ok := kindBit(kind)
	return ok && s.mask&bit != 0
}

// Empty reports whether the set requests no kinds.
func (s ArtifactSet) Empty() bool { return s.mask == 0 }

// Remove returns a copy of the set without kind (idempotent if absent).
func (s ArtifactSet) Remove(kind Kind) ArtifactSet {
	if bit, ok := kindBit(kind); ok {
		s.mask &^= bit
	}
	return s
}

// Add returns a copy of the set with kind included (idempotent if present).
func (s ArtifactSet) Add(kind Kind) ArtifactSet {
	if bit, ok := kindBit(kind); ok {
		s.mask |= bit
	}
	return s
}

// Kinds returns the requested kinds in canonical (allKinds) order.
func (s ArtifactSet) Kinds() []Kind {
	var out []Kind
	for i, k := range allKinds {
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

// ingestConfig translates the set into the merged ingest.Config (which selects
// data types by bool). Only the requested kinds are enabled, so RunColdChunk
// drives exactly the cold ingesters processChunk asked for.
func (s ArtifactSet) ingestConfig() ingest.Config { //nolint:unused // called from processChunk in a later layer
	return ingest.Config{
		Ledgers: s.Has(KindLedgers),
	}
}
