package txhash

// candidate_source.go defines the federation seam for getTransaction(byHash).
// A CandidateSource yields the ledger sequences a tx hash might live in; the
// read assembly (read_assembly.go) verifies each candidate against the
// ledger's real contents. One interface spans both retention tiers — the hot
// RocksDB store (an exact key lookup) and the cold streamhash indexes (a
// fingerprinted MPHF, which may over-report) — so the assembly can federate
// hot + cold without knowing which tier any given source belongs to.

import (
	"errors"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// CandidateSource yields candidate ledger sequences for a tx hash.
//
// A source may return more than one candidate, or — if it is not exact — a
// candidate the hash does not actually live in: the cold index keeps only a
// 1-byte fingerprint per key, so ~1/256 of absent hashes survive lookup as
// false positives. The read assembly verifies every candidate against the
// ledger before trusting it. An empty (or nil) slice means "no candidate
// here" and is distinct from an error.
//
// Implemented by *HotStore (exact), *ColdReader (one fingerprinted index),
// and *ColdReaderSet (a fan-out across many cold indexes). Which sources are
// live, and their lifecycle, is the serving layer's concern; this seam only
// fans a lookup out across whatever sources it is handed.
type CandidateSource interface {
	Candidates(hash [32]byte) ([]uint32, error)

	// Exact reports whether this source's candidates are authoritative. An
	// exact source never returns a false positive, so a candidate it produces
	// MUST be present in the ledger it names — if it is not, the source and the
	// ledger store disagree, which is data corruption, not a normal miss. The
	// assembly consults exact sources first and treats a verification failure
	// from one as a hard error. The hot store is exact; cold streamhash indexes
	// are not.
	Exact() bool
}

// singleCandidate adapts a store's exact Get — (seq, with stores.ErrNotFound on
// a miss) — to the CandidateSource shape: no candidate on a miss, one candidate
// on a hit, the error otherwise. Shared by the hot store and a single cold
// reader, which expose the same Get signature.
func singleCandidate(seq uint32, err error) ([]uint32, error) {
	if errors.Is(err, stores.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return []uint32{seq}, nil
}

// Candidates makes *HotStore a CandidateSource. The hot store is an exact key
// lookup, so it returns at most one candidate and never a false positive; the
// hit still flows through the assembly's verification, which for an exact
// source doubles as an integrity check.
func (h *HotStore) Candidates(hash [32]byte) ([]uint32, error) {
	return singleCandidate(h.Get(hash))
}

// Exact reports that the hot store is authoritative: a returned seq is a real
// (txhash → ledger) mapping, never a fingerprint guess.
func (h *HotStore) Exact() bool { return true }

// Candidates makes *ColdReader a CandidateSource. The returned seq has cleared
// the index's fingerprint check but may still be a false positive (~1/256 of
// absent hashes); the assembly confirms it against the ledger.
func (r *ColdReader) Candidates(hash [32]byte) ([]uint32, error) {
	return singleCandidate(r.Get(hash))
}

// Exact reports that a cold index is NOT authoritative: a returned seq has only
// passed the fingerprint check and may be a false positive the assembly must
// verify against the ledger.
func (r *ColdReader) Exact() bool { return false }
