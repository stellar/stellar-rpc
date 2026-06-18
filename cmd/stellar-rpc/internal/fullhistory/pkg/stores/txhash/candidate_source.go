package txhash

import (
	"errors"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// CandidateSource yields the ledger sequences a tx hash might live in. Exact
// sources (the hot store) never produce a false positive; inexact ones (cold
// fingerprinted indexes) may, so the read assembly verifies every candidate.
type CandidateSource interface {
	Candidates(hash [32]byte) ([]uint32, error)
	Exact() bool
}

// singleCandidate adapts an exact Get (seq, stores.ErrNotFound on miss) to the
// CandidateSource shape.
func singleCandidate(seq uint32, err error) ([]uint32, error) {
	if errors.Is(err, stores.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return []uint32{seq}, nil
}

func (h *HotStore) Candidates(hash [32]byte) ([]uint32, error) { return singleCandidate(h.Get(hash)) }

func (h *HotStore) Exact() bool { return true }

func (r *ColdReader) Candidates(hash [32]byte) ([]uint32, error) { return singleCandidate(r.Get(hash)) }

func (r *ColdReader) Exact() bool { return false }
