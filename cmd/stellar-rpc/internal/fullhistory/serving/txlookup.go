package serving

import (
	"cmp"
	"slices"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/txhash"
)

// The by-hash lookup groundwork. A transaction hash does not identify its chunk,
// so routing cannot resolve it directly; instead the getTransaction path probes
// the hot transaction indexes (a match is definitive) and then the frozen window
// indexes (a match is a candidate, verified against the full hash). These two
// methods supply what that path needs from the admitted state — the hot indexes
// and the window coverages — leaving the probe order, per-coverage .idx opening,
// candidate verification, and the floor/latest gate to the lookup itself.

// HotTxIndexes returns the transaction hash index of every published hot chunk,
// newest chunk first. A hot match is exact and definitive, so the newest indexes
// are probed first. The returned indexes are router-owned handles; the caller does
// not close them.
//
// It is deliberately unfiltered — every published handle, regardless of the
// admitted floor/latest. A match can therefore name a ledger in a chunk below the
// floor (a handle that predates this admission); the lookup's floor/latest gate on
// the resolved ledger is the only thing that keeps such a match from being served.
func (a *Admission) HotTxIndexes() []txhash.HashIndex {
	ids := make([]chunk.ID, 0, len(a.handles.byChunk))
	for c := range a.handles.byChunk {
		ids = append(ids, c)
	}
	slices.SortFunc(ids, func(x, y chunk.ID) int { return cmp.Compare(y, x) }) // newest first

	idxs := make([]txhash.HashIndex, 0, len(ids))
	for _, c := range ids {
		idxs = append(idxs, a.handles.byChunk[c].Txhash())
	}
	return idxs
}

// TxHashCoverages returns the frozen window index coverages in the admitted
// snapshot, newest coverage first (by upper chunk). Each names a generation of an
// on-disk .idx the lookup opens as it probes; a cold match is a fingerprinted
// candidate. Reading them through the snapshot keeps the probe set fixed for the
// request even as an index rebuild swaps a coverage concurrently.
func (a *Admission) TxHashCoverages() ([]geometry.TxHashIndexCoverage, error) {
	all, err := a.catalog.AllTxHashIndexKeysAsOf(a.snap)
	if err != nil {
		return nil, err
	}
	frozen := make([]geometry.TxHashIndexCoverage, 0, len(all))
	for _, cov := range all {
		if cov.State == geometry.StateFrozen {
			frozen = append(frozen, cov)
		}
	}
	slices.SortFunc(frozen, func(x, y geometry.TxHashIndexCoverage) int { return cmp.Compare(y.Hi, x.Hi) })
	return frozen, nil
}
