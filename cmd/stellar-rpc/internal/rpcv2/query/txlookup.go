package query

import (
	"cmp"
	"fmt"
	"maps"
	"slices"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
)

// The by-hash lookup groundwork. A transaction hash does not identify its chunk,
// so routing cannot resolve it directly; instead the getTransaction path probes
// the hot transaction indexes (a match is definitive) and then the frozen window
// indexes (a match is a candidate, verified against the full hash). These two
// methods supply what that path needs from the read view — the hot indexes and
// the opened cold window indexes — leaving the probe order, candidate
// verification, and the floor/latest gate to the lookup itself.

// HotTxHashIndexes returns the transaction hash index of every published hot chunk,
// newest chunk first. A hot match is exact and definitive, so the newest indexes
// are probed first. The returned indexes are registry-owned handles; the caller does
// not close them.
//
// It is deliberately unfiltered — every published handle, regardless of the
// view's floor/latest. A match can therefore name a ledger in a chunk below the
// floor (a handle that predates this view); the lookup's floor/latest gate on
// the resolved ledger is the only thing that keeps such a match from being served.
func (a *ReadView) HotTxHashIndexes() []txhash.HashIndex {
	ids := slices.Sorted(maps.Keys(a.handles.byChunk))
	slices.Reverse(ids) // newest first

	idxs := make([]txhash.HashIndex, 0, len(ids))
	for _, c := range ids {
		idxs = append(idxs, a.handles.byChunk[c].Txhash())
	}
	return idxs
}

// ColdTxIndexes opens a reader for every frozen window index in the view's
// snapshot, newest coverage first — a cold match is a fingerprinted candidate,
// so the lookup verifies it against the full hash. The readers are view-owned:
// Release closes them (also the ones already opened when a later open fails).
func (a *ReadView) ColdTxIndexes() ([]txhash.HashIndex, error) {
	covs, err := a.coldTxHashIndexCoverages()
	if err != nil {
		return nil, err
	}
	idxs := make([]txhash.HashIndex, 0, len(covs))
	for _, cov := range covs {
		r, err := txhash.OpenColdReader(a.catalog.Layout().TxHashIndexFilePath(cov))
		if err != nil {
			return nil, fmt.Errorf("query: open cold tx index [%s, %s]: %w", cov.Lo, cov.Hi, err)
		}
		a.closers = append(a.closers, r.Close)
		idxs = append(idxs, r)
	}
	return idxs, nil
}

// coldTxHashIndexCoverages returns the frozen window index coverages in the view's
// snapshot, newest coverage first (by upper chunk). Each names a generation of an
// on-disk .idx. Reading them through the snapshot keeps the probe set fixed for the
// request even as an index rebuild swaps a coverage concurrently.
func (a *ReadView) coldTxHashIndexCoverages() ([]geometry.TxHashIndexCoverage, error) {
	all, err := a.snap.AllTxHashIndexKeys()
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
