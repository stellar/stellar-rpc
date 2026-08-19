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
// the (lazily opened) cold window indexes — leaving the probe order, candidate
// verification, and the floor/latest gate to the lookup itself.

// HotTxHashIndexes returns the transaction hash index of every published hot chunk,
// newest chunk first. A hot match is exact and definitive, so the newest indexes
// are probed first. The returned indexes are registry-owned handles; the caller does
// not close them.
//
// It is deliberately unfiltered — every published handle, regardless of the
// view's floor/latest. A match can therefore name a ledger in a chunk below the
// floor (a handle that predates this view); the caller's window gate, wrapped
// around every index it hands the probe, is the only thing that keeps such a
// match from being served.
func (a *ReadView) HotTxHashIndexes() []txhash.HashIndex {
	ids := slices.Sorted(maps.Keys(a.handles.byChunk))
	slices.Reverse(ids) // newest first

	idxs := make([]txhash.HashIndex, 0, len(ids))
	for _, c := range ids {
		idxs = append(idxs, a.handles.byChunk[c].Txhash())
	}
	return idxs
}

// ColdTxIndexes returns one reader per frozen window index in the view's
// snapshot, newest coverage first — a cold match is a fingerprinted candidate,
// so the lookup verifies it against the full hash. Each .idx file is opened on
// its first Get, not here: the common case — a recent transaction resolved by
// the hot indexes — must not pay one file open per frozen window. Opened
// readers are view-owned: Release closes them.
func (a *ReadView) ColdTxIndexes() ([]txhash.HashIndex, error) {
	covs, err := a.coldTxHashIndexCoverages()
	if err != nil {
		return nil, err
	}
	idxs := make([]txhash.HashIndex, 0, len(covs))
	for _, cov := range covs {
		idxs = append(idxs, &lazyColdTxIndex{view: a, cov: cov})
	}
	return idxs, nil
}

// lazyColdTxIndex defers opening a frozen window index's .idx file until the
// first Get, then caches the reader for the view's remaining probes. A
// ReadView serves one request on one goroutine, so no locking. An open failure
// surfaces from Get; the tx lookup records it as a soft error and reports it
// only if no other index resolves the hash.
type lazyColdTxIndex struct {
	view   *ReadView
	cov    geometry.TxHashIndexCoverage
	reader *txhash.ColdReader
}

func (l *lazyColdTxIndex) Get(hash [32]byte) (uint32, error) {
	if l.reader == nil {
		r, err := txhash.OpenColdReader(l.view.catalog.Layout().TxHashIndexFilePath(l.cov))
		if err != nil {
			return 0, fmt.Errorf("query: open cold tx index [%s, %s]: %w", l.cov.Lo, l.cov.Hi, err)
		}
		l.view.closers = append(l.view.closers, r.Close)
		l.reader = r
	}
	return l.reader.Get(hash)
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
