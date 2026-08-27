package query

import (
	"cmp"
	"fmt"
	"maps"
	"slices"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
)

// The by-hash lookup groundwork. A transaction hash does not identify its chunk,
// so routing cannot resolve it directly; instead the getTransaction path probes
// the hot transaction indexes (a match is definitive) and then the frozen window
// indexes (a match is a candidate, verified against the full hash). These two
// methods supply what that path needs from the read view — the hot indexes, the
// (lazily opened) cold window indexes, each wrapped in the servable-window gate
// (see windowGatedIndex) — leaving the probe order and candidate verification
// to the lookup itself. With the routed ledger read the view already offers
// (WithLedger, in resolve.go), that makes the read view the probe's whole
// ledger-side dependency, so nothing has to wrap it to hand it over.

// HotTxHashIndexes returns the transaction hash index of every published hot chunk,
// newest chunk first. A hot match is exact and definitive, so the newest indexes
// are probed first. Every index is window-gated (see windowGatedIndex): the handle
// set can include chunks below the view's floor, and a match there must read as a
// miss. The returned indexes are registry-owned handles; the caller does not close
// them.
func (a *ReadView) HotTxHashIndexes() []txhash.HashIndex {
	ids := slices.Sorted(maps.Keys(a.handles.byChunk))
	slices.Reverse(ids) // newest first

	idxs := make([]txhash.HashIndex, 0, len(ids))
	for _, c := range ids {
		idxs = append(idxs, &windowGatedIndex{inner: a.handles.byChunk[c].Txhash(), view: a})
	}
	return idxs
}

// ColdTxIndexes returns one reader per frozen window index in the view's
// snapshot, newest coverage first — a cold match is a fingerprinted candidate,
// so the lookup verifies it against the full hash. Every index is window-gated
// (see windowGatedIndex). Each .idx file is opened on its first Get, not here:
// the common case — a recent transaction resolved by the hot indexes — must not
// pay one file open per frozen window. Opened readers are view-owned: Release
// closes them.
func (a *ReadView) ColdTxIndexes() ([]txhash.HashIndex, error) {
	covs, err := a.coldTxHashIndexCoverages()
	if err != nil {
		return nil, err
	}
	idxs := make([]txhash.HashIndex, 0, len(covs))
	for _, cov := range covs {
		idxs = append(idxs, &windowGatedIndex{inner: &lazyColdTxIndex{view: a, cov: cov}, view: a})
	}
	return idxs, nil
}

// windowGatedIndex wraps a tx-hash index — hot or cold — so a hit outside the
// view's servable window [OldestLedger, LatestLedger] reads as a plain miss.
// Gating BEFORE the probe fetches the candidate's ledger matters because both
// tiers can name a ledger whose files are already gone, and an ungated probe
// turns that failed fetch into an error where the truthful answer is
// not-found:
//
//   - Cold: a frozen tx-hash index covers 1000 chunks and is deleted only when
//     its WHOLE window falls below the retention floor, but each chunk's
//     ledger files are deleted as soon as that one chunk falls below the
//     floor. So for most of its life a frozen index names transactions whose
//     ledgers no longer exist. Example: the index covers chunks 0–999 and the
//     floor sits at chunk 500; a lookup for a transaction in chunk 3 still
//     answers with chunk 3's ledger, whose files are gone — ungated, the
//     client gets an internal error ("lookup incomplete") where v1 answers
//     NOT_FOUND.
//   - Hot: the view's handle set loads before its catalog snapshot, so a
//     request racing a prune can hold the handle of a chunk the snapshot
//     already retired. The hot index hits, the ledger read fails as
//     ErrUnavailable, and — hot indexes being exact — the probe treats that
//     as hard: the request fails for a transaction that is simply pruned.
//     Gated, the below-floor hit is the miss it truly is, and the next view's
//     coherent state is never even needed. This gate is also what makes an
//     in-window ErrUnavailable a violated invariant worth counting (see
//     ErrUnavailable).
//
// Skipping a candidate here is safe in both tiers: even a fully verified
// out-of-window match must be answered not-found — retention is the
// observable behavior, not handle or file lifecycle.
type windowGatedIndex struct {
	inner txhash.HashIndex
	view  *ReadView
}

func (g *windowGatedIndex) Get(hash [32]byte) (uint32, error) {
	seq, err := g.inner.Get(hash)
	if err != nil {
		return 0, err
	}
	if seq < g.view.OldestLedger() || seq > g.view.LatestLedger() {
		return 0, stores.ErrNotFound
	}
	return seq, nil
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
