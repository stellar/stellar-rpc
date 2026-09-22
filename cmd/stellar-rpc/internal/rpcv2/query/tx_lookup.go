package query

import (
	"cmp"
	"fmt"
	"maps"
	"slices"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
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
//
// Both methods take the request's ledger bounds [first, last], inclusive, as
// the caller received them (0 and MaxUint32 for an unbounded side), clamp them
// into the view's servable window, and return only the indexes that can hold a
// ledger in the clamped range, each gated to it. Bounds near the tip are the
// polling case: a just-submitted transaction looked up from the latest ledger
// of its sendTransaction response. The hot chunks cover such a range, so the
// cold tier is never enumerated or probed (see ColdTxIndexes).

// bounded clamps [first, last] into the view's servable window [OldestLedger,
// LatestLedger]. The bool is false when no ledger in the window lies within
// the bounds.
func (a *ReadView) bounded(first, last uint32) (uint32, uint32, bool) {
	lo, hi := max(first, a.OldestLedger()), min(last, a.LatestLedger())
	return lo, hi, lo <= hi
}

// HotTxHashIndexes returns the transaction hash index of every published hot
// chunk that meets the clamped bounds, newest chunk first. A hot match is exact
// and definitive, so the newest indexes are probed first. Every index is gated
// to the bounds (see windowGatedIndex): the handle set can include chunks below
// the view's floor, and a match there must read as a miss. The returned indexes
// are registry-owned handles; the caller does not close them.
func (a *ReadView) HotTxHashIndexes(first, last uint32) []txhash.HashIndex {
	lo, hi, ok := a.bounded(first, last)
	if !ok {
		return nil
	}
	ids := slices.Sorted(maps.Keys(a.handles.byChunk))
	slices.Reverse(ids) // newest first

	idxs := make([]txhash.HashIndex, 0, len(ids))
	for _, c := range ids {
		if c.LastLedger() < lo || c.FirstLedger() > hi {
			continue
		}
		idxs = append(idxs, &windowGatedIndex{inner: a.handles.byChunk[c].Txhash(), lo: lo, hi: hi})
	}
	return idxs
}

// hotChunksCover reports whether every ledger in [lo, hi] lies in a chunk with
// a published hot handle. The hot chunks run contiguously through the tip (the
// live chunk plus the frozen chunks no window index covers yet), and a chunk's
// hot index holds every transaction of every ledger committed to it, so a miss
// across the hot indexes of a covered range is final.
func (a *ReadView) hotChunksCover(lo, hi uint32) bool {
	last := chunk.IDFromLedger(hi)
	for c := chunk.IDFromLedger(lo); c <= last; c++ {
		if _, ok := a.handles.byChunk[c]; !ok {
			return false
		}
	}
	return true
}

// ColdTxIndexes returns one reader per frozen window index in the view's
// snapshot whose coverage meets the clamped bounds, newest coverage first — a
// cold match is a fingerprinted candidate, so the lookup verifies it against
// the full hash. Every index is gated to the bounds (see windowGatedIndex).
// Each .idx file is opened on its first Get, not here: the common case — a
// recent transaction resolved by the hot indexes — must not pay one file open
// per frozen window. Opened readers are view-owned: Release closes them.
//
// When the published hot chunks cover the whole clamped range, the cold tier
// cannot hold a ledger in it, so the coverages are not even enumerated (a
// catalog scan): a hot miss is then the final answer.
func (a *ReadView) ColdTxIndexes(first, last uint32) ([]txhash.HashIndex, error) {
	lo, hi, ok := a.bounded(first, last)
	if !ok || a.hotChunksCover(lo, hi) {
		return nil, nil
	}
	covs, err := a.coldTxHashIndexCoverages()
	if err != nil {
		return nil, err
	}
	idxs := make([]txhash.HashIndex, 0, len(covs))
	for _, cov := range covs {
		if cov.Hi.LastLedger() < lo || cov.Lo.FirstLedger() > hi {
			continue
		}
		idxs = append(idxs, &windowGatedIndex{inner: &lazyColdTxIndex{view: a, cov: cov}, lo: lo, hi: hi})
	}
	return idxs, nil
}

// windowGatedIndex wraps a tx-hash index — hot or cold — so a hit outside
// [lo, hi] reads as a plain miss. [lo, hi] is the request's bounds clamped into
// the view's servable window [OldestLedger, LatestLedger], so the gate enforces
// retention and the admitted latest as well as the caller's bounds.
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
	inner  txhash.HashIndex
	lo, hi uint32
}

func (g *windowGatedIndex) Get(hash [32]byte) (uint32, error) {
	seq, err := g.inner.Get(hash)
	if err != nil {
		return 0, err
	}
	if seq < g.lo || seq > g.hi {
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
