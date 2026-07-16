// Package registry implements the read-side coordination layer of the
// query-routing design (design-docs/query-routing-design.md): an in-memory
// serving map owned by a Registry, published to queries as immutable View
// snapshots, with a grace-period Reaper deferring the destruction of retired
// resources and per-kind LRU caches bounding open cold readers.
//
// A query calls Registry.Admit once, then resolves every chunk in its range
// against the admitted Snapshot for its whole lifetime:
//
//	snap := reg.Admit()                                // latest + one immutable View
//	h, err := reg.LedgerReaderFor(snap.View, chunkID)  // cold wins, else hot
//	raw, err := h.GetLedgerRaw(seq)
//
// The write side (ingestion + lifecycle) publishes
// serving-map changes through the hook methods (PublishHot, AdvanceLatest,
// PublishFrozen, SwapTxIndex, UnpublishHot, AdvanceFloor); each hook clones
// the current View, applies the change, and atomically republishes, handing
// removed resources to the Reaper.
package registry

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/txhash"
)

// ErrUnavailable is returned when a chunk has no serving store in the admitted
// View — neither a cold artifact flag nor a hot handle for the requested kind.
var ErrUnavailable = errors.New("registry: chunk has no serving store in this view")

// View is one immutable snapshot of the serving map. A query admits exactly
// one View (via Registry.Admit) and uses it for its entire lifetime; the
// Registry never mutates a published View — every update clones it first.
type View struct {
	floor   chunk.ID
	hot     map[chunk.ID]*hotchunk.DB // registry-owned shared handles (ingestion + queries)
	cold    map[chunk.ID]ColdChunk    // availability flags; readers come from the caches
	indexes []IndexCoverage           // ascending by Window; one open reader per in-retention window
}

// ColdChunk records which of a chunk's cold artifacts are frozen and servable.
// It holds flags, not readers: with thousands of cold chunks, reader objects
// are opened on demand through the Registry's LRU caches.
type ColdChunk struct {
	Ledgers bool
	Events  bool
}

// has reports whether kind k is cold-servable. KindTxHash is never
// chunk-served — the per-chunk .bin is an index-build input, and tx-hash
// lookups go through the window indexes instead.
func (cc ColdChunk) has(k geometry.Kind) bool {
	switch k {
	case geometry.KindLedgers:
		return cc.Ledgers
	case geometry.KindEvents:
		return cc.Events
	case geometry.KindTxHash:
		return false
	default:
		return false
	}
}

// IndexCoverage is one window's transaction-hash index: the coverage range the
// frozen .idx actually spans (Hi trails the tip while the window is current;
// Lo is the retention floor at build time) and its open reader.
type IndexCoverage struct {
	Window geometry.TxHashIndexID
	Lo, Hi chunk.ID
	Idx    *txhash.ColdReader
}

// Floor is the lowest chunk this View serves. Requests whose leading edge
// falls below it are rejected with the available range (R2).
func (v *View) Floor() chunk.ID { return v.floor }

// FloorLedger is the first ledger of the floor chunk — the low bound queries
// clamp against.
func (v *View) FloorLedger() uint32 { return v.floor.FirstLedger() }

// HotChunks returns the chunks with a hot handle in this View, ascending.
func (v *View) HotChunks() []chunk.ID {
	ids := slices.Collect(maps.Keys(v.hot))
	slices.Sort(ids)
	return ids
}

// HotDB returns chunk c's shared hot handle, if this View has one. The handle
// is registry-owned and always a read-write open, so its Events() facade is
// warmed and usable. Callers must never Close it.
func (v *View) HotDB(c chunk.ID) (*hotchunk.DB, bool) {
	db, ok := v.hot[c]
	return db, ok
}

// Indexes returns the View's window tx-hash index coverages, ascending by
// window. The slice is a copy; the readers inside are registry-owned — callers
// must never Close them.
func (v *View) Indexes() []IndexCoverage {
	return slices.Clone(v.indexes)
}

// clone returns a mutable copy for the next publish. Maps and the coverage
// slice are copied; the handles and readers they point at are shared.
func (v *View) clone() *View {
	return &View{
		floor:   v.floor,
		hot:     maps.Clone(v.hot),
		cold:    maps.Clone(v.cold),
		indexes: slices.Clone(v.indexes),
	}
}

// tier is resolve's answer: which store serves a (chunk, kind). Exactly one of
// cold / hot is set.
type tier struct {
	cold bool
	hot  *hotchunk.DB
}

// resolve picks chunk c's serving store for kind k. The order is
// deterministic: when both a cold artifact and a hot handle exist, cold wins.
// A chunk with no serving home returns ErrUnavailable (wrapped with the chunk
// and kind). The exported faces are Registry.LedgerReaderFor/EventReaderFor.
func (v *View) resolve(c chunk.ID, k geometry.Kind) (tier, error) {
	if v.cold[c].has(k) {
		return tier{cold: true}, nil
	}
	if db, ok := v.hot[c]; ok {
		return tier{hot: db}, nil
	}
	return tier{}, fmt.Errorf("%w: chunk %s, kind %s", ErrUnavailable, c, k)
}

// emptyView is the post-Close serving map: same floor, nothing servable.
func emptyView(floor chunk.ID) *View {
	return &View{
		floor: floor,
		hot:   map[chunk.ID]*hotchunk.DB{},
		cold:  map[chunk.ID]ColdChunk{},
	}
}
