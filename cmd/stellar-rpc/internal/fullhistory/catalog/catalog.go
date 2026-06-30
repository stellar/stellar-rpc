package catalog

import (
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
)

// Catalog is the streaming daemon's view of durable state. It WRAPS
// metastore.Store — the merged RocksDB KV store with sync Put/Delete, atomic
// Batch, and PrefixScan — never reaching around it to RocksDB directly. On top
// of the geometry package (the key schema + its bijection to disk paths, the
// tx-hash-index arithmetic, and the fsync helpers) it adds the one-write
// protocol (catalog_protocol.go) and the key-driven sweeps (catalog_sweep.go).
//
// Every key names a file/dir state or a config pin; progress is derived, never
// stored.
//
// The read-then-act sequences in the write protocol and the sweeps carry no
// concurrency guard: the design's Concurrency model guarantees one writer per
// key (see the header note in catalog_protocol.go).
type Catalog struct {
	store       *metastore.Store
	layout      geometry.Layout
	txhashIndex geometry.TxHashIndexLayout
}

// NewCatalog binds a catalog to an open metastore.Store, the on-disk layout,
// and the tx-hash-index arithmetic. The store is caller-owned; the catalog never
// closes it.
func NewCatalog(store *metastore.Store, layout geometry.Layout, txhashIndex geometry.TxHashIndexLayout) *Catalog {
	return &Catalog{store: store, layout: layout, txhashIndex: txhashIndex}
}

func (c *Catalog) Layout() geometry.Layout { return c.layout }

func (c *Catalog) TxHashIndexLayout() geometry.TxHashIndexLayout { return c.txhashIndex }

// ---------------------------------------------------------------------------
// Typed artifact-state accessors.
// ---------------------------------------------------------------------------

// State returns the lifecycle State of a per-chunk artifact key, or the empty
// State when the key is absent — neither file nor in-progress write exists.
func (c *Catalog) State(chunkID chunk.ID, kind geometry.Kind) (geometry.State, error) {
	v, ok, err := c.get(geometry.ChunkKey(chunkID, kind))
	if err != nil || !ok {
		return "", err
	}
	return geometry.State(v), nil
}

// HotState returns the HotState of a chunk's hot-DB key, or empty (key absent).
// The key's mere existence (any value) marks the chunk as owned by ingestion;
// only the watermark derivation cares which value (see ReadyHotChunkKeys).
func (c *Catalog) HotState(chunkID chunk.ID) (geometry.HotState, error) {
	v, ok, err := c.get(geometry.HotChunkKey(chunkID))
	if err != nil || !ok {
		return "", err
	}
	return geometry.HotState(v), nil
}

// ---------------------------------------------------------------------------
// Scans. Every "find work" operation iterates keys via PrefixScan; nothing
// lists a directory. Results are returned sorted so callers need no second
// pass.
// ---------------------------------------------------------------------------

// ChunkArtifactKeys returns every per-chunk artifact key with its value, sorted
// by key — the deletion/audit surface for chunk:* keys.
func (c *Catalog) ChunkArtifactKeys() ([]ArtifactRef, error) {
	var refs []ArtifactRef
	for e, err := range c.store.PrefixScan(geometry.ChunkPrefix) {
		if err != nil {
			return nil, err
		}
		id, kind, ok := geometry.ParseChunkKey(e.Key)
		if !ok {
			return nil, fmt.Errorf("streaming: malformed chunk key %q", e.Key)
		}
		refs = append(refs, ArtifactRef{Chunk: id, Kind: kind, State: geometry.State(e.Value)})
	}
	return refs, nil
}

// TxHashIndexKeys returns index w's coverage keys with their State, sorted by key —
// the frozen one plus transient debris.
func (c *Catalog) TxHashIndexKeys(w geometry.TxHashIndexID) ([]geometry.TxHashIndexCoverage, error) {
	return c.txhashIndexKeysByPrefix(geometry.TxHashIndexPrefixFor(w))
}

// HotChunkKeys returns every hot-DB chunk id (value-blind), sorted ascending.
// The highest is the live chunk — the ingestion/lifecycle partition boundary.
func (c *Catalog) HotChunkKeys() ([]chunk.ID, error) {
	return c.hotChunkKeysWith(nil)
}

// ReadyHotChunkKeys returns only the chunks whose hot-DB key is "ready", sorted
// ascending. The watermark counts only these — a "transient" key never advances
// the bound, which lets recovery demote any hot key without disturbing it.
func (c *Catalog) ReadyHotChunkKeys() ([]chunk.ID, error) {
	return c.hotChunkKeysWith(func(s geometry.HotState) bool { return s == geometry.HotReady })
}

// AllTxHashIndexKeys is TxHashIndexKeys across all indexes.
func (c *Catalog) AllTxHashIndexKeys() ([]geometry.TxHashIndexCoverage, error) {
	return c.txhashIndexKeysByPrefix(geometry.TxHashIndexPrefix)
}

// FrozenTxHashIndex returns the index's UNIQUE "frozen" coverage — the key
// readers resolve as "the index" — or ok=false if the index has none
// yet. It asserts INV-2 (at most one frozen coverage per index at any moment)
// by erroring if it observes two — a detectable bug, not a tie-break to resolve.
func (c *Catalog) FrozenTxHashIndex(w geometry.TxHashIndexID) (geometry.TxHashIndexCoverage, bool, error) {
	covs, err := c.TxHashIndexKeys(w)
	if err != nil {
		return geometry.TxHashIndexCoverage{}, false, err
	}
	var (
		frozen geometry.TxHashIndexCoverage
		found  bool
	)
	for _, candidate := range covs {
		if candidate.State != geometry.StateFrozen {
			continue
		}
		if found {
			return geometry.TxHashIndexCoverage{}, false, fmt.Errorf(
				"streaming: index %s has two frozen coverages (%s and %s) — "+
					"uniqueness invariant violated",
				w, frozen.Key, candidate.Key,
			)
		}
		frozen, found = candidate, true
	}
	return frozen, found, nil
}

// ---------------------------------------------------------------------------
// Config pins. Written once on first start, immutable thereafter.
// ---------------------------------------------------------------------------

// EarliestLedger returns the pinned config:earliest_ledger (chunk-aligned). ok
// is false if the pin has not been written yet (a pristine store).
func (c *Catalog) EarliestLedger() (uint32, bool, error) {
	return c.uint32Pin(geometry.ConfigEarliestLedger)
}

// PinEarliestLedger commits the config:earliest_ledger pin in one synced write —
// the first-start commit validateConfig mandates. Its presence is the sentinel
// that a prior first start completed: once written, earliest_ledger is immutable
// and validated-or-abort on every restart. chunks_per_txhash_index is no longer
// pinned — it is the fixed geometry.ChunksPerTxhashIndex constant.
func (c *Catalog) PinEarliestLedger(earliestLedger uint32) error {
	return c.store.Put(geometry.ConfigEarliestLedger, strconv.FormatUint(uint64(earliestLedger), 10))
}

// ArtifactRef names one per-chunk artifact and the State observed for it — the
// (chunk, kind, State) unit the sweeps and resolver pass around.
type ArtifactRef struct {
	Chunk chunk.ID
	Kind  geometry.Kind
	State geometry.State
}

func (r ArtifactRef) Key() string { return geometry.ChunkKey(r.Chunk, r.Kind) }

// ---------------------------------------------------------------------------
// Unexported helpers backing the scans and pin getters above.
// ---------------------------------------------------------------------------

// get returns the value at key. The bool is false (err nil) on a clean miss,
// distinguishing "absent" from a backing-store error — the value-blind primitive
// the typed reads above build on.
func (c *Catalog) get(key string) (string, bool, error) {
	v, err := c.store.Get(key)
	if errors.Is(err, stores.ErrNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return v, true, nil
}

// has reports whether key exists.
func (c *Catalog) has(key string) (bool, error) {
	_, ok, err := c.get(key)
	return ok, err
}

// hotChunkKeysWith returns the chunks whose hot-DB key matches keep, sorted
// ascending. A nil keep matches every value (value-blind).
func (c *Catalog) hotChunkKeysWith(keep func(geometry.HotState) bool) ([]chunk.ID, error) {
	var ids []chunk.ID
	for e, err := range c.store.PrefixScan(geometry.HotChunkPrefix) {
		if err != nil {
			return nil, err
		}
		id, ok := geometry.ParseHotChunkKey(e.Key)
		if !ok {
			return nil, fmt.Errorf("malformed hot key %q", e.Key)
		}
		if keep == nil || keep(geometry.HotState(e.Value)) {
			ids = append(ids, id)
		}
	}
	// PrefixScan yields byte-lex order == numeric under the 8-digit padding, so
	// the slice is already ascending; sort defensively against a width change.
	slices.Sort(ids)
	return ids, nil
}

// txhashIndexKeysByPrefix scans coverage keys under prefix, attaching each scanned
// value as State.
func (c *Catalog) txhashIndexKeysByPrefix(prefix string) ([]geometry.TxHashIndexCoverage, error) {
	var covs []geometry.TxHashIndexCoverage
	for e, err := range c.store.PrefixScan(prefix) {
		if err != nil {
			return nil, err
		}
		cov, ok := geometry.ParseTxHashIndexKey(e.Key)
		if !ok {
			return nil, fmt.Errorf("streaming: malformed index key %q", e.Key)
		}
		cov.State = geometry.State(e.Value)
		covs = append(covs, cov)
	}
	return covs, nil
}

func (c *Catalog) uint32Pin(key string) (uint32, bool, error) {
	v, ok, err := c.get(key)
	if err != nil || !ok {
		return 0, false, err
	}
	n, parseErr := strconv.ParseUint(v, 10, 32)
	if parseErr != nil {
		return 0, false, fmt.Errorf("streaming: config pin %q is not a uint32: %q", key, v)
	}
	return uint32(n), true, nil
}
