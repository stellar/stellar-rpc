package streaming

import (
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
)

// Catalog is the streaming daemon's view of durable state. It WRAPS
// metastore.Store — the merged RocksDB KV store with sync Put/Delete, atomic
// Batch, and PrefixScan — and never reaches around it to RocksDB directly. The
// catalog adds: the key schema and its bijection to disk paths (keys.go,
// paths.go), window arithmetic (window.go), the one-write protocol
// (protocol.go), and the key-driven sweeps (sweep.go).
//
// Every method here is a pure function of meta-store keys plus the on-disk
// layout. The catalog stays a *pure* catalog — every key names a file/dir
// state or a config pin; progress is derived, never stored (see the data
// model's "Progress is derived, never stored").
type Catalog struct {
	store   *metastore.Store
	layout  Layout
	windows Windows

	// hooks are test-only fault-injection points (see hooks.go); every field
	// is nil in production, making each call site a no-op nil-check.
	hooks crashHooks
}

// NewCatalog binds a catalog to an open metastore.Store, the on-disk layout,
// and the window arithmetic. The store is owned by the caller (the catalog
// does not close it) so a single Store can back both the catalog and any other
// consumer in the process.
func NewCatalog(store *metastore.Store, layout Layout, windows Windows) *Catalog {
	return &Catalog{store: store, layout: layout, windows: windows}
}

// Layout returns the path layout bound to this catalog.
func (c *Catalog) Layout() Layout { return c.layout }

// Windows returns the window arithmetic bound to this catalog.
func (c *Catalog) Windows() Windows { return c.windows }

// ---------------------------------------------------------------------------
// Raw key access. Get/Has are the value-blind primitives the rest build on.
// ---------------------------------------------------------------------------

// Get returns the value at key. The bool is false (and err nil) on a clean
// miss, distinguishing "absent" from a real backing-store error.
func (c *Catalog) Get(key string) (string, bool, error) {
	v, err := c.store.Get(key)
	if errors.Is(err, stores.ErrNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return v, true, nil
}

// Has reports whether key exists (value-blind).
func (c *Catalog) Has(key string) (bool, error) {
	_, ok, err := c.Get(key)
	return ok, err
}

// ---------------------------------------------------------------------------
// Typed artifact-state accessors.
// ---------------------------------------------------------------------------

// State returns the lifecycle State of a per-chunk artifact key, or the empty
// State (key absent). Empty State means neither file nor in-progress write
// exists — the absent case in the per-chunk lifecycle.
func (c *Catalog) State(chunkID chunk.ID, kind Kind) (State, error) {
	v, ok, err := c.Get(chunkKey(chunkID, kind))
	if err != nil || !ok {
		return "", err
	}
	return State(v), nil
}

// HotState returns the HotState of a chunk's hot-DB key, or the empty HotState
// (key absent). The value-blind existence of the key — any value — marks the
// chunk as owned by ingestion (the live-chunk partition); only the watermark
// derivation cares which value (see readyHotChunkKeys).
func (c *Catalog) HotState(chunkID chunk.ID) (HotState, error) {
	v, ok, err := c.Get(hotChunkKey(chunkID))
	if err != nil || !ok {
		return "", err
	}
	return HotState(v), nil
}

// ---------------------------------------------------------------------------
// Scans. Every "find work" operation iterates keys via PrefixScan; nothing
// lists a directory. Results are returned sorted so callers (maxChunk,
// uniqueness checks) need no second pass.
// ---------------------------------------------------------------------------

// ChunkArtifactKeys returns every per-chunk artifact key (all kinds, all
// chunks) with its value, sorted by key. This is the deletion/audit surface
// for chunk:* keys.
func (c *Catalog) ChunkArtifactKeys() ([]ArtifactRef, error) {
	var refs []ArtifactRef
	for e, err := range c.store.PrefixScan(chunkPrefix) {
		if err != nil {
			return nil, err
		}
		id, kind, ok := parseChunkKey(e.Key)
		if !ok {
			return nil, fmt.Errorf("streaming: malformed chunk key %q", e.Key)
		}
		refs = append(refs, ArtifactRef{Chunk: id, Kind: kind, State: State(e.Value)})
	}
	return refs, nil
}

// HotChunkKeys returns every hot-DB chunk id (value-blind), sorted ascending.
// The highest is the live chunk — the ingestion/lifecycle partition boundary.
func (c *Catalog) HotChunkKeys() ([]chunk.ID, error) {
	return c.hotChunkKeysWith(nil)
}

// ReadyHotChunkKeys returns only the chunks whose hot-DB key is "ready",
// sorted ascending. The watermark derivation counts only these — a "transient"
// key never advances the bound on its own, which is what lets recovery demote
// any hot key without disturbing the watermark.
func (c *Catalog) ReadyHotChunkKeys() ([]chunk.ID, error) {
	return c.hotChunkKeysWith(func(s HotState) bool { return s == HotReady })
}

// IndexKeys returns every coverage key under window w with its State, sorted by
// key. Used to enumerate a window's coverages (the frozen one plus transient
// debris).
func (c *Catalog) IndexKeys(w WindowID) ([]IndexCoverage, error) {
	return c.indexKeysPrefix(indexWindowPrefix(w))
}

// AllIndexKeys returns every coverage key across all windows with its State,
// sorted by key.
func (c *Catalog) AllIndexKeys() ([]IndexCoverage, error) {
	return c.indexKeysPrefix(indexPrefix)
}

// FrozenCoverage returns the window's UNIQUE "frozen" coverage, or ok=false if
// the window has none yet. It asserts the uniqueness invariant — at most one
// coverage per window is "frozen" at any moment (INV-2) — by erroring if it
// observes two. More than one frozen key in a window is a detectable bug, not
// a tie-break to resolve: readers resolve "the window's index" as exactly this
// key.
func (c *Catalog) FrozenCoverage(w WindowID) (IndexCoverage, bool, error) {
	covs, err := c.IndexKeys(w)
	if err != nil {
		return IndexCoverage{}, false, err
	}
	var (
		frozen IndexCoverage
		found  bool
	)
	for _, candidate := range covs {
		if candidate.State != StateFrozen {
			continue
		}
		if found {
			return IndexCoverage{}, false, fmt.Errorf(
				"streaming: window %s has two frozen coverages (%s and %s) — "+
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

// EarliestLedger returns the pinned config:earliest_ledger (chunk-aligned).
// ok is false if the pin has not been written yet (a pristine store).
func (c *Catalog) EarliestLedger() (uint32, bool, error) {
	return c.uint32Pin(configEarliestLedger)
}

// ChunksPerTxhashIndex returns the pinned config:chunks_per_txhash_index. ok
// is false if the pin has not been written yet.
func (c *Catalog) ChunksPerTxhashIndex() (uint32, bool, error) {
	return c.uint32Pin(configChunksPerTxhashIdx)
}

// PutEarliestLedger writes the config:earliest_ledger pin (decimal string).
// The immutability check (abort if a later value differs) is the caller's
// validateConfig responsibility, not the catalog's.
func (c *Catalog) PutEarliestLedger(ledger uint32) error {
	return c.store.Put(configEarliestLedger, strconv.FormatUint(uint64(ledger), 10))
}

// PutChunksPerTxhashIndex writes the config:chunks_per_txhash_index pin.
func (c *Catalog) PutChunksPerTxhashIndex(n uint32) error {
	return c.store.Put(configChunksPerTxhashIdx, strconv.FormatUint(uint64(n), 10))
}

// ---------------------------------------------------------------------------
// ArtifactRef — a (chunk, kind) handle with its observed State. The unit the
// sweeps and resolver pass around.
// ---------------------------------------------------------------------------

// ArtifactRef names one per-chunk artifact and the State observed for it.
type ArtifactRef struct {
	Chunk chunk.ID
	Kind  Kind
	State State
}

// Key returns the meta-store key for this ref.
func (r ArtifactRef) Key() string { return chunkKey(r.Chunk, r.Kind) }

// ---------------------------------------------------------------------------
// Unexported helpers backing the scans and pin getters above.
// ---------------------------------------------------------------------------

// hotChunkKeysWith returns the chunks whose hot-DB key matches keep, sorted
// ascending. A nil keep matches every value (value-blind).
func (c *Catalog) hotChunkKeysWith(keep func(HotState) bool) ([]chunk.ID, error) {
	var ids []chunk.ID
	for e, err := range c.store.PrefixScan(hotPrefix) {
		if err != nil {
			return nil, err
		}
		id, ok := parseHotChunkKey(e.Key)
		if !ok {
			return nil, fmt.Errorf("streaming: malformed hot key %q", e.Key)
		}
		if keep == nil || keep(HotState(e.Value)) {
			ids = append(ids, id)
		}
	}
	// PrefixScan yields byte-lex order; the 8-digit zero-padded ids make
	// lex == numeric, so the slice is already ascending. Sort defensively in
	// case the key width ever changes — cheap and keeps maxChunk honest.
	slices.Sort(ids)
	return ids, nil
}

// indexKeysPrefix scans coverage keys under prefix, parsing each name and
// attaching its scanned lifecycle value as State.
func (c *Catalog) indexKeysPrefix(prefix string) ([]IndexCoverage, error) {
	var covs []IndexCoverage
	for e, err := range c.store.PrefixScan(prefix) {
		if err != nil {
			return nil, err
		}
		cov, ok := parseIndexKey(e.Key)
		if !ok {
			return nil, fmt.Errorf("streaming: malformed index key %q", e.Key)
		}
		cov.State = State(e.Value)
		covs = append(covs, cov)
	}
	return covs, nil
}

// uint32Pin reads a config pin as a uint32 decimal string.
func (c *Catalog) uint32Pin(key string) (uint32, bool, error) {
	v, ok, err := c.Get(key)
	if err != nil || !ok {
		return 0, false, err
	}
	n, parseErr := strconv.ParseUint(v, 10, 32)
	if parseErr != nil {
		return 0, false, fmt.Errorf("streaming: config pin %q is not a uint32: %q", key, v)
	}
	return uint32(n), true, nil
}
