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

// Catalog is the streaming daemon's view of durable state: a wrapper over
// metastore.Store (sync Put/Delete, atomic Batch, PrefixScan) that never
// reaches around it to RocksDB directly. It adds the key schema and its path
// bijection (keys.go, paths.go), the one-write protocol (catalog_protocol.go),
// and the key-driven sweeps (catalog_sweep.go).
//
// It stays a *pure* catalog: every key names a file/dir state or a config pin,
// and progress is always derived, never stored.
type Catalog struct {
	store  *metastore.Store
	layout Layout

	// hooks are test-only fault-injection points (see hooks.go); every field
	// is nil in production, making each call site a no-op nil-check.
	hooks crashHooks
}

// NewCatalog binds a catalog to an open metastore.Store and the on-disk layout.
// The store is owned by the caller (the catalog does not close it) so a single
// Store can back both the catalog and any other consumer in the process.
func NewCatalog(store *metastore.Store, layout Layout) *Catalog {
	return &Catalog{store: store, layout: layout}
}

// Layout returns the path layout bound to this catalog.
func (c *Catalog) Layout() Layout { return c.layout }

// Raw key access — Get/Has are the value-blind primitives the rest build on.

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

// Typed artifact-state accessors.

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

// Scans. Every "find work" operation iterates keys via PrefixScan; results
// are sorted so callers (maxChunk, uniqueness checks) need no second pass.

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

// Config pins. Written once on first start, immutable thereafter.

// EarliestLedger returns the pinned config:earliest_ledger (chunk-aligned).
// ok is false if the pin has not been written yet (a pristine store).
func (c *Catalog) EarliestLedger() (uint32, bool, error) {
	return c.uint32Pin(configEarliestLedger)
}

// PutEarliestLedger writes the config:earliest_ledger pin (decimal string).
// The immutability check (abort if a later value differs) is the caller's
// validateConfig responsibility, not the catalog's.
func (c *Catalog) PutEarliestLedger(ledger uint32) error {
	return c.store.Put(configEarliestLedger, strconv.FormatUint(uint64(ledger), 10))
}

// PinLayout commits the layout pin (config:earliest_ledger) in one atomic
// synced batch — the first-start commit validateConfig mandates. Its presence
// ⟹ a prior first start completed and the layout is immutable.
func (c *Catalog) PinLayout(earliestLedger uint32) error {
	return c.store.Batch(func(w *metastore.BatchWriter) error {
		w.Put(configEarliestLedger, strconv.FormatUint(uint64(earliestLedger), 10))
		return nil
	})
}

// ArtifactRef — a (chunk, kind) handle with its observed State, the unit the
// sweeps and resolver pass around.

// ArtifactRef names one per-chunk artifact and the State observed for it.
type ArtifactRef struct {
	Chunk chunk.ID
	Kind  Kind
	State State
}

// Key returns the meta-store key for this ref.
func (r ArtifactRef) Key() string { return chunkKey(r.Chunk, r.Kind) }

// Unexported helpers backing the scans and pin getters above.

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
	// lex == numeric, so this is already ascending. Sort defensively against a
	// future key-width change.
	slices.Sort(ids)
	return ids, nil
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
