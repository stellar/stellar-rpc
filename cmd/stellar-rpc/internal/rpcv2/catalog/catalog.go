package catalog

import (
	"fmt"
	"slices"
	"strconv"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
)

// Catalog is the full-history daemon's view of durable state. It OWNS the
// RocksDB-backed string-KV store behind it (kv.go — sync Put/Delete, atomic
// Batch, prefix scans over the single default CF); no other package reaches
// that store. On top of the geometry package (the key schema + its bijection
// to disk paths and the tx-hash-index arithmetic) it adds the one-write
// protocol (catalog_protocol.go) and the key-driven sweeps (catalog_sweep.go).
//
// Every key names a file/dir state or a config pin; progress is derived, never
// stored.
//
// The read-then-act sequences in the write protocol and the sweeps carry no
// concurrency guard: the design's Concurrency model guarantees one writer per
// key (see the header note in catalog_protocol.go).
type Catalog struct {
	store       *rocksdb.Store
	logger      *supportlog.Entry
	layout      geometry.Layout
	txhashIndex geometry.TxHashIndexLayout
}

// Open opens the catalog's backing KV store at path (created if absent) and
// binds the catalog to it, the on-disk layout, and the tx-hash-index
// arithmetic. path and logger are required (rocksdb.New validates both). The
// catalog owns the store: Close releases it.
func Open(
	path string, layout geometry.Layout, txhashIndex geometry.TxHashIndexLayout, logger *supportlog.Entry,
) (*Catalog, error) {
	store, err := rocksdb.New(rocksdb.Config{Path: path, Logger: logger})
	if err != nil {
		return nil, err
	}
	return &Catalog{store: store, logger: logger, layout: layout, txhashIndex: txhashIndex}, nil
}

// Close releases the backing store. Idempotent.
func (c *Catalog) Close() error { return c.store.Close() }

// Logger returns the logger the catalog was opened with — the one derived
// reads (the last-committed refinement's read-only hot-DB open) reuse.
func (c *Catalog) Logger() *supportlog.Entry { return c.logger }

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
// The key's mere existence (any value) marks the chunk as owned by ingestion, and
// most consumers branch on the value: the freeze source and last-committed
// derivation treat only "ready" as usable (see ReadyHotChunkKeys), and
// openHotDBForChunk picks its recovery action from it. Only the discard scan is
// value-blind (any state means "a hot dir may exist, sweep it").
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
	for e, err := range c.prefixScan(geometry.ChunkPrefix) {
		if err != nil {
			return nil, err
		}
		id, kind, ok := geometry.ParseChunkKey(e.Key)
		if !ok {
			return nil, fmt.Errorf("malformed chunk key %q", e.Key)
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
// ascending. The last-committed ledger counts only these — a "transient" key never advances
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
				"index %s has two frozen coverages (%s and %s) — "+
					"uniqueness invariant violated",
				w, frozen.Key, candidate.Key,
			)
		}
		frozen, found = candidate, true
	}
	return frozen, found, nil
}

// FrozenIndexCoversRange reports whether index w's UNIQUE frozen coverage spans
// the whole inclusive [lo, hi] chunk range. It reads through FrozenTxHashIndex,
// so INV-2 (at most one frozen coverage per index) is asserted on every call.
// This is the single "covered by a frozen index" predicate the resolve diff
// (backfill), the discard eligibility scan, and the last-committed-ledger derivation
// all share, so they can never disagree about the same catalog snapshot. Reports
// false (no error) when the index has no frozen coverage yet.
func (c *Catalog) FrozenIndexCoversRange(w geometry.TxHashIndexID, lo, hi chunk.ID) (bool, error) {
	frozen, ok, err := c.FrozenTxHashIndex(w)
	if err != nil {
		return false, err
	}
	return ok && frozen.Lo <= lo && hi <= frozen.Hi, nil
}

// FrozenIndexCovers reports whether chunk ch's OWN index window has a frozen
// coverage containing it. A chunk belongs to exactly one window, so its own
// window is the only one that can cover it — the degenerate single-chunk case of
// FrozenIndexCoversRange.
func (c *Catalog) FrozenIndexCovers(ch chunk.ID) (bool, error) {
	return c.FrozenIndexCoversRange(c.txhashIndex.TxHashIndexID(ch), ch, ch)
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
// and validated-or-abort on every restart. (The tx-hash index width is not pinned;
// it is the fixed geometry.ChunksPerTxhashIndex constant.)
func (c *Catalog) PinEarliestLedger(earliestLedger uint32) error {
	return c.put(geometry.ConfigEarliestLedger, strconv.FormatUint(uint64(earliestLedger), 10))
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

// has reports whether key exists.
func (c *Catalog) has(key string) (bool, error) {
	_, ok, err := c.get(key)
	return ok, err
}

// hotChunkKeysWith returns the chunks whose hot-DB key matches keep, sorted
// ascending. A nil keep matches every value (value-blind).
func (c *Catalog) hotChunkKeysWith(keep func(geometry.HotState) bool) ([]chunk.ID, error) {
	var ids []chunk.ID
	for e, err := range c.prefixScan(geometry.HotChunkPrefix) {
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
	for e, err := range c.prefixScan(prefix) {
		if err != nil {
			return nil, err
		}
		cov, ok := geometry.ParseTxHashIndexKey(e.Key)
		if !ok {
			return nil, fmt.Errorf("malformed index key %q", e.Key)
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
		return 0, false, fmt.Errorf("config pin %q is not a uint32: %q", key, v)
	}
	return uint32(n), true, nil
}
