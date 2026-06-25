package streaming

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
)

// Catalog is the streaming daemon's view of durable state. It WRAPS
// metastore.Store — the merged RocksDB KV store with sync Put/Delete, atomic
// Batch, and PrefixScan — never reaching around it to RocksDB directly. On top
// it adds: the key schema and its bijection to disk paths (keys.go, paths.go),
// tx-hash-index arithmetic (window.go), the one-write protocol
// (catalog_protocol.go), and the key-driven sweeps (catalog_sweep.go).
//
// Every key names a file/dir state or a config pin; progress is derived, never
// stored.
type Catalog struct {
	store       *metastore.Store
	layout      Layout
	txhashIndex TxHashIndexLayout

	// hooks are test-only fault-injection points (see hooks.go); nil in
	// production.
	hooks crashHooks
}

// NewCatalog binds a catalog to an open metastore.Store, the on-disk layout,
// and the tx-hash-index arithmetic. The store is caller-owned; the catalog never
// closes it.
func NewCatalog(store *metastore.Store, layout Layout, txhashIndex TxHashIndexLayout) *Catalog {
	return &Catalog{store: store, layout: layout, txhashIndex: txhashIndex}
}

func (c *Catalog) Layout() Layout { return c.layout }

func (c *Catalog) TxHashIndexLayout() TxHashIndexLayout { return c.txhashIndex }

// ---------------------------------------------------------------------------
// Raw key access — the value-blind primitives the rest build on.
// ---------------------------------------------------------------------------

// get returns the value at key. The bool is false (err nil) on a clean miss,
// distinguishing "absent" from a backing-store error.
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

// ---------------------------------------------------------------------------
// Typed artifact-state accessors.
// ---------------------------------------------------------------------------

// State returns the lifecycle State of a per-chunk artifact key, or the empty
// State when the key is absent — neither file nor in-progress write exists.
func (c *Catalog) State(chunkID chunk.ID, kind Kind) (State, error) {
	v, ok, err := c.get(chunkKey(chunkID, kind))
	if err != nil || !ok {
		return "", err
	}
	return State(v), nil
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

// TxHashIndexKeys returns index w's coverage keys with their State, sorted by key —
// the frozen one plus transient debris.
func (c *Catalog) TxHashIndexKeys(w TxHashIndexID) ([]TxHashIndexCoverage, error) {
	return c.txhashIndexKeysByPrefix(txhashIndexPrefixFor(w))
}

// AllTxHashIndexKeys is TxHashIndexKeys across all indexes.
func (c *Catalog) AllTxHashIndexKeys() ([]TxHashIndexCoverage, error) {
	return c.txhashIndexKeysByPrefix(txhashIndexPrefix)
}

// FrozenTxHashIndex returns the index's UNIQUE "frozen" coverage — the key
// readers resolve as "the index" — or ok=false if the index has none
// yet. It asserts INV-2 (at most one frozen coverage per index at any moment)
// by erroring if it observes two — a detectable bug, not a tie-break to resolve.
func (c *Catalog) FrozenTxHashIndex(w TxHashIndexID) (TxHashIndexCoverage, bool, error) {
	covs, err := c.TxHashIndexKeys(w)
	if err != nil {
		return TxHashIndexCoverage{}, false, err
	}
	var (
		frozen TxHashIndexCoverage
		found  bool
	)
	for _, candidate := range covs {
		if candidate.State != StateFrozen {
			continue
		}
		if found {
			return TxHashIndexCoverage{}, false, fmt.Errorf(
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
	return c.uint32Pin(configEarliestLedger)
}

// ChunksPerTxhashIndex returns the pinned config:chunks_per_txhash_index.
func (c *Catalog) ChunksPerTxhashIndex() (uint32, bool, error) {
	return c.uint32Pin(configChunksPerTxhashIdx)
}

// PinLayout commits BOTH layout pins (config:chunks_per_txhash_index and
// config:earliest_ledger) in ONE atomic synced batch — the first-start commit
// validateConfig mandates. The shared batch holds the all-or-nothing invariant:
// BOTH present ⟹ a prior first start completed and the layout is immutable;
// otherwise startup never cleared config validation, so re-validating +
// re-pinning is safe. A torn write pinning only one would break that.
func (c *Catalog) PinLayout(chunksPerTxhashIndex, earliestLedger uint32) error {
	return c.store.Batch(func(w *metastore.BatchWriter) error {
		w.Put(configChunksPerTxhashIdx, strconv.FormatUint(uint64(chunksPerTxhashIndex), 10))
		w.Put(configEarliestLedger, strconv.FormatUint(uint64(earliestLedger), 10))
		return nil
	})
}

// ArtifactRef names one per-chunk artifact and the State observed for it — the
// (chunk, kind, State) unit the sweeps and resolver pass around.
type ArtifactRef struct {
	Chunk chunk.ID
	Kind  Kind
	State State
}

func (r ArtifactRef) Key() string { return chunkKey(r.Chunk, r.Kind) }

// ---------------------------------------------------------------------------
// Unexported helpers backing the scans and pin getters above.
// ---------------------------------------------------------------------------

// txhashIndexKeysByPrefix scans coverage keys under prefix, attaching each scanned
// value as State.
func (c *Catalog) txhashIndexKeysByPrefix(prefix string) ([]TxHashIndexCoverage, error) {
	var covs []TxHashIndexCoverage
	for e, err := range c.store.PrefixScan(prefix) {
		if err != nil {
			return nil, err
		}
		cov, ok := parseTxHashIndexKey(e.Key)
		if !ok {
			return nil, fmt.Errorf("streaming: malformed index key %q", e.Key)
		}
		cov.State = State(e.Value)
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
