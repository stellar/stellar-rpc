package rocksdb

import (
	"fmt"
	"sync"
	"sync/atomic"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// Schema keys — single source of truth for what the metastore stores
// on disk.
// Format-string constants for the per-thing flag families; literal
// constants for the singletons.
const (
	keyChunkLFSFormat       = "chunk:%08d:lfs"
	keyChunkTxHashRawFormat = "chunk:%08d:txhash"
	keyChunkEventsFormat    = "chunk:%08d:events"
	keyIndexTxHashFormat    = "index:%08d:txhash"

	keyConfigLedgersPerTxIndex      = "config:ledgers_per_tx_index"
	keyStreamingLastCommittedLedger = "streaming:last_committed_ledger"
)

// MetaStore is the concrete RocksDB-backed implementation of
// stores.MetaStore.
//
// Wraps a Layer-1 *Store with the default column family only
// (the metastore is a flat key/value space).
// All key derivation, value encoding, and CF naming is unexported;
// the stores.MetaStore interface stays clean of any RocksDB-specific
// shape.
//
// Two-phase lifecycle: NewMetaStore validates inputs and returns a
// constructed instance; (*MetaStore).Open brings the backing RocksDB
// up; (*MetaStore).Close drains the active memtable and tears the
// underlying store down.
// Open and Close are both idempotent at the facade level
// (sync.Once + atomic.Bool) in addition to the Layer-1 wrapper's own
// idempotency — keeps every Layer-2 facade's lifecycle shape
// identical.
type MetaStore struct {
	log   *supportlog.Entry
	store *Store

	openOnce sync.Once
	openErr  error
	closed   atomic.Bool
}

// NewMetaStore validates the inputs and returns a MetaStore ready
// to be Opened.
//
// path is the on-disk directory the metastore occupies (created,
// with parents, on Open if missing).
// logger receives the wrapper's on-open state line plus any
// close-time diagnostics.
// Both are required.
//
// Tuning is hardcoded inside this constructor for the metastore
// workload: a very small store (single-digit MB typical), high
// durability (each entry is a witness to a fsynced upstream
// artifact), no bloom filter (the keyspace is small enough that a
// bloom would waste RAM).
func NewMetaStore(path string, logger *supportlog.Entry) (*MetaStore, error) {
	if path == "" {
		return nil, ErrInvalidConfig
	}
	if logger == nil {
		return nil, ErrInvalidConfig
	}
	store, err := New(Config{
		Path:   path,
		Logger: logger,
		Tuning: metaStoreTuning(),
		// Default-CF only; no ColumnFamilies entry.
	})
	if err != nil {
		return nil, err
	}
	return &MetaStore{log: logger, store: store}, nil
}

// metaStoreTuning returns the hardcoded RocksDB knobs for this
// facade's workload.
//
// Sizes chosen for a small (single-digit MB total) store with
// frequent small writes: tiny memtable, modest L0 triggers, no
// bloom filter (the working set is small enough to live in the
// block cache).
func metaStoreTuning() Tuning {
	return Tuning{
		// Write path — tiny memtable; the metastore's working set is
		// in the low tens of MB even on a multi-million-chunk run.
		WriteBufferMB:        4,
		MaxWriteBufferNumber: 2,

		// L0 / compaction shape — defaults that match the explicit
		// pattern other facades use, so a future grocksdb default
		// change can't silently drift this store.
		Level0FileNumCompactionTrigger: 4,
		Level0SlowdownWritesTrigger:    20,
		Level0StopWritesTrigger:        36,

		// SST file size targets.
		TargetFileSizeMB:       16,
		MaxBytesForLevelBaseMB: 64,

		// Resources.
		MaxBackgroundJobs: 2,
		MaxOpenFiles:      1000,

		// Read path.
		// 8 MB block cache fits the metastore's working set
		// comfortably.
		// No bloom filter — the keyspace is small enough that a
		// positive Has / Get always hits the cache after the first
		// scan; bloom-filter RAM (~10 bits per key) would be wasted.
		BlockCacheMB:          8,
		BloomFilterBitsPerKey: 0,

		// WAL cap.
		// 8 MB matches the natural memtable budget; graceful Close
		// drains the memtable to SST (see Close below), so a
		// graceful restart has zero WAL replay regardless of cap.
		// This cap only bounds work on ungraceful-shutdown recovery
		// (kernel panic, power loss, OOM kill).
		MaxTotalWalSizeMB: 8,
	}
}

// Open opens the underlying RocksDB store and prepares it for reads
// and writes.
// Idempotent: first call performs the open, subsequent calls return
// the same result.
// Creates the on-disk directory (with parents) if missing.
func (m *MetaStore) Open() error {
	m.openOnce.Do(func() {
		m.openErr = m.store.Open()
	})
	return m.openErr
}

// Close drains the active memtable to an SST file and then tears
// the underlying RocksDB instance down.
// Idempotent: a second Close is a no-op.
// Best-effort Flush: on Flush failure the log records a warning
// and Close proceeds with teardown — the WAL is still on disk so
// data is durable, and the next Open replays the WAL.
// After a successful Close, the WAL is recyclable on next open
// and a graceful restart has zero WAL replay.
func (m *MetaStore) Close() error {
	if !m.closed.CompareAndSwap(false, true) {
		return nil
	}
	if err := m.store.Flush(); err != nil {
		m.log.WithError(err).Warn("metastore: graceful close Flush failed; next Open will replay WAL")
	}
	return m.store.Close()
}

// AddEntries writes any combination of MetaStoreEntry types
// atomically — one fsync per call regardless of how many records
// land or what types they cover.
//
// Mixed-type batching is the whole point of this method: callers can
// commit ChunkEntry and IndexEntry rows in the same transaction,
// which is how the cleanup_txhash pattern stays atomic without a
// special-case wrapper (MarkTxHashIndexComplete uses this primitive
// under the hood).
//
// Dispatch:
//
//   - 0 entries → no-op.
//   - 1 entry  → direct Store.Put. Same durability as the batch
//     path; the single-Put branch skips the WriteBatch object for
//     the common per-write case.
//   - N > 1    → Store.Batch covering all N writes.
//
// The closed-fence at the top short-circuits AFTER Close has flipped
// the atomic — without this, a writer racing Close to this point
// would leak one or more WAL records past the Flush-on-Close and
// break the "zero WAL replay on next graceful open" guarantee.
func (m *MetaStore) AddEntries(entries []stores.MetaStoreEntry) error {
	if m.closed.Load() {
		return stores.ErrStoreClosed
	}
	switch len(entries) {
	case 0:
		return nil
	case 1:
		key, value := entryKeyValue(entries[0])
		return translateError(m.store.Put(defaultCFName, []byte(key), []byte{value}))
	default:
		return translateError(m.store.Batch(func(b *BatchWriter) error {
			for _, e := range entries {
				key, value := entryKeyValue(e)
				b.Put(defaultCFName, []byte(key), []byte{value})
			}
			return nil
		}))
	}
}

// DeleteEntries removes the given keys atomically.
// Missing keys are silently skipped (RocksDB's DeleteCF behaves that
// way; the wrapper preserves it).
//
// Same single-vs-batch dispatch as AddEntries: 0 entries → no-op,
// 1 entry → direct Store.Delete, N > 1 → Store.Batch.
//
// Used today by random-pruning paths in the streaming side that
// need to drop arbitrary sets of chunk or index entries; the typed
// MetaStoreKey discriminator keeps callers from constructing raw
// key strings.
func (m *MetaStore) DeleteEntries(keys []stores.MetaStoreKey) error {
	if m.closed.Load() {
		return stores.ErrStoreClosed
	}
	switch len(keys) {
	case 0:
		return nil
	case 1:
		return translateError(m.store.Delete(defaultCFName, []byte(keyToString(keys[0]))))
	default:
		return translateError(m.store.Batch(func(b *BatchWriter) error {
			for _, k := range keys {
				b.Delete(defaultCFName, []byte(keyToString(k)))
			}
			return nil
		}))
	}
}

// GetChunkEntry returns the value stored under (chunkID, kind), or
// stores.ErrNotFound if the entry has never been written (or has
// been deleted).
func (m *MetaStore) GetChunkEntry(chunkID uint32, kind stores.ChunkArtifactKind) (uint8, error) {
	if m.closed.Load() {
		return 0, stores.ErrStoreClosed
	}
	return m.getU8(chunkArtifactKey(chunkID, kind))
}

// GetIndexEntry returns the value stored under indexID, or
// stores.ErrNotFound if absent.
func (m *MetaStore) GetIndexEntry(indexID uint32) (uint8, error) {
	if m.closed.Load() {
		return 0, stores.ErrStoreClosed
	}
	return m.getU8(fmt.Sprintf(keyIndexTxHashFormat, indexID))
}

// UpdateLastCommittedLedger sets the streaming-side checkpoint to
// seq. Overwrites any prior value.
func (m *MetaStore) UpdateLastCommittedLedger(seq uint32) error {
	if m.closed.Load() {
		return stores.ErrStoreClosed
	}
	return translateError(m.store.Put(defaultCFName, []byte(keyStreamingLastCommittedLedger), EncodeUint32(seq)))
}

// GetLastCommittedLedger returns the most recent
// last-committed-ledger value, or stores.ErrNotFound on a fresh
// install where streaming has never committed anything yet.
func (m *MetaStore) GetLastCommittedLedger() (uint32, error) {
	if m.closed.Load() {
		return 0, stores.ErrStoreClosed
	}
	return m.getU32(keyStreamingLastCommittedLedger)
}

// UpdateConfigLedgersPerTxIndex sets the immutability marker.
// Write-once enforcement lives at the call site (the service's
// startup logic), not here — keeping the storage layer policy-free
// makes the misuse path visible at the caller.
func (m *MetaStore) UpdateConfigLedgersPerTxIndex(n uint32) error {
	if m.closed.Load() {
		return stores.ErrStoreClosed
	}
	return translateError(m.store.Put(defaultCFName, []byte(keyConfigLedgersPerTxIndex), EncodeUint32(n)))
}

// GetConfigLedgersPerTxIndex returns the stored ledgers-per-tx-index,
// or stores.ErrNotFound on a fresh install.
func (m *MetaStore) GetConfigLedgersPerTxIndex() (uint32, error) {
	if m.closed.Load() {
		return 0, stores.ErrStoreClosed
	}
	return m.getU32(keyConfigLedgersPerTxIndex)
}

// MarkTxHashIndexComplete is the cleanup_txhash atomic transition:
// set index:<txIndexID>:txhash = value AND delete chunk:*:txhashRaw
// for every chunk in this tx-index, in one transaction.
//
// Reads ledgersPerTxIndex from the store's own config (the
// immutability marker), uses pkg/geometry to compute the chunk
// range, then commits the index-set + N-deletes as a single
// Layer-1 Batch — either all flips happen or none do.
// Returns an error wrapping stores.ErrNotFound if
// ledgersPerTxIndex has never been written (the caller must set
// the immutability marker before invoking cleanup).
func (m *MetaStore) MarkTxHashIndexComplete(txIndexID uint32, value uint8) error {
	if m.closed.Load() {
		return stores.ErrStoreClosed
	}
	ledgersPerTxIndex, err := m.GetConfigLedgersPerTxIndex()
	if err != nil {
		return fmt.Errorf("metastore: MarkTxHashIndexComplete: read ledgersPerTxIndex: %w", err)
	}
	chunkIDs := geometry.ChunksInTxIndex(txIndexID, ledgersPerTxIndex)
	return translateError(m.store.Batch(func(b *BatchWriter) error {
		// Set the index entry to `value`.
		b.Put(defaultCFName, fmt.Appendf(nil, keyIndexTxHashFormat, txIndexID), []byte{value})
		// Delete every chunk's txhashRaw entry in this tx-index.
		for _, chunkID := range chunkIDs {
			b.Delete(defaultCFName, fmt.Appendf(nil, keyChunkTxHashRawFormat, chunkID))
		}
		return nil
	}))
}

// getU8 reads a single-byte value at key and returns it.
// (nil, found=false) → stores.ErrNotFound.
// Mismatched value length (a corrupt write from outside the
// metastore?) surfaces as a wrapped error rather than silently
// truncating.
func (m *MetaStore) getU8(key string) (uint8, error) {
	v, found, err := m.store.Get(defaultCFName, []byte(key))
	if err != nil {
		return 0, translateError(err)
	}
	if !found {
		return 0, stores.ErrNotFound
	}
	if len(v) != 1 {
		return 0, fmt.Errorf("metastore: malformed uint8 value at key %q (got %d bytes, expected 1)", key, len(v))
	}
	return v[0], nil
}

// getU32 reads a 4-byte big-endian-encoded value at key.
// Same shape as getU8 but for the two singleton uint32 entries
// (last-committed-ledger, ledgers-per-tx-index).
func (m *MetaStore) getU32(key string) (uint32, error) {
	v, found, err := m.store.Get(defaultCFName, []byte(key))
	if err != nil {
		return 0, translateError(err)
	}
	if !found {
		return 0, stores.ErrNotFound
	}
	if len(v) != 4 {
		return 0, fmt.Errorf("metastore: malformed uint32 value at key %q (got %d bytes, expected 4)", key, len(v))
	}
	return DecodeUint32(v), nil
}

// entryKeyValue derives the (rocksdb-key, byte-value) tuple for a
// given MetaStoreEntry.
// The default case panics — the marker interface is sealed in
// pkg/stores, so any new implementer requires a matching case here.
// Panic-on-default catches the schema-drift bug at the first call
// site that exercises the new type, instead of silently no-op-ing.
func entryKeyValue(e stores.MetaStoreEntry) (string, uint8) {
	switch v := e.(type) {
	case stores.ChunkEntry:
		return chunkArtifactKey(v.ChunkID, v.Kind), v.Value
	case stores.IndexEntry:
		return fmt.Sprintf(keyIndexTxHashFormat, v.IndexID), v.Value
	default:
		panic(fmt.Sprintf("metastore: unsupported MetaStoreEntry type %T (add a case to entryKeyValue)", e))
	}
}

// keyToString derives the rocksdb key for a MetaStoreKey descriptor.
// Same sealed-type pattern as entryKeyValue.
func keyToString(k stores.MetaStoreKey) string {
	switch v := k.(type) {
	case stores.ChunkKey:
		return chunkArtifactKey(v.ChunkID, v.Kind)
	case stores.IndexKey:
		return fmt.Sprintf(keyIndexTxHashFormat, v.IndexID)
	default:
		panic(fmt.Sprintf("metastore: unsupported MetaStoreKey type %T (add a case to keyToString)", k))
	}
}

// chunkArtifactKey maps a (chunkID, ChunkArtifactKind) tuple to the
// canonical key string used on disk.
// The mapping is the schema's single source of truth — the suffixes
// "lfs", "txhash", "events" are pinned here once and referenced
// everywhere else by their format-constant names.
func chunkArtifactKey(chunkID uint32, kind stores.ChunkArtifactKind) string {
	switch kind {
	case stores.ChunkArtifactLFS:
		return fmt.Sprintf(keyChunkLFSFormat, chunkID)
	case stores.ChunkArtifactTxHashRaw:
		return fmt.Sprintf(keyChunkTxHashRawFormat, chunkID)
	case stores.ChunkArtifactEvents:
		return fmt.Sprintf(keyChunkEventsFormat, chunkID)
	default:
		panic(fmt.Sprintf("metastore: unsupported ChunkArtifactKind %d (add a case to chunkArtifactKey)", kind))
	}
}
