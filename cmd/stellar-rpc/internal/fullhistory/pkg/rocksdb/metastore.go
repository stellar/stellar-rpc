package rocksdb

import (
	"fmt"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// On-disk schema (RocksDB key strings).
const (
	keyChunkLFSFormat       = "chunk:%08d:lfs"
	keyChunkTxHashRawFormat = "chunk:%08d:txhash"
	keyChunkEventsFormat    = "chunk:%08d:events"
	keyIndexTxHashFormat    = "index:%08d:txhash"

	keyConfigLedgersPerTxIndex      = "config:ledgers_per_tx_index"
	keyStreamingLastCommittedLedger = "streaming:last_committed_ledger"
)

// MetaStore — RocksDB-backed stores.MetaStore. Default-CF only; flat
// key/value space. Key derivation, value encoding, CF naming all
// internal.
type MetaStore struct {
	store *Store
}

// NewMetaStore validates inputs and returns a MetaStore ready to be
// Opened. path and logger are both required.
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
	})
	if err != nil {
		return nil, err
	}
	return &MetaStore{store: store}, nil
}

// metaStoreTuning — tiny store (single-digit MB total), no bloom
// (cache covers reads). Standard compaction (unlike txhash) — typed
// deletes (e.g., cleanup_txhash clearing chunk:* entries) rely on
// compaction to reclaim space.
func metaStoreTuning() Tuning {
	return Tuning{
		WriteBufferMB:                  4,
		MaxWriteBufferNumber:           2,
		Level0FileNumCompactionTrigger: 4,
		Level0SlowdownWritesTrigger:    20,
		Level0StopWritesTrigger:        36,
		TargetFileSizeMB:               16,
		MaxBytesForLevelBaseMB:         64,
		MaxBackgroundJobs:              2,
		MaxOpenFiles:                   1000,
		BlockCacheMB:                   8,
		BloomFilterBitsPerKey:          0,
		MaxTotalWalSizeMB:              8,
	}
}

func (m *MetaStore) Open() error  { return m.store.Open() }
func (m *MetaStore) Close() error { return m.store.Close() }

// AddEntries writes any mix of MetaStoreEntry types atomically.
// Mixed-type batching lets cleanup_txhash commit ChunkEntry +
// IndexEntry rows in one transaction (MarkTxHashIndexComplete uses
// this under the hood).
// Empty/single/multi dispatch: 0 → no-op; 1 → Store.Put; N → Store.Batch.
func (m *MetaStore) AddEntries(entries []stores.MetaStoreEntry) error {
	if m.store.IsClosed() {
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

// DeleteEntries removes the given keys atomically. Missing keys are
// silently skipped.
func (m *MetaStore) DeleteEntries(keys []stores.MetaStoreKey) error {
	if m.store.IsClosed() {
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

func (m *MetaStore) GetChunkArtifactState(chunkID uint32, kind stores.ChunkArtifactKind) (uint8, error) {
	return m.getU8(chunkArtifactKey(chunkID, kind))
}

func (m *MetaStore) GetTxHashIndexState(indexID uint32) (uint8, error) {
	return m.getU8(fmt.Sprintf(keyIndexTxHashFormat, indexID))
}

func (m *MetaStore) UpdateLastCommittedLedger(seq uint32) error {
	return translateError(m.store.Put(defaultCFName, []byte(keyStreamingLastCommittedLedger), EncodeUint32(seq)))
}

func (m *MetaStore) GetLastCommittedLedger() (uint32, error) {
	return m.getU32(keyStreamingLastCommittedLedger)
}

// UpdateConfigLedgersPerTxIndex sets the immutability marker.
// Write-once enforcement lives at the call site, not here.
func (m *MetaStore) UpdateConfigLedgersPerTxIndex(n uint32) error {
	return translateError(m.store.Put(defaultCFName, []byte(keyConfigLedgersPerTxIndex), EncodeUint32(n)))
}

func (m *MetaStore) GetConfigLedgersPerTxIndex() (uint32, error) {
	return m.getU32(keyConfigLedgersPerTxIndex)
}

// MarkTxHashIndexComplete is the cleanup_txhash atomic transition:
// set index:<txIndexID>:txhash = value AND delete chunk:*:txhashRaw
// for every chunk in this tx-index, in one transaction.
func (m *MetaStore) MarkTxHashIndexComplete(txIndexID uint32, value uint8) error {
	if m.store.IsClosed() {
		return stores.ErrStoreClosed
	}
	ledgersPerTxIndex, err := m.GetConfigLedgersPerTxIndex()
	if err != nil {
		return fmt.Errorf("metastore: MarkTxHashIndexComplete: read ledgersPerTxIndex: %w", err)
	}
	chunksPerTxIndex := ledgersPerTxIndex / geometry.LedgersPerChunk
	firstChunkID := txIndexID * chunksPerTxIndex
	return translateError(m.store.Batch(func(b *BatchWriter) error {
		b.Put(defaultCFName, fmt.Appendf(nil, keyIndexTxHashFormat, txIndexID), []byte{value})
		for i := range chunksPerTxIndex {
			b.Delete(defaultCFName, fmt.Appendf(nil, keyChunkTxHashRawFormat, firstChunkID+i))
		}
		return nil
	}))
}

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

// entryKeyValue maps a MetaStoreEntry to its (key, value). Default
// case panics — sealed-type drift is caught at the first call site.
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
