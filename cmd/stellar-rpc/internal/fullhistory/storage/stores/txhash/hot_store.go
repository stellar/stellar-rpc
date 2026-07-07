// Package txhash holds the hot transaction-hash store (RocksDB-backed, a single
// txhash CF) and its value types. A future cold reader (RecSplit-backed) will
// live alongside the HotStore in this package.
package txhash

import (
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores"
)

// txhashCF is the single column family holding every (txhash → ledgerSeq)
// entry for the chunk, per the design's hot-tier spec (one `txhash` CF).
const txhashCF = "txhash"

// Entry — one (txhash → ledgerSeq) mapping.
type Entry struct {
	Hash      [32]byte
	LedgerSeq uint32
}

// HotStore — RocksDB-backed hot transaction-hash store. A single txhash CF
// holding the full 32-byte hash as key and the big-endian ledgerSeq as value.
// The CF name and encoding are internal.
//
// Like every hot store, a HotStore instance is chunk-bound: it
// accumulates exactly one chunk's (txhash → seq) tuples before being
// frozen into the chunk's cold .bin artifact. The store does not itself
// range-check writes (the driver's drain loop already validates every ledger
// sequence against the chunk).
type HotStore struct {
	store *rocksdb.Store
}

// NewWithStore wraps an ALREADY-OPEN rocksdb.Store as a txhash HotStore on the
// single txhash CF (CFNames()). The store is owned by the caller — in production,
// hotchunk.DB composes this facade over the shared per-chunk DB and closes that DB
// once. The store must have CFNames() registered.
func NewWithStore(store *rocksdb.Store) *HotStore {
	return &HotStore{store: store}
}

// CFNames returns the single txhash CF name this facade owns. Exported so
// the hotchunk shared-DB opener can register it alongside the other CFs.
func CFNames() []string { return []string{txhashCF} }

// Tuning returns this facade's RocksDB tuning, applied to the shared per-chunk
// DB by the hotchunk opener. The hot txhash workload is write-once /
// point-lookup; the cross-knob interactions below are non-obvious enough that
// they get an explicit per-stanza rationale. The other facades ride on RocksDB
// defaults by contrast — only this workload earned the calibration.
func Tuning() rocksdb.Tuning {
	return rocksdb.Tuning{
		// 64 MB memtable so one flush produces one ~64 MB SST under
		// uniform writes.
		WriteBufferMB:        64,
		MaxWriteBufferNumber: 2,

		// L0 triggers pinned high + DisableAutoCompactions=true:
		// compaction would re-write the same data with no reordering
		// benefit (txhash is write-once, random-key, point-lookup).
		// The L0 999s match DisableAutoCompactions so even if a future
		// flush somehow exceeded the trigger, the engine still
		// wouldn't try to compact. NOTE: DisableAutoCompactions and
		// MaxBackgroundJobs are orthogonal — the former turns
		// compaction off entirely, the latter only caps the thread
		// budget for background work.
		Level0FileNumCompactionTrigger: 999,
		Level0SlowdownWritesTrigger:    999,
		Level0StopWritesTrigger:        999,
		DisableAutoCompactions:         true,

		// 64 MB target file matches WriteBufferMB so one memtable
		// flush produces one ~64 MB SST — fewer bloom checks per
		// query at no-compaction scale.
		// MaxBytesForLevelBaseMB is set explicitly even though it's
		// irrelevant under DisableAutoCompactions (compaction never
		// promotes past L0); explicit > implicit so a future reader
		// doesn't have to derive that it's a no-op.
		TargetFileSizeMB:       64,
		MaxBytesForLevelBaseMB: 256,

		// Background-job budget for the periodic memtable flushes.
		MaxBackgroundJobs: 8,
		MaxOpenFiles:      10_000,

		// 512 MB block cache — bloom-filter blocks are the hot
		// working set; the cache needs to hold recently-touched
		// bloom blocks at scale.
		// 12 bits/key bloom (~0.4% false-positive) is tighter than
		// the standard 10 bits/key because every false positive at
		// no-compaction SST count costs a disk seek across many SSTs.
		BlockCacheMB:          512,
		BloomFilterBitsPerKey: 12,

		// 1 GB WAL cap. Graceful Close auto-Flushes (see
		// rocksdb.Store.Close), so this cap only bounds ungraceful-shutdown
		// recovery (kernel panic, power loss, OOM kill).
		MaxTotalWalSizeMB: 1024,
	}
}

// AddEntriesToBatch queues each (txhash → ledgerSeq) Put into b on the txhash
// CF — the building block hotchunk uses to fold the tx-hash writes into the one
// shared per-ledger WriteBatch (decision (a)). Does not commit (caller owns the
// batch). It cannot fail: BatchWriter.Put latches any CF error, surfaced by the
// enclosing Store.Batch, whose lifecycle RLock + checkOpen is the authoritative
// closed-store guard.
func (h *HotStore) AddEntriesToBatch(b *rocksdb.BatchWriter, entries []Entry) {
	for _, e := range entries {
		b.Put(txhashCF, e.Hash[:], rocksdb.EncodeUint32(e.LedgerSeq))
	}
}

// Get returns the ledger sequence the hash was committed in, or
// (0, stores.ErrNotFound) on miss.
func (h *HotStore) Get(hash [32]byte) (uint32, error) {
	v, found, err := h.store.Get(txhashCF, hash[:])
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, stores.ErrNotFound
	}
	return rocksdb.DecodeUint32(v), nil
}
