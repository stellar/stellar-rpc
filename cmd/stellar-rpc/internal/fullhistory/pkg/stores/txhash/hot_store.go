// Package txhash holds the hot transaction-hash store (RocksDB-backed,
// 16-CF nibble-routed) and its value types. A future cold reader
// (RecSplit-backed) will live alongside the HotStore in this package.
package txhash

import (
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// 16 CFs — one per high-nibble bucket of byte 0 of the txhash.
// Same routing the cold RecSplit index uses.
const numCFs = 16

// cfNameByNibble is the precomputed (cf-0..cf-f) table indexed by
// hash[0]>>4. Single source of truth used by both cfNames (open-time
// CF list) and cfNameForTxHash (hot path).
//
//nolint:gochecknoglobals
var cfNameByNibble = [16]string{
	"cf-0", "cf-1", "cf-2", "cf-3", "cf-4", "cf-5", "cf-6", "cf-7",
	"cf-8", "cf-9", "cf-a", "cf-b", "cf-c", "cf-d", "cf-e", "cf-f",
}

// Entry — one (txhash → ledgerSeq) mapping.
type Entry struct {
	Hash      [32]byte
	LedgerSeq uint32
}

// HotStore — RocksDB-backed hot transaction-hash store. 16 CFs named
// cf-0..cf-f; each hash routes to cf-{txhash[0]>>4}; ledgerSeq
// encoded big-endian. Routing, CF names, and encoding are internal.
//
// Concurrency: RocksDB is concurrent-safe for in-flight read/write
// operations. Close, however, must not be called concurrently with
// any in-flight Get / AddEntries / iteration — drain reads/writes
// first. Close is idempotent (delegates to rocksdb.Store.Close
// which guards with atomic.Bool + CompareAndSwap).
type HotStore struct {
	store *rocksdb.Store
}

func OpenHotStore(path string, logger *supportlog.Entry) (*HotStore, error) {
	if path == "" {
		return nil, rocksdb.ErrInvalidConfig
	}
	if logger == nil {
		return nil, rocksdb.ErrInvalidConfig
	}
	store, err := rocksdb.New(rocksdb.Config{
		Path:           path,
		ColumnFamilies: cfNames(),
		Logger:         logger,
		Tuning:         tuning(),
	})
	if err != nil {
		return nil, err
	}
	return &HotStore{store: store}, nil
}

func cfNames() []string {
	out := make([]string, numCFs)
	copy(out, cfNameByNibble[:])
	return out
}

func cfNameForTxHash(hash [32]byte) string {
	return cfNameByNibble[hash[0]>>4]
}

// tuning — the hot txhash workload is write-once / point-lookup over
// 16 CFs; the cross-knob interactions below are non-obvious enough
// that they get an explicit per-stanza rationale. The other facades
// ride on RocksDB defaults by contrast — only this workload earned
// the calibration.
func tuning() rocksdb.Tuning {
	return rocksdb.Tuning{
		// Per-CF memtable budget × 16 CFs (64 MB × 16 = 1024 MB)
		// matches the MaxTotalWalSizeMB cap below. Memtable-fill
		// cadence and WAL-cap cadence align under uniform writes;
		// either trigger fires at roughly the same time and produces
		// ~64 MB SSTs.
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

		// High background-job budget for the periodic memtable
		// flushes across 16 CFs.
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

		// 1 GB WAL cap matches the natural memtable budget above.
		// Graceful Close auto-Flushes (see rocksdb.Store.Close), so
		// this cap only bounds ungraceful-shutdown recovery (kernel
		// panic, power loss, OOM kill).
		MaxTotalWalSizeMB: 1024,
	}
}

// Close releases the underlying RocksDB store. Idempotent —
// delegates to rocksdb.Store.Close. Must not be called concurrently
// with in-flight reads/writes on this HotStore.
func (h *HotStore) Close() error { return h.store.Close() }

// AddEntries writes a batch of (txhash → ledgerSeq) atomically
// across however many CFs the hashes' nibbles cover. One fsync per
// call.
func (h *HotStore) AddEntries(entries []Entry) error {
	if h.store.IsClosed() {
		return rocksdb.ErrStoreClosed
	}
	switch len(entries) {
	case 0:
		return nil
	case 1:
		e := entries[0]
		return h.store.Put(cfNameForTxHash(e.Hash), e.Hash[:], rocksdb.EncodeUint32(e.LedgerSeq))
	default:
		return h.store.Batch(func(b *rocksdb.BatchWriter) error {
			for _, e := range entries {
				b.Put(cfNameForTxHash(e.Hash), e.Hash[:], rocksdb.EncodeUint32(e.LedgerSeq))
			}
			return nil
		})
	}
}

// Get returns the ledger sequence the hash was committed in, or
// (0, stores.ErrNotFound) on miss. Only the routed CF is queried.
func (h *HotStore) Lookup(hash [32]byte) (uint32, error) {
	v, found, err := h.store.Get(cfNameForTxHash(hash), hash[:])
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, stores.ErrNotFound
	}
	return rocksdb.DecodeUint32(v), nil
}
