package rocksdb

import (
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// 16 CFs — one per high-nibble bucket of byte 0 of the txhash.
// Same routing the cold RecSplit index uses.
const txHashNumCFs = 16

// cfNameByNibble is the precomputed (cf-0..cf-f) table indexed by
// hash[0]>>4. Single source of truth — used by both txHashCFNames
// (constructor-time CF list) and cfNameForTxHash (hot path).
//
//nolint:gochecknoglobals
var cfNameByNibble = [16]string{
	"cf-0", "cf-1", "cf-2", "cf-3", "cf-4", "cf-5", "cf-6", "cf-7",
	"cf-8", "cf-9", "cf-a", "cf-b", "cf-c", "cf-d", "cf-e", "cf-f",
}

// TxHashHotStore — RocksDB-backed stores.TxHashHotStore. 16 CFs named
// cf-0..cf-f; each hash routes to cf-{txhash[0]>>4}; ledgerSeq
// encoded via EncodeUint32. Routing, CF names, encoding all internal.
type TxHashHotStore struct {
	store *Store
}

func NewTxHashHotStore(path string, logger *supportlog.Entry) (*TxHashHotStore, error) {
	if path == "" {
		return nil, ErrInvalidConfig
	}
	if logger == nil {
		return nil, ErrInvalidConfig
	}
	store, err := New(Config{
		Path:           path,
		ColumnFamilies: txHashCFNames(),
		Logger:         logger,
		Tuning:         txHashTuning(),
	})
	if err != nil {
		return nil, err
	}
	return &TxHashHotStore{store: store}, nil
}

func txHashCFNames() []string {
	out := make([]string, txHashNumCFs)
	copy(out, cfNameByNibble[:])
	return out
}

func cfNameForTxHash(hash [32]byte) string {
	return cfNameByNibble[hash[0]>>4]
}

// txHashTuning — the hot txhash workload is write-once / point-lookup
// over 16 CFs; the cross-knob interactions below are non-obvious
// enough that they get an explicit per-stanza rationale. metaTuning
// and ledgerTuning are intentionally terse by contrast — they're the
// "ordinary" workloads where defaults hold.
func txHashTuning() Tuning {
	return Tuning{
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
		// Graceful Close auto-Flushes (see Store.Close), so this cap
		// only bounds ungraceful-shutdown recovery (kernel panic,
		// power loss, OOM kill).
		MaxTotalWalSizeMB: 1024,
	}
}

func (s *TxHashHotStore) Close() error { return s.store.Close() }

// AddEntries writes a batch of (txhash → ledgerSeq) atomically across
// however many CFs the hashes' nibbles cover. One fsync per call.
func (s *TxHashHotStore) AddEntries(entries []stores.TxHashToLedgerSeqEntry) error {
	if s.store.IsClosed() {
		return stores.ErrStoreClosed
	}
	switch len(entries) {
	case 0:
		return nil
	case 1:
		e := entries[0]
		return translateError(s.store.Put(cfNameForTxHash(e.Hash), e.Hash[:], EncodeUint32(e.LedgerSeq)))
	default:
		return translateError(s.store.Batch(func(b *BatchWriter) error {
			for _, e := range entries {
				b.Put(cfNameForTxHash(e.Hash), e.Hash[:], EncodeUint32(e.LedgerSeq))
			}
			return nil
		}))
	}
}

// Get returns the ledger sequence the hash was committed in, or
// (0, stores.ErrNotFound) on miss. Only the routed CF is queried.
func (s *TxHashHotStore) Get(hash [32]byte) (uint32, error) {
	v, found, err := s.store.Get(cfNameForTxHash(hash), hash[:])
	if err != nil {
		return 0, translateError(err)
	}
	if !found {
		return 0, stores.ErrNotFound
	}
	return DecodeUint32(v), nil
}
