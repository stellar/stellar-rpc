package rocksdb

import (
	"errors"
	"fmt"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// 16 CFs — one per high-nibble bucket of byte 0 of the txhash.
// Same routing the cold RecSplit index uses.
const txHashNumCFs = 16

// TxHashStore — RocksDB-backed stores.TxHashStore. 16 CFs named
// cf-0..cf-f; each hash routes to cf-{txhash[0]>>4}; ledgerSeq
// encoded via EncodeUint32. Routing, CF names, encoding all internal.
type TxHashStore struct {
	store *Store
}

func NewTxHashStore(path string, logger *supportlog.Entry) (*TxHashStore, error) {
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
	return &TxHashStore{store: store}, nil
}

func txHashCFNames() []string {
	names := make([]string, txHashNumCFs)
	for i := range txHashNumCFs {
		names[i] = fmt.Sprintf("cf-%x", i)
	}
	return names
}

func cfNameForTxHash(hash [32]byte) string {
	return fmt.Sprintf("cf-%x", hash[0]>>4)
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

func (s *TxHashStore) Open() error  { return s.store.Open() }
func (s *TxHashStore) Close() error { return s.store.Close() }

// AddEntries writes a batch of (txhash → ledgerSeq) atomically across
// however many CFs the hashes' nibbles cover. One fsync per call.
func (s *TxHashStore) AddEntries(entries []stores.TxHashToLedgerSeqEntry) error {
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

// RemoveEntries deletes by hash atomically. Missing hashes are
// silently skipped.
func (s *TxHashStore) RemoveEntries(hashes [][32]byte) error {
	if s.store.IsClosed() {
		return stores.ErrStoreClosed
	}
	switch len(hashes) {
	case 0:
		return nil
	case 1:
		h := hashes[0]
		return translateError(s.store.Delete(cfNameForTxHash(h), h[:]))
	default:
		return translateError(s.store.Batch(func(b *BatchWriter) error {
			for _, h := range hashes {
				b.Delete(cfNameForTxHash(h), h[:])
			}
			return nil
		}))
	}
}

// Get returns the ledger sequence the hash was committed in, or
// (0, stores.ErrNotFound) on miss. Only the routed CF is queried.
func (s *TxHashStore) Get(hash [32]byte) (uint32, error) {
	v, found, err := s.store.Get(cfNameForTxHash(hash), hash[:])
	if err != nil {
		return 0, translateError(err)
	}
	if !found {
		return 0, stores.ErrNotFound
	}
	return DecodeUint32(v), nil
}

// translateError maps wrapper closed-state sentinels to the
// stores-package equivalent. Other errors pass through unchanged.
func translateError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrStoreClosed) || errors.Is(err, ErrStoreNotOpened) {
		return stores.ErrStoreClosed
	}
	return err
}
