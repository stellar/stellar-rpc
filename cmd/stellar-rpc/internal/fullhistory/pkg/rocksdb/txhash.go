package rocksdb

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// txHashNumCFs is the number of column families the hot txhash store
// runs with.
// Pinned at 16 — one CF per high-nibble bucket of byte 0 of the
// txhash, the same routing the cold RecSplit index uses.
// Stays internal to this package; callers never see CF names.
const txHashNumCFs = 16

// TxHashStore is the concrete RocksDB-backed implementation of
// stores.TxHashStore.
//
// Wraps a Layer-1 *Store with 16 column families named cf-0 … cf-f,
// internally routes each transaction hash to the CF identified by
// the high nibble of byte 0 (txhash[0] >> 4), and encodes ledger
// sequence values via this package's big-endian EncodeUint32.
//
// All routing, CF names, and value encoding are unexported; the
// stores.TxHashStore interface stays clean of any RocksDB-specific
// shape.
//
// Two-phase lifecycle: NewTxHashStore validates inputs and returns a
// constructed instance; (*TxHashStore).Open brings the backing
// RocksDB up; (*TxHashStore).Close drains the active memtable and
// tears the underlying store down.
// Both Open and Close are idempotent at the facade level (sync.Once
// + atomic.Bool) in addition to the Layer-1 wrapper's own
// idempotency.
type TxHashStore struct {
	log   *supportlog.Entry
	store *Store

	openOnce sync.Once
	openErr  error
	closed   atomic.Bool
}

// NewTxHashStore validates the inputs and returns a TxHashStore
// ready to be Opened.
//
// path is the on-disk directory the hot txhash store occupies (created,
// with parents, on Open if missing).
// logger receives the wrapper's on-open state line plus any close-time
// diagnostics.
// Both are required — an empty path or nil logger returns
// ErrInvalidConfig.
//
// Tuning is hardcoded inside this constructor for the hot txhash
// workload (random-hash point lookups, no compaction, retention-
// window-scale store).
// Operators see only the path; everything else is pinned in code.
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
	return &TxHashStore{log: logger, store: store}, nil
}

// txHashCFNames returns the list of 16 CFs the store opens against:
// cf-0, cf-1, …, cf-f, in order.
// Built lazily inside NewTxHashStore; nothing outside this package
// references the names.
func txHashCFNames() []string {
	names := make([]string, txHashNumCFs)
	for i := range txHashNumCFs {
		names[i] = fmt.Sprintf("cf-%x", i)
	}
	return names
}

// cfNameForTxHash returns the column-family name the given
// transaction hash routes to.
// Mapping: cf-{x} where x is the high nibble of byte 0
// (txhash[0] >> 4) interpreted as a hex digit — so cf-0 through cf-f.
// Unexported on purpose; CF names never leave this package.
func cfNameForTxHash(hash [32]byte) string {
	return fmt.Sprintf("cf-%x", hash[0]>>4)
}

// txHashTuning returns the hardcoded RocksDB knobs for the hot
// txhash workload.
// Values match the txhash row in the project's tuning ADR:
// no compaction, 12-bits/key bloom for ~0.4% false-positive rate,
// memtable budget chosen to match the 1 GB WAL cap.
func txHashTuning() Tuning {
	return Tuning{
		// Write path — per-CF memtable budget.
		// 64 MB × 16 CFs = 1024 MB total in-memory memtable space,
		// equal to the WAL cap below.
		// Flushes happen at memtable-fill cadence (a per-CF
		// memtable hitting 64 MB) which lines up with WAL-cap-fill
		// cadence (total WAL hitting 1 GB) under uniform writes;
		// either trigger fires at roughly the same time and
		// produces ~64 MB SSTs.
		WriteBufferMB:        64,
		MaxWriteBufferNumber: 2,

		// L0 / compaction triggers set high so automatic
		// compaction never fires under normal operation.
		// The hot txhash store is write-once / point-lookup; the
		// natural memtable-flush cadence produces a manageable
		// L0 SST count even at full retention, and a bloom filter
		// on each SST keeps point lookups fast.
		// Compaction would re-write the same data with no
		// reordering benefit.
		Level0FileNumCompactionTrigger: 999,
		Level0SlowdownWritesTrigger:    999,
		Level0StopWritesTrigger:        999,
		DisableAutoCompactions:         true,

		// SST file size targets.
		// 64 MB target files match the memtable size — a memtable
		// flush produces one ~64 MB SST.
		// Level-base size is irrelevant under
		// DisableAutoCompactions (compaction never promotes past
		// L0) but pinned for clarity.
		TargetFileSizeMB:       64,
		MaxBytesForLevelBaseMB: 256,

		// Resources — high background-job count for the periodic
		// memtable flushes across 16 CFs.
		MaxBackgroundJobs: 8,
		MaxOpenFiles:      10_000,

		// Read path.
		// 512 MB block cache shared across all 16 CFs — txhash
		// point-lookups are dominated by bloom-filter block reads,
		// and the working set of recently-touched bloom blocks at
		// scale needs to stay cache-resident.
		// 12 bits/key bloom (~0.4% false-positive rate); at the
		// no-compaction SST count, the lookup-cost knob.
		BlockCacheMB:          512,
		BloomFilterBitsPerKey: 12,

		// WAL cap.
		// 1 GB matches the natural memtable budget above; flushes
		// happen at memtable-fill cadence rather than WAL-cap-
		// forced cadence.
		// Graceful Close drains the memtable to SST (see Close
		// below), so a graceful restart has zero WAL to replay
		// regardless of cap; this cap only bounds work on the
		// ungraceful-shutdown recovery path (kernel panic, power
		// loss, OOM kill).
		MaxTotalWalSizeMB: 1024,
	}
}

// Open opens the underlying RocksDB store with the 16 CFs and
// prepares it for reads and writes.
// Idempotent: first call performs the open, subsequent calls return
// the same result.
// Creates the on-disk directory (with parents) if missing.
func (s *TxHashStore) Open() error {
	s.openOnce.Do(func() {
		s.openErr = s.store.Open()
	})
	return s.openErr
}

// Close drains the active memtable to an SST file and then tears
// the underlying RocksDB instance down.
// Idempotent: a second Close is a no-op.
// Best-effort Flush: on Flush failure the log records a warning and
// Close proceeds with teardown — the WAL is still on disk so data
// is durable, and the next Open will replay the WAL (one-time
// startup-latency cost, not a correctness issue).
// After a successful Close, the WAL is recyclable on next open and
// a graceful restart has zero WAL replay.
func (s *TxHashStore) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	if err := s.store.Flush(); err != nil {
		s.log.WithError(err).Warn("txhash store: graceful close Flush failed; next Open will replay WAL")
	}
	return s.store.Close()
}

// AddEntries writes a batch of (txhash → ledgerSeq) entries
// atomically.
//
// Routing is internal: each entry's hash is mapped to its CF via
// the unexported nibble helper, and ledgerSeq is encoded via
// EncodeUint32 — callers see none of that.
//
// Single-vs-batch dispatch is by len(entries):
//
//   - 0 entries → no-op, returns nil. No fsync.
//   - 1 entry  → single Store.Put.
//     Same one-fsync atomicity as the batch path; this
//     branch skips constructing a WriteBatch object for
//     the common live-ingestion case of one tx per call.
//   - N > 1    → Store.Batch covering all N writes.
//     Atomic across however many of the 16 CFs the hashes'
//     nibbles touch; one fsync covers the whole batch.
//
// Either way, AddEntries returning nil means every queued write is
// durable on disk by the time the caller proceeds.
func (s *TxHashStore) AddEntries(entries []stores.TxHashToLedgerSeqEntry) error {
	// Closed-fence: short-circuit at the facade level before reaching
	// the Layer-1 store.
	// Gives Close()'s Flush a quiet store to drain — a writer that
	// raced Close to this point would otherwise leak one or more WAL
	// records past Flush and break the "zero WAL replay on next
	// graceful open" guarantee.
	// Cost: one atomic.Load per call, ~ns.
	if s.closed.Load() {
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

// RemoveEntries deletes a batch of transaction-hash entries
// atomically.
//
// Same single-vs-batch dispatch as AddEntries: empty slice is a no-
// op, one hash goes through a direct Store.Delete, more than one
// goes through Store.Batch.
//
// Idempotent: a hash that isn't currently in the store is silently
// dropped — RocksDB's DeleteCF returns no error for an absent key,
// and the wrapper preserves that.
func (s *TxHashStore) RemoveEntries(hashes [][32]byte) error {
	// Closed-fence — see AddEntries.
	if s.closed.Load() {
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

// Get returns the ledger sequence the given transaction hash was
// committed in.
// Routes the lookup internally by nibble; only one of the 16 CFs is
// queried, the others are not touched.
// Returns (0, stores.ErrNotFound) when the hash is not in the store.
func (s *TxHashStore) Get(hash [32]byte) (uint32, error) {
	// Closed-fence — same shape as AddEntries / RemoveEntries so a
	// reader racing Close sees ErrStoreClosed without ever touching
	// the Layer-1 store.
	if s.closed.Load() {
		return 0, stores.ErrStoreClosed
	}
	v, found, err := s.store.Get(cfNameForTxHash(hash), hash[:])
	if err != nil {
		return 0, translateError(err)
	}
	if !found {
		return 0, stores.ErrNotFound
	}
	return DecodeUint32(v), nil
}

// translateError maps internal RocksDB-wrapper sentinels into their
// stores-package equivalents.
// Keeps the interface boundary clean so consumers depending only
// on the stores package can detect closed-state errors via
// errors.Is(err, stores.ErrStoreClosed) without importing rocksdb.
// All other errors pass through unchanged (callers either expect
// nil or accept an opaque error).
func translateError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrStoreClosed) || errors.Is(err, ErrStoreNotOpened) {
		return stores.ErrStoreClosed
	}
	return err
}
