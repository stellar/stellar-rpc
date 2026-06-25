// Package txhash holds the hot transaction-hash store (RocksDB-backed,
// 16-CF nibble-routed) and its value types. A future cold reader
// (RecSplit-backed) will live alongside the HotStore in this package.
package txhash

import (
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
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

// HotStore — RocksDB-backed hot transaction-hash store. 16 CFs cf-0..cf-f; each
// hash routes to cf-{txhash[0]>>4}; ledgerSeq BE-encoded (all internal).
// Chunk-bound like every hot store: accumulates one chunk's (txhash → seq)
// tuples before freezing, with the binding recorded at open time (ChunkID) so
// the ingest driver can reject a mismatched store. The store does not itself
// range-check writes (the driver's drain loop already validates every sequence).
type HotStore struct {
	store   *rocksdb.Store
	chunkID chunk.ID
	// ownsStore is true on the standalone NewHotStore path; false when wrapping
	// the SHARED per-chunk DB via NewWithStore (decision (a)), which
	// hotchunk.DB owns and closes once.
	ownsStore bool
}

// NewHotStore validates inputs and returns an open HotStore bound to
// chunkID (see the HotStore doc on chunk binding).
func NewHotStore(path string, chunkID chunk.ID, logger *supportlog.Entry) (*HotStore, error) {
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
	return &HotStore{store: store, chunkID: chunkID, ownsStore: true}, nil
}

// NewWithStore wraps an ALREADY-OPEN rocksdb.Store as a txhash HotStore on the
// 16 nibble-routed CFs (CFNames()). The store is NOT owned (Close is a no-op) —
// the constructor hotchunk uses to compose this facade over the shared per-chunk
// DB. The store must have CFNames() registered.
func NewWithStore(store *rocksdb.Store, chunkID chunk.ID) *HotStore {
	return &HotStore{store: store, chunkID: chunkID}
}

// CFNames returns the 16 nibble-routed CF names this facade owns. Exported so
// the hotchunk shared-DB opener can register them alongside the other CFs.
func CFNames() []string { return cfNames() }

// Tuning returns this facade's RocksDB tuning, applied to the shared per-chunk
// DB by the hotchunk opener.
func Tuning() rocksdb.Tuning { return tuning() }

func cfNames() []string {
	out := make([]string, numCFs)
	copy(out, cfNameByNibble[:])
	return out
}

func cfNameForTxHash(hash [32]byte) string {
	return cfNameByNibble[hash[0]>>4]
}

// tuning — calibrated for the hot txhash workload (write-once / point-lookup over
// 16 CFs). Cross-knob interactions are non-obvious, hence the per-stanza WHY;
// the other facades ride on defaults.
func tuning() rocksdb.Tuning {
	return rocksdb.Tuning{
		// Per-CF memtable budget × 16 (64 MB × 16 = 1024 MB) matches
		// MaxTotalWalSizeMB below, so memtable-fill and WAL-cap cadence align
		// and each flush produces a ~64 MB SST.
		WriteBufferMB:        64,
		MaxWriteBufferNumber: 2,

		// Compaction off: write-once random-key data gains no reordering
		// benefit. L0 999s match DisableAutoCompactions so even an over-trigger
		// flush won't compact. (DisableAutoCompactions and MaxBackgroundJobs are
		// orthogonal — off vs. thread budget.)
		Level0FileNumCompactionTrigger: 999,
		Level0SlowdownWritesTrigger:    999,
		Level0StopWritesTrigger:        999,
		DisableAutoCompactions:         true,

		// 64 MB target file matches WriteBufferMB (one flush → one SST). MaxBytes
		// is a no-op under DisableAutoCompactions but set explicitly so a reader
		// needn't derive that.
		TargetFileSizeMB:       64,
		MaxBytesForLevelBaseMB: 256,

		// High background-job budget for periodic flushes across 16 CFs.
		MaxBackgroundJobs: 8,
		MaxOpenFiles:      10_000,

		// 512 MB block cache holds the hot working set (bloom-filter blocks).
		// 12 bits/key (~0.4% FP) is tighter than the standard 10 because each FP
		// costs a disk seek across many no-compaction SSTs.
		BlockCacheMB:          512,
		BloomFilterBitsPerKey: 12,

		// 1 GB WAL cap matches the memtable budget. Graceful Close auto-Flushes,
		// so this only bounds ungraceful-recovery (panic / power loss / OOM).
		MaxTotalWalSizeMB: 1024,
	}
}

// Close releases the store IF this HotStore owns it (standalone NewHotStore);
// a no-op when wrapping the shared per-chunk DB (NewWithStore), which hotchunk.DB
// closes once. Idempotent.
func (h *HotStore) Close() error {
	if !h.ownsStore {
		return nil
	}
	return h.store.Close()
}

// ChunkID returns the chunk this store is bound to (constructor-supplied;
// never reads the store).
func (h *HotStore) ChunkID() chunk.ID { return h.chunkID }

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

// AddEntriesToBatch queues each (txhash → ledgerSeq) Put into b on its
// nibble-routed CF — the building block hotchunk uses to fold the tx-hash writes
// into the one shared per-ledger WriteBatch (decision (a)). Does not commit
// (caller owns the batch). A closed store returns ErrStoreClosed.
func (h *HotStore) AddEntriesToBatch(b *rocksdb.BatchWriter, entries []Entry) error {
	if h.store.IsClosed() {
		return rocksdb.ErrStoreClosed
	}
	for _, e := range entries {
		b.Put(cfNameForTxHash(e.Hash), e.Hash[:], rocksdb.EncodeUint32(e.LedgerSeq))
	}
	return nil
}

// Get returns the ledger sequence the hash was committed in, or
// (0, stores.ErrNotFound) on miss. Only the routed CF is queried.
func (h *HotStore) Get(hash [32]byte) (uint32, error) {
	v, found, err := h.store.Get(cfNameForTxHash(hash), hash[:])
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, stores.ErrNotFound
	}
	return rocksdb.DecodeUint32(v), nil
}
