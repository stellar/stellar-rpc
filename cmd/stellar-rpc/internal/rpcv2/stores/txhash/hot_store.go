// Package txhash holds the hot transaction-hash tier (one packed row per
// ledger in a RocksDB CF, served by the in-RAM HotIndex over window + sealed
// runs), the cold artifacts (the raw per-chunk .bin file and the
// streamhash-MPHF-backed cold index reader/builder), and the read-assembly
// layer that resolves a hash across tiers.
package txhash

import (
	"fmt"
	"path/filepath"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
)

const (
	// txhashCF is the single column family holding ONE packed row per
	// ledger: key rocksdb.EncodeUint32(seq), value = the ledger's sorted
	// full 32-byte tx hashes concatenated (hot_rows.go). A row exists for
	// EVERY committed ledger — empty value for tx-less ones — so the CF is
	// a dense chain. It is the durable authority warmup and the freeze
	// rebuild from; queries never point-read it (Get is served entirely by
	// the HotIndex).
	txhashCF = "txhash"

	// rowKeyLen is the packed-row key width (rocksdb.EncodeUint32). Any
	// other key length in the CF is the pre-release hash-keyed format — a
	// loud open failure, never migrated (see checkRowFormat).
	rowKeyLen = 4

	// txhashRunDir is the sealed-run directory name inside the chunk's DB
	// dir — wiped with the chunk directory like every other chunk artifact
	// (RocksDB ignores foreign subdirectories in its dir). Mirrors the
	// events facade's hotIndexRunDir.
	txhashRunDir = "txhash-runs"
)

// HotStore is the txhash facade over one chunk's shared hot RocksDB store:
// the write path queues ONE packed-row Put per ledger into the shared commit
// batch, and reads are answered by the HotIndex (window + sealed runs)
// rebuilt from the CF at open. The store is owned by the caller
// (hotchunk.DB); this facade wraps it without owning it.
//
// Concurrency: writes (AddLedgerToBatch + the returned apply hook) are
// single-writer — the hotchunk ingest goroutine; Get is safe alongside that
// writer (the HotIndex publishes through one atomic view pointer).
type HotStore struct {
	store   *rocksdb.Store
	chunkID chunk.ID

	// hotIdx serves Get and absorbs the post-commit applies. nil on a
	// read-only open, which structurally disables queries (Get panics —
	// see NewReadOnlyWithStore).
	hotIdx *HotIndex
}

// NewWithStore wraps an ALREADY-OPEN rocksdb.Store as a read-WRITE txhash
// HotStore on the single txhash CF (CFNames()), running the mandatory warmup:
// reopen the sealed runs from the manifest, replay the un-sealed packed-row
// tail into the window, and only then arm sealing. A warmup failure returns
// the error WITHOUT closing the caller-owned store. The store must have
// CFNames() registered.
func NewWithStore(store *rocksdb.Store, chunkID chunk.ID) (*HotStore, error) {
	hotIdx, err := warmupHotIndex(store, chunkID)
	if err != nil {
		return nil, fmt.Errorf("txhash: warmup chunk %s: %w", chunkID, err)
	}
	return &HotStore{store: store, chunkID: chunkID, hotIdx: hotIdx}, nil
}

// NewReadOnlyWithStore wraps an already-open store as a QUERY-DISABLED
// txhash facade: no warmup runs (the whole point of a read-only open — see
// hotchunk.OpenReadOnly), so there is no HotIndex to answer Get, and calling
// it panics. The two read-only callers need nothing warm: the freeze reads
// the CF and run files through the shared store (FreezeColdFromStore), and
// the startup refiner reads only the ledgers CF. A warmed read-only variant
// is a named prerequisite for #772 coverage-lag serving — it must not land
// by silently serving from a cold index.
func NewReadOnlyWithStore(store *rocksdb.Store, chunkID chunk.ID) *HotStore {
	return &HotStore{store: store, chunkID: chunkID}
}

// AddLedgerToBatch queues seq's ONE packed-row Put into the shared batch b
// and returns the post-commit apply hook: run it only after b commits
// durably — it publishes the row into the HotIndex window (the ONLY thing
// that makes these hashes hot-queryable) and drives sealing. rowBytes must
// be EncodeRow output for exactly this ledger and count its hash count
// (validated against each other and the dense chain by ApplyRow). The
// window RETAINS rowBytes — BatchWriter.Put copies synchronously, so the
// caller must not reuse the slice after this call (the same retained-row
// discipline as the events packed row). A row is queued for EVERY ledger;
// the empty value for a tx-less one keeps the CF's dense chain.
//
// Read-WRITE opens only: hotchunk never routes ingest at a read-only facade
// (IngestLedger rejects it before any queue step).
func (h *HotStore) AddLedgerToBatch(
	b *rocksdb.BatchWriter, seq uint32, rowBytes []byte, count int,
) func() error {
	b.Put(txhashCF, rocksdb.EncodeUint32(seq), rowBytes)
	return func() error { return h.hotIdx.ApplyRow(seq, rowBytes, count) }
}

// Get returns the ledger sequence the hash was committed in, or
// (0, stores.ErrNotFound) on miss — served entirely from the HotIndex
// (window rows, then sealed runs; every route ends in a full 32-byte
// compare). Safe alongside the single writer.
//
// Panics on a read-only open: txhash queries are disabled there because no
// warmup ran — reaching for them is a programming error, never silently
// answered from a cold index (mirrors hotchunk.DB.Events on a read-only DB).
func (h *HotStore) Get(hash [32]byte) (uint32, error) {
	if h.hotIdx == nil {
		panic(fmt.Sprintf("txhash: Get on read-only chunk %s: txhash queries are disabled on read-only opens "+
			"(no warmup ran; open read-write for a query-capable facade)", h.chunkID))
	}
	return h.hotIdx.Get(hash)
}

// Shutdown drains the hot index's background seal and releases its run
// handles. hotchunk.DB.Close calls it before closing the shared store so no
// seal goroutine outlives the DB. No-op on a read-only open.
func (h *HotStore) Shutdown() {
	if h.hotIdx != nil {
		h.hotIdx.Close()
	}
}

// CFNames returns the single txhash CF name this facade owns. Exported so
// the hotchunk shared-DB opener can register it alongside the other CFs.
func CFNames() []string { return []string{txhashCF} }

// CFOptions returns the calibration the hotchunk opener installs on the
// txhash CF alone. The workload is write-once, seq-keyed packed rows: one
// Put per ledger in ascending key order, read back only by warmup's tail
// scan and the freeze's sequential scan — never point-probed (Get is served
// by the HotIndex). No bloom filter: on 4-byte ascending keys probed only
// by scans it is pure memory/build cost.
func CFOptions() map[string]rocksdb.CFOptions {
	return map[string]rocksdb.CFOptions{
		txhashCF: {
			// 64 MB memtable so one flush produces one ~64 MB SST under
			// the append-only row stream.
			WriteBufferMB:        64,
			MaxWriteBufferNumber: 2,

			// L0 triggers pinned high + DisableAutoCompactions=true:
			// compaction would re-write the same data for nothing —
			// write-once ascending keys mean L0 SSTs are already
			// non-overlapping and every read is a scan. The L0 999s match
			// DisableAutoCompactions so even if a future flush somehow
			// exceeded the trigger, the engine still wouldn't try to
			// compact. NOTE: DisableAutoCompactions and
			// Tuning.MaxBackgroundJobs are orthogonal — the former turns
			// compaction off entirely, the latter only caps the thread
			// budget for background work.
			Level0FileNumCompactionTrigger: 999,
			Level0SlowdownWritesTrigger:    999,
			Level0StopWritesTrigger:        999,
			DisableAutoCompactions:         true,

			// 64 MB target file matches WriteBufferMB so one memtable
			// flush produces one ~64 MB SST.
			TargetFileSizeMB: 64,
		},
	}
}

// warmupHotIndex rebuilds the read-WRITE facade's HotIndex from the chunk's
// durable state: manifest-listed runs (drain-verified), then the un-sealed
// packed-row tail, then the stale-format tripwire, and ArmSealing LAST — the
// replay runs disarmed so a failed open writes nothing durable and every
// retry faces the same state.
func warmupHotIndex(store *rocksdb.Store, chunkID chunk.ID) (*HotIndex, error) {
	runDir := filepath.Join(store.Path(), txhashRunDir)
	hotIdx, lastSealed, err := OpenHotIndex(runDir, chunkID.FirstLedger(), rocksdbManifest{store: store})
	if err != nil {
		return nil, err
	}
	if err := replayTail(store, hotIdx, max(lastSealed+1, chunkID.FirstLedger())); err != nil {
		hotIdx.Close()
		return nil, err
	}
	if err := checkRowFormat(store); err != nil {
		hotIdx.Close()
		return nil, err
	}
	hotIdx.ArmSealing()
	return hotIdx, nil
}

// replayTail replays the un-sealed packed rows (ledgers from the sealed
// frontier's successor onward) into the window: the same validateRow +
// ApplyRow pair the write path runs, with the row bytes COPIED out of the
// borrowed iterator buffer (the window retains them). ApplyRow's anchored
// dense-chain check makes a gap, disorder, or wrong first ledger a loud open
// failure. Sealed rows are never touched — their CRC-verified runs are the
// authority — so warmup time scales with the tail, not the chunk.
func replayTail(store *rocksdb.Store, hotIdx *HotIndex, from uint32) error {
	for entry, ierr := range store.IterateRange(txhashCF, rocksdb.EncodeUint32(from), nil) {
		if ierr != nil {
			return fmt.Errorf("txhash: warmup scan %s: %w", txhashCF, ierr)
		}
		if len(entry.Key) != rowKeyLen {
			return fmt.Errorf("txhash: warmup: %d-byte key in %s: stale pre-release txhash format",
				len(entry.Key), txhashCF)
		}
		seq := rocksdb.DecodeUint32(entry.Key)
		count, verr := validateRow(entry.Value)
		if verr != nil {
			return fmt.Errorf("txhash: warmup row %d: %w", seq, verr)
		}
		if aerr := hotIdx.ApplyRow(seq, append([]byte(nil), entry.Value...), count); aerr != nil {
			return aerr
		}
	}
	return nil
}

// checkRowFormat is the open tripwire against the pre-release hash-keyed
// txhash format (one 32-byte-hash-keyed Put per tx): the packed-row CF only
// ever holds 4-byte seq keys, and under byte-lex ordering a 32-byte key
// sorts after any 4-byte key sharing its prefix, so a stale CF's LAST key
// is (with the tail scan's own key-length check covering keys past the
// frontier) reliably long. There is no migration — a stale chunk is a loud
// open failure, never silently reindexed.
func checkRowFormat(store *rocksdb.Store) error {
	last, found, err := store.LastKey(txhashCF)
	if err != nil {
		return fmt.Errorf("txhash: warmup last-key check: %w", err)
	}
	if found && len(last) != rowKeyLen {
		return fmt.Errorf("txhash: %d-byte key in %s: stale pre-release txhash format (no migration; "+
			"re-ingest the chunk)", len(last), txhashCF)
	}
	return nil
}
