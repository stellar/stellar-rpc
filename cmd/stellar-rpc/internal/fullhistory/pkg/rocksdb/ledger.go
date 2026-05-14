package rocksdb

import (
	"iter"
	"sync"
	"sync/atomic"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// LedgerStore is the concrete RocksDB-backed implementation of
// stores.LedgerStore.
//
// Wraps a Layer-1 *Store with the default column family only.
// Keys are 4-byte big-endian sequence numbers (via EncodeUint32);
// values are caller-supplied opaque bytes stored verbatim.
//
// Two-phase lifecycle: NewLedgerStore validates inputs and returns
// a constructed instance; (*LedgerStore).Open brings the backing
// RocksDB up plus does an O(1) FirstLastKey to initialize the
// in-memory range bounds; (*LedgerStore).Close drains the active
// memtable and tears the underlying store down.
type LedgerStore struct {
	log   *supportlog.Entry
	store *Store

	openOnce sync.Once
	openErr  error
	closed   atomic.Bool

	// rangeMu protects the in-memory (minSeq, maxSeq, hasAny)
	// triple — separate from the wrapper's lifecycle RWMutex; they
	// guard different state.
	// The wrapper's lock prevents teardown-during-op at the C
	// boundary; rangeMu prevents readers from seeing a torn (min,
	// max) pair while a writer is updating one of them.
	rangeMu sync.RWMutex
	minSeq  uint32
	maxSeq  uint32
	hasAny  bool
}

// NewLedgerStore validates inputs and returns a LedgerStore ready
// to be Opened.
//
// path is the on-disk directory the hot ledger store occupies
// (created, with parents, on Open if missing).
// logger receives the wrapper's on-open state line plus any
// close-time diagnostics.
// Both are required.
//
// Tuning is hardcoded inside this constructor for the hot ledger
// workload: bounded retention window (a few thousand to tens of
// thousands of ledgers, total store size in the low GB), high read
// concurrency from query handlers, one writer at a time (live
// ingestion).
func NewLedgerStore(path string, logger *supportlog.Entry) (*LedgerStore, error) {
	if path == "" {
		return nil, ErrInvalidConfig
	}
	if logger == nil {
		return nil, ErrInvalidConfig
	}
	store, err := New(Config{
		Path:   path,
		Logger: logger,
		Tuning: ledgerTuning(),
	})
	if err != nil {
		return nil, err
	}
	return &LedgerStore{log: logger, store: store}, nil
}

// ledgerTuning returns the hardcoded RocksDB knobs for this
// facade's workload.
//
// Ledger keys are dense sequential uint32s with strong locality;
// one CF compacts them naturally into ordered SST files and a range
// scan becomes a single iterator pass.
// Block cache sized to fit the typical hot window working set;
// bloom filter at 12 bits/key for the rare point-lookup that
// doesn't hit cache.
// WAL cap of 1 GB matches the txhash store — common cross-store
// crash-recovery upper bound; graceful Close zeros the WAL via
// Flush-on-Close so this only bounds ungraceful shutdowns.
func ledgerTuning() Tuning {
	return Tuning{
		// Write path.
		// 64 MB memtable absorbs many ledgers of writes before flush.
		WriteBufferMB:        64,
		MaxWriteBufferNumber: 2,

		// L0 / compaction shape.
		// Defaults that match other facades' explicit pattern, so a
		// future grocksdb default change can't silently drift this
		// store.
		Level0FileNumCompactionTrigger: 4,
		Level0SlowdownWritesTrigger:    20,
		Level0StopWritesTrigger:        36,

		// SST file size targets.
		// 64 MB target files match the memtable size — a memtable
		// flush produces one ~64 MB SST.
		// 128 MB level-base size gives one or two L1 files before
		// compaction promotes to L2; the dense-sequential keyspace
		// means each level holds an ordered contiguous range.
		TargetFileSizeMB:       64,
		MaxBytesForLevelBaseMB: 128,

		// Resources.
		MaxBackgroundJobs: 8,
		MaxOpenFiles:      10_000,

		// Read path.
		// 256 MB block cache fits the working set of a typical
		// retention window comfortably; a query that walks
		// IterateLedgers across the whole window hits L0/L1 SSTs
		// that stay cache-resident.
		// 12 bits/key bloom (~0.4% false-positive rate) keeps point
		// lookups (the federated reader handing seq → hot or cold)
		// fast on cache miss.
		BlockCacheMB:          256,
		BloomFilterBitsPerKey: 12,

		// WAL cap.
		// 1 GB matches the txhash store; graceful Close drains the
		// memtable to SST so the cap only bounds ungraceful
		// shutdown recovery.
		MaxTotalWalSizeMB: 1024,
	}
}

// Open opens the underlying RocksDB store and initializes the
// in-memory range bounds via a single FirstLastKey call at the
// layer below.
// Idempotent: first call performs the open + bounds scan,
// subsequent calls return the cached result.
// Creates the on-disk directory (with parents) if missing.
func (l *LedgerStore) Open() error {
	l.openOnce.Do(func() {
		if err := l.store.Open(); err != nil {
			l.openErr = err
			return
		}
		l.openErr = l.refreshRange()
	})
	return l.openErr
}

// Close drains the active memtable to an SST file and then tears
// the underlying RocksDB instance down.
// Idempotent: a second Close is a no-op.
// Best-effort Flush: on Flush failure the log records a warning
// and Close proceeds with teardown — the WAL is still on disk so
// data is durable, the next Open replays the WAL.
func (l *LedgerStore) Close() error {
	if !l.closed.CompareAndSwap(false, true) {
		return nil
	}
	if err := l.store.Flush(); err != nil {
		l.log.WithError(err).Warn("ledger store: graceful close Flush failed; next Open will replay WAL")
	}
	return l.store.Close()
}

// AddLedgers writes a batch of (seq, bytes) entries.
//
// Bytes are stored verbatim — the store has no opinion on encoding
// or compression of the value.
//
// Dispatch:
//
//   - 0 entries → no-op.
//   - 1 entry  → direct Store.Put.
//   - N > 1    → Store.Batch covering all N writes.
//
// On success, widens the in-memory range bounds to include every
// written sequence (a sequence below the current min lowers it,
// above the current max raises it; sequences in between leave
// bounds unchanged).
//
// Closed-fence at entry — see translateError's docstring for the
// Layer-2-facade-wide closed-fence contract.
func (l *LedgerStore) AddLedgers(entries []stores.LedgerEntry) error {
	if l.closed.Load() {
		return stores.ErrStoreClosed
	}
	switch len(entries) {
	case 0:
		return nil
	case 1:
		e := entries[0]
		if err := l.store.Put(defaultCFName, EncodeUint32(e.Seq), e.Bytes); err != nil {
			return translateError(err)
		}
	default:
		err := l.store.Batch(func(b *BatchWriter) error {
			for _, e := range entries {
				b.Put(defaultCFName, EncodeUint32(e.Seq), e.Bytes)
			}
			return nil
		})
		if err != nil {
			return translateError(err)
		}
	}
	l.widenRangeBounds(entries)
	return nil
}

// DeleteLedgers removes the given sequences.
// Idempotent: missing sequences are silently skipped.
//
// Dispatch:
//
//   - 0 seqs → no-op.
//   - 1 seq  → direct Store.Delete.
//   - N > 1  → Store.Batch covering all N deletes.
//
// On success, the in-memory range bounds may shrink: if any deleted
// sequence equals the current minSeq or maxSeq, the bounds are
// recomputed via FirstLastKey (O(1)).
// Deletes that don't touch a boundary leave bounds alone.
func (l *LedgerStore) DeleteLedgers(seqs []uint32) error {
	if l.closed.Load() {
		return stores.ErrStoreClosed
	}
	switch len(seqs) {
	case 0:
		return nil
	case 1:
		if err := l.store.Delete(defaultCFName, EncodeUint32(seqs[0])); err != nil {
			return translateError(err)
		}
	default:
		err := l.store.Batch(func(b *BatchWriter) error {
			for _, s := range seqs {
				b.Delete(defaultCFName, EncodeUint32(s))
			}
			return nil
		})
		if err != nil {
			return translateError(err)
		}
	}
	// Boundary-aware bound update: if any deleted seq equaled the
	// current min or max, a recomputation is needed; otherwise the
	// bounds are still valid.
	// Check boundary touch under RLock first (cheap) before
	// upgrading to recompute.
	if l.deleteTouchedBoundary(seqs) {
		if err := l.refreshRange(); err != nil {
			return translateError(err)
		}
	}
	return nil
}

// GetLedgerRaw returns the bytes stored under seq, verbatim.
// Returns (nil, stores.ErrNotFound) when seq is absent.
func (l *LedgerStore) GetLedgerRaw(seq uint32) ([]byte, error) {
	if l.closed.Load() {
		return nil, stores.ErrStoreClosed
	}
	v, found, err := l.store.Get(defaultCFName, EncodeUint32(seq))
	if err != nil {
		return nil, translateError(err)
	}
	if !found {
		return nil, stores.ErrNotFound
	}
	return v, nil
}

// IterateLedgers walks the (seq, bytes) pairs in [start, end]
// (inclusive), in ascending sequence order.
//
// Implementation seeks directly to start via the Layer-1
// IterateFrom primitive — keys below start are never touched.
// Stops on the first key with seq > end.
//
// Gap handling: yields every key that exists in [start, end].
// Holes in the keyspace are visible to the caller as a missing
// entry between two yielded sequences; the iterator has no
// opinion on whether a gap is fatal — callers decide.
func (l *LedgerStore) IterateLedgers(start, end uint32) iter.Seq2[stores.LedgerEntry, error] {
	return func(yield func(stores.LedgerEntry, error) bool) {
		if l.closed.Load() {
			yield(stores.LedgerEntry{}, stores.ErrStoreClosed)
			return
		}
		if start > end {
			return
		}
		for e, err := range l.store.IterateFrom(defaultCFName, EncodeUint32(start)) {
			if err != nil {
				yield(stores.LedgerEntry{}, translateError(err))
				return
			}
			seq := DecodeUint32(e.Key)
			if seq > end {
				return
			}
			// e.Value is a zero-copy ref into the iterator's
			// internal buffer; copy it so the yielded LedgerEntry
			// is safe to retain past the next iteration step.
			bytesCopy := append([]byte(nil), e.Value...)
			if !yield(stores.LedgerEntry{Seq: seq, Bytes: bytesCopy}, nil) {
				return
			}
		}
	}
}

// GetLedgerRange returns the smallest and largest sequences
// currently in the store, or (0, 0) when the store is empty.
// O(1), no disk IO — reads the in-memory bounds.
func (l *LedgerStore) GetLedgerRange() (uint32, uint32) {
	l.rangeMu.RLock()
	defer l.rangeMu.RUnlock()
	if !l.hasAny {
		return 0, 0
	}
	return l.minSeq, l.maxSeq
}

// widenRangeBounds expands the in-memory (min, max) window to
// include every sequence in entries.
// Called from AddLedgers on the success path only — a failed write
// must not move the bounds.
func (l *LedgerStore) widenRangeBounds(entries []stores.LedgerEntry) {
	l.rangeMu.Lock()
	defer l.rangeMu.Unlock()
	for _, e := range entries {
		if !l.hasAny {
			l.minSeq, l.maxSeq, l.hasAny = e.Seq, e.Seq, true
			continue
		}
		if e.Seq < l.minSeq {
			l.minSeq = e.Seq
		}
		if e.Seq > l.maxSeq {
			l.maxSeq = e.Seq
		}
	}
}

// deleteTouchedBoundary reports whether any of the just-deleted
// sequences was the current minSeq or maxSeq.
// Cheap (RLock + linear scan of seqs); the caller upgrades to a
// recompute only when this returns true.
func (l *LedgerStore) deleteTouchedBoundary(seqs []uint32) bool {
	l.rangeMu.RLock()
	defer l.rangeMu.RUnlock()
	if !l.hasAny {
		return false
	}
	for _, s := range seqs {
		if s == l.minSeq || s == l.maxSeq {
			return true
		}
	}
	return false
}

// refreshRange replaces the in-memory bounds with a fresh
// FirstLastKey scan of the underlying store.
// Called once at Open (initial population) and from DeleteLedgers
// when a boundary deletion invalidated the cached min or max.
// Single grocksdb iterator-metadata read each direction — O(1)
// regardless of store size.
func (l *LedgerStore) refreshRange() error {
	first, last, found, err := l.store.FirstLastKey(defaultCFName)
	if err != nil {
		return err
	}
	l.rangeMu.Lock()
	defer l.rangeMu.Unlock()
	if !found {
		l.minSeq, l.maxSeq, l.hasAny = 0, 0, false
		return nil
	}
	l.minSeq = DecodeUint32(first)
	l.maxSeq = DecodeUint32(last)
	l.hasAny = true
	return nil
}
