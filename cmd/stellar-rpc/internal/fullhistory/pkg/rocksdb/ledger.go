package rocksdb

import (
	"iter"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// LedgerStore — RocksDB-backed stores.LedgerStore. Default-CF only;
// keys are 4-byte big-endian sequences; values are opaque bytes.
type LedgerStore struct {
	store *Store
}

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
	return &LedgerStore{store: store}, nil
}

// ledgerTuning — dense sequential keys, range-scan-heavy reads,
// single writer. Standard compaction (unlike txhash) — tombstones
// from DeleteLedgers retention pruning need compaction to reclaim
// space.
func ledgerTuning() Tuning {
	return Tuning{
		WriteBufferMB:                  64,
		MaxWriteBufferNumber:           2,
		Level0FileNumCompactionTrigger: 4,
		Level0SlowdownWritesTrigger:    20,
		Level0StopWritesTrigger:        36,
		TargetFileSizeMB:               64,
		MaxBytesForLevelBaseMB:         128,
		MaxBackgroundJobs:              8,
		MaxOpenFiles:                   10_000,
		BlockCacheMB:                   256,
		BloomFilterBitsPerKey:          12,
		MaxTotalWalSizeMB:              1024,
	}
}

func (l *LedgerStore) Open() error  { return l.store.Open() }
func (l *LedgerStore) Close() error { return l.store.Close() }

// AddLedgers writes a batch of (seq, bytes) entries atomically.
// Bytes stored verbatim. Empty slice is a no-op; single-entry uses
// Store.Put; multi-entry uses Store.Batch.
func (l *LedgerStore) AddLedgers(entries []stores.LedgerEntry) error {
	if l.store.IsClosed() {
		return stores.ErrStoreClosed
	}
	switch len(entries) {
	case 0:
		return nil
	case 1:
		e := entries[0]
		return translateError(l.store.Put(defaultCFName, EncodeUint32(e.Seq), e.Bytes))
	default:
		return translateError(l.store.Batch(func(b *BatchWriter) error {
			for _, e := range entries {
				b.Put(defaultCFName, EncodeUint32(e.Seq), e.Bytes)
			}
			return nil
		}))
	}
}

// DeleteLedgers removes the given sequences. Missing sequences are
// silently skipped. Same single-vs-batch dispatch as AddLedgers.
func (l *LedgerStore) DeleteLedgers(seqs []uint32) error {
	if l.store.IsClosed() {
		return stores.ErrStoreClosed
	}
	switch len(seqs) {
	case 0:
		return nil
	case 1:
		return translateError(l.store.Delete(defaultCFName, EncodeUint32(seqs[0])))
	default:
		return translateError(l.store.Batch(func(b *BatchWriter) error {
			for _, s := range seqs {
				b.Delete(defaultCFName, EncodeUint32(s))
			}
			return nil
		}))
	}
}

// GetLedgerRaw returns the bytes stored under seq, verbatim, or
// stores.ErrNotFound when seq is absent.
func (l *LedgerStore) GetLedgerRaw(seq uint32) ([]byte, error) {
	v, found, err := l.store.Get(defaultCFName, EncodeUint32(seq))
	if err != nil {
		return nil, translateError(err)
	}
	if !found {
		return nil, stores.ErrNotFound
	}
	return v, nil
}

// IterateLedgers walks (seq, bytes) pairs in [start, end] inclusive,
// ascending. start > end yields no entries and no error. Gaps in
// the keyspace are visible as missing sequences between yielded
// entries.
func (l *LedgerStore) IterateLedgers(start, end uint32) iter.Seq2[stores.LedgerEntry, error] {
	return func(yield func(stores.LedgerEntry, error) bool) {
		if l.store.IsClosed() {
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
			// Copy the value: e.Value is a zero-copy ref into the
			// iterator's internal buffer.
			bytesCopy := append([]byte(nil), e.Value...)
			if !yield(stores.LedgerEntry{Seq: seq, Bytes: bytesCopy}, nil) {
				return
			}
		}
	}
}

// GetLedgerRange returns the smallest and largest sequences in the
// store, or (0, 0, nil) when empty. O(1) — backed by RocksDB
// SeekToFirst + SeekToLast on SST file metadata, no data I/O.
func (l *LedgerStore) GetLedgerRange() (uint32, uint32, error) {
	first, last, found, err := l.store.FirstLastKey(defaultCFName)
	if err != nil {
		return 0, 0, translateError(err)
	}
	if !found {
		return 0, 0, nil
	}
	return DecodeUint32(first), DecodeUint32(last), nil
}
