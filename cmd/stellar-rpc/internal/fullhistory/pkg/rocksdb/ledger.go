package rocksdb

import (
	"iter"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// LedgerHotStore — RocksDB-backed stores.LedgerHotStore. Default-CF only;
// keys are 4-byte big-endian sequences; values are opaque bytes.
type LedgerHotStore struct {
	store *Store
}

// NewLedgerHotStore validates inputs and returns an open LedgerHotStore.
// path and logger are both required. The hot ledger store rides on
// RocksDB defaults — no explicit block cache (RocksDB's per-CF
// default plus OS page cache cover range scans), no bloom filter
// (callers know in advance which sequences this store holds, so it
// is never asked for a key it doesn't have), no WAL cap (graceful
// Close flushes the memtable; ungraceful WAL replay at this scale
// is sub-second). Re-tune only with a workload measurement.
func NewLedgerHotStore(path string, logger *supportlog.Entry) (*LedgerHotStore, error) {
	if path == "" {
		return nil, ErrInvalidConfig
	}
	if logger == nil {
		return nil, ErrInvalidConfig
	}
	store, err := New(Config{
		Path:   path,
		Logger: logger,
	})
	if err != nil {
		return nil, err
	}
	return &LedgerHotStore{store: store}, nil
}

func (l *LedgerHotStore) Close() error { return l.store.Close() }

// AddLedgers writes a batch of (seq, bytes) entries atomically.
// Bytes stored verbatim. Empty slice is a no-op; single-entry uses
// Store.Put; multi-entry uses Store.Batch.
func (l *LedgerHotStore) AddLedgers(entries []stores.LedgerEntry) error {
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

// GetLedgerRaw returns the bytes stored under seq, verbatim, or
// stores.ErrNotFound when seq is absent.
func (l *LedgerHotStore) GetLedgerRaw(seq uint32) ([]byte, error) {
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
func (l *LedgerHotStore) IterateLedgers(start, end uint32) iter.Seq2[stores.LedgerEntry, error] {
	return func(yield func(stores.LedgerEntry, error) bool) {
		if l.store.IsClosed() {
			yield(stores.LedgerEntry{}, stores.ErrStoreClosed)
			return
		}
		if start > end {
			return
		}
		for e, err := range l.store.IterateRange(defaultCFName, EncodeUint32(start), EncodeUint32(end)) {
			if err != nil {
				yield(stores.LedgerEntry{}, translateError(err))
				return
			}
			// Copy the value: e.Value is a zero-copy ref into the
			// iterator's internal buffer.
			bytesCopy := append([]byte(nil), e.Value...)
			if !yield(stores.LedgerEntry{Seq: DecodeUint32(e.Key), Bytes: bytesCopy}, nil) {
				return
			}
		}
	}
}
