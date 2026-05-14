// Package ledger holds the hot ledger store (RocksDB-backed) and
// its value types. A future cold reader (packfile-backed) will live
// alongside the HotStore in this package.
package ledger

import (
	"iter"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// Entry — one (sequence, opaque bytes) pair. The store stores Bytes
// verbatim and has no opinion on encoding. By convention the
// streaming write path compresses Bytes with zstd before AddLedgers
// and readers decompress after GetLedgerRaw / IterateLedgers; that's
// a caller-side contract, not enforced here.
type Entry struct {
	Seq   uint32
	Bytes []byte
}

// HotStore — RocksDB-backed hot ledger store. Default-CF only; keys
// are 4-byte big-endian sequences; values are opaque bytes.
type HotStore struct {
	store *rocksdb.Store
}

// NewHotStore validates inputs and returns an open HotStore. path
// and logger are both required. Rides on RocksDB defaults — no
// explicit block cache (RocksDB's per-CF default plus OS page cache
// cover range scans), no bloom filter (callers know in advance
// which sequences this store holds, so it is never asked for a key
// it doesn't have), no WAL cap (graceful Close flushes the
// memtable; ungraceful WAL replay at this scale is sub-second).
// Re-tune only with a workload measurement.
func NewHotStore(path string, logger *supportlog.Entry) (*HotStore, error) {
	if path == "" {
		return nil, rocksdb.ErrInvalidConfig
	}
	if logger == nil {
		return nil, rocksdb.ErrInvalidConfig
	}
	store, err := rocksdb.New(rocksdb.Config{
		Path:   path,
		Logger: logger,
	})
	if err != nil {
		return nil, err
	}
	return &HotStore{store: store}, nil
}

func (h *HotStore) Close() error { return h.store.Close() }

// AddLedgers writes a batch of (seq, bytes) entries atomically.
// Bytes stored verbatim. Empty slice is a no-op; single-entry uses
// Store.Put; multi-entry uses Store.Batch.
func (h *HotStore) AddLedgers(entries []Entry) error {
	if h.store.IsClosed() {
		return rocksdb.ErrStoreClosed
	}
	switch len(entries) {
	case 0:
		return nil
	case 1:
		e := entries[0]
		return h.store.Put("", rocksdb.EncodeUint32(e.Seq), e.Bytes)
	default:
		return h.store.Batch(func(b *rocksdb.BatchWriter) error {
			for _, e := range entries {
				b.Put("", rocksdb.EncodeUint32(e.Seq), e.Bytes)
			}
			return nil
		})
	}
}

// GetLedgerRaw returns the bytes stored under seq, verbatim, or
// stores.ErrNotFound on miss.
func (h *HotStore) GetLedgerRaw(seq uint32) ([]byte, error) {
	v, found, err := h.store.Get("", rocksdb.EncodeUint32(seq))
	if err != nil {
		return nil, err
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
func (h *HotStore) IterateLedgers(start, end uint32) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		if h.store.IsClosed() {
			yield(Entry{}, rocksdb.ErrStoreClosed)
			return
		}
		if start > end {
			return
		}
		for e, err := range h.store.IterateRange("", rocksdb.EncodeUint32(start), rocksdb.EncodeUint32(end)) {
			if err != nil {
				yield(Entry{}, err)
				return
			}
			// Copy the value: e.Value is a zero-copy ref into the
			// iterator's internal buffer.
			bytesCopy := append([]byte(nil), e.Value...)
			if !yield(Entry{Seq: rocksdb.DecodeUint32(e.Key), Bytes: bytesCopy}, nil) {
				return
			}
		}
	}
}
