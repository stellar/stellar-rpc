// Package ledger holds the hot ledger store (RocksDB-backed) and
// the cold ledger store (packfile-backed) plus their shared value
// types.
package ledger

import (
	"errors"
	"fmt"
	"iter"
	"sync"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

// Entry — one (sequence, uncompressed ledger bytes) pair. Both
// hot and cold stores compress on write and decompress on read,
// so callers always pass and receive raw ledger bytes here.
type Entry struct {
	Seq   uint32
	Bytes []byte
}

// HotStore — RocksDB-backed hot ledger store. Default-CF only;
// keys are 4-byte big-endian sequences; values are zstd-compressed
// ledger bytes. Compression is internal: callers see raw bytes on
// the boundary.
//
// Concurrency: RocksDB is concurrent-safe for in-flight read/write
// operations. Close, however, must not be called concurrently with
// any in-flight GetLedgerRaw / AddLedgers / IterateLedgers — drain
// reads/writes first. Close is idempotent (delegates to
// rocksdb.Store.Close, which guards with atomic.Bool +
// CompareAndSwap).
type HotStore struct {
	store *rocksdb.Store
	dec   *zstd.Decompressor
	// compPool — per-store pool of zstd.Compressors. Each
	// concurrent AddLedgers borrows one for the duration of its
	// Encode call; the pool's GC finalizer (set inside
	// zstd.NewCompressor) frees the C context when the compressor
	// is dropped between GC cycles.
	compPool sync.Pool
}

// OpenHotStore validates inputs and returns an open HotStore. path
// and logger are both required; logger is forwarded to the
// pkg/rocksdb wrapper (rocksdb writes the on-open state line and
// the close-time Flush warning through it). HotStore itself does
// not emit any logs — the cold store, by contrast, takes no
// logger because packfile is silent. Rides on RocksDB defaults —
// no explicit block cache (RocksDB's per-CF default plus OS page
// cache cover range scans), no bloom filter (callers know in
// advance which sequences this store holds, so it is never asked
// for a key it doesn't have), no WAL cap (graceful Close flushes
// the memtable; ungraceful WAL replay at this scale is sub-second).
// Re-tune only with a workload measurement.
func OpenHotStore(path string, logger *supportlog.Entry) (*HotStore, error) {
	if path == "" {
		return nil, stores.ErrInvalidConfig
	}
	if logger == nil {
		return nil, stores.ErrInvalidConfig
	}
	store, err := rocksdb.New(rocksdb.Config{
		Path:   path,
		Logger: logger,
	})
	if err != nil {
		return nil, err
	}
	return &HotStore{
		store: store,
		dec:   zstd.NewDecompressor(),
		compPool: sync.Pool{
			New: func() any { return zstd.NewCompressor() },
		},
	}, nil
}

// Close releases the underlying RocksDB store. Idempotent —
// delegates to rocksdb.Store.Close. Must not be called concurrently
// with in-flight reads/writes on this HotStore.
func (h *HotStore) Close() error { return h.store.Close() }

// AddLedgers writes (seq, raw-bytes) entries to rocksdb. Bytes is
// the uncompressed ledger payload; AddLedgers compresses each
// entry with zstd before write. Variadic so callers can pass
// individual entries (h.AddLedgers(e)), a literal batch
// (h.AddLedgers(e1, e2, e3)), or a slice (h.AddLedgers(entries...)).
// Zero entries is a no-op; one entry uses Store.Put; multiple
// entries use Store.Batch (one atomic write, one fsync — versus N
// fsyncs for N Put calls).
func (h *HotStore) AddLedgers(entries ...Entry) error {
	if h.store.IsClosed() {
		return stores.ErrStoreClosed
	}
	if len(entries) == 0 {
		return nil
	}
	c, _ := h.compPool.Get().(*zstd.Compressor)
	defer h.compPool.Put(c)

	if len(entries) == 1 {
		e := entries[0]
		compressed, err := c.Encode(nil, e.Bytes)
		if err != nil {
			return err
		}
		return translateRocksErr(h.store.Put("", rocksdb.EncodeUint32(e.Seq), compressed))
	}
	// Multi-entry path: compress each into its own fresh slice so
	// the batch can hold them all simultaneously (the compressor's
	// internal buffer would otherwise be overwritten on the next
	// Encode call).
	compressed := make([][]byte, len(entries))
	for i, e := range entries {
		out, err := c.Encode(nil, e.Bytes)
		if err != nil {
			return err
		}
		compressed[i] = out
	}
	return translateRocksErr(h.store.Batch(func(b *rocksdb.BatchWriter) error {
		for i, e := range entries {
			b.Put("", rocksdb.EncodeUint32(e.Seq), compressed[i])
		}
		return nil
	}))
}

// GetLedgerRaw returns the uncompressed ledger bytes stored under
// seq, or stores.ErrNotFound on miss. A zstd decode failure
// surfaces as stores.ErrCorrupt.
func (h *HotStore) GetLedgerRaw(seq uint32) ([]byte, error) {
	v, found, err := h.store.Get("", rocksdb.EncodeUint32(seq))
	if err != nil {
		return nil, translateRocksErr(err)
	}
	if !found {
		return nil, stores.ErrNotFound
	}
	out, derr := h.dec.Decode(nil, v)
	if derr != nil {
		return nil, fmt.Errorf("%w: hot decode seq %d: %w", stores.ErrCorrupt, seq, derr)
	}
	return out, nil
}

// IterateLedgers walks (seq, uncompressed bytes) pairs in
// [start, end] inclusive, ascending. start > end yields no entries
// and no error. Gaps in the keyspace are visible as missing
// sequences between yielded entries.
func (h *HotStore) IterateLedgers(start, end uint32) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		if h.store.IsClosed() {
			yield(Entry{}, stores.ErrStoreClosed)
			return
		}
		if start > end {
			return
		}
		// scratch is the reused decompression buffer; Entry.Bytes aliases it
		// and is therefore BORROWED — valid only until the next iteration step
		// decodes the following ledger into it. Copy it if you need to retain
		// it past the loop body. The read benches consume each ledger in-scope,
		// so this avoids a per-ledger decode allocation.
		var scratch []byte
		for e, err := range h.store.IterateRange("", rocksdb.EncodeUint32(start), rocksdb.EncodeUint32(end)) {
			if err != nil {
				yield(Entry{}, translateRocksErr(err))
				return
			}
			// e.Value is itself a zero-copy ref into the iterator's internal
			// buffer; decompress it into the reused scratch buffer.
			seq := rocksdb.DecodeUint32(e.Key)
			decoded, derr := h.dec.Decode(scratch[:0], e.Value)
			if derr != nil {
				yield(Entry{}, fmt.Errorf("%w: hot decode seq %d: %w", stores.ErrCorrupt, seq, derr))
				return
			}
			scratch = decoded
			if !yield(Entry{Seq: seq, Bytes: decoded}, nil) {
				return
			}
		}
	}
}

// translateRocksErr maps rocksdb-level lifecycle errors to the
// pkg/stores sentinels so callers depend only on stores.* errors.
func translateRocksErr(err error) error {
	if errors.Is(err, rocksdb.ErrStoreClosed) {
		return stores.ErrStoreClosed
	}
	return err
}
