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

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

// LedgersCF is the column family the hot ledger data lives in. Registered the
// same whether the DB is the shared per-chunk multi-CF DB (decision (a)) or a
// standalone single-purpose DB (OpenHotStore), so the on-disk layout is
// identical either way.
const LedgersCF = "ledgers"

// Entry — one (sequence, uncompressed ledger bytes) pair. Compression is
// internal to the store, so callers pass and receive raw bytes here.
type Entry struct {
	Seq   uint32
	Bytes []byte
}

// HotStore — RocksDB-backed hot ledger store. Keys are 4-byte BE sequences;
// values are zstd-compressed (internal). Chunk-bound: accumulates one chunk's
// ledgers before freezing, with the binding recorded at open time (ChunkID) so
// the ingest driver can reject a mismatched store. The store does not itself
// range-check writes (the driver's drain loop already validates every sequence).
//
// Concurrency: all methods, including Close, are safe for concurrent
// use. rocksdb.Store.Close CAS-marks the store closed and then drains
// in-flight ops (each holds an RLock for its duration) before releasing
// resources; a read/write racing Close either completes first or
// observes the closed store and returns stores.ErrStoreClosed. Close is
// idempotent. HotStore adds no unguarded state of its own — the
// compressor pool and decompressor are both concurrent-safe.
type HotStore struct {
	store   *rocksdb.Store
	chunkID chunk.ID
	// ownsStore is true on the standalone OpenHotStore path (Close closes the
	// store); false when wrapping the SHARED per-chunk DB via NewWithStore,
	// which hotchunk.DB owns and closes once.
	ownsStore bool
	dec       *zstd.Decompressor
	// compPool — per-store pool of zstd.Compressors; each concurrent AddLedgers
	// borrows one for its Encode call.
	compPool sync.Pool
}

// OpenHotStore validates inputs and returns an open HotStore bound
// to chunkID (see the HotStore doc on chunk binding). path and
// logger are both required; logger is forwarded to the
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
func OpenHotStore(path string, chunkID chunk.ID, logger *supportlog.Entry) (*HotStore, error) {
	if path == "" {
		return nil, stores.ErrInvalidConfig
	}
	if logger == nil {
		return nil, stores.ErrInvalidConfig
	}
	store, err := rocksdb.New(rocksdb.Config{
		Path:           path,
		ColumnFamilies: []string{LedgersCF},
		Logger:         logger,
	})
	if err != nil {
		return nil, err
	}
	h := NewWithStore(store, chunkID)
	h.ownsStore = true
	return h, nil
}

// NewWithStore wraps an ALREADY-OPEN rocksdb.Store as a ledger HotStore on
// LedgersCF. The store is NOT owned (Close is a no-op) — the constructor hotchunk
// uses to compose this facade over the shared multi-CF DB (decision (a)). The
// store must have LedgersCF registered.
func NewWithStore(store *rocksdb.Store, chunkID chunk.ID) *HotStore {
	return &HotStore{
		store:   store,
		chunkID: chunkID,
		dec:     zstd.NewDecompressor(),
		compPool: sync.Pool{
			New: func() any { return zstd.NewCompressor() },
		},
	}
}

// Close releases the store IF this HotStore owns it (standalone OpenHotStore);
// a no-op when wrapping the shared per-chunk DB (NewWithStore), which hotchunk.DB
// closes once. Idempotent; not safe to call alongside in-flight reads/writes.
func (h *HotStore) Close() error {
	if !h.ownsStore {
		return nil
	}
	return h.store.Close()
}

// ChunkID returns the chunk this store is bound to (constructor-supplied;
// never reads the store).
func (h *HotStore) ChunkID() chunk.ID { return h.chunkID }

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
		return translateRocksErr(h.store.Put(LedgersCF, rocksdb.EncodeUint32(e.Seq), compressed))
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
			b.Put(LedgersCF, rocksdb.EncodeUint32(e.Seq), compressed[i])
		}
		return nil
	}))
}

// AddLedgerToBatch compresses one ledger and queues its Put into b on LedgersCF
// — the building block hotchunk uses to fold the ledger write into the one
// shared per-ledger WriteBatch (decision (a)). Does not commit (caller owns the
// batch). Compresses into a fresh buffer BatchWriter.Put copies, so e.Bytes need
// not outlive this call.
func (h *HotStore) AddLedgerToBatch(b *rocksdb.BatchWriter, e Entry) error {
	if h.store.IsClosed() {
		return stores.ErrStoreClosed
	}
	c, _ := h.compPool.Get().(*zstd.Compressor)
	defer h.compPool.Put(c)
	compressed, err := c.Encode(nil, e.Bytes)
	if err != nil {
		return err
	}
	b.Put(LedgersCF, rocksdb.EncodeUint32(e.Seq), compressed)
	return nil
}

// GetLedgerRaw decodes the ledger stored under seq into a fresh,
// caller-owned buffer, or returns stores.ErrNotFound on miss. A zstd
// decode failure surfaces as stores.ErrCorrupt. Sequential bulk readers
// should prefer IterateLedgers, which yields borrows without the
// per-ledger decode allocation.
func (h *HotStore) GetLedgerRaw(seq uint32) ([]byte, error) {
	v, found, err := h.store.Get(LedgersCF, rocksdb.EncodeUint32(seq))
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

// FirstSeq returns the lowest ledger sequence in the store, or ok=false
// if the store is empty. Cheap (a single RocksDB boundary seek): lets a
// caller learn the store's ledger range without an external chunk hint.
func (h *HotStore) FirstSeq() (uint32, bool, error) { return h.edgeSeq(false) }

// LastSeq returns the highest ledger sequence in the store, or ok=false
// if the store is empty.
func (h *HotStore) LastSeq() (uint32, bool, error) { return h.edgeSeq(true) }

//nolint:funcorder // helper grouped with FirstSeq/LastSeq for readability
func (h *HotStore) edgeSeq(last bool) (uint32, bool, error) {
	edge := h.store.FirstKey
	if last {
		edge = h.store.LastKey
	}
	k, ok, err := edge(LedgersCF)
	if err != nil {
		return 0, false, translateRocksErr(err)
	}
	if !ok {
		return 0, false, nil
	}
	return rocksdb.DecodeUint32(k), true, nil
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
		for e, err := range h.store.IterateRange(LedgersCF, rocksdb.EncodeUint32(start), rocksdb.EncodeUint32(end)) {
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
