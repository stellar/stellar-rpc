// Package ledger holds the hot ledger store (RocksDB-backed) and
// the cold ledger store (packfile-backed) plus their shared value
// types.
package ledger

import (
	"errors"
	"fmt"
	"iter"
	"sync"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

// LedgersCF is the column family the hot ledger data lives in. Registered the
// shared per-chunk multi-CF DB (decision (a)).
const LedgersCF = "ledgers"

// CFNames returns the CFs this facade owns, so the hotchunk shared-DB opener
// assembles the union the same way it does for txhash and eventstore (every
// facade exports CFNames()).
func CFNames() []string { return []string{LedgersCF} }

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
// Concurrency: all methods are safe for concurrent use, including use alongside
// the caller-owned rocksdb.Store.Close. A read/write racing Close either completes
// first or observes the closed store and returns stores.ErrStoreClosed. HotStore
// adds no unguarded state of its own — the compressor pool and decompressor are
// both concurrent-safe.
type HotStore struct {
	store   *rocksdb.Store
	chunkID chunk.ID
	dec     *zstd.Decompressor
	// compPool — per-store pool of zstd.Compressors; each concurrent
	// AddLedgerToBatch borrows one for its Encode call.
	compPool sync.Pool
}

// NewWithStore wraps an ALREADY-OPEN rocksdb.Store as a ledger HotStore on
// LedgersCF. The store is owned by the caller — in production, hotchunk.DB
// composes this facade over the shared multi-CF DB and closes that DB once. The
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

// ChunkID returns the chunk this store is bound to (constructor-supplied;
// never reads the store).
func (h *HotStore) ChunkID() chunk.ID { return h.chunkID }

// AddLedgerToBatch compresses one ledger and queues its Put into b on LedgersCF
// — the building block hotchunk uses to fold the ledger write into the one
// shared per-ledger WriteBatch (decision (a)). Does not commit (caller owns the
// batch). Compresses into a fresh buffer BatchWriter.Put copies, so e.Bytes need
// not outlive this call. The caller runs inside Store.Batch, whose lifecycle
// RLock + checkOpen is the authoritative closed-store guard, so this adds none.
func (h *HotStore) AddLedgerToBatch(b *rocksdb.BatchWriter, e Entry) error {
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

// LastSeq returns the highest ledger sequence in the store, or ok=false
// if the store is empty. This is the chunk's authoritative last-committed
// ledger (hotchunk.DB.MaxCommittedSeq reads it). Cheap — a single RocksDB
// boundary seek on the last key.
func (h *HotStore) LastSeq() (uint32, bool, error) {
	k, ok, err := h.store.LastKey(LedgersCF)
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
