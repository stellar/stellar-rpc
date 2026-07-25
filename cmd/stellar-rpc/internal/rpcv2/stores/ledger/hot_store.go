// Package ledger holds the hot ledger store (RocksDB-backed) and
// the cold ledger store (packfile-backed) plus their shared value
// types.
package ledger

import (
	"errors"
	"fmt"
	"iter"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/zstd"
)

// LedgersCF is the column family the hot ledger data lives in. Registered the
// shared per-chunk multi-CF DB (decision (a)).
const LedgersCF = "ledgers"

// CFNames returns the CFs this facade owns, so the hotchunk shared-DB opener
// assembles the union the same way it does for txhash and the event store (every
// facade exports CFNames()).
func CFNames() []string { return []string{LedgersCF} }

// Entry — one (sequence, uncompressed ledger bytes) pair. Compression is
// internal to the store, so callers pass and receive raw bytes here.
type Entry struct {
	Seq   uint32
	Bytes []byte
}

// HotStore — RocksDB-backed hot ledger store. Keys are 4-byte BE sequences;
// values are zstd-compressed (internal). It accumulates one chunk's ledgers
// before freezing; it does not itself range-check writes (the driver's drain loop
// already validates every sequence against the chunk).
//
// Concurrency: all READ methods are safe for concurrent use — with each
// other, with the write side, and alongside the caller-owned
// rocksdb.Store.Close (a read racing Close either completes first or
// observes the closed store and returns stores.ErrStoreClosed). The WRITE
// side (StartCompress / AddPendingToBatch / AddLedgerToBatch / Discard) is
// SINGLE-FLIGHT per store: at most one compression may be in flight at a
// time — hotchunk's single-writer ingest loop is the sole production
// caller. The store owns one reusable encode state under that contract;
// violations panic loudly (see encBusy) rather than racing silently.
type HotStore struct {
	store *rocksdb.Store
	dec   *zstd.Decompressor
	// enc — the store's single OWNED zstd encode state (context + retained
	// dst buffer). The write side is single-flight (see the concurrency
	// contract above), so one state suffices and can never be dropped —
	// its sync.Pool predecessor was measured losing the state ~1-in-5
	// ledgers to GC pool-emptying, re-allocating a worst-case dst each
	// time. encBusy makes a contract violation loud instead of a silent
	// data race. BatchWriter.Put copies synchronously, so the buffer is
	// safe to reuse on the next ledger.
	enc     *zstd.EncoderState
	encBusy atomic.Bool
	// scratch — decode buffers WithLedger lends. Pooled as *[]byte so a buffer
	// the decode had to grow goes back in place of the one that was lent.
	scratch sync.Pool
}

// maxPooledLedgerBytes is the largest decode buffer this store keeps. Capacity
// only ratchets upward and sync.Pool accepts whatever it is given, so without a
// ceiling N concurrent borrows can park N outsized buffers for the store's life.
// 64MiB is several times the largest raw ledgers on the heaviest profile.
const maxPooledLedgerBytes = 64 << 20

// NewWithStore wraps an ALREADY-OPEN rocksdb.Store as a ledger HotStore on
// LedgersCF. The store is owned by the caller — in production, hotchunk.DB
// composes this facade over the shared multi-CF DB and closes that DB once. The
// store must have LedgersCF registered.
func NewWithStore(store *rocksdb.Store) *HotStore {
	return &HotStore{
		store: store,
		dec:   zstd.NewDecompressor(),
		enc:   zstd.NewEncoderState(),
		scratch: sync.Pool{
			New: func() any { return new([]byte) },
		},
	}
}

// PendingCompression is an in-flight background compression started by
// StartCompress: the fork half of the fork/join that takes the ~20ms zstd
// encode off the hot loop's critical path (it runs concurrent with extract
// and the other queue steps, all read-only on the ledger bytes).
//
// Exactly one of AddPendingToBatch (the join) or Discard must be called, on
// the same goroutine that started it; both block until the encode goroutine
// has finished with the input bytes, so the caller's borrowed ledger view
// never outlives its call even on error paths.
type PendingCompression struct {
	seq  uint32
	h    *HotStore
	done chan struct{}

	compressed []byte
	err        error
	consumed   bool
}

// StartCompress begins compressing e.Bytes into a pooled buffer on its own
// goroutine. e.Bytes is read until the returned pending resolves — join or
// Discard before invalidating it.
func (h *HotStore) StartCompress(e Entry) *PendingCompression {
	if !h.encBusy.CompareAndSwap(false, true) {
		panic("ledger: concurrent StartCompress violates the single-flight write contract")
	}
	p := &PendingCompression{seq: e.Seq, h: h, done: make(chan struct{})}
	go func() {
		defer close(p.done)
		p.compressed, p.err = h.enc.Encode(e.Bytes)
	}()
	return p
}

// AddPendingToBatch joins p and queues its compressed ledger into b on
// LedgersCF — the deferred-join twin of AddLedgerToBatch. Put copies
// synchronously, so the pooled buffer is released for reuse before returning.
func (h *HotStore) AddPendingToBatch(b *rocksdb.BatchWriter, p *PendingCompression) error {
	<-p.done
	p.consumed = true
	defer p.release()
	if p.err != nil {
		return p.err
	}
	b.Put(LedgersCF, rocksdb.EncodeUint32(p.seq), p.compressed)
	return nil
}

// Discard joins p and drops its result, returning the pooled state. No-op if
// the pending was already consumed — safe to defer unconditionally alongside
// a conditional AddPendingToBatch.
func (p *PendingCompression) Discard() {
	if p.consumed {
		return
	}
	<-p.done
	p.consumed = true
	p.release()
}

// release clears the single-flight latch. Callers reach here only after
// joining p.done, so the encode goroutine is finished with the owned state.
func (p *PendingCompression) release() {
	p.h.encBusy.Store(false)
	p.compressed = nil
}

// AddLedgerToBatch compresses one ledger and queues its Put into b on
// LedgersCF — the synchronous convenience over the StartCompress /
// AddPendingToBatch fork-join pair (production ingest uses the pair
// directly to overlap compression with the other batch arms; this
// composition keeps ONE write path for the ledger row). Does not commit
// (caller owns the batch). Neither e.Bytes nor the pooled buffer need
// outlive this call. The caller runs inside Store.Batch, whose lifecycle
// RLock + checkOpen is the authoritative closed-store guard, so this adds
// none.
func (h *HotStore) AddLedgerToBatch(b *rocksdb.BatchWriter, e Entry) error {
	return h.AddPendingToBatch(b, h.StartCompress(e))
}

// WithLedger calls fn with seq's decoded bytes; see query.LedgerReader for the
// loan rule. The buffer returns to the store's pool as fn returns.
func (h *HotStore) WithLedger(seq uint32, fn func(raw []byte) error) error {
	buf, _ := h.scratch.Get().(*[]byte)
	defer h.recycle(buf)
	raw, err := h.getLedgerInto((*buf)[:0], seq)
	if err != nil {
		return err
	}
	// A ledger too big for the pooled capacity got a fresh, larger array; keep it.
	*buf = raw
	return fn(slices.Clip(raw))
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
		if start > end {
			return
		}
		// Entry.Bytes aliases the pooled buffer: valid only until the loop body
		// ends, break included. Copy it to retain it.
		buf, _ := h.scratch.Get().(*[]byte)
		defer h.recycle(buf)
		for e, err := range h.store.IterateRange(LedgersCF, rocksdb.EncodeUint32(start), rocksdb.EncodeUint32(end)) {
			if err != nil {
				yield(Entry{}, translateRocksErr(err))
				return
			}
			// e.Value is itself a zero-copy ref into the iterator's internal
			// buffer; decompress it into the reused scratch buffer.
			seq := rocksdb.DecodeUint32(e.Key)
			decoded, derr := h.dec.Decode((*buf)[:0], e.Value)
			if derr != nil {
				yield(Entry{}, decodeErr(seq, derr))
				return
			}
			*buf = decoded
			if !yield(Entry{Seq: seq, Bytes: slices.Clip(decoded)}, nil) {
				return
			}
		}
	}
}

// recycle pools a decode buffer the store still wants back.
func (h *HotStore) recycle(buf *[]byte) {
	if !poolable(*buf) {
		return
	}
	h.scratch.Put(buf)
}

// poolable reports whether a decode buffer is worth keeping. See
// maxPooledLedgerBytes.
func poolable(buf []byte) bool { return cap(buf) <= maxPooledLedgerBytes }

// getLedgerInto decodes seq into dst, nil for a fresh allocation. The decode
// runs inside GetPinned's callback, so the compressed value is never copied.
func (h *HotStore) getLedgerInto(dst []byte, seq uint32) ([]byte, error) {
	var out []byte
	found, err := h.store.GetPinned(LedgersCF, rocksdb.EncodeUint32(seq), func(v []byte) error {
		decoded, derr := h.dec.Decode(dst, v)
		if derr != nil {
			return decodeErr(seq, derr)
		}
		out = decoded
		return nil
	})
	switch {
	case errors.Is(err, stores.ErrCorrupt):
		return nil, err
	case err != nil:
		return nil, translateRocksErr(err)
	case !found:
		return nil, stores.ErrNotFound
	}
	return out, nil
}

// decodeErr reports a stored frame that would not decompress: the store wrote
// it, so this is corruption.
func decodeErr(seq uint32, err error) error {
	return fmt.Errorf("%w: hot decode seq %d: %w", stores.ErrCorrupt, seq, err)
}

// translateRocksErr maps rocksdb-level lifecycle errors to the
// stores sentinels so callers depend only on stores.* errors.
func translateRocksErr(err error) error {
	if errors.Is(err, rocksdb.ErrStoreClosed) {
		return stores.ErrStoreClosed
	}
	return err
}
