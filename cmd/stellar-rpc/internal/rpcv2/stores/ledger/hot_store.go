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
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/txspan"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/zstd"
)

// LedgersCF is the column family the hot ledger data lives in. Registered the
// shared per-chunk multi-CF DB (decision (a)).
const LedgersCF = "ledgers"

// TxSpansCF is the column family the per-ledger transaction span tables live
// in, keyed by the same 4-byte BE sequence as LedgersCF. A ledger may have no
// entry here — the table is an accelerator, and a reader without one decodes
// the ledger instead — but every entry is written in the SAME batch as its
// ledger, and a ledger written without a table deletes whatever stood under
// its key, so a table that exists always describes the ledger stored beside
// it.
const TxSpansCF = "txspans"

// CFNames returns the CFs this facade owns, so the hotchunk shared-DB opener
// assembles the union the same way it does for txhash and the event store (every
// facade exports CFNames()).
func CFNames() []string { return []string{LedgersCF, TxSpansCF} }

// tablesWritten and tablesSkipped tally the span table's two WRITE outcomes
// across the process: written into a ledger's batch, and refused by the build
// (an unsupported ledger or a self-check that tripped, so the reader decodes
// instead). A table that cannot be read back is not tallied here: the read
// fails, and the read path counts it where it is surfaced (see
// txhash.TableErrors). Process-wide by design; the metrics exporter reads them
// through the accessors below.
//
//nolint:gochecknoglobals // one tally across all stores; read-only outside this file
var (
	tablesWritten atomic.Uint64
	tablesSkipped atomic.Uint64
)

// TablesWritten returns the process-wide count of span tables queued into a
// ledger's ingest batch.
func TablesWritten() uint64 { return tablesWritten.Load() }

// TablesSkipped returns the process-wide count of ledgers ingested without a
// span table because the build refused them. Routine for a ledger shape the
// builder does not describe; every such ledger is still served, by decoding.
func TablesSkipped() uint64 { return tablesSkipped.Load() }

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

// DefaultZstdEncodeWorkers is the settled ledger-frame encode parallelism
// (zstd.WithWorkers): 2 measured equal to 3 within noise on the hot-ingest
// cell (total p50 33.4 -> 29.2ms, join 7.35 -> 1.9ms vs single-threaded)
// while claiming one fewer core. It is the default every configuration
// surface resolves to (daemon TOML, bench --zstd-workers, hotchunk
// DefaultTuning).
const DefaultZstdEncodeWorkers = 2

// NewWithStore wraps an ALREADY-OPEN rocksdb.Store as a ledger HotStore on
// LedgersCF. The store is owned by the caller — in production, hotchunk.DB
// composes this facade over the shared multi-CF DB and closes that DB once. The
// store must have LedgersCF registered.
//
// zstdEncodeWorkers is FORMAT-AFFECTING (see hotchunk.Tuning's field doc):
// it selects the stored ledger frames' encode mode (0 = single-threaded,
// >=1 = libzstd multithreaded — a different frame byte stream), and the
// walk/backfill cold writer must encode with the SAME value because the
// freeze copies these frames into the cold pack verbatim.
func NewWithStore(store *rocksdb.Store, zstdEncodeWorkers int) *HotStore {
	return &HotStore{
		store: store,
		dec:   zstd.NewDecompressor(),
		enc:   zstd.NewEncoderState(encoderOptions(zstdEncodeWorkers)...),
		scratch: sync.Pool{
			New: func() any { return new([]byte) },
		},
	}
}

// encoderOptions maps the resolved workers count to the encoder's option set:
// <=0 = single-threaded encode, >=1 = libzstd's internal multithreading
// (zstd.WithWorkers). Validation (>=0) lives at the configuration
// boundaries; here any non-positive value is simply single-threaded.
func encoderOptions(workers int) []zstd.CompressorOption {
	if workers > 0 {
		return []zstd.CompressorOption{zstd.WithWorkers(workers)}
	}
	return nil
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

// AddTableToBatch queues seq's encoded span table into b on TxSpansCF. The
// caller puts it in the SAME batch as the ledger and the tx-hash row, so a
// table never outlives or precedes the ledger it describes. Put copies
// synchronously, so table need not outlive this call.
//
// An EMPTY table means the build refused this ledger: the key is DELETED in
// the same batch and the ledger is tallied as skipped. The delete is what
// makes the family's rule hold through a replay — re-ingesting a sequence
// whose build now refuses a table must not leave the previous table standing
// over different bytes. Every ledger passes through here exactly once, so the
// two tallies partition the ingested ledgers.
//
// The store must have the table family, which every write open creates.
func (h *HotStore) AddTableToBatch(b *rocksdb.BatchWriter, seq uint32, table []byte) {
	key := rocksdb.EncodeUint32(seq)
	if len(table) == 0 {
		tablesSkipped.Add(1)
		b.Delete(TxSpansCF, key)
		return
	}
	tablesWritten.Add(1)
	b.Put(TxSpansCF, key, table)
}

// WithTxTable calls fn with seq's transaction span table, the ledger's own
// header fields, and a reader for the byte spans the table names.
//
// The header comes from the LEDGER, not from the table: the two scalars a
// served transaction carries, and the union discriminant its element is read
// under, are read out of the ledger's own header, and the table's stamps are
// asserted against them rather than trusted.
//
// It returns stores.ErrNoTable when this store holds no table the caller can
// use: no row under the key, or a row a LATER build wrote in a format version
// this one does not read (txspan.ErrUnknownVersion — a newer artifact, not a
// broken one). The table family itself is always there, since every open names
// it and fails without it. The caller then reads the ledger through WithLedger
// and walks it, which is the pre-table read path. A stored table that will not
// parse for any OTHER reason is an ERROR naming the ledger and the reason, not
// an absent accelerator: the store wrote it and cannot read it back, and a
// read that quietly walked instead would leave nothing for an operator to see.
// A missing LEDGER is stores.ErrNotFound, as it is everywhere else.
//
// THE LOAN RULE covers everything fn sees: the table is RocksDB's own pinned
// block and every piece the reader returns aliases a decode buffer this store
// lends, all of it valid inside fn only. fn must retain nothing, must not
// block, and must not read back through this store while it runs — the two
// pinned handles are held for its whole duration.
func (h *HotStore) WithTxTable(
	seq uint32, fn func(t txspan.Table, header txspan.LedgerHeader, pieces txspan.PieceReader) error,
) error {
	key := rocksdb.EncodeUint32(seq)
	var (
		fnErr    error
		tableErr error
		// unreadable is a table a LATER build wrote: this one cannot locate a
		// field in it, so the ledger reads as untabled rather than failing.
		unreadable bool
	)
	// One pinned read for both: the table and the value must be live together,
	// and nesting two GetPinned calls would take the store's lifecycle read
	// lock twice, which deadlocks behind a waiting Close.
	foundTable, foundLedger, err := h.store.GetPinnedPair(TxSpansCF, key, LedgersCF, key,
		func(tableBytes, value []byte) error {
			t, perr := txspan.Parse(tableBytes)
			if errors.Is(perr, txspan.ErrUnknownVersion) {
				unreadable = true
				return nil
			}
			if perr != nil {
				tableErr = fmt.Errorf("%w: hot ledger %d: unusable span table: %w",
					stores.ErrCorrupt, seq, perr)
				return nil
			}
			// The pairing check the table exists to allow: a row stored under
			// this key that was built for another ledger describes bytes that
			// are not here.
			if stamped := t.LedgerSeq(); stamped != seq {
				tableErr = fmt.Errorf("%w: hot ledger %d: its span table is stamped for ledger %d",
					stores.ErrCorrupt, seq, stamped)
				return nil
			}
			pieces, derr := h.borrowPieces(seq, t, value)
			if derr != nil {
				tableErr = derr
				return nil
			}
			defer pieces.release()
			header, herr := pieces.header()
			if herr != nil {
				tableErr = herr
				return nil
			}
			fnErr = fn(t, header, pieces.read)
			return nil
		})
	switch {
	case fnErr != nil:
		return fnErr
	case tableErr != nil:
		return tableErr
	case err != nil:
		return translateRocksErr(err)
	case unreadable, !foundTable:
		return stores.ErrNoTable
	case !foundLedger:
		return stores.ErrNotFound
	}
	return nil
}

// pieceReader hands a lookup the byte spans a row names, out of the ledger
// this read decoded. Both pieces are windows on one pooled buffer, so they
// stay valid together for as long as the call that lent them.
type pieceReader struct {
	h     *HotStore
	seq   uint32
	table txspan.Table
	buf   *[]byte
	raw   []byte
}

// header reads from the ledger's own header the fields a served transaction
// takes from the ledger rather than from its table, proving as it goes that
// this really is the ledger the read resolved.
func (p *pieceReader) header() (txspan.LedgerHeader, error) {
	return ledgerHeader(p.raw, p.seq)
}

// read is the PieceReader: it slices both of a row's spans out of the ledger.
func (p *pieceReader) read(r txspan.Row) ([]byte, []byte, error) {
	if err := p.bounds(r); err != nil {
		return nil, nil, err
	}
	return p.raw[r.EnvStart:r.EnvEnd], p.raw[r.ElemStart:r.ElemEnd], nil
}

// bounds proves both spans lie inside the ledger stored under this key, so a
// table paired with the wrong value fails here rather than mid-slice.
func (p *pieceReader) bounds(r txspan.Row) error {
	size := uint64(len(p.raw))
	if r.EnvStart > r.EnvEnd || uint64(r.EnvEnd) > size ||
		r.ElemStart > r.ElemEnd || uint64(r.ElemEnd) > size {
		return fmt.Errorf("%w: ledger %d: spans [%d, %d) and [%d, %d) are not inside a %d-byte ledger",
			stores.ErrCorrupt, p.seq, r.EnvStart, r.EnvEnd, r.ElemStart, r.ElemEnd, size)
	}
	return nil
}

// release returns the scratch buffer to the store's pool.
func (p *pieceReader) release() {
	p.h.recycle(p.buf)
	p.buf, p.raw = nil, nil
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

// borrowPieces decodes the stored value into a pooled buffer and binds it to
// the table that describes it, so a lookup slices spans instead of walking.
func (h *HotStore) borrowPieces(seq uint32, t txspan.Table, value []byte) (*pieceReader, error) {
	buf, _ := h.scratch.Get().(*[]byte)
	raw, err := h.dec.Decode((*buf)[:0], value)
	if err != nil {
		h.recycle(buf)
		return nil, decodeErr(seq, err)
	}
	// A ledger too big for the pooled capacity got a fresh, larger array; keep it.
	*buf = raw
	return &pieceReader{h: h, seq: seq, table: t, buf: buf, raw: raw}, nil
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
