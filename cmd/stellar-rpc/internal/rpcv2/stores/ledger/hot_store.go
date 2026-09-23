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

// FrameWindow is the raw byte window of one stored ledger frame. A ledger at
// or under it is stored as ONE frame, byte-for-byte what this store wrote
// before frames existed; a larger one is cut into a frame holding its header
// and then fixed FrameWindow-sized frames, so a reader after two byte spans
// decompresses only the frames those spans fall in.
//
// It is a constant, not a configuration knob: it selects the stored frame
// bytes, the same way the encode-workers count does, and a deployment whose
// two materializers disagreed on it would stop writing byte-identical packs.
const FrameWindow = 4 << 20

// frameWindow is the window every write actually uses. It is a variable only
// so a test can lower it below the size of any LedgerCloseMeta a fixture can
// build; production never changes it.
//
//nolint:gochecknoglobals // the constant above is the contract; this is its test seam
var frameWindow = FrameWindow

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
	frames     []txspan.Frame
	err        error
	consumed   bool
}

// StartCompress begins compressing e.Bytes into a pooled buffer on its own
// goroutine. e.Bytes is read until the returned pending resolves — join or
// Discard before invalidating it.
//
// A ledger past FrameWindow is cut into frames (see FrameWindow); one that
// fits, or one whose header this store cannot locate, is the single frame it
// always was. Frames reports the cut once the pending has been joined. The
// cut changes the stored value only — the raw ledger bytes a reader gets back,
// and the content the cold pack's hash is taken over, are unchanged.
func (h *HotStore) StartCompress(e Entry) *PendingCompression {
	if !h.encBusy.CompareAndSwap(false, true) {
		panic("ledger: concurrent StartCompress violates the single-flight write contract")
	}
	p := &PendingCompression{seq: e.Seq, h: h, done: make(chan struct{})}
	go func() {
		defer close(p.done)
		var sizes []zstd.FrameSize
		end, window := frameCut(e.Bytes)
		p.compressed, sizes, p.err = h.enc.EncodeFrames(e.Bytes, end, window)
		p.frames = framesOf(sizes)
	}()
	return p
}

// frameCut resolves how a value is cut: the offset the first frame ends at —
// the end of the ledger's own header — and the window the rest is cut by.
// A value that already fits the window is left alone, and one whose header
// will not resolve gets a window wide enough to hold it whole, so a payload
// this store cannot navigate stays the single frame it has always been.
func frameCut(raw []byte) (int, int) {
	if len(raw) <= frameWindow {
		return 0, frameWindow
	}
	end, err := txspan.HeaderEnd(raw)
	if err != nil {
		return 0, len(raw)
	}
	return end, frameWindow
}

// framesOf converts the encoder's per-frame sizes into the directory shape a
// span table stores. Sizes past uint32 cannot occur: a frame's raw extent is
// bounded by the window and its compressed extent by the value's own length,
// which the store's uint32 offsets already bound.
func framesOf(sizes []zstd.FrameSize) []txspan.Frame {
	if len(sizes) == 0 {
		return nil
	}
	out := make([]txspan.Frame, len(sizes))
	for i, s := range sizes {
		//nolint:gosec // bounded by the ledger value's own uint32-bounded length
		out[i] = txspan.Frame{Compressed: uint32(s.Compressed), Raw: uint32(s.Raw)}
	}
	return out
}

// Frames is the joined compression's frame directory, in order. Valid after
// AddPendingToBatch or Discard has joined the pending; empty when the encode
// failed. It is what stamps the span table's directory, so a reader can map a
// raw offset onto the frame holding it.
func (p *PendingCompression) Frames() []txspan.Frame { return p.frames }

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
// header fields, and a reader for the byte spans the table names, without
// decoding the whole ledger: a span's bytes come from the frames it falls in
// and nothing else.
//
// The header comes from the ledger's FIRST FRAME, which a framed value cuts at
// the end of the header — so the two scalars a served transaction carries, and
// the union discriminant its element is read under, are the ledger's own and
// not the table's stamps. A value stored as one frame is decoded whole for it,
// which is the same decode its spans would have needed anyway.
//
// It returns stores.ErrNoTable when this store holds no table the caller can
// use: no row under the key, or a row a LATER build wrote in a format version
// this one does not read (txspan.ErrUnknownVersion — a newer artifact, not a
// broken one). The table family itself is always there, since every open names
// it and fails without it. The caller then reads the ledger through WithLedger
// and walks it, which is the pre-table read path. A stored table that will not
// parse for any OTHER reason is an ERROR naming the ledger and the reason, not an absent
// accelerator: the store wrote it and cannot read it back, and a read that
// quietly walked instead would leave nothing for an operator to see. A missing
// LEDGER is stores.ErrNotFound, as it is everywhere else.
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
			pieces := h.borrowPieces(seq, t, value)
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

// pieceReader decodes, per read, only the frames a row's two spans fall in.
// Each piece owns a scratch buffer holding the contiguous raw bytes of the run
// of frames covering it; a second piece inside the same run reuses the first's
// buffer rather than decoding it again.
type pieceReader struct {
	h     *HotStore
	seq   uint32
	table txspan.Table
	value []byte

	env, elem *frameRun
}

// frameRun is one decoded run of consecutive frames: the raw bytes of frames
// [lo, hi) and the raw offset they start at.
type frameRun struct {
	buf      *[]byte
	raw      []byte
	rawStart uint32
	lo, hi   int
	loaded   bool
}

// header decodes the frame the ledger's own header lives in — frame 0, which
// a framed value cuts at the header's end — and reads from it the fields a
// served transaction takes from the ledger rather than from its table, proving
// as it goes that this really is the ledger the read resolved.
//
// It loads the ENVELOPE's run, so a value stored as one frame pays one decode
// for the header and the spans together, which is what it paid before.
func (p *pieceReader) header() (txspan.LedgerHeader, error) {
	if p.table.FrameCount() == 0 {
		return txspan.LedgerHeader{}, fmt.Errorf("%w: ledger %d: its span table has no frame directory",
			stores.ErrCorrupt, p.seq)
	}
	raw, err := p.slice(p.env, 0, p.table.Frame(0).Raw)
	if err != nil {
		return txspan.LedgerHeader{}, err
	}
	return ledgerHeader(raw, p.seq)
}

// read is the PieceReader: it resolves each span to the run of frames covering
// it, decodes the runs that are not already in hand, and slices.
func (p *pieceReader) read(r txspan.Row) ([]byte, []byte, error) {
	if err := p.bounds(r); err != nil {
		return nil, nil, err
	}
	env, err := p.slice(p.env, r.EnvStart, r.EnvEnd)
	if err != nil {
		return nil, nil, err
	}
	// The element usually shares the envelope's run; reusing it keeps the
	// common read at one decode.
	if p.env.covers(r.ElemStart, r.ElemEnd) {
		elem, serr := p.slice(p.env, r.ElemStart, r.ElemEnd)
		return env, elem, serr
	}
	elem, err := p.slice(p.elem, r.ElemStart, r.ElemEnd)
	return env, elem, err
}

// bounds proves both spans lie inside the ledger the directory describes, so a
// table paired with the wrong value fails here rather than mid-decode.
func (p *pieceReader) bounds(r txspan.Row) error {
	size := p.table.RawSize()
	if r.EnvStart > r.EnvEnd || r.EnvEnd > size || r.ElemStart > r.ElemEnd || r.ElemEnd > size {
		return fmt.Errorf("%w: ledger %d: spans [%d, %d) and [%d, %d) are not inside a %d-byte ledger",
			stores.ErrCorrupt, p.seq, r.EnvStart, r.EnvEnd, r.ElemStart, r.ElemEnd, size)
	}
	return nil
}

// slice returns [start, end) of the raw ledger, decoding into run the frames
// covering it when run does not already hold them.
func (p *pieceReader) slice(run *frameRun, start, end uint32) ([]byte, error) {
	if !run.covers(start, end) {
		if err := p.load(run, start, end); err != nil {
			return nil, err
		}
	}
	return run.raw[start-run.rawStart : end-run.rawStart], nil
}

// load decodes into run the shortest run of frames covering [start, end).
func (p *pieceReader) load(run *frameRun, start, end uint32) error {
	lo, rawStart, compStart, ok := p.table.RawOffsetFrame(start)
	if !ok {
		return fmt.Errorf("%w: ledger %d: offset %d is outside the frame directory",
			stores.ErrCorrupt, p.seq, start)
	}
	// end is exclusive, so the last covering frame is the one holding end-1;
	// an empty span covers the frame it starts in.
	last := start
	if end > start {
		last = end - 1
	}
	hi, lastRawStart, lastCompStart, ok := p.table.RawOffsetFrame(last)
	if !ok {
		return fmt.Errorf("%w: ledger %d: offset %d is outside the frame directory",
			stores.ErrCorrupt, p.seq, last)
	}
	lastFrame := p.table.Frame(hi)
	compEnd := lastCompStart + lastFrame.Compressed
	if uint64(compEnd) > uint64(len(p.value)) {
		return fmt.Errorf("%w: ledger %d: frames [%d, %d] end at %d in a %d-byte value",
			stores.ErrCorrupt, p.seq, lo, hi, compEnd, len(p.value))
	}
	decoded, err := p.h.dec.Decode((*run.buf)[:0], p.value[compStart:compEnd])
	if err != nil {
		return decodeErr(p.seq, err)
	}
	if want := int(lastRawStart + lastFrame.Raw - rawStart); len(decoded) != want {
		return fmt.Errorf("%w: ledger %d: frames [%d, %d] decoded to %d bytes, the directory says %d",
			stores.ErrCorrupt, p.seq, lo, hi, len(decoded), want)
	}
	*run.buf = decoded
	run.raw, run.rawStart, run.lo, run.hi, run.loaded = decoded, rawStart, lo, hi+1, true
	return nil
}

// covers reports whether the run already holds [start, end).
func (f *frameRun) covers(start, end uint32) bool {
	return f.loaded && start >= f.rawStart && uint64(end) <= uint64(f.rawStart)+uint64(len(f.raw))
}

// release returns both scratch buffers to the store's pool.
func (p *pieceReader) release() {
	p.h.recycle(p.env.buf)
	p.h.recycle(p.elem.buf)
	p.env, p.elem = nil, nil
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

// borrowPieces binds a table and its compressed value to two pooled decode
// buffers, one per piece, so the envelope and the element of a single read
// stay valid together even when they land in different frames.
func (h *HotStore) borrowPieces(seq uint32, t txspan.Table, value []byte) *pieceReader {
	return &pieceReader{
		h: h, seq: seq, table: t, value: value,
		env:  h.borrowScratch(),
		elem: h.borrowScratch(),
	}
}

// borrowScratch takes a decode buffer from the pool for one frame run.
func (h *HotStore) borrowScratch() *frameRun {
	buf, _ := h.scratch.Get().(*[]byte)
	return &frameRun{buf: buf}
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
