package ledger

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"math"
	"sync"
	"sync/atomic"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/txspan"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/zstd"
)

// missingPackOpens counts cold packs whose file was gone on first read.
// Routing only opens packs the catalog snapshot holds, so each count is a
// pack deleted underneath a reader that outlived the deletion grace period —
// or a freeze/metadata bug. Process-wide by design — the metrics exporter
// reads it via MissingPackOpens.
//
//nolint:gochecknoglobals // one tally across all readers; read-only outside this file
var missingPackOpens atomic.Uint64

// MissingPackOpens returns the process-wide count of cold-pack opens that
// found no file. See missingPackOpens.
func MissingPackOpens() uint64 { return missingPackOpens.Load() }

// tableFrontProbes counts the front reads WithTxTable made looking for a
// record's span table. A pack whose app-data says no record carries one
// contributes nothing here, which is the whole point of the flag.
//
//nolint:gochecknoglobals // one tally across all readers; read-only outside this file
var tableFrontProbes atomic.Uint64

// TableFrontProbes returns the process-wide count of cold records whose front
// was read looking for a span table. Compare it with the table-served lookups:
// a gap is probes that found nothing, which the app-data flag is there to
// eliminate for a pack that has no tables at all.
func TableFrontProbes() uint64 { return tableFrontProbes.Load() }

// formatLedgerCold tags the packfile format used by the cold ledger
// store. Shared by the reader and the writer (same package).
const formatLedgerCold packfile.Format = 1

// AppData layout: a leading version byte, then firstSeq (4 BE), then — since
// records grew span tables — a flags byte, the table digest (32) and the
// largest front any record needs (4 BE). lastSeq is derived from
// trailer.TotalItems at open. Shared by the reader and the writer (same
// package). Every app-data blob leads with its own version byte so it is
// self-describing on its own, independent of the trailer Format that names the
// whole encoding.
//
// The two shapes are told apart by LENGTH, not by the version byte, which is
// why the version stays 0x01: a 5-byte blob is a pack written before tables
// existed and a 42-byte one carries the flag, the digest and the front. Both
// are valid; an older reader reads the first five bytes of either and is right
// about them, and this reader treats the short form as a pack with no tables,
// which is what every pack of that vintage is.
const coldAppDataVersion byte = 0x01

const (
	appDataSizeV1    = 1 + 4                               // version byte + firstSeq (uint32 BE)
	appDataSize      = appDataSizeV1 + 1 + sha256.Size + 4 // + flags + table digest + max front
	offAppDataFlags  = appDataSizeV1                       // the flags byte
	offAppDataDigest = appDataSizeV1 + 1                   // the 32-byte table digest
	offAppDataFront  = offAppDataDigest + sha256.Size      // the max front (uint32 BE)
)

// coldFlagTables is app-data flags bit 0: the pack's records MAY carry span
// tables, so a reader looking for one has to read a record's front. Clear
// means none of them does — every ledger fit FrameWindow — and that read is
// pure waste, so a reader skips straight to the whole-record read.
//
// It is deliberately a "may", not a "does": it is set when at least one record
// carried a table, and a reader that probes a record which turns out to have
// none still gets the same answer, only slower.
const coldFlagTables byte = 1 << 0

// coldTailRead is how much of a cold ledger pack's tail the reader pulls in on
// Open, in place of the packfile's 256 KiB default. A chunk's pack holds one
// record per ledger and the record index is a FOR-packed offset per record, so
// 10k records plus the app data and the trailer fit inside this
// comfortably; a pack that somehow does not still opens, at the cost of the
// second read packfile makes when the tail falls short.
//
// The saving is per OPEN, not per read: routing opens a pack the first time a
// request lands on the chunk, and 192 KiB of pages per pack is worth not
// faulting in when the useful part is a few tens of kilobytes.
const coldTailRead = 64 << 10

// coldPackDecoder is the process-wide zstd decoder for cold ledger
// pack records. packfile.RecordDecoder must be concurrent-safe and
// zstd.Decompressor satisfies that, so a single shared instance
// serves every ColdReader. Mirrors the event store's pattern.
//
//nolint:gochecknoglobals // shared by design; the decoder is stateless + concurrent-safe
var coldPackDecoder = zstd.NewDecompressor()

// ColdReader is lazy: OpenColdReader does no synchronous I/O and
// returns no error. OpenPack begins the open in a background
// goroutine immediately; the trailer + AppData are read and validated
// on the first method call, via a sync.OnceValues-cached loadHeader,
// where a failed open also surfaces. Read methods (LastSeq,
// WithLedger, IterateLedgers) are safe for concurrent use; Close
// is NOT — callers must ensure all in-flight reads have returned
// before invoking it, matching the underlying packfile.Reader.Close
// contract.
type ColdReader struct {
	r    *stores.PackReader
	path string
	init func() (coldHeader, error)
}

// coldHeader carries the validated app-data returned by loadHeader and cached
// by sync.OnceValues: the sequence range, whether any record may carry a span
// table, the front a table read must pull to have that table and the ledger
// header in hand, and — for a pack whose app-data carries one — the digest
// over every record's table bytes.
type coldHeader struct {
	firstSeq, lastSeq uint32
	tables            bool
	// maxFront is the largest "table frame + header frame" prefix over the
	// pack's records, so one read of it covers both for EVERY record. Zero for
	// a pack with no tables.
	maxFront       uint32
	tableDigest    [sha256.Size]byte
	hasTableDigest bool
}

// OpenColdReader returns a lazy reader for the cold pack at path.
// It does no synchronous I/O and returns no error for a valid path;
// OpenPack starts the open in the background immediately, and
// trailer + AppData read/validation (plus any open failure) surface
// on the first method call. Uses the package-level coldPackDecoder,
// shared across all readers in the process.
func OpenColdReader(path string) (*ColdReader, error) {
	return openColdReaderWithTail(path, coldTailRead)
}

// openColdReaderWithTail is OpenColdReader with the speculative tail read
// sized explicitly. It is package-private because it is a test seam — the tail
// exercised from both sides, one that covers the pack's index and one that
// does not, without fabricating a pack big enough to overrun the default.
// OpenColdReader is the only way in from outside.
func openColdReaderWithTail(path string, tail int) (*ColdReader, error) {
	if path == "" {
		return nil, stores.ErrInvalidConfig
	}
	c := &ColdReader{
		r: stores.OpenPack(path, packfile.ReaderOptions{
			RecordDecoder:       coldPackDecoder,
			SpeculativeTailSize: tail,
		}),
		path: path,
	}
	c.init = sync.OnceValues(c.loadHeader)
	return c, nil
}

// loadHeader reads the trailer + AppData, enforces format, AppData
// layout, and uint32 overflow on the derived lastSeq. Cached by
// sync.OnceValues; runs at most once per reader.
//
//nolint:funcorder // grouped near init/Open call site for readability; the exported reader API follows
func (c *ColdReader) loadHeader() (coldHeader, error) {
	tr, err := c.r.Trailer()
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			missingPackOpens.Add(1)
		}
		return coldHeader{}, fmt.Errorf("cold: open %q: %w", c.path, err)
	}
	if tr.Format != formatLedgerCold {
		return coldHeader{}, fmt.Errorf("cold %q: expected format %d, got %d", c.path, formatLedgerCold, tr.Format)
	}
	if tr.TotalItems == 0 {
		return coldHeader{}, fmt.Errorf("cold %q: pack contains no items", c.path)
	}
	ad, err := c.r.AppData()
	if err != nil {
		return coldHeader{}, fmt.Errorf("cold: read AppData %q: %w", c.path, err)
	}
	if err := stores.CheckBlobVersion(ad, coldAppDataVersion); err != nil {
		return coldHeader{}, fmt.Errorf("cold %q: AppData: %w", c.path, err)
	}
	if len(ad) != appDataSize && len(ad) != appDataSizeV1 {
		return coldHeader{}, fmt.Errorf("cold %q: expected a %d- or %d-byte AppData, got %d",
			c.path, appDataSizeV1, appDataSize, len(ad))
	}
	first := binary.BigEndian.Uint32(ad[1:])
	if uint64(first)+uint64(tr.TotalItems)-1 > math.MaxUint32 {
		return coldHeader{}, fmt.Errorf(
			"cold %q: lastSeq overflows uint32 (firstSeq=%d, items=%d)",
			c.path, first, tr.TotalItems)
	}
	h := coldHeader{firstSeq: first, lastSeq: first + tr.TotalItems - 1}
	if len(ad) == appDataSize {
		h.tables = ad[offAppDataFlags]&coldFlagTables != 0
		h.tableDigest = [sha256.Size]byte(ad[offAppDataDigest:])
		h.hasTableDigest = true
		h.maxFront = binary.BigEndian.Uint32(ad[offAppDataFront:])
	}
	return h, nil
}

func (c *ColdReader) LastSeq() (uint32, error) { h, err := c.init(); return h.lastSeq, err }

// WithLedger calls fn with seq's bytes; see query.LedgerReader for the loan
// rule. The bytes are the packfile reader's own record buffer, passed through.
//
// A record may be a single zstd frame (every ledger at or under
// FrameWindow — today that is all of them on the public network) or a leading
// skippable frame holding the span table followed by the value's frames; the
// shared decoder handles both, skipping what it must. The pack format is NOT
// bumped for the second shape, so a reader predating it decodes such a record
// to nothing and fails its lookup loudly rather than serving a wrong ledger —
// the deliberate trade for leaving single-frame packs byte-identical.
func (c *ColdReader) WithLedger(seq uint32, fn func(raw []byte) error) error {
	h, err := c.init()
	if err != nil {
		return err
	}
	if seq < h.firstSeq || seq > h.lastSeq {
		return fmt.Errorf("%w: seq %d outside store coverage [%d, %d]",
			stores.ErrOutOfRange, seq, h.firstSeq, h.lastSeq)
	}
	// Carried out rather than returned through the reader: the handle
	// translates every error ReadItem returns, so a caller's error routed
	// that way would be reclassified as a store failure.
	var fnErr error
	rerr := c.r.ReadItem(int(seq-h.firstSeq), func(b []byte) error {
		if cerr := checkHeaderSeq(b, seq); cerr != nil {
			fnErr = fmt.Errorf("cold %q: %w", c.path, cerr)
			return nil
		}
		fnErr = fn(b)
		return nil
	})
	switch {
	case fnErr != nil:
		return fnErr
	case rerr != nil:
		return rerr
	}
	return nil
}

// WithTxTable calls fn with seq's transaction span table, the ledger's own
// header fields, and a reader for the byte spans the table names, reading only
// the frames those spans fall in.
//
// The header comes from the record's FIRST VALUE FRAME, which a framed value
// cuts at the end of the ledger header — so the two scalars a served
// transaction carries, and the union discriminant its element is read under,
// are the ledger's own and not the table's stamps.
//
// ONE read brings back both, always: the pack's app data records the largest
// "table frame + header frame" prefix over its records, so a table read pulls
// exactly that many bytes (or the whole record, if it is shorter) and has the
// table and the header in hand. A record whose own prefix exceeds what the
// pack recorded is corruption of that pack — the reader does not go back for
// more, because a pack that mis-states its own geometry has no geometry to
// trust.
//
// It reports stores.ErrNoTable for a record this reader has no usable table
// for: every record of a ledger that fits ledger.FrameWindow, every record of
// a pack whose app data says none of them carries one, and a table a LATER
// build wrote in a format version this one does not read
// (txspan.ErrUnknownVersion — a newer artifact, not a broken one). That is the
// caller's cue to read the ledger whole and walk it, which is what every read
// did before tables existed.
//
// A record that carries a table the reader cannot use for any OTHER reason is
// an ERROR naming the pack, the ledger and the reason, never an absent
// accelerator: such a pack is a bad artifact, and a read that quietly answered
// from the ledger instead would leave nothing for an operator to see.
//
// THE LOAN RULE covers both arguments: the table and every piece the reader
// returns alias buffers this call owns, valid inside fn only.
func (c *ColdReader) WithTxTable(
	seq uint32, fn func(t txspan.Table, header txspan.LedgerHeader, pieces txspan.PieceReader) error,
) error {
	h, err := c.init()
	if err != nil {
		return err
	}
	if seq < h.firstSeq || seq > h.lastSeq {
		return fmt.Errorf("%w: seq %d outside store coverage [%d, %d]",
			stores.ErrOutOfRange, seq, h.firstSeq, h.lastSeq)
	}
	if !h.tables {
		// The pack itself says no record carries a table, so there is nothing
		// to probe for: the caller goes straight to WithLedger.
		return stores.ErrNoTable
	}
	tableFrontProbes.Add(1)
	offset, size, err := c.r.RecordRange(int(seq - h.firstSeq))
	if err != nil {
		return err
	}
	front := make([]byte, min(int64(h.maxFront), size))
	if err := c.r.ReadAt(front, offset); err != nil {
		return err
	}
	if !zstd.IsSkippable(front) {
		return stores.ErrNoTable
	}
	table, frameLen, err := c.recordTable(seq, front)
	if err != nil {
		return err
	}
	valueAt := offset + int64(frameLen)
	// The pairing check the stamp exists for: a record whose table was built
	// for another ledger describes bytes that are not in this record.
	if stamped := table.LedgerSeq(); stamped != seq {
		return c.unusableTable(seq, fmt.Errorf("it is stamped for ledger %d", stamped))
	}
	pieces := &coldPieces{
		r: c, seq: seq, table: table,
		valueAt: valueAt, valueLen: size - valueAt + offset,
		front: front, frontAt: offset,
	}
	header, err := pieces.header()
	if err != nil {
		return err
	}
	return fn(table, header, pieces.read)
}

// skippableFrameLen reads the whole on-wire length of the skippable frame src
// begins with, from its header alone — the payload need not be present. Its
// error is plain: the one caller classifies it against the record it read.
func skippableFrameLen(src []byte) (int, error) {
	if len(src) < 8 || !zstd.IsSkippable(src) {
		return 0, errors.New("record does not begin with a skippable frame")
	}
	return int(binary.LittleEndian.Uint32(src[4:])) + 8, nil
}

// coldPieces reads a row's two byte spans out of a cold record: it maps each
// span to the run of frames covering it through the table's directory, reads
// those frames, and decodes them. The envelope and the element get separate
// buffers so both stay valid together when they fall in different runs; a
// second span inside the first's run costs neither a read nor a decode.
type coldPieces struct {
	r     *ColdReader
	seq   uint32
	table txspan.Table
	// valueAt is where the value's frames begin in the file and valueLen how
	// many bytes of them there are, so a frame's compressed extent from the
	// directory lands at an absolute offset.
	valueAt, valueLen int64
	// front is the record prefix the table was read from, starting at frontAt
	// in the file. Compressed bytes it already covers are taken from it
	// instead of read again — which is what makes the header frame, and often
	// the first spans, free.
	front   []byte
	frontAt int64

	env, elem coldRun
}

// header decodes the frame the ledger's own header lives in — frame 0, which a
// framed value cuts at the header's end — and reads from it the fields a
// served transaction takes from the ledger rather than from its table, proving
// as it goes that this really is the ledger the read resolved.
//
// It loads the ENVELOPE's run, so a record whose value is one frame pays one
// decode for the header and the spans together. The frame's compressed bytes
// are already in the front the table came from, so this costs no read.
func (p *coldPieces) header() (txspan.LedgerHeader, error) {
	at, err := p.plan(0, p.table.Frame(0).Raw)
	if err != nil {
		return txspan.LedgerHeader{}, err
	}
	if lerr := p.ensure(&p.env, at); lerr != nil {
		return txspan.LedgerHeader{}, lerr
	}
	h, herr := ledgerHeader(p.env.at(0, p.table.Frame(0).Raw), p.seq)
	if herr != nil {
		return txspan.LedgerHeader{}, fmt.Errorf("cold %q: %w", p.r.path, herr)
	}
	return h, nil
}

// coldRun is one decoded run of consecutive frames: the raw bytes of the
// frames covering a span, and the raw offset they start at.
type coldRun struct {
	raw      []byte
	rawStart uint32
	loaded   bool
}

// coldExtent is the run of frames covering one span: where the run's
// compressed bytes lie inside the value, and the raw range they decode to.
type coldExtent struct {
	compStart, compEnd int64
	rawStart, rawEnd   uint32
}

// touches reports whether two extents overlap or abut. Either way one read of
// their union fetches both and brings back no byte neither side needs, which
// is why the union is read instead of two adjacent pieces of it. Extents that
// do NOT touch are never joined: the gap between them is frames this row has
// no use for, and a ledger is megabytes of them.
func (e coldExtent) touches(o coldExtent) bool {
	return e.compStart <= o.compEnd && o.compStart <= e.compEnd
}

// union is the extent covering both, which touches() has proved is the two of
// them and nothing else.
func (e coldExtent) union(o coldExtent) coldExtent {
	return coldExtent{
		compStart: min(e.compStart, o.compStart),
		compEnd:   max(e.compEnd, o.compEnd),
		rawStart:  min(e.rawStart, o.rawStart),
		rawEnd:    max(e.rawEnd, o.rawEnd),
	}
}

// read resolves BOTH spans to their runs of frames before reading either, so
// the two can be weighed against each other: runs that touch are one read of
// the union, and runs that lie apart are read and decoded side by side rather
// than one after the other. A run already in hand — frame 0 is, the header
// came out of it — is neither read nor decoded again.
//
// Both returned slices are valid together, which is what the two buffers are
// for: the element never decodes over the envelope's bytes.
func (p *coldPieces) read(r txspan.Row) ([]byte, []byte, error) {
	size := p.table.RawSize()
	if r.EnvStart > r.EnvEnd || r.EnvEnd > size || r.ElemStart > r.ElemEnd || r.ElemEnd > size {
		return nil, nil, fmt.Errorf("%w: cold %q ledger %d: spans [%d, %d) and [%d, %d) are not inside "+
			"a %d-byte ledger", stores.ErrCorrupt, p.r.path, p.seq,
			r.EnvStart, r.EnvEnd, r.ElemStart, r.ElemEnd, size)
	}
	envAt, err := p.plan(r.EnvStart, r.EnvEnd)
	if err != nil {
		return nil, nil, err
	}
	elemAt, err := p.plan(r.ElemStart, r.ElemEnd)
	if err != nil {
		return nil, nil, err
	}
	// The usual row: the two spans share a frame, or their runs abut. One
	// buffer holds both, and the element costs neither a read nor a decode.
	if envAt.touches(elemAt) {
		if lerr := p.ensure(&p.env, envAt.union(elemAt)); lerr != nil {
			return nil, nil, lerr
		}
		return p.env.at(r.EnvStart, r.EnvEnd), p.env.at(r.ElemStart, r.ElemEnd), nil
	}
	// Runs that lie apart: the element's read and decode go on their own
	// goroutine while the envelope's run on this one, so a row whose pieces
	// are far apart in the ledger — the common shape, with the TxSet ahead of
	// txProcessing — waits once rather than twice.
	var elemErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		elemErr = p.ensure(&p.elem, elemAt)
	}()
	envErr := p.ensure(&p.env, envAt)
	<-done
	if jerr := errors.Join(envErr, elemErr); jerr != nil {
		return nil, nil, jerr
	}
	return p.env.at(r.EnvStart, r.EnvEnd), p.elem.at(r.ElemStart, r.ElemEnd), nil
}

// plan resolves [start, end) to the run of frames covering it, reading
// nothing: a row's two runs are both resolved before either is fetched, which
// is what lets the reader choose between one read and two.
func (p *coldPieces) plan(start, end uint32) (coldExtent, error) {
	_, rawStart, compStart, ok := p.table.RawOffsetFrame(start)
	if !ok {
		return coldExtent{}, p.outside(start)
	}
	// end is exclusive, so the last covering frame is the one holding end-1;
	// an empty span covers the frame it starts in.
	last := start
	if end > start {
		last = end - 1
	}
	hi, lastRawStart, lastCompStart, ok := p.table.RawOffsetFrame(last)
	if !ok {
		return coldExtent{}, p.outside(last)
	}
	lastFrame := p.table.Frame(hi)
	e := coldExtent{
		compStart: int64(compStart),
		compEnd:   int64(lastCompStart) + int64(lastFrame.Compressed),
		rawStart:  rawStart,
		rawEnd:    lastRawStart + lastFrame.Raw,
	}
	if e.compEnd > p.valueLen {
		return coldExtent{}, fmt.Errorf("%w: cold %q ledger %d: the directory's frames end at %d in a %d-byte value",
			stores.ErrCorrupt, p.r.path, p.seq, e.compEnd, p.valueLen)
	}
	return e, nil
}

// ensure puts the frames e names into run, reading and decoding them unless
// the run already holds them. It runs on either goroutine of a split read, so
// it touches nothing but run, the read-only front and the concurrent-safe
// reader and decoder.
func (p *coldPieces) ensure(run *coldRun, e coldExtent) error {
	if run.covers(e.rawStart, e.rawEnd) {
		return nil
	}
	buf, err := p.compressed(p.valueAt+e.compStart, e.compEnd-e.compStart)
	if err != nil {
		return err
	}
	decoded, err := coldPackDecoder.Decode(run.raw[:0], buf)
	if err != nil {
		return fmt.Errorf("%w: cold %q ledger %d: frame decode: %w", stores.ErrCorrupt, p.r.path, p.seq, err)
	}
	if want := int(e.rawEnd - e.rawStart); len(decoded) != want {
		return fmt.Errorf("%w: cold %q ledger %d: frames decoded to %d bytes, the directory says %d",
			stores.ErrCorrupt, p.r.path, p.seq, len(decoded), want)
	}
	run.raw, run.rawStart, run.loaded = decoded, e.rawStart, true
	return nil
}

// compressed returns n bytes of the record at file offset at, out of the front
// read when it covers them and with a read of its own when it does not. The
// returned slice may ALIAS the front buffer, which the call this reader
// belongs to owns — the decode that follows copies what it needs.
func (p *coldPieces) compressed(at, n int64) ([]byte, error) {
	if lo := at - p.frontAt; lo >= 0 && lo+n <= int64(len(p.front)) {
		return p.front[lo : lo+n], nil
	}
	buf := make([]byte, n)
	if err := p.r.r.ReadAt(buf, at); err != nil {
		return nil, err
	}
	return buf, nil
}

func (p *coldPieces) outside(off uint32) error {
	return fmt.Errorf("%w: cold %q ledger %d: offset %d is outside the frame directory",
		stores.ErrCorrupt, p.r.path, p.seq, off)
}

// at slices [start, end) of the raw ledger out of a run that covers it.
func (f *coldRun) at(start, end uint32) []byte {
	return f.raw[start-f.rawStart : end-f.rawStart]
}

func (f *coldRun) covers(start, end uint32) bool {
	return f.loaded && start >= f.rawStart && uint64(end) <= uint64(f.rawStart)+uint64(len(f.raw))
}

// IterateLedgers walks (seq, raw bytes) pairs in [start, end] inclusive,
// ascending. The requested range must be fully contained within the
// store's coverage [firstSeq, lastSeq]; any out-of-range portion — or
// an invalid start > end — is reported as stores.ErrOutOfRange on the
// first yield (no entries are produced). Callers that span chunk
// boundaries should clip explicitly against the store's coverage
// (the chunk's ledger window, or LastSeq) before calling.
func (c *ColdReader) IterateLedgers(start, end uint32) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		h, err := c.init()
		if err != nil {
			yield(Entry{}, err)
			return
		}
		if start > end {
			yield(Entry{}, fmt.Errorf("%w: invalid range start %d > end %d",
				stores.ErrOutOfRange, start, end))
			return
		}
		if start < h.firstSeq || end > h.lastSeq {
			yield(Entry{}, fmt.Errorf("%w: requested [%d, %d] outside store coverage [%d, %d]",
				stores.ErrOutOfRange, start, end, h.firstSeq, h.lastSeq))
			return
		}
		startPos := int(start - h.firstSeq)
		count := int(end-start) + 1

		seq := start
		for item, err := range c.r.ReadRange(startPos, count) {
			if err != nil {
				yield(Entry{}, err)
				return
			}
			if cerr := checkHeaderSeq(item, seq); cerr != nil {
				yield(Entry{}, fmt.Errorf("cold %q: %w", c.path, cerr))
				return
			}
			// Entry.Bytes is the packfile's: valid only until the loop body
			// ends, break included. Copy it to retain it.
			if !yield(Entry{Seq: seq, Bytes: item}, nil) {
				return
			}
			seq++
		}
	}
}

func (c *ColdReader) Close() error { return c.r.Close() }

// recordTable parses the span table out of the record front the caller read
// and returns it with the on-wire length of the frame it rode in, which is
// where the value's own frames begin.
//
// The front is exact, not speculative: it is what the pack says covers the
// widest table and header frame it holds, so everything this needs is already
// in hand and there is no second read to make. A record that does not fit it
// is a record the pack mis-describes, and saying so is the whole point — a
// reader that went back for more would be trusting geometry it has just caught
// being wrong.
//
// Every way the front can fail to yield a usable table is errTableUnusable,
// naming the record and the reason; a table a LATER build wrote is the one
// exception, reported as no table at all so the ledger is walked.
func (c *ColdReader) recordTable(seq uint32, front []byte) (txspan.Table, int, error) {
	payload, frameLen, err := zstd.SkippablePayload(front)
	if err != nil {
		// The payload did not all arrive, but the frame's own length field is
		// in the header that did: naming it says by how much the record
		// overran the front the pack recorded.
		want, herr := skippableFrameLen(front)
		if herr != nil {
			return nil, 0, c.unusableTable(seq, herr)
		}
		return nil, 0, c.unusableTable(seq, fmt.Errorf(
			"its leading frame claims %d bytes, past the %d-byte front the pack records", want, len(front)))
	}
	t, perr := txspan.Parse(payload)
	if errors.Is(perr, txspan.ErrUnknownVersion) {
		// A record a LATER build wrote: this one cannot locate a field in its
		// table, and the ledger is in the same record either way.
		return nil, 0, stores.ErrNoTable
	}
	if perr != nil {
		return nil, 0, c.unusableTable(seq, perr)
	}
	if t.FrameCount() == 0 {
		// Without a directory the header frame has no stated extent, so the
		// front cannot be checked and no span can be resolved.
		return nil, 0, c.unusableTable(seq, errors.New("its span table has no frame directory"))
	}
	if prefix := int64(frameLen) + int64(t.Frame(0).Compressed); prefix > int64(len(front)) {
		return nil, 0, c.unusableTable(seq, fmt.Errorf(
			"its table and header frame take %d bytes, past the %d-byte front the pack records",
			prefix, len(front)))
	}
	return t, frameLen, nil
}

// unusableTable names the record whose table could not be read and the reason
// it could not, so the failure is attributable to one ledger of one pack.
func (c *ColdReader) unusableTable(seq uint32, reason error) error {
	return fmt.Errorf("%w: cold %q ledger %d: unusable span table: %w", stores.ErrCorrupt, c.path, seq, reason)
}
