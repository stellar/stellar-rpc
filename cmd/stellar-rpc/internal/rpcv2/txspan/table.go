// Package txspan encodes, per ledger, the byte spans that locate one
// transaction inside a LedgerCloseMeta: its envelope inside the TxSet and its
// txProcessing element. A reader holding the table slices both pieces straight
// out of the ledger bytes; without it, locating a transaction means decoding
// the whole ledger and hashing TxSet envelopes until one matches, because the
// TxSet is in agreed-set order and txProcessing is in apply order.
//
// Offsets are byte offsets into the RAW (decompressed) LedgerCloseMeta, so a
// table is only valid for the exact ledger bytes it was built from. NOTHING A
// RESPONSE CARRIES COMES FROM THE TABLE: the ledger sequence, the close time
// and the union discriminant the element is read under are all read from the
// ledger's own header (LedgerHeader, ReadLedgerHeader), which a framed value's
// header-only first frame is enough for. The table stamps the sequence and the
// version only so that LookupPieces can assert them against that header and
// refuse a table paired with the wrong ledger; the element's extension width
// is derived from the version rather than stamped, so the two cannot name
// different offsets.
package txspan

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"iter"
	"math"
	"slices"
	"sort"
)

// Encoded layout, all integers big-endian:
//
//	header    magic "TXSP" | version u8 | lcmVersion u8 | reserved u16 |
//	          txCount u32 | indexCount u32 | frameCount u32 |
//	          txProcessingEnd u32 | ledgerSeq u32
//	directory frameCount × (compressedSize u32, rawSize u32)
//	rows      txCount × (envStart u32, envEnd u32, elemStart u32), apply order
//	index     indexCount × (hashPrefix [4]byte, applyIdx u16), prefix order
//	trailer   crc32c u32 over every preceding byte
//
// Rows are in APPLY order, so a row's position is the transaction's apply
// index and its element's end is the next row's elemStart (txProcessingEnd for
// the last) — the elements tile the txProcessing array, which is what lets a
// build size one element instead of all of them. The index is the routing
// layer: one entry per transaction under its hash, plus one per fee-bump under
// its inner hash, so indexCount exceeds txCount by the ledger's fee-bump count.
//
// A table therefore costs 18 bytes per transaction — a 12-byte row and a
// 6-byte index entry — plus 6 more for each fee bump. The index entry is
// deliberately the narrowest thing that still routes: four prefix bytes
// collide often enough to matter only as extra candidates, each settled by the
// 32-byte hash in the element the row points at, and never as a wrong answer.
//
// The uint16 apply index is a CONTRACT ON THE LEDGER, not just on the entry: a
// ledger with more than 65,535 transactions cannot be described by this layout
// and the build refuses it outright (ErrUnsupportedLedger), leaving it to the
// walk. Truncating or capping the count would route a hash to the wrong row.
//
// frameCount == 0 means the directory has not been stamped yet — Encode leaves
// it empty and WithFrames fills it once the value's frames are known; a
// non-empty directory describes the frames the ledger value is cut into, in
// order, a one-entry directory being a whole-frame value.
//
// The header's VERSION is the whole layout's: a build that does not know it
// cannot locate one field past the magic, so it reports the table as absent
// (ErrUnknownVersion) and the reader walks the ledger. A newer artifact — an
// older binary reading a pack or a hot DB a later one wrote — is not
// corruption, and refusing to serve it would take a tier offline over an
// accelerator it is free to ignore.
//
// ledgerSeq is the ledger header's own sequence, and it is a PAIRING CHECK,
// not a served value: a reader asserts it against the sequence it resolved and
// against the header it read, so a table stored against the wrong ledger is
// caught before it can answer. Nothing a response carries comes from this
// header — the ledger's own bytes serve that (see LedgerHeader).
//
// lcmVersion is checked the same way, against the ledger's own union
// discriminant, and the element's extension width is derived from it rather
// than stamped: the two cannot then name different offsets.
const (
	magic         = "TXSP"
	formatVersion = 1
	rowWidth      = 12
	indexWidth    = 6
	prefixLen     = 4
	headerSize    = 28
	dirEntrySize  = 8
	trailerSize   = 4
	// reservedLen is the width of the header's zero-filled padding, which
	// keeps the counts that follow it four-byte aligned.
	reservedLen = 2
)

// Header field offsets.
const (
	offVersion         = 4
	offLCMVersion      = 5
	offReserved        = 6
	offTxCount         = 8
	offIndexCount      = 12
	offFrameCount      = 16
	offTxProcessingEnd = 20
	offLedgerSeq       = 24
)

// Row field offsets, relative to the row's first byte.
const (
	offEnvStart  = 0
	offEnvEnd    = 4
	offElemStart = 8
)

// offApplyIdx is the apply index's offset inside an index entry, past the
// prefix it is keyed by.
const offApplyIdx = prefixLen

// ErrCorrupt classifies a buffer Parse refused: a bad checksum, an unknown
// magic, counts that do not match the buffer length, or a row or index entry
// that contradicts the header. Use errors.Is to separate it from I/O errors
// raised by whatever produced the bytes.
var (
	ErrCorrupt  = errors.New("txspan: corrupt table")
	ErrChecksum = fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
)

// ErrUnknownVersion reports a table whose format version this build does not
// know. It is deliberately NOT an ErrCorrupt: a newer artifact is not a broken
// one, and the binary that cannot read it still has the ledger beside it, so
// every reader maps this to "no table" and walks (see the package doc's
// version contract). Every other parse failure stays an error.
var ErrUnknownVersion = errors.New("txspan: span table version this build does not read")

var crc32cTable = crc32.MakeTable(crc32.Castagnoli) //nolint:gochecknoglobals // immutable lookup table

func crc32c(b []byte) uint32 { return crc32.Checksum(b, crc32cTable) }

// Row locates one transaction's two pieces in the raw LedgerCloseMeta bytes:
// [EnvStart, EnvEnd) is the whole TransactionEnvelope inside the TxSet and
// [ElemStart, ElemEnd) is the whole txProcessing element (a
// TransactionResultMeta for LCM V0 and V1, a TransactionResultMetaV1 for LCM
// V2).
//
// ElemEnd is DERIVED, not stored: the elements tile the txProcessing array, so
// a row's element ends where the next apply-order row's begins. Encode reads
// the last row's ElemEnd as the array's end and stores that one value.
type Row struct {
	EnvStart, EnvEnd, ElemStart, ElemEnd uint32
}

// Match is one index hit: the transaction's row and its 0-based apply index,
// which is ApplicationOrder − 1.
type Match struct {
	Row

	ApplyIdx int
}

// IndexEntry routes a four-byte hash prefix to an apply index. A fee-bump
// transaction contributes two entries with the same apply index, one under
// each of its hashes.
//
// ApplyIdx is a uint16 because the encoded entry is: a ledger past 65,535
// transactions has no table at all (see the layout above), so the narrow field
// can never be asked to hold an index it cannot.
type IndexEntry struct {
	HashPrefix [prefixLen]byte
	ApplyIdx   uint16
}

// Frame is one compressed frame of a framed ledger value: the bytes it
// occupies on the wire and the bytes it expands to.
type Frame struct {
	Compressed, Raw uint32
}

// Layout is the per-ledger header material Encode stamps and Parse hands
// back: the LCM union discriminant the offsets were computed against, the
// ledger header's own sequence, and the ledger value's frame directory (empty
// until WithFrames stamps it). Both scalars are PAIRING CHECKS a reader
// asserts against the ledger it is holding, never values it serves.
type Layout struct {
	LCMVersion uint8
	LedgerSeq  uint32
	Frames     []Frame
}

// Table is an encoded span table that Parse has validated. It ALIASES the
// buffer it was parsed from — nothing is copied — so the caller keeps that
// buffer alive and unmodified for as long as it reads the table.
type Table []byte

// Parse validates b and returns it as a Table. The checksum is verified before
// any field is interpreted, so every single-byte corruption is reported as
// ErrChecksum rather than as a misleading field error. The buffer must end
// exactly where the header's counts say it does; trailing bytes are a
// corruption, not padding.
//
// Past the header, Parse proves the two invariants a reader would otherwise
// discover as a wrong answer: every index entry names a row that exists and
// the entries are in the prefix order Find binary-searches, and the rows'
// element starts ascend strictly and stay inside the txProcessing array. So a
// Table that parses can be read with slice arithmetic alone.
//
// A table stamped with a format version this build does not know is
// ErrUnknownVersion and nothing else — not corruption, and not a partially
// interpreted table.
func Parse(b []byte) (Table, error) {
	if len(b) < headerSize+trailerSize {
		return nil, shortBufferErr(len(b))
	}
	body := b[:len(b)-trailerSize]
	if stored := binary.BigEndian.Uint32(b[len(b)-trailerSize:]); stored != crc32c(body) {
		return nil, ErrChecksum
	}
	if err := checkHeaderStamp(b); err != nil {
		return nil, err
	}
	if _, ok := extBytesFor(b[offLCMVersion]); !ok {
		return nil, fmt.Errorf("%w: LedgerCloseMeta version %d, which this package does not describe",
			ErrCorrupt, b[offLCMVersion])
	}
	for i, r := range b[offReserved : offReserved+reservedLen] {
		if r != 0 {
			return nil, fmt.Errorf("%w: reserved byte %d is %d, want 0", ErrCorrupt, i, r)
		}
	}
	txs := uint64(binary.BigEndian.Uint32(b[offTxCount:]))
	entries := uint64(binary.BigEndian.Uint32(b[offIndexCount:]))
	frames := uint64(binary.BigEndian.Uint32(b[offFrameCount:]))
	want := uint64(headerSize) + frames*dirEntrySize + txs*rowWidth + entries*indexWidth + trailerSize
	if want != uint64(len(b)) {
		return nil, fmt.Errorf("%w: %d rows, %d index entries and %d frames need %d bytes, buffer holds %d",
			ErrCorrupt, txs, entries, frames, want, len(b))
	}
	t := Table(b)
	if err := t.checkRows(); err != nil {
		return nil, err
	}
	if err := t.checkIndex(); err != nil {
		return nil, err
	}
	return t, nil
}

// StampedSeq reads the ledger sequence an encoded table is stamped with,
// proving only that b opens with a table header this package wrote. It is for
// the one producer that copies a table VERBATIM without reading it — the cold
// freeze, which must still prove the row it copied belongs to the ledger it
// copied it for — and deliberately leaves the checksum alone: verifying it
// costs a pass over every table of a chunk, and whoever parses the table
// verifies it then.
//
// A version this build does not know fails HERE, where a serving read would
// walk instead: the sequence it would report sits at an offset only that
// version's layout defines, so the pairing cannot be checked and a copier must
// not carry the table on as if it had been.
func StampedSeq(b []byte) (uint32, error) {
	if len(b) < headerSize+trailerSize {
		return 0, shortBufferErr(len(b))
	}
	if err := checkHeaderStamp(b); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b[offLedgerSeq:]), nil
}

// shortBufferErr reports a buffer too small to be a table at all.
func shortBufferErr(n int) error {
	return fmt.Errorf("%w: %d bytes, shorter than the %d-byte header and trailer",
		ErrCorrupt, n, headerSize+trailerSize)
}

// checkHeaderStamp proves b opens with the magic and the format version this
// package writes — as far as a reader can get before deciding the bytes are a
// table of ours at all.
//
// The two failures are different in kind. Foreign magic is corruption: nothing
// this lineage wrote begins that way. An unknown VERSION is a table a later
// build wrote, whose layout past this point is not ours to interpret, so it is
// ErrUnknownVersion and every reader treats it as no table at all.
func checkHeaderStamp(b []byte) error {
	if string(b[:len(magic)]) != magic {
		return fmt.Errorf("%w: magic %q, want %q", ErrCorrupt, b[:len(magic)], magic)
	}
	if v := b[offVersion]; v != formatVersion {
		return fmt.Errorf("%w: version %d, this build reads %d", ErrUnknownVersion, v, formatVersion)
	}
	return nil
}

// LCMVersion is the LedgerCloseMeta union discriminant the offsets were
// computed against. A READER MUST CHECK IT against the discriminant of the
// ledger it is holding: the same offsets mean different element shapes across
// versions, and the element's extension width is derived from this.
func (t Table) LCMVersion() uint8 { return t[offLCMVersion] }

// ExtBytes is the width of the extension point at the head of a txProcessing
// element under this table's LCM version — the distance from a row's ElemStart
// to the transaction hash its result pair opens with. It is DERIVED from the
// version, never stored, so the two can never name different offsets; Parse
// has already refused a version this package has no width for.
func (t Table) ExtBytes() int {
	ext, _ := extBytesFor(t.LCMVersion())
	return int(ext)
}

// extBytesFor is the width of the extension point an LCM version puts at the
// head of a txProcessing element, and whether this package describes that
// version at all. A TransactionResultMeta (V0 and V1) opens with its result
// pair; a TransactionResultMetaV1 (V2) opens with an ExtensionPoint, which is
// always the four bytes of its only (void) arm.
//
// It is the one place that relation lives: the build reads the width from here
// and so does every read, so the offset a row's element carries its hash at
// cannot be claimed two ways.
func extBytesFor(lcmVersion uint8) (uint8, bool) {
	switch lcmVersion {
	case 0, 1:
		return 0, true
	case 2:
		return 4, true
	default:
		return 0, false
	}
}

// TxCount returns the ledger's transaction count, which is the number of rows.
func (t Table) TxCount() int { return int(binary.BigEndian.Uint32(t[offTxCount:])) }

// IndexCount returns the number of index entries. It exceeds TxCount by one
// per fee-bump transaction, which is routed under both of its hashes.
func (t Table) IndexCount() int { return int(binary.BigEndian.Uint32(t[offIndexCount:])) }

// FrameCount returns the number of entries in the frame directory. Zero means
// the ledger value is one whole frame.
func (t Table) FrameCount() int { return int(binary.BigEndian.Uint32(t[offFrameCount:])) }

// LedgerSeq returns the sequence of the ledger this table was built from,
// stamped at build time. A reader asserts it against the ledger in front of
// it; it is never served.
func (t Table) LedgerSeq() uint32 { return binary.BigEndian.Uint32(t[offLedgerSeq:]) }

// Frame returns directory entry i. i must be in [0, FrameCount()).
func (t Table) Frame(i int) Frame {
	off := headerSize + i*dirEntrySize
	return Frame{
		Compressed: binary.BigEndian.Uint32(t[off:]),
		Raw:        binary.BigEndian.Uint32(t[off+4:]),
	}
}

// Row returns the transaction at apply index i, with its element end resolved
// against its successor. i must be in [0, TxCount()).
func (t Table) Row(i int) Row {
	off := t.rowsOffset() + i*rowWidth
	row := Row{
		EnvStart:  binary.BigEndian.Uint32(t[off+offEnvStart:]),
		EnvEnd:    binary.BigEndian.Uint32(t[off+offEnvEnd:]),
		ElemStart: binary.BigEndian.Uint32(t[off+offElemStart:]),
	}
	if i+1 < t.TxCount() {
		row.ElemEnd = binary.BigEndian.Uint32(t[off+rowWidth+offElemStart:])
	} else {
		row.ElemEnd = t.txProcessingEnd()
	}
	return row
}

// Find yields every transaction whose four-byte hash prefix equals the first
// four bytes of hash, in stored order.
//
// A prefix match is NOT a transaction match: four bytes collide — on a
// six-thousand-transaction ledger a few hashes per ledger share one — and a
// fee-bump transaction is routed under two prefixes. The caller MUST confirm
// the full hash against the element bytes the row points at — the element
// opens, past ExtBytes, with its TransactionResultPair, whose first 32 bytes
// are the transaction hash — and must be prepared for zero surviving
// candidates.
func (t Table) Find(hash [32]byte) iter.Seq[Match] {
	return func(yield func(Match) bool) {
		n := t.IndexCount()
		base := t.indexOffset()
		want := hash[:prefixLen]
		i := sort.Search(n, func(i int) bool {
			return bytes.Compare(t.prefixAt(i), want) >= 0
		})
		for ; i < n; i++ {
			off := base + i*indexWidth
			if !bytes.Equal(t[off:off+prefixLen], want) {
				return
			}
			idx := int(binary.BigEndian.Uint16(t[off+offApplyIdx:]))
			if !yield(Match{ApplyIdx: idx, Row: t.Row(idx)}) {
				return
			}
		}
	}
}

// RawOffsetFrame maps a byte offset in the raw ledger onto the directory entry
// whose frame holds it, returning that frame's index, the offset its raw bytes
// begin at, and the offset its compressed bytes begin at within the value.
// found is false when the directory does not cover off, which is what a table
// paired with the wrong value looks like.
//
// The window the frames were cut at is deliberately not a parameter: a reader
// resolves offsets through the directory alone, so the cut plan can change
// without invalidating a stored table.
func (t Table) RawOffsetFrame(off uint32) (int, uint32, uint32, bool) {
	var raw, comp uint32
	for i := range t.FrameCount() {
		f := t.Frame(i)
		if off < raw+f.Raw {
			return i, raw, comp, true
		}
		raw += f.Raw
		comp += f.Compressed
	}
	return 0, 0, 0, false
}

// RawSize is the ledger value's decompressed length, summed over the frame
// directory.
func (t Table) RawSize() uint32 {
	var raw uint32
	for i := range t.FrameCount() {
		raw += t.Frame(i).Raw
	}
	return raw
}

// checkRows proves the rows tile the txProcessing array: strictly ascending
// element starts, all below the stored array end.
func (t Table) checkRows() error {
	end := t.txProcessingEnd()
	base := t.rowsOffset()
	var prev uint32
	for i := range t.TxCount() {
		start := binary.BigEndian.Uint32(t[base+i*rowWidth+offElemStart:])
		if start >= end {
			return fmt.Errorf("%w: row %d's element starts at %d, at or past the txProcessing end %d",
				ErrCorrupt, i, start, end)
		}
		if i > 0 && start <= prev {
			return fmt.Errorf("%w: row %d's element starts at %d, not past row %d's at %d",
				ErrCorrupt, i, start, i-1, prev)
		}
		prev = start
	}
	return nil
}

// checkIndex proves every entry names an existing row and that the entries are
// in the non-descending prefix order Find binary-searches.
func (t Table) checkIndex() error {
	txs := t.TxCount()
	base := t.indexOffset()
	for i := range t.IndexCount() {
		off := base + i*indexWidth
		if idx := int(binary.BigEndian.Uint16(t[off+offApplyIdx:])); idx >= txs {
			return fmt.Errorf("%w: index entry %d names apply index %d of %d rows",
				ErrCorrupt, i, idx, txs)
		}
		if i > 0 && bytes.Compare(t[off-indexWidth:off-indexWidth+prefixLen], t[off:off+prefixLen]) > 0 {
			return fmt.Errorf("%w: index entry %d's prefix precedes entry %d's", ErrCorrupt, i, i-1)
		}
	}
	return nil
}

// txProcessingEnd is the end offset of the last txProcessing element, which is
// the array's own end.
func (t Table) txProcessingEnd() uint32 { return binary.BigEndian.Uint32(t[offTxProcessingEnd:]) }

// rowsOffset is the byte offset of the first row, past the frame directory.
func (t Table) rowsOffset() int { return headerSize + t.FrameCount()*dirEntrySize }

// indexOffset is the byte offset of the first index entry, past the rows.
func (t Table) indexOffset() int { return t.rowsOffset() + t.TxCount()*rowWidth }

// prefixAt returns index entry i's stored hash prefix, aliasing the table.
func (t Table) prefixAt(i int) []byte {
	off := t.indexOffset() + i*indexWidth
	return t[off : off+prefixLen]
}

// Encode appends the encoded table to dst and returns the extended buffer, so
// a caller may reuse one scratch buffer across ledgers. rows must be in apply
// order and must tile the txProcessing array — each row's ElemEnd is the next
// row's ElemStart — since only the last row's ElemEnd is stored, as the
// array's end.
//
// Encode SORTS index in place by hash prefix — Find binary-searches that
// order — so the caller must not rely on the slice's original order
// afterwards. An empty l.Frames records frameCount 0: the ledger value is one
// whole frame.
func Encode(dst []byte, l Layout, rows []Row, index []IndexEntry) []byte {
	// Big-endian decoding turns the prefix into a uint32 whose numeric order is
	// the prefix's bytes.Compare order, which is the order Find searches in.
	slices.SortFunc(index, func(a, b IndexEntry) int {
		return cmp.Compare(binary.BigEndian.Uint32(a.HashPrefix[:]), binary.BigEndian.Uint32(b.HashPrefix[:]))
	})

	var txProcessingEnd uint32
	if len(rows) > 0 {
		txProcessingEnd = rows[len(rows)-1].ElemEnd
	}

	dst = slices.Grow(dst,
		headerSize+len(l.Frames)*dirEntrySize+len(rows)*rowWidth+len(index)*indexWidth+trailerSize)
	start := len(dst)
	dst = append(dst, magic...)
	dst = append(dst, formatVersion, l.LCMVersion, 0, 0)
	//nolint:gosec // every count is bounded by the ledger's transaction count
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(rows)))
	//nolint:gosec // every count is bounded by the ledger's transaction count
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(index)))
	//nolint:gosec // every count is bounded by the ledger's transaction count
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(l.Frames)))
	dst = binary.BigEndian.AppendUint32(dst, txProcessingEnd)
	dst = binary.BigEndian.AppendUint32(dst, l.LedgerSeq)
	for _, f := range l.Frames {
		dst = binary.BigEndian.AppendUint32(dst, f.Compressed)
		dst = binary.BigEndian.AppendUint32(dst, f.Raw)
	}
	for _, r := range rows {
		dst = binary.BigEndian.AppendUint32(dst, r.EnvStart)
		dst = binary.BigEndian.AppendUint32(dst, r.EnvEnd)
		dst = binary.BigEndian.AppendUint32(dst, r.ElemStart)
	}
	for _, e := range index {
		dst = append(dst, e.HashPrefix[:]...)
		dst = binary.BigEndian.AppendUint16(dst, e.ApplyIdx)
	}
	return binary.BigEndian.AppendUint32(dst, crc32c(dst[start:]))
}

// WithFrames returns table with frames stamped as its frame directory,
// replacing whatever directory it held and recomputing the checksum. The
// input is left untouched; the result is a fresh buffer.
//
// It exists because the two halves of a ledger's write are concurrent: the
// spans are built from the raw bytes while the value is compressed, so the
// frame sizes are only known once both have joined. Stamping is a copy of the
// table — tens to hundreds of kilobytes — not a rebuild.
//
// An empty table stays empty: a ledger the build refused has no directory to
// carry.
func WithFrames(table []byte, frames []Frame) ([]byte, error) {
	if len(table) == 0 {
		return nil, nil
	}
	t, err := Parse(table)
	if err != nil {
		return nil, err
	}
	if len(frames) > math.MaxUint32 {
		return nil, fmt.Errorf("%w: %d frames exceed the directory's count field", ErrCorrupt, len(frames))
	}
	body := t.rowsOffset()
	out := make([]byte, 0, headerSize+len(frames)*dirEntrySize+len(table)-body)
	out = append(out, table[:headerSize]...)
	//nolint:gosec // bounded by the check above
	binary.BigEndian.PutUint32(out[offFrameCount:], uint32(len(frames)))
	for _, f := range frames {
		out = binary.BigEndian.AppendUint32(out, f.Compressed)
		out = binary.BigEndian.AppendUint32(out, f.Raw)
	}
	out = append(out, table[body:len(table)-trailerSize]...)
	return binary.BigEndian.AppendUint32(out, crc32c(out)), nil
}
