// Package runspill is the cold events build's external-memory spill: a
// pointer-free, byte-capped slab of (term, eventID) records that sorts in
// place and spills as a term-sorted run file in the shared packed-row
// encoding (events.AppendTermPostings). Runs are scratch — non-durable,
// wiped on retry — but checksummed: the merge that consumes them must fail
// loudly on corruption rather than build a wrong index (the chunk retry
// contract rebuilds from scratch).
//
// The slab holds fixed 20-byte records (16-byte term ‖ 4-byte big-endian
// event ID). Big-endian IDs make the composite record memcmp-ordered:
// sorting by plain bytes.Compare yields (term ascending, ID ascending),
// which is exactly the (term-sorted, per-term-ascending-IDs) order the
// packed-row encoding requires.
package runspill

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
)

// RecordSize is one slab record: 16-byte term ‖ 4-byte BE event ID.
const RecordSize = 16 + 4

// runMagic heads every run file; a version bump changes the letter.
var runMagic = [4]byte{'E', 'V', 'R', '1'} //nolint:gochecknoglobals // fixed format tag

// HeaderLen is the run-file header size: magic (4) ‖ u64 payload length (8).
// Record offsets within the payload are relative to the END of this header —
// exported so consumers that pread records directly (the hot index's sealed-run
// lookup) stay aligned with the framing if it ever changes.
const HeaderLen = 12

// crcTable is CRC-32C (Castagnoli), the stdlib-available integrity check for
// scratch runs.
var crcTable = crc32.MakeTable(crc32.Castagnoli) //nolint:gochecknoglobals // fixed table

// ErrCorruptRun reports a run file whose structure or checksum does not
// verify. The consumer must abandon the chunk build (retry rebuilds from
// scratch); there is nothing to repair.
var ErrCorruptRun = errors.New("runspill: corrupt run file")

// Slab is the byte-capped in-memory record buffer. Not safe for concurrent
// use: the producer appends on one goroutine and hands the full slab to a
// background sorter (double-buffering is the caller's composition).
type Slab struct {
	buf []byte
}

// NewSlab returns a slab that accepts records until capBytes is reached.
// Capacity is allocated up front — the slab never reallocates, so a
// handed-off slab's memory is stable for the background sorter.
func NewSlab(capBytes int) *Slab {
	capBytes -= capBytes % RecordSize
	return &Slab{buf: make([]byte, 0, capBytes)}
}

// Append adds one record. It reports false — WITHOUT appending — when the
// slab is full; the caller rotates slabs and retries on the fresh one.
func (s *Slab) Append(term events.TermKey, id uint32) bool {
	if len(s.buf)+RecordSize > cap(s.buf) {
		return false
	}
	s.buf = append(s.buf, term[:]...)
	s.buf = binary.BigEndian.AppendUint32(s.buf, id)
	return true
}

// Records returns the number of buffered records.
func (s *Slab) Records() int { return len(s.buf) / RecordSize }

// Reset empties the slab for reuse, keeping its allocation.
func (s *Slab) Reset() { s.buf = s.buf[:0] }

// slabRecords adapts the slab's bytes to sort.Interface: memcmp order over
// whole 20-byte records = (term asc, ID asc) thanks to the BE ID encoding.
type slabRecords struct {
	buf     []byte
	scratch [RecordSize]byte
}

func (r *slabRecords) Len() int { return len(r.buf) / RecordSize }
func (r *slabRecords) Less(i, j int) bool {
	return bytes.Compare(r.buf[i*RecordSize:(i+1)*RecordSize], r.buf[j*RecordSize:(j+1)*RecordSize]) < 0
}

func (r *slabRecords) Swap(i, j int) {
	a := r.buf[i*RecordSize : (i+1)*RecordSize]
	b := r.buf[j*RecordSize : (j+1)*RecordSize]
	copy(r.scratch[:], a)
	copy(a, b)
	copy(b, r.scratch[:])
}

// SortEncode sorts the slab in place (unstable — duplicates are collapsed
// anyway), dedups exact duplicate records, and encodes the result as
// packed-row postings into dst (reused across spills), returning it. IDs
// within a term come out ascending by construction of the composite order.
func (s *Slab) SortEncode(dst []byte) []byte {
	sort.Sort(&slabRecords{buf: s.buf})
	n := s.Records()
	var (
		curTerm  events.TermKey
		ids      []uint32
		haveTerm bool
		prevRec  []byte
	)
	flush := func() {
		if haveTerm {
			dst = events.AppendTermPostings(dst, curTerm, ids)
		}
	}
	for i := range n {
		rec := s.buf[i*RecordSize : (i+1)*RecordSize]
		if prevRec != nil && bytes.Equal(prevRec, rec) {
			continue // exact duplicate record (defensive; ingest never emits them)
		}
		prevRec = rec
		var term events.TermKey
		copy(term[:], rec[:16])
		id := binary.BigEndian.Uint32(rec[16:])
		if !haveTerm || term != curTerm {
			flush()
			curTerm = term
			haveTerm = true
			ids = ids[:0]
		}
		ids = append(ids, id)
	}
	flush()
	return dst
}

// WriteRun writes payload (a SortEncode result) to path as one run file:
// magic ‖ u64 payload length ‖ payload ‖ CRC-32C(payload). The file is
// written via a temp name and renamed, then synced — runs are scratch, but a
// half-written file must never be mistaken for a short valid one.
func WriteRun(path string, payload []byte) error {
	rw, err := NewRunWriter(path)
	if err != nil {
		return err
	}
	// One raw write through RunWriter's framing (incremental CRC, patched
	// header, temp+rename in Close) — byte-identical to the historical
	// whole-payload writer, with a single implementation of the container.
	if werr := rw.writeRaw(payload); werr != nil {
		_ = rw.f.Close()
		_ = os.Remove(rw.path + ".tmp")
		return fmt.Errorf("runspill: write %s: %w", path, werr)
	}
	return rw.Close()
}

// RunWriter streams records into a run file without buffering the payload:
// incremental CRC, header length patched at Close, temp+rename like WriteRun.
// The record-at-a-time transient replaces WriteRun's whole-payload buffer —
// the hot tier's late-chunk merges write ~GBs through here.
type RunWriter struct {
	path    string
	f       *os.File
	w       *bufio.Writer
	crc     uint32
	written uint64
	buf     []byte
}

// NewRunWriter creates path (via a temp name) with a placeholder header.
func NewRunWriter(path string) (*RunWriter, error) {
	f, err := os.Create(path + ".tmp")
	if err != nil {
		return nil, fmt.Errorf("runspill: create %s.tmp: %w", path, err)
	}
	w := bufio.NewWriterSize(f, 1<<20)
	var hdr [HeaderLen]byte
	copy(hdr[:4], runMagic[:])
	if _, err := w.Write(hdr[:]); err != nil {
		_ = f.Close()
		_ = os.Remove(path + ".tmp")
		return nil, fmt.Errorf("runspill: write header %s.tmp: %w", path, err)
	}
	return &RunWriter{path: path, f: f, w: w}, nil
}

// Append writes one term's postings record. Terms must arrive in ascending
// order with ascending IDs — the merge's natural emission order.
func (rw *RunWriter) Append(term events.TermKey, ids []uint32) error {
	rw.buf = events.AppendTermPostings(rw.buf[:0], term, ids)
	return rw.writeRaw(rw.buf)
}

// Close writes the trailer, patches the header's payload length, syncs, and
// renames into place. On error the temp file is removed.
func (rw *RunWriter) Close() error {
	fail := func(err error) error {
		_ = rw.f.Close()
		_ = os.Remove(rw.path + ".tmp")
		return err
	}
	var tr [4]byte
	binary.BigEndian.PutUint32(tr[:], rw.crc)
	if _, err := rw.w.Write(tr[:]); err != nil {
		return fail(fmt.Errorf("runspill: write trailer: %w", err))
	}
	if err := rw.w.Flush(); err != nil {
		return fail(fmt.Errorf("runspill: flush: %w", err))
	}
	var lenb [8]byte
	binary.BigEndian.PutUint64(lenb[:], rw.written)
	if _, err := rw.f.WriteAt(lenb[:], 4); err != nil {
		return fail(fmt.Errorf("runspill: patch header: %w", err))
	}
	if err := rw.f.Sync(); err != nil {
		return fail(fmt.Errorf("runspill: sync: %w", err))
	}
	if err := rw.f.Close(); err != nil {
		return fail(err)
	}
	if err := os.Rename(rw.path+".tmp", rw.path); err != nil {
		_ = os.Remove(rw.path + ".tmp")
		return fmt.Errorf("runspill: rename %s: %w", rw.path, err)
	}
	return nil
}

// writeRaw sends pre-encoded payload bytes through the container accounting —
// incremental CRC, payload-length tally, buffered write. The ONE site that
// touches those fields, so Append and WriteRun cannot drift.
func (rw *RunWriter) writeRaw(p []byte) error {
	rw.crc = crc32.Update(rw.crc, crcTable, p)
	rw.written += uint64(len(p))
	_, err := rw.w.Write(p)
	return err
}

// RunReader streams a run file's (term, ids) records in term order. The
// CRC is accumulated while streaming and verified when the payload is
// exhausted — a consumer that reads to io.EOF has verified integrity; a
// consumer that stops early has not (fine for the merge, which always
// drains or aborts the whole build).
type RunReader struct {
	f       *os.File
	br      *bufio.Reader
	remain  uint64
	crc     uint32
	ids     []uint32
	trailer bool
}

// OpenRun opens path and validates its header.
func OpenRun(path string) (*RunReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("runspill: open %s: %w", path, err)
	}
	br := bufio.NewReaderSize(f, 1<<20)
	var hdr [HeaderLen]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("%w: %s: short header", ErrCorruptRun, path)
	}
	if !bytes.Equal(hdr[:4], runMagic[:]) {
		_ = f.Close()
		return nil, fmt.Errorf("%w: %s: bad magic", ErrCorruptRun, path)
	}
	return &RunReader{f: f, br: br, remain: binary.BigEndian.Uint64(hdr[4:])}, nil
}

// Next returns the next term's postings. The ids slice is reused across
// calls — consume before the next Next. Returns io.EOF after the last
// record, at which point the CRC has been verified.
func (r *RunReader) Next() (events.TermKey, []uint32, error) {
	var term events.TermKey
	if r.remain == 0 {
		if !r.trailer {
			if err := r.verifyTrailer(); err != nil {
				return term, nil, err
			}
		}
		return term, nil, io.EOF
	}
	if r.remain < 16 {
		return term, nil, fmt.Errorf("%w: %d trailing payload bytes", ErrCorruptRun, r.remain)
	}
	if err := r.consume(term[:]); err != nil {
		return term, nil, err
	}
	count, err := r.uvarint()
	if err != nil {
		return term, nil, err
	}
	if count == 0 || count > r.remain {
		return term, nil, fmt.Errorf("%w: id count %d exceeds %d remaining", ErrCorruptRun, count, r.remain)
	}
	// The ID-stream validation (raw-varint reject before accumulation, zero
	// deltas, uint32 overflow) is the shared events.DecodeAscendingIDs core —
	// one definition site for both this streaming decoder and the slice-based
	// DecodePackedRow. r.uvarint feeds it through the CRC/budget accounting.
	ids, err := events.DecodeAscendingIDs(r.uvarint, count, r.ids[:0])
	if err != nil {
		if !errors.Is(err, ErrCorruptRun) { // r.uvarint errors arrive pre-wrapped
			//nolint:errorlint // opaque on purpose: io.EOF inside corruption must not read as clean EOF
			err = fmt.Errorf("%w: %v", ErrCorruptRun, err)
		}
		return term, nil, err
	}
	r.ids = ids
	return term, r.ids, nil
}

// Close releases the file. It does NOT imply integrity — only draining to
// io.EOF does.
func (r *RunReader) Close() error { return r.f.Close() }

// consume reads len(p) payload bytes, folding them into the CRC.
func (r *RunReader) consume(p []byte) error {
	if uint64(len(p)) > r.remain {
		return fmt.Errorf("%w: truncated payload", ErrCorruptRun)
	}
	if _, err := io.ReadFull(r.br, p); err != nil {
		//nolint:errorlint // opaque on purpose: io.EOF inside corruption must not read as clean EOF
		return fmt.Errorf("%w: %v", ErrCorruptRun, err)
	}
	r.crc = crc32.Update(r.crc, crcTable, p)
	r.remain -= uint64(len(p))
	return nil
}

// uvarint reads one payload uvarint through the CRC accounting, delegating
// the varint state machine to the stdlib via the crcByteReader adapter.
func (r *RunReader) uvarint() (uint64, error) {
	v, err := binary.ReadUvarint((*crcByteReader)(r))
	if err != nil {
		//nolint:errorlint // opaque on purpose: io.EOF inside corruption must not read as clean EOF
		return 0, fmt.Errorf("%w: uvarint: %v", ErrCorruptRun, err)
	}
	return v, nil
}

// crcByteReader adapts RunReader's consume (payload budget + CRC accounting)
// to io.ByteReader for binary.ReadUvarint.
type crcByteReader RunReader

func (c *crcByteReader) ReadByte() (byte, error) {
	var one [1]byte
	if err := (*RunReader)(c).consume(one[:]); err != nil {
		return 0, err
	}
	return one[0], nil
}

// verifyTrailer reads and checks the CRC after the payload is exhausted.
func (r *RunReader) verifyTrailer() error {
	var tr [4]byte
	if _, err := io.ReadFull(r.br, tr[:]); err != nil {
		return fmt.Errorf("%w: short trailer", ErrCorruptRun)
	}
	if got := binary.BigEndian.Uint32(tr[:]); got != r.crc {
		return fmt.Errorf("%w: crc mismatch (file %08x, computed %08x)", ErrCorruptRun, got, r.crc)
	}
	r.trailer = true
	return nil
}
