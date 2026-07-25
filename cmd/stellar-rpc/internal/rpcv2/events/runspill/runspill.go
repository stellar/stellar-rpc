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
var runMagic = [4]byte{'E', 'V', 'R', '1'}

// HeaderLen is the run-file header size: magic (4) ‖ u64 payload length (8).
// Record offsets within the payload are relative to the END of this header —
// exported so consumers that pread records directly stay aligned with the
// framing if it ever changes.
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
	cap int
}

// NewSlab returns a slab that accepts records until capBytes is reached.
// Capacity is allocated up front — the slab never reallocates, so a
// handed-off slab's memory is stable for the background sorter.
func NewSlab(capBytes int) *Slab {
	capBytes -= capBytes % RecordSize
	return &Slab{buf: make([]byte, 0, capBytes), cap: capBytes}
}

// Append adds one record. It reports false — WITHOUT appending — when the
// slab is full; the caller rotates slabs and retries on the fresh one.
func (s *Slab) Append(term events.TermKey, id uint32) bool {
	if len(s.buf)+RecordSize > s.cap {
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
	for i := 0; i < n; i++ {
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
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("runspill: create %s: %w", tmp, err)
	}
	w := bufio.NewWriterSize(f, 1<<20)
	var hdr [HeaderLen]byte
	copy(hdr[:4], runMagic[:])
	binary.BigEndian.PutUint64(hdr[4:], uint64(len(payload)))
	_, err = w.Write(hdr[:])
	if err == nil {
		_, err = w.Write(payload)
	}
	if err == nil {
		var tr [4]byte
		binary.BigEndian.PutUint32(tr[:], crc32.Checksum(payload, crcTable))
		_, err = w.Write(tr[:])
	}
	if err == nil {
		err = w.Flush()
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("runspill: write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("runspill: rename %s: %w", path, err)
	}
	return nil
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
	r.ids = r.ids[:0]
	var prev uint64
	for i := uint64(0); i < count; i++ {
		v, err := r.uvarint()
		if err != nil {
			return term, nil, err
		}
		abs := v
		if i > 0 {
			if v == 0 {
				return term, nil, fmt.Errorf("%w: zero delta", ErrCorruptRun)
			}
			abs = prev + v
		}
		if abs > 0xFFFFFFFF {
			return term, nil, fmt.Errorf("%w: id overflows uint32", ErrCorruptRun)
		}
		r.ids = append(r.ids, uint32(abs))
		prev = abs
	}
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
		return fmt.Errorf("%w: %v", ErrCorruptRun, err)
	}
	r.crc = crc32.Update(r.crc, crcTable, p)
	r.remain -= uint64(len(p))
	return nil
}

// uvarint reads one payload uvarint through the CRC accounting.
func (r *RunReader) uvarint() (uint64, error) {
	var v uint64
	var shift uint
	var one [1]byte
	for {
		if err := r.consume(one[:]); err != nil {
			return 0, err
		}
		b := one[0]
		if shift >= 64 || (shift == 63 && b > 1) {
			return 0, fmt.Errorf("%w: uvarint overflow", ErrCorruptRun)
		}
		v |= uint64(b&0x7f) << shift
		if b < 0x80 {
			return v, nil
		}
		shift += 7
	}
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
