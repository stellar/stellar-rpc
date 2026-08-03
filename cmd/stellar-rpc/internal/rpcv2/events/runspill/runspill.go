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
var runMagic = [4]byte{'E', 'V', 'R', '2'} //nolint:gochecknoglobals // fixed format tag

// HeaderLen is the run-file header size: magic (4) ‖ u64 payload length (8) ‖
// u64 record count (8). Record offsets within the payload are relative to the
// END of this header — exported so consumers that pread records directly (the
// hot index's sealed-run lookup) stay aligned with the framing if it ever
// changes. The record count lets a reopen size its routing state (bloom,
// fences) exactly before draining, instead of buffering per-record state for
// a second pass; OpenRun bounds it and the drain cross-checks it.
const HeaderLen = 20

// minRecordBytes is the smallest possible encoded record: 16-byte term ‖
// 1-byte id count ‖ 1-byte id. Bounds how many records a payload can hold,
// so a corrupt header count is rejected before it can size an allocation.
const minRecordBytes = 18

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
	// ids is EmitSorted's reused postings buffer. Slab-owned so that its
	// lifetime rides the same hand-off that protects buf: whoever holds the
	// slab holds the buffer, and nothing else can touch either.
	ids []uint32
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

// EmitSorted sorts the slab in place (unstable — exact duplicate records are
// collapsed anyway), dedups, and walks the result grouped by term: one emit
// per unique term, terms ascending, IDs within a term ascending by
// construction of the composite record order. The ids slice is slab-owned
// and reused across emits — consume it before the next call. Streamed into
// RunWriter.Append this IS the spill: the same record-at-a-time idiom every
// other run producer uses, with no whole-payload buffer in between.
func (s *Slab) EmitSorted(emit func(term events.TermKey, ids []uint32) error) error {
	sort.Sort(&slabRecords{buf: s.buf})
	n := s.Records()
	var (
		curTerm  events.TermKey
		haveTerm bool
		prevRec  []byte
	)
	s.ids = s.ids[:0]
	flush := func() error {
		if !haveTerm {
			return nil
		}
		return emit(curTerm, s.ids)
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
			if err := flush(); err != nil {
				return err
			}
			curTerm = term
			haveTerm = true
			s.ids = s.ids[:0]
		}
		s.ids = append(s.ids, id)
	}
	return flush()
}

// RunWriter streams records into a run file without buffering the payload:
// incremental CRC, header patched at Commit. Everything that produces a run
// writes through here — the spiller's sorted-slab walk, the seal, the hot
// tier's late-chunk merges (~GBs), the freeze scan — one write path, one
// container implementation.
//
// Two-phase lifecycle: the file is created at its FINAL name and is not a run
// until Commit — flush, patch header, fsync, close — has returned nil; Close
// (a no-op once either has run) unlinks it. Call sites `defer rw.Close()` and
// commit explicitly on success: abandonment is the default, publication the
// deliberate act.
//
// There is no tmp+rename step, because nothing anywhere trusts a run by NAME
// — the discipline the rest of the tree already follows (ingest/doc.go's
// artifact model, txhash's run ladder). The hot engines trust only the
// manifest, which may name a run solely after its Commit and the dirent
// barrier that follows (stores/internal/runset), and warmup deletes every
// file the manifest does not list; the cold build's runs live in a scratch
// dir wiped before its first write (NewSpiller, FreezeColdFromStore), so a
// crashed attempt's leftovers cannot be mistaken for this attempt's; and a
// torn payload fails the drain's CRC. A half-written file under a final name
// is garbage nothing reads, exactly as its .tmp form was.
type RunWriter struct {
	path    string
	f       *os.File
	w       *bufio.Writer
	crc     uint32
	written uint64
	records uint64
	buf     []byte
	done    bool
}

// NewRunWriter creates path — the run's FINAL name — with a placeholder
// header. os.Create truncates, which is safe because a run name is never
// reused while a run of that name exists: the hot engines' seal sequence
// resumes past every manifest-listed run (runset.NextSealSeq) and only rises
// within a process, and the cold build's scratch dirs are wiped before their
// first write.
func NewRunWriter(path string) (*RunWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("runspill: create %s: %w", path, err)
	}
	w := bufio.NewWriterSize(f, 1<<20)
	var hdr [HeaderLen]byte
	copy(hdr[:4], runMagic[:])
	if _, err := w.Write(hdr[:]); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("runspill: write header %s: %w", path, err)
	}
	return &RunWriter{path: path, f: f, w: w}, nil
}

// Append writes one term's postings record. Terms must arrive in ascending
// order with ascending IDs — the merge's natural emission order.
func (rw *RunWriter) Append(term events.TermKey, ids []uint32) error {
	rw.buf = events.AppendTermPostings(rw.buf[:0], term, ids)
	rw.records++
	return rw.writeRaw(rw.buf)
}

// Written returns the payload bytes appended so far — i.e. the NEXT record's
// payload-relative offset. Consumers that build routing state (fences) in the
// same pass as the write read it before each Append, so their offsets can
// never drift from the bytes actually written.
func (rw *RunWriter) Written() int64 {
	return int64(rw.written) //nolint:gosec // payload length, far below 2^63
}

// Commit publishes the run: trailer, patched header (payload length and
// record count), fsync, close — the same ladder txhash's writeRun and the
// cold .bin stream writer run, and the reason no run needs a rename. Every
// step is load-bearing: the flush precedes the WriteAt because the
// placeholder header may still be sitting in the buffer and a later flush
// would lay it back over the patch; the patch precedes the Sync so one Sync
// covers header and payload; the Close error is checked because on many
// filesystems ENOSPC/EIO surface only when the fd is consumed. Any failure
// routes through Close, so a failed Commit leaves nothing behind.
func (rw *RunWriter) Commit() error {
	fail := func(err error) error {
		rw.Close()
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
	var hdr [16]byte
	binary.BigEndian.PutUint64(hdr[:8], rw.written)
	binary.BigEndian.PutUint64(hdr[8:], rw.records)
	if _, err := rw.f.WriteAt(hdr[:], 4); err != nil {
		return fail(fmt.Errorf("runspill: patch header: %w", err))
	}
	if err := rw.f.Sync(); err != nil {
		return fail(fmt.Errorf("runspill: sync: %w", err))
	}
	if err := rw.f.Close(); err != nil {
		return fail(fmt.Errorf("runspill: close %s: %w", rw.path, err))
	}
	rw.done = true
	return nil
}

// Close abandons the run: it closes the writer and unlinks the file. Every
// other two-phase writer in the tree spells the abandonment verb the same
// way (packfile.Writer, ledger.ColdWriter, event.ColdWriter, txhash's cold
// .bin stream); the first three unlink too, while the .bin stream only
// closes — its completion record, not the file's absence, is the authority
// on existence, so the partial is inert scratch the retry overwrites.
// A no-op after Commit or a prior Close, so call sites defer it
// unconditionally.
func (rw *RunWriter) Close() {
	if rw.done {
		return
	}
	rw.done = true
	_ = rw.f.Close()
	_ = os.Remove(rw.path)
}

// writeRaw sends pre-encoded payload bytes through the container accounting —
// incremental CRC, payload-length tally, buffered write. The ONE site that
// touches those fields: whatever grows the payload grows the CRC with it.
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
	f          *os.File
	br         *bufio.Reader
	payloadLen uint64
	remain     uint64
	records    uint64
	read       uint64
	crc        uint32
	ids        []uint32
	trailer    bool
}

// OpenRun opens path and validates its header: magic, payload length against
// the file's actual size, and record count against the smallest possible
// record — both bounds hold for any file the writer produced, so a violation
// is corruption caught before either number can size anything.
func OpenRun(path string) (*RunReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("runspill: open %s: %w", path, err)
	}
	fail := func(format string, args ...any) (*RunReader, error) {
		_ = f.Close()
		return nil, fmt.Errorf("%w: %s: %s", ErrCorruptRun, path, fmt.Sprintf(format, args...))
	}
	br := bufio.NewReaderSize(f, 1<<20)
	var hdr [HeaderLen]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		return fail("short header")
	}
	if !bytes.Equal(hdr[:4], runMagic[:]) {
		return fail("bad magic")
	}
	payloadLen := binary.BigEndian.Uint64(hdr[4:12])
	records := binary.BigEndian.Uint64(hdr[12:20])
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("runspill: stat %s: %w", path, err)
	}
	maxPayload := fi.Size() - HeaderLen - 4
	if maxPayload < 0 || payloadLen > uint64(maxPayload) {
		return fail("payload length %d exceeds file capacity %d", payloadLen, max(maxPayload, 0))
	}
	if records > payloadLen/minRecordBytes {
		return fail("record count %d exceeds payload capacity %d", records, payloadLen/minRecordBytes)
	}
	return &RunReader{f: f, br: br, payloadLen: payloadLen, remain: payloadLen, records: records}, nil
}

// Records returns the header's record count: how many Next calls a full
// drain yields. Validated against payload capacity at open and against the
// actual drain at io.EOF, so routing state may be sized from it up front.
func (r *RunReader) Records() uint64 { return r.records }

// Offset returns payload bytes consumed so far — between Next calls, the
// payload-relative offset of the next record, the read side of
// RunWriter.Written. Consumers that build routing state read it before each
// Next, mirroring the write pass, so offsets cannot drift between the two.
func (r *RunReader) Offset() int64 {
	return int64(r.payloadLen - r.remain) //nolint:gosec // bounded by payloadLen, checked against file size at open
}

// Next returns the next term's postings. The ids slice is reused across
// calls — consume before the next Next. Returns io.EOF after the last
// record, at which point the CRC has been verified.
func (r *RunReader) Next() (events.TermKey, []uint32, error) {
	var term events.TermKey
	if r.remain == 0 {
		if err := r.finish(); err != nil {
			return term, nil, err
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
	r.read++
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

// finish runs the end-of-payload checks: the CRC trailer, then the header's
// record count against the records actually drained. The latch is set only
// once every check has passed.
func (r *RunReader) finish() error {
	if r.trailer {
		return nil
	}
	var tr [4]byte
	if _, err := io.ReadFull(r.br, tr[:]); err != nil {
		return fmt.Errorf("%w: short trailer", ErrCorruptRun)
	}
	if got := binary.BigEndian.Uint32(tr[:]); got != r.crc {
		return fmt.Errorf("%w: crc mismatch (file %08x, computed %08x)", ErrCorruptRun, got, r.crc)
	}
	if r.read != r.records {
		return fmt.Errorf("%w: header says %d records, drained %d", ErrCorruptRun, r.records, r.read)
	}
	r.trailer = true
	return nil
}
