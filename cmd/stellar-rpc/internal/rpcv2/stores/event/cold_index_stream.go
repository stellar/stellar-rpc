package event

// cold_index_stream.go is the external-memory twin of cold_index.go's
// WriteColdIndex: it produces the SAME two artifacts (index.pack +
// index.hash, byte-identical) from spill runs instead of an in-memory
// Bitmaps — the piece that removes the cold build's O(unique terms)
// RAM. Shape (design doc: ~/bench-artifacts/cold-ingest-design.md):
//
//  1. MergeRuns → one scratch terms.run: per unique term, the final
//     index.pack item body under a 16B term and a uvarint length, CRC-framed,
//     so pass B copies the body through untouched.
//     Unique-term count N falls out — streamhash needs it exactly, up front.
//  2. Pass A: stream terms.run keys → streamhash.NewSortedBuilder(N) →
//     index.hash. Memcmp-sorted 16-byte keys are valid block-sorted input,
//     and with default options the result is byte-identical to the unsorted
//     builder's (pinned by streamhash's own lifecycle tests).
//  3. Pass B: re-stream terms.run → mphf.Lookup per term → index.pack in
//     dense slot order via a bounded reorder heap: bodies at or under
//     inlineBodyMax ride the heap inline (every delta-coded body does, by
//     construction), larger ones are buffered as references and re-read
//     from terms.run at emit. Slots deviate from key order only within one
//     MPHF block; an entry-count backstop bounds the heap's RAM when slot
//     assignment is corrupt, and a stranded-records check catches
//     non-dense output.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event/runspill"
)

// inlineBodyMax is the copy/reference boundary for buffered records: bodies
// at or under it are copied into the heap entry at push time, larger ones
// are re-read from terms.run at emit. It is derived so that EVERY
// delta-coded body inlines by construction — 1 codec byte, a count varint,
// and up to deltaPostingMaxCardinality posting varints — and moves with
// that constant if the codec split is ever retuned; only roaring bodies
// — multi-MiB at the extreme, and unbounded in aggregate per block —
// take the reference path. Straying into the heap is the norm, not the
// exception (~99.7% of records: within-block slot order is effectively
// random), so without the inline copy every record pays an emit-time pread
// — measured on sac-6000 (c6id.8xlarge NVMe, page-cache-hot) at ~1.4µs
// each = +29% on the freeze's events arm. Re-measure before retuning.
// RAM: the heap provably drains at every block boundary, so occupancy is
// one MPHF block (≲3.5K entries ≈ 17MB inline); the enforced ceiling —
// the derived backstop × inlineBodyMax ≈ 320MiB — is reachable only with
// a corrupt index.hash, which the backstop then turns into a loud abort.
const inlineBodyMax = 1 + binary.MaxVarintLen32 + deltaPostingMaxCardinality*binary.MaxVarintLen32

var errCorruptTermsRun = errors.New("events: corrupt terms.run scratch")

// WriteColdIndexFromRuns builds index.pack + index.hash for chunkID in
// bucketDir from term-sorted spill runs (runspill format). scratchDir hosts
// the intermediate terms.run; the caller owns scratch lifecycle (wipe on
// retry). Artifacts are byte-identical to WriteColdIndex fed the equivalent
// Bitmaps — pinned by TestWriteColdIndexFromRuns_ByteIdentical.
// secret is the chunk's routing secret: the run KEYS are already blinded
// with it (ingest blinds before the spill), so it is not applied to any key
// here — it is recorded in index.hash's user metadata so readers can route
// queries identically.
func WriteColdIndexFromRuns(
	ctx context.Context, chunkID chunk.ID, runPaths []string, scratchDir, bucketDir string,
	secret [stores.SecretLen]byte,
) (err error) {
	indexPackPath := filepath.Join(bucketDir, IndexPackName(chunkID))
	indexHashPath := filepath.Join(bucketDir, IndexHashName(chunkID))
	termsRunPath := filepath.Join(scratchDir, "terms.run")

	// Match WriteColdIndex's failure contract: no orphan index.hash on error.
	defer func() {
		if err == nil {
			return
		}
		if rmErr := os.Remove(indexHashPath); rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("events: remove orphan %s: %w", indexHashPath, rmErr))
		}
	}()

	total, err := writeTermsRun(termsRunPath, runPaths)
	if err != nil {
		return err
	}
	// Scratch removal is best-effort and MUST NOT write to err: this defer
	// runs before the orphan-index.hash guard above (LIFO), so a scratch
	// unlink failure surfacing as err would delete the finalized index.hash
	// while index.pack survives — a torn artifact pair on an otherwise
	// successful build. The caller owns scratchDir and wipes it wholesale.
	defer func() { _ = os.Remove(termsRunPath) }()

	// Pass A: keys → SortedBuilder → index.hash. The run keys are already
	// blinded (see the function doc); the secret rides only in the metadata.
	if err = buildSortedHash(ctx, termsRunPath, indexHashPath, total, secret); err != nil {
		return err
	}
	m, err := openMPHF(indexHashPath)
	if err != nil {
		return err
	}
	defer m.Close()

	// Pass B: records → slot order → index.pack.
	pw, err := packfile.Create(indexPackPath, packfile.WriterOptions{
		Format:         indexPackFormat,
		ItemsPerRecord: indexPackItemsPerRecord,
		Overwrite:      true,
		// Same smoothing rationale as the non-streaming builder: never let
		// the index.pack accumulate as one Finish-time flush burst.
		BytesPerSync: indexPackBytesPerSync,
	})
	if err != nil {
		return fmt.Errorf("events: create index.pack at %s: %w", indexPackPath, err)
	}
	// ONE cleanup for every post-Create failure (Close after a successful
	// Finish is a no-op, so success passes through untouched); pw.Close
	// removes the partial index.pack.
	defer func() {
		if err == nil {
			return
		}
		if closeErr := pw.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("events: close partial index.pack: %w", closeErr))
		}
	}()
	if err = writeSlotOrdered(pw, termsRunPath, m); err != nil {
		return err
	}
	// Finish lives beside Create: writeSlotOrdered only emits records, so
	// no early return added to it can ever ship an unfinalized index.pack
	// that reports success.
	if err = pw.Finish(nil); err != nil {
		return fmt.Errorf("events: finish index.pack: %w", err)
	}
	return nil
}

// buildSortedHash is pass A: every terms.run key feeds a SortedBuilder and
// index.hash is finalized. Default options only — that is what pins
// byte-identity with buildMPHF's unsorted output.
func buildSortedHash(
	ctx context.Context, termsRunPath, indexHashPath string, total uint64,
	secret [stores.SecretLen]byte,
) error {
	// Options must mirror buildMPHF's exactly (metadata included) — that is
	// what pins byte-identity with the unsorted oracle's output.
	builder, err := streamhash.NewSortedBuilder(ctx, indexHashPath, total,
		streamhash.WithMetadata(encodeEventsMeta(secret)))
	if err != nil {
		return fmt.Errorf("events: create sorted streamhash builder: %w", err)
	}
	finished := false
	defer func() {
		if !finished {
			_ = builder.Close()
		}
	}()
	f, err := os.Open(termsRunPath)
	if err != nil {
		return fmt.Errorf("events: open %s: %w", termsRunPath, err)
	}
	defer f.Close()
	var fed int
	if err := streamTermsRun(f, func(term TermKey, _ []byte, _ int64) error {
		// ctx.Err takes a mutex; per-term polling costs tens of ms over
		// millions of terms. Poll on the freeze scans' shared cadence.
		if fed%256 == 0 {
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
		}
		fed++
		return builder.AddKey(term[:], 0)
	}); err != nil {
		return fmt.Errorf("events: feed sorted builder: %w", err)
	}
	if err := builder.Finish(); err != nil {
		return fmt.Errorf("events: finalize index.hash at %s: %w", indexHashPath, err)
	}
	finished = true
	return nil
}

// writeTermsRun merges the spill runs into one terms.run scratch file and
// returns the unique-term count. Record: 16B term ‖ uvarint len ‖ the
// index.pack item body. File framing: magic ‖ u64 record count ‖ records ‖
// CRC-32C(records) — the count doubles as streamhash's totalKeys.
func writeTermsRun(path string, runPaths []string) (uint64, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, fmt.Errorf("events: create %s: %w", path, err)
	}
	w := bufio.NewWriterSize(f, 1<<20)
	var hdr [12]byte
	copy(hdr[:4], termsRunMagic[:])
	if _, err := w.Write(hdr[:]); err != nil { // count patched below
		_ = f.Close()
		return 0, fmt.Errorf("events: write %s header: %w", path, err)
	}

	var (
		count   uint64
		crc     uint32
		bodyBuf []byte
		recBuf  []byte
		mergeED = func(rawTerm [16]byte, ids []uint32) error {
			term := TermKey(rawTerm)
			var berr error
			if bodyBuf, berr = encodeIndexBody(bodyBuf[:0], ids); berr != nil {
				return berr
			}
			recBuf = recBuf[:0]
			recBuf = append(recBuf, term[:]...)
			recBuf = binary.AppendUvarint(recBuf, uint64(len(bodyBuf)))
			recBuf = append(recBuf, bodyBuf...)
			crc = crc32.Update(crc, termsRunCRC, recBuf)
			count++
			_, werr := w.Write(recBuf)
			return werr
		}
	)
	if err := runspill.MergeRuns(runPaths, mergeED); err != nil {
		_ = f.Close()
		return 0, err
	}
	var tr [4]byte
	binary.BigEndian.PutUint32(tr[:], crc)
	_, err = w.Write(tr[:])
	if err == nil {
		err = w.Flush()
	}
	if err == nil {
		binary.BigEndian.PutUint64(hdr[4:], count)
		_, err = f.WriteAt(hdr[4:12], 4) // patch the record count
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return 0, fmt.Errorf("events: write %s: %w", path, err)
	}
	return count, nil
}

var (
	termsRunMagic = [4]byte{'E', 'T', 'R', '1'}       //nolint:gochecknoglobals // fixed format tag
	termsRunCRC   = crc32.MakeTable(crc32.Castagnoli) //nolint:gochecknoglobals // fixed table
)

// crcFoldReader is an io.ByteReader that folds every byte it yields into the
// running CRC — binary.ReadUvarint's adapter for CRC-framed streams.
type crcFoldReader struct {
	br      *bufio.Reader
	crc     uint32
	n       int64
	scratch [1]byte
}

func (c *crcFoldReader) ReadByte() (byte, error) {
	b, err := c.br.ReadByte()
	if err != nil {
		return 0, err
	}
	c.scratch[0] = b
	c.crc = crc32.Update(c.crc, termsRunCRC, c.scratch[:])
	c.n++
	return b, nil
}

// streamTermsRun replays terms.run from f (opened and owned by the caller,
// which lets pass B share ONE fd between this sequential scan and its
// positional emit re-reads — same inode by construction), calling emit per
// record with the term, item body (reused buffer), and absolute file offset
// of the body. Integrity (CRC over all records) is verified before
// returning nil — both passes fully drain, so a CRC-detectable corrupt
// scratch can never yield a completed build. Bytes re-read positionally at
// emit are covered by this scan's verification of the same fd, not
// re-hashed at emit.
func streamTermsRun(f *os.File, emit func(term TermKey, body []byte, bodyOff int64) error) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("events: seek %s: %w", f.Name(), err)
	}
	br := bufio.NewReaderSize(f, 1<<20)
	var hdr [12]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		return fmt.Errorf("%w: short header", errCorruptTermsRun)
	}
	if !bytes.Equal(hdr[:4], termsRunMagic[:]) {
		return fmt.Errorf("%w: bad magic", errCorruptTermsRun)
	}
	count := binary.BigEndian.Uint64(hdr[4:])

	var (
		crc     uint32
		buf     []byte
		fileOff int64 = 12
		// One CRC-folding varint reader for the whole replay — per-record
		// state is reset in place, so the loop allocates nothing for it.
		cbr = &crcFoldReader{br: br}
	)
	for i := range count {
		var term TermKey
		if _, err := io.ReadFull(br, term[:]); err != nil {
			return fmt.Errorf("%w: record %d term: %w", errCorruptTermsRun, i, err)
		}
		crc = crc32.Update(crc, termsRunCRC, term[:])
		fileOff += int64(len(term))
		// stdlib varint over the CRC-folding ByteReader.
		cbr.crc, cbr.n = crc, 0
		length, rerr := binary.ReadUvarint(cbr)
		if rerr != nil {
			return fmt.Errorf("%w: record %d length: %w", errCorruptTermsRun, i, rerr)
		}
		crc = cbr.crc
		fileOff += cbr.n
		if length > 1<<31 {
			return fmt.Errorf("%w: record %d body length %d", errCorruptTermsRun, i, length)
		}
		if uint64(cap(buf)) < length {
			buf = make([]byte, length)
		}
		buf = buf[:length]
		if _, err := io.ReadFull(br, buf); err != nil {
			return fmt.Errorf("%w: record %d body: %w", errCorruptTermsRun, i, err)
		}
		crc = crc32.Update(crc, termsRunCRC, buf)
		if err := emit(term, buf, fileOff); err != nil {
			return err
		}
		fileOff += int64(length)
	}
	var tr [4]byte
	if _, err := io.ReadFull(br, tr[:]); err != nil {
		return fmt.Errorf("%w: short trailer", errCorruptTermsRun)
	}
	if got := binary.BigEndian.Uint32(tr[:]); got != crc {
		return fmt.Errorf("%w: crc mismatch (file %08x computed %08x)", errCorruptTermsRun, got, crc)
	}
	return nil
}

// materializeBody returns rec's index.pack item body: the inline copy when
// the record carries one, otherwise a positional re-read from terms.run
// into readBuf — grown as needed and returned for the caller to reuse.
func materializeBody(termsRun *os.File, rec slotRecord, readBuf []byte) ([]byte, []byte, error) {
	if rec.inline != nil {
		return rec.inline, readBuf, nil
	}
	if cap(readBuf) < int(rec.n) {
		readBuf = make([]byte, rec.n)
	}
	body := readBuf[:rec.n]
	if _, err := termsRun.ReadAt(body, rec.off); err != nil {
		return nil, readBuf, fmt.Errorf("events: read slot %d body at offset %d: %w", rec.slot, rec.off, err)
	}
	return body, readBuf, nil
}

// slotRecord is one reorder-heap element waiting for its slot's turn: a
// small body rides inline (copied at push), a large one is referenced and
// re-read from terms.run at (off, n) on emit.
type slotRecord struct {
	slot uint32
	fp   [IndexRecordFingerprintLen]byte
	off  int64
	n    uint32
	// inline holds the body copied at buffering time when it fits
	// inlineBodyMax; nil means emit re-reads terms.run at (off, n).
	inline []byte
}

// slotHeap is a typed slice-backed binary min-heap of buffered records keyed
// by MPHF slot — a value heap, so a push neither boxes the record into an
// `any` nor routes its compare through an interface. Dense MPHF slots are
// unique, so the key needs no tie-break and the pop order is the slot order.
type slotHeap []slotRecord

func (h *slotHeap) peek() slotRecord { return (*h)[0] }

func (h *slotHeap) push(rec slotRecord) {
	*h = append(*h, rec)
	h.siftUp(len(*h) - 1)
}

// popMin removes and returns the lowest-slot record.
func (h *slotHeap) popMin() slotRecord {
	old := *h
	minRec := old[0]
	last := len(old) - 1
	old[0] = old[last]
	// Zero the vacated slot: stale headers lingering in the capacity tail
	// would pin already-emitted inline bodies (and any heap-backed field
	// slotRecord grows later) until the heap regrows over them.
	old[last] = slotRecord{}
	*h = old[:last]
	h.siftDown(0)
	return minRec
}

func (h *slotHeap) siftUp(i int) {
	h2 := *h
	for i > 0 {
		parent := (i - 1) / 2
		if h2[parent].slot <= h2[i].slot {
			break
		}
		h2[i], h2[parent] = h2[parent], h2[i]
		i = parent
	}
}

func (h *slotHeap) siftDown(i int) {
	h2 := *h
	n := len(h2)
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		j := left
		if right := left + 1; right < n && h2[right].slot < h2[j].slot {
			j = right
		}
		if h2[i].slot <= h2[j].slot { // heap property already holds
			break
		}
		h2[i], h2[j] = h2[j], h2[i]
		i = j
	}
}

// writeSlotOrdered replays terms.run, looks up each term's dense slot, and
// appends records to pw in exact slot order via the bounded reorder heap.
func writeSlotOrdered(pw *packfile.Writer, termsRunPath string, m *mphf) error {
	// ONE fd serves both the sequential scan (streamTermsRun) and the
	// emit-time positional re-reads: pread leaves the file offset alone,
	// and sharing the handle pins scan and re-read to the same inode.
	termsRun, err := os.Open(termsRunPath)
	if err != nil {
		return fmt.Errorf("events: open %s for slot reorder: %w", termsRunPath, err)
	}
	defer termsRun.Close()

	// The heap's backstop, derived from the artifact itself: sorted keys
	// route to non-decreasing blocks and a block's minimum-slot key always
	// takes the fast path, so a legal heap never holds more than
	// MaxBlockKeys−1 records. One more push is provably impossible —
	// corrupt slot assignment — and aborts loudly (the chunk retry
	// contract rebuilds). Deriving the bound from the opened index keeps
	// it correct across format revisions with no transcribed constant.
	maxBuffered := int(m.MaxBlockKeys()) - 1

	var (
		h       slotHeap
		next    uint32
		readBuf []byte
	)
	flush := func() error {
		for len(h) > 0 && h.peek().slot == next {
			rec := h.popMin()
			body, buf, rerr := materializeBody(termsRun, rec, readBuf)
			if rerr != nil {
				return rerr
			}
			readBuf = buf
			// The streaming pass folds these bytes into the CRC and fully drains
			// before the build succeeds; positional reads need not fold them again.
			if err := pw.AppendItem(rec.fp[:], body); err != nil {
				return fmt.Errorf("events: write slot %d to index.pack: %w", rec.slot, err)
			}
			next++
		}
		return nil
	}
	err = streamTermsRun(termsRun, func(term TermKey, body []byte, bodyOff int64) error {
		slot, lerr := m.LookupRouted(term)
		if lerr != nil {
			return fmt.Errorf("events: MPHF lookup during index.pack build: %w", lerr)
		}
		var fp [IndexRecordFingerprintLen]byte
		copy(fp[:], term[:IndexRecordFingerprintLen])
		if slot == next {
			// Fast path: already in order — write through, then drain any
			// buffered successors.
			if err := pw.AppendItem(fp[:], body); err != nil {
				return fmt.Errorf("events: write slot %d to index.pack: %w", slot, err)
			}
			next++
			return flush()
		}
		if len(h) >= maxBuffered {
			return fmt.Errorf("events: slot reorder heap exceeded %d records at slot %d — "+
				"MPHF slots deviate beyond one block (non-dense or corrupt index.hash)", maxBuffered, slot)
		}
		n := uint32(len(body)) //nolint:gosec // streamTermsRun limits body lengths to 1 << 31.
		rec := slotRecord{slot: slot, fp: fp, off: bodyOff, n: n}
		if n <= inlineBodyMax {
			rec.inline = append([]byte(nil), body...)
		}
		h.push(rec)
		return nil
	})
	if err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	if len(h) != 0 {
		return fmt.Errorf("events: %d index.pack records stranded (non-dense MPHF slots?)", len(h))
	}
	return nil
}
