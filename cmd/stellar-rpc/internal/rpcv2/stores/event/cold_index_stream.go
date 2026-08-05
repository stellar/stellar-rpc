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
//     dense slot order via a bounded reorder heap (slots deviate from key
//     order only within one MPHF block; an entry-count backstop turns a
//     corrupt slot assignment into a loud abort — the chunk retry
//     contract).

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event/runspill"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// maxReorderEntries bounds pass B's slot-reorder heap by RECORD COUNT — the
// quantity the block contract actually bounds: slots stray from stream order
// only within one MPHF block, and streamhash blocks hold at most 65,535 keys
// (per-block cumulative counts are uint16). Crossing this means non-dense or
// corrupt slot assignment — abort loudly, the retry contract rebuilds. A
// byte bound would be wrong in kind: a legitimate block's byte mass is
// unbounded (few terms with multi-MiB bodies), only its key count is capped.
const maxReorderEntries = 1 << 16

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
	defer func() {
		if rmErr := os.Remove(termsRunPath); rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) && err == nil {
			err = fmt.Errorf("events: remove %s: %w", termsRunPath, rmErr)
		}
	}()

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
	if err = writeSlotOrdered(pw, termsRunPath, m); err != nil {
		if closeErr := pw.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("events: close partial index.pack: %w", closeErr))
		}
		return err
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
	var fed int
	if err := streamTermsRun(termsRunPath, func(term TermKey, _ []byte) error {
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
	br  *bufio.Reader
	crc uint32
}

func (c *crcFoldReader) ReadByte() (byte, error) {
	b, err := c.br.ReadByte()
	if err != nil {
		return 0, err
	}
	c.crc = crc32.Update(c.crc, termsRunCRC, []byte{b})
	return b, nil
}

// streamTermsRun replays terms.run, calling emit per record with the term
// and the item body (reused buffer). Integrity (CRC over all
// records) is verified before returning nil — both passes fully drain, so a
// corrupt scratch can never produce artifacts.
func streamTermsRun(path string, emit func(term TermKey, body []byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("events: open %s: %w", path, err)
	}
	defer f.Close()
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
		crc uint32
		buf []byte
	)
	for i := range count {
		var term TermKey
		if _, err := io.ReadFull(br, term[:]); err != nil {
			return fmt.Errorf("%w: record %d term: %w", errCorruptTermsRun, i, err)
		}
		crc = crc32.Update(crc, termsRunCRC, term[:])
		// stdlib varint over a CRC-folding ByteReader.
		cbr := &crcFoldReader{br: br, crc: crc}
		length, rerr := binary.ReadUvarint(cbr)
		if rerr != nil {
			return fmt.Errorf("%w: record %d length: %w", errCorruptTermsRun, i, rerr)
		}
		crc = cbr.crc
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
		if err := emit(term, buf); err != nil {
			return err
		}
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

// slotRecord is one reorder-heap element: an assembled index.pack record
// waiting for its slot's turn.
type slotRecord struct {
	slot uint32
	fp   [IndexRecordFingerprintLen]byte
	body []byte // owned copy
}

type slotHeap []slotRecord

func (h *slotHeap) Len() int           { return len(*h) }
func (h *slotHeap) Less(i, j int) bool { return (*h)[i].slot < (*h)[j].slot }
func (h *slotHeap) Swap(i, j int)      { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }
func (h *slotHeap) Push(x any) {
	rec, ok := x.(slotRecord)
	if !ok { // heap.Push only ever receives slotRecord
		panic("events: foreign type pushed into slotHeap")
	}
	*h = append(*h, rec)
}
func (h *slotHeap) Pop() any         { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }
func (h *slotHeap) peek() slotRecord { return (*h)[0] }

// writeSlotOrdered replays terms.run, looks up each term's dense slot, and
// appends records to pw in exact slot order via the bounded reorder heap.
func writeSlotOrdered(pw *packfile.Writer, termsRunPath string, m *mphf) error {
	var (
		h    slotHeap
		next uint32
	)
	flush := func() error {
		for len(h) > 0 && h.peek().slot == next {
			rec, ok := heap.Pop(&h).(slotRecord)
			if !ok { // Push admits only slotRecord
				panic("events: slotHeap yielded a foreign type")
			}
			if err := pw.AppendItem(rec.fp[:], rec.body); err != nil {
				return fmt.Errorf("events: write slot %d to index.pack: %w", rec.slot, err)
			}
			next++
		}
		return nil
	}
	err := streamTermsRun(termsRunPath, func(term TermKey, body []byte) error {
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
		if len(h) >= maxReorderEntries {
			return fmt.Errorf("events: slot reorder heap exceeded %d records at slot %d — "+
				"MPHF slots deviate beyond one block (non-dense or corrupt index.hash)", maxReorderEntries, slot)
		}
		heap.Push(&h, slotRecord{slot: slot, fp: fp, body: append([]byte(nil), body...)})
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
	return pw.Finish(nil)
}
