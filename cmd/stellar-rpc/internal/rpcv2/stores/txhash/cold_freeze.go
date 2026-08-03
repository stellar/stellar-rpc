package txhash

// cold_freeze.go — the txhash half of the zero-decompression freeze: build a
// completed hot chunk's cold .bin DIRECTLY from its durable hot state. The
// packed-row tier stores hashes ledger-keyed (hot_store.go), so the .bin's
// hash order is recovered by a k-way merge over already-sorted sources: the
// manifest-listed sealed runs (hash-sorted 36-byte records, CRC64-framed)
// plus each un-sealed tail row (hash-sorted by EncodeRow) fed AS ITS OWN
// merge source — no tail explode-and-sort, so freeze RAM stays flat even for
// a crash-inherited tail larger than one seal window. Orphan run files (a
// crash between run write and manifest Put) are ignored by construction:
// only manifest-named runs feed the merge, and the CF rows past the sealed
// frontier cover everything an orphan holds — freeze output is identical
// from every crash state.
//
// Two byte-discipline rules, unchanged from the CF-scan freeze this
// replaces:
//   - truncation to ColdKeySize happens exactly once, at the .bin emit;
//   - sequences are decoded and re-encoded (BE row key / LE run record →
//     LE .bin), never memcpy'd.
//
// Duplicate emission is VERBATIM — equal full hashes merge in ledger order
// (run-then-tail source order) with no dedup and no error: byte parity with
// the walk path is the freeze's gate, and the downstream streamhash build
// already rejects a truncated-key collision loudly (cold_bin.go). An
// intra-ledger duplicate was rejected at write time and cannot reach here.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
)

// freezeCtxPollEvery is how many entries the merge emits between context
// checks. Entries are ~36 bytes of work apiece, so a coarse cadence still
// cancels promptly.
const freezeCtxPollEvery = 4096

// FreezeColdFromStore builds the chunk's cold txhash .bin at binPath from
// the chunk's hot store (read-only opens included — nothing here needs a
// warmed facade, and nothing is mutated). Entries stream from the source
// merge straight to the .bin — no whole-chunk accumulator; the header's
// leading count is patched in once the merge completes, and the bytes are
// identical to WriteColdBin's over the same entries. Returns the entries
// written.
func FreezeColdFromStore(
	ctx context.Context,
	chunkID chunk.ID,
	store *rocksdb.Store,
	binPath string,
) (int, error) {
	if err := os.MkdirAll(filepath.Dir(binPath), 0o755); err != nil {
		return 0, fmt.Errorf("txhash freeze %s: mkdir: %w", chunkID, err)
	}
	sources, err := openFreezeSources(ctx, store, chunkID)
	if err != nil {
		return 0, err
	}
	defer closeFreezeSources(sources)
	w, err := newColdBinStream(binPath)
	if err != nil {
		return 0, err
	}
	defer w.abort()
	n, err := mergeFreezeSources(ctx, chunkID, sources, w)
	if err != nil {
		return 0, err
	}
	if ferr := w.finish(); ferr != nil {
		return 0, ferr
	}
	return n, nil
}

// openFreezeSources assembles the merge inputs in tie-break order: the
// manifest-listed sealed runs (oldest first), then the un-sealed tail rows
// (ledger order) — so equal hashes emit in global ledger order.
func openFreezeSources(
	ctx context.Context, store *rocksdb.Store, chunkID chunk.ID,
) ([]freezeSource, error) {
	names, lastSealed, err := rocksdbManifest{store: store}.GetRuns()
	if err != nil {
		return nil, fmt.Errorf("txhash freeze %s: manifest: %w", chunkID, err)
	}
	runDir := filepath.Join(store.Path(), txhashRunDir)
	sources := make([]freezeSource, 0, len(names))
	for _, name := range names {
		rs, oerr := openRunSource(filepath.Join(runDir, name))
		if oerr != nil {
			closeFreezeSources(sources)
			return nil, fmt.Errorf("txhash freeze %s: %w", chunkID, oerr)
		}
		sources = append(sources, rs)
	}
	tails, terr := collectTailSources(ctx, store, chunkID, lastSealed)
	if terr != nil {
		closeFreezeSources(sources)
		return nil, terr
	}
	return append(sources, tails...), nil
}

// collectTailSources copies every un-sealed packed row (ledgers past the
// sealed frontier) out of the CF as its own merge source, enforcing the
// same properties warmup does: 4-byte keys (a longer key is the stale
// pre-release format), a dense chain from the frontier, and EncodeRow
// shape. Values are copied — the iterator buffer is borrowed.
func collectTailSources(
	ctx context.Context, store *rocksdb.Store, chunkID chunk.ID, lastSealed uint32,
) ([]freezeSource, error) {
	expected := max(lastSealed+1, chunkID.FirstLedger())
	var sources []freezeSource
	for entry, ierr := range store.IterateRange(txhashCF, rocksdb.EncodeUint32(expected), nil) {
		if ierr != nil {
			return nil, fmt.Errorf("txhash freeze %s: scan %s: %w", chunkID, txhashCF, ierr)
		}
		if cerr := pollCtx(ctx, len(sources)); cerr != nil {
			return nil, cerr
		}
		if len(entry.Key) != rowKeyLen {
			return nil, fmt.Errorf("txhash freeze %s: %d-byte key in %s: stale pre-release txhash format",
				chunkID, len(entry.Key), txhashCF)
		}
		seq := rocksdb.DecodeUint32(entry.Key)
		if seq != expected {
			return nil, fmt.Errorf("txhash freeze %s: tail row %d breaks the dense chain (want %d)",
				chunkID, seq, expected)
		}
		expected++
		if _, verr := validateRow(entry.Value); verr != nil {
			return nil, fmt.Errorf("txhash freeze %s: tail row %d: %w", chunkID, seq, verr)
		}
		if len(entry.Value) == 0 {
			continue // tx-less ledger: nothing to merge
		}
		sources = append(sources, &tailRowSource{
			row: append([]byte(nil), entry.Value...), rowSeq: seq, off: -rowHashLen,
		})
	}
	return sources, nil
}

// mergeFreezeSources runs the k-way merge, emitting each record as a cold
// entry: hash truncated to ColdKeySize, seq range-checked against the chunk.
func mergeFreezeSources(
	ctx context.Context, chunkID chunk.ID, sources []freezeSource, w *coldBinStream,
) (int, error) {
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	h, err := newFreezeHeap(sources)
	if err != nil {
		return 0, fmt.Errorf("txhash freeze %s: %w", chunkID, err)
	}
	n := 0
	for h.len() > 0 {
		if cerr := pollCtx(ctx, n); cerr != nil {
			return 0, cerr
		}
		src := h.min()
		seq := src.seq()
		if seq < first || seq > last {
			return 0, fmt.Errorf("txhash freeze %s: entry seq %d outside [%d, %d]", chunkID, seq, first, last)
		}
		var ce ColdEntry
		copy(ce.Key[:], src.hash()[:ColdKeySize])
		ce.Seq = seq
		if werr := w.append(ce); werr != nil {
			return 0, werr
		}
		n++
		if serr := h.step(); serr != nil {
			return 0, fmt.Errorf("txhash freeze %s: %w", chunkID, serr)
		}
	}
	return n, nil
}

// pollCtx checks ctx once every freezeCtxPollEvery iterations.
func pollCtx(ctx context.Context, i int) error {
	if i%freezeCtxPollEvery == 0 {
		return ctx.Err()
	}
	return nil
}

// ─────────────────────────── merge sources ───────────────────────────

// freezeSource is one hash-sorted stream of (full hash, seq) records feeding
// the freeze merge: a sealed run file or a single un-sealed tail row.
// Sources start positioned BEFORE their first record; advance steps to the
// next one.
type freezeSource interface {
	// hash returns the current record's full 32-byte hash — valid only
	// until the next advance.
	hash() []byte
	// seq returns the current record's ledger sequence.
	seq() uint32
	// advance steps to the next record, reporting whether one is now
	// current. (false, nil) is clean end-of-stream — for a run source that
	// includes the whole-payload CRC64 verification passing.
	advance() (bool, error)
	// close releases resources; safe in any state.
	close()
}

// closeFreezeSources releases every source (error path and normal exit).
func closeFreezeSources(sources []freezeSource) {
	for _, s := range sources {
		s.close()
	}
}

// runSource streams one sealed run file sequentially through a 128KiB
// buffer, folding CRC64 over the drain: the CRC (and the header count vs
// file size) is verified by the time the source reports end-of-stream, so a
// torn or corrupt run fails the freeze loudly before the .bin is finished.
// Per-record hash order is checked as it streams (the merge's correctness
// AND the .bin's sort order both ride on it).
type runSource struct {
	path      string
	f         *os.File
	br        *bufio.Reader
	crc       hash.Hash64
	hdr       runHeader
	records   int
	remaining int
	rec       [runRecordLen]byte
	prev      [rowHashLen]byte
}

// openRunSource opens and structurally validates a run for sequential
// draining (header magic/range, count vs file size — the same gates
// openSealedRun applies before ITS drain).
func openRunSource(path string) (*runSource, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("run %s: %w", path, err)
	}
	hdr, err := readRunHeader(f, path)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	records, err := runRecordCount(f, path, hdr)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &runSource{
		path: path, f: f, br: bufio.NewReaderSize(f, 128<<10),
		crc: crc64.New(crcRunTable), hdr: hdr, records: records, remaining: records,
	}, nil
}

func (r *runSource) hash() []byte { return r.rec[:rowHashLen] }

func (r *runSource) seq() uint32 { return binary.LittleEndian.Uint32(r.rec[rowHashLen:]) }

func (r *runSource) advance() (bool, error) {
	if r.remaining == 0 {
		if r.crc.Sum64() != r.hdr.crc {
			return false, fmt.Errorf("run %s: payload crc mismatch (file %016x, computed %016x)",
				r.path, r.hdr.crc, r.crc.Sum64())
		}
		return false, nil
	}
	idx := r.records - r.remaining
	if idx > 0 {
		copy(r.prev[:], r.rec[:rowHashLen])
	}
	if _, err := io.ReadFull(r.br, r.rec[:]); err != nil {
		return false, fmt.Errorf("run %s: record %d: %w", r.path, idx, err)
	}
	_, _ = r.crc.Write(r.rec[:])
	if idx > 0 && bytes.Compare(r.prev[:], r.rec[:rowHashLen]) > 0 {
		return false, fmt.Errorf("run %s: record %d out of hash order", r.path, idx)
	}
	r.remaining--
	return true, nil
}

func (r *runSource) close() { _ = r.f.Close() }

// tailRowSource walks one retained packed row (32-byte stride, already
// hash-sorted by EncodeRow) with a fixed ledger. off starts at -rowHashLen
// (before the first hash) per the freezeSource contract.
type tailRowSource struct {
	row    []byte
	rowSeq uint32
	off    int
}

func (t *tailRowSource) hash() []byte { return t.row[t.off : t.off+rowHashLen] }

func (t *tailRowSource) seq() uint32 { return t.rowSeq }

func (t *tailRowSource) advance() (bool, error) {
	t.off += rowHashLen
	return t.off < len(t.row), nil
}

func (t *tailRowSource) close() {}

// ─────────────────────────── merge heap ───────────────────────────

// freezeHeap is a slice-backed min-heap of source indices ordered by
// (current hash, source index) — the same shape as the seal path's
// mergeHeap, over freezeSources instead of window rows. The index tie-break
// emits equal cross-source duplicate hashes in source order (runs oldest
// first, then tail rows in ledger order), and equal hashes WITHIN a run are
// already in ledger order from the seal merge — so global duplicate order is
// ascending ledger, deterministically.
type freezeHeap struct {
	sources []freezeSource
	heap    []int
}

// newFreezeHeap primes every source (first advance) and heapifies the
// non-empty ones. An empty source that fails its end-of-stream verification
// (torn empty run) fails here.
func newFreezeHeap(sources []freezeSource) (*freezeHeap, error) {
	h := &freezeHeap{sources: sources, heap: make([]int, 0, len(sources))}
	for i, s := range sources {
		ok, err := s.advance()
		if err != nil {
			return nil, err
		}
		if ok {
			h.heap = append(h.heap, i)
			h.up(len(h.heap) - 1)
		}
	}
	return h, nil
}

func (h *freezeHeap) len() int { return len(h.heap) }

// min returns the source holding the smallest current record.
func (h *freezeHeap) min() freezeSource { return h.sources[h.heap[0]] }

// step advances the minimum source past its emitted record, dropping the
// source once drained (its end-of-stream verification ran inside advance).
func (h *freezeHeap) step() error {
	ok, err := h.sources[h.heap[0]].advance()
	if err != nil {
		return err
	}
	if !ok {
		lastIdx := len(h.heap) - 1
		h.heap[0] = h.heap[lastIdx]
		h.heap = h.heap[:lastIdx]
	}
	if len(h.heap) > 0 {
		h.down(0)
	}
	return nil
}

// less orders heap positions a, b by (current hash, source index).
func (h *freezeHeap) less(a, b int) bool {
	sa, sb := h.heap[a], h.heap[b]
	if c := bytes.Compare(h.sources[sa].hash(), h.sources[sb].hash()); c != 0 {
		return c < 0
	}
	return sa < sb
}

func (h *freezeHeap) up(pos int) {
	for pos > 0 {
		parent := (pos - 1) / 2
		if !h.less(pos, parent) {
			return
		}
		h.heap[pos], h.heap[parent] = h.heap[parent], h.heap[pos]
		pos = parent
	}
}

func (h *freezeHeap) down(pos int) {
	for {
		child := 2*pos + 1
		if child >= len(h.heap) {
			return
		}
		if r := child + 1; r < len(h.heap) && h.less(r, child) {
			child = r
		}
		if !h.less(child, pos) {
			return
		}
		h.heap[pos], h.heap[child] = h.heap[child], h.heap[pos]
		pos = child
	}
}
