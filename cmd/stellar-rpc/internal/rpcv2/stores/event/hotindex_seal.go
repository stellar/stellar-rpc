package event

// hotindex_seal.go — the Sorted-Run Tier's background half: sealing the
// window into run files, the single-level merge that caps live runs, the
// run lookup path (bloom → fence → pread → verify), and manifest-anchored
// warmup. One background job runs at a time (writer-owned lifecycle);
// results fold back in on the writer goroutine (reapSeal).

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events/runspill"
)

// ─────────────────────────── bloom filter ───────────────────────────

// bloomFilter is a plain double-hashed bloom over term fp64s: ~10 bits/key,
// 7 probes. Pointerless (one []uint64), GC-invisible.
type bloomFilter struct {
	bits []uint64
	mask uint64
}

const bloomProbes = 7

// newBloom sizes for n keys at ~10 bits/key, rounded up to a power of two
// (mask-indexable).
func newBloom(n int) bloomFilter {
	bits := 1
	for bits < n*10 {
		bits <<= 1
	}
	return bloomFilter{bits: make([]uint64, bits/64+1), mask: uint64(bits - 1)} //nolint:gosec // bits >= 1
}

func (b *bloomFilter) add(fp uint64) {
	h2 := fp>>33 | fp<<31 | 1 // odd second hash
	for i := range bloomProbes {
		bit := (fp + uint64(i)*h2) & b.mask
		b.bits[bit/64] |= 1 << (bit % 64)
	}
}

func (b *bloomFilter) mayContain(fp uint64) bool {
	h2 := fp>>33 | fp<<31 | 1
	for i := range bloomProbes {
		bit := (fp + uint64(i)*h2) & b.mask
		if b.bits[bit/64]&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

// ─────────────────────────── sealing ───────────────────────────

// startSeal kicks the background job: fold the given window rows into one
// run and, if that would push the live-run count past maxLiveRuns, merge
// everything into one. Writer goroutine only; h.sealInFlight guards the
// single-job invariant.
func (h *HotIndex) startSeal(rows []windowRow) {
	runsSnapshot := h.view.Load().runs
	sealed := len(rows)
	name := filepath.Join(h.dir, fmt.Sprintf("seal-%06d.run", h.sealSeq))
	h.sealSeq++
	h.sealInFlight = true
	go func() {
		res := sealResult{rows: sealed, lastSeq: rows[sealed-1].seq}
		res.run, res.err = sealWindow(rows, name)
		if res.err == nil && len(runsSnapshot)+1 > h.maxRuns {
			merged, obsolete, merr := mergeSealedRuns(
				append(append([]*sealedRun{}, runsSnapshot...), res.run),
				filepath.Join(h.dir, fmt.Sprintf("merge-%06d.run", h.sealSeq)))
			if merr != nil {
				res.err = merr
			} else {
				res.run = merged
				res.obsolete = obsolete
				res.replaceAll = true
			}
		}
		h.pendingSeal <- res
	}()
}

// reapSeal folds a completed background job into the view: manifest first
// (the durability point), then ONE atomic view swap that adds the run(s) and
// trims the sealed window rows — readers never see a coverage gap — then
// obsolete file deletion (crash between manifest and delete leaves orphans
// for warmup's sweep). block=true waits for an in-flight job (Close/tests).
func (h *HotIndex) reapSeal(block bool) error {
	if !h.sealInFlight {
		return nil
	}
	var res sealResult
	if block {
		res = <-h.pendingSeal
	} else {
		select {
		case res = <-h.pendingSeal:
		default:
			return nil
		}
	}
	h.sealInFlight = false
	if res.err != nil {
		return fmt.Errorf("events: hotindex seal: %w", res.err)
	}

	old := h.view.Load()
	var runs []*sealedRun
	if res.replaceAll {
		runs = []*sealedRun{res.run}
	} else {
		runs = append(append([]*sealedRun{}, old.runs...), res.run)
	}
	names := make([]string, len(runs))
	for i, r := range runs {
		names[i] = filepath.Base(r.path)
	}
	if err := h.manifest.PutRuns(names, res.lastSeq); err != nil {
		return fmt.Errorf("events: hotindex manifest: %w", err)
	}
	h.view.Store(&hotIndexView{rows: old.rows[res.rows:], runs: runs})
	h.ledgersInWindow -= res.rows
	// Unlink obsolete runs now (the merged run replaces them durably) but do
	// NOT close their handles: a lock-free reader may still hold the previous
	// view and probe them mid-lookup. The open handle keeps the unlinked file
	// readable; Close releases them when no reader can remain.
	for _, r := range res.obsolete {
		_ = os.Remove(r.path)
	}
	h.retired = append(h.retired, res.obsolete...)
	return nil
}

// runRouting accumulates a freshly written run's in-RAM routing state (bloom
// + fences + record count) record by record, in the SAME pass that writes the
// file — freshly written runs are trusted without a post-write re-read
// (owner-accepted: RocksDB/file-level integrity plus the corruption gates
// cover it). The open-time drain-verify of PRE-EXISTING files at warmup
// (openSealedRun) is untouched; it remains the crash-recovery trust anchor.
//
// The bloom is sized up front so fingerprints stream straight into it and no
// whole-run fp accumulation is ever retained: seals size from the window
// map's exact term count, merges from the inputs' summed counts — an upper
// bound on the union, which newBloom's power-of-two rounding absorbs (an
// over-sized bloom only lowers the false-positive rate).
type runRouting struct {
	bloom  bloomFilter
	fences []fence
	terms  int
}

func newRunRouting(expectedTerms int) *runRouting {
	return &runRouting{bloom: newBloom(max(expectedTerms, 1))}
}

// observe records one written record: term, at payload-relative offset off.
// Must be called once per record, in emission order, with the writer's
// pre-Append offset.
func (rt *runRouting) observe(term events.TermKey, off int64) {
	if rt.terms%fenceEvery == 0 {
		rt.fences = append(rt.fences, fence{term: term, off: off})
	}
	rt.bloom.add(fp64(term))
	rt.terms++
}

// open finalizes the routing state over the just-closed run file at path:
// the end-sentinel fence at payloadLen plus the open handle for lookups.
func (rt *runRouting) open(path string, payloadLen int64) (*sealedRun, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &sealedRun{
		path:   path,
		bloom:  rt.bloom,
		fences: append(rt.fences, fence{off: payloadLen}),
		terms:  rt.terms,
		file:   f,
	}, nil
}

// sealWindow folds window rows into one run file, building its in-RAM
// routing state in the same pass as the write. Runs on the background
// goroutine over immutable inputs.
func sealWindow(rows []windowRow, path string) (*sealedRun, error) {
	window := make(map[events.TermKey][]uint32, 1<<15)
	for i := range rows {
		if err := events.DecodePackedRow(rows[i].bytes, func(term events.TermKey, ids []uint32) {
			window[term] = append(window[term], ids...)
		}); err != nil {
			return nil, err
		}
	}
	rt := newRunRouting(len(window)) // exact: one record per window term
	payloadLen, err := writeSortedRun(window, path, rt.observe)
	if err != nil {
		return nil, err
	}
	return rt.open(path, payloadLen)
}

// writeSortedRun streams a folded (term → ids) window to path as one
// term-sorted run file — no whole-payload buffer — and returns the payload
// length. observe, when non-nil, sees each record's term and payload-relative
// offset just before it is written (one-pass routing-state construction).
// Shared by the seal and by the events freeze's window flushes (observe=nil),
// so the fold-and-stream shape has one implementation.
func writeSortedRun(
	window map[events.TermKey][]uint32, path string, observe func(term events.TermKey, off int64),
) (int64, error) {
	terms := make([]events.TermKey, 0, len(window))
	for k := range window {
		terms = append(terms, k)
	}
	slices.SortFunc(terms, func(a, b events.TermKey) int { return bytes.Compare(a[:], b[:]) })
	rw, err := runspill.NewRunWriter(path)
	if err != nil {
		return 0, err
	}
	for _, t := range terms {
		if observe != nil {
			observe(t, rw.Written())
		}
		if err := rw.Append(t, window[t]); err != nil {
			_ = rw.Close()
			_ = os.Remove(path)
			return 0, err
		}
	}
	return rw.Written(), rw.Close()
}

// mergeSealedRuns merges live runs into one new run file (union semantics —
// runspill.MergeRuns), returning it plus the now-obsolete inputs. Routing
// state is built as records stream out — no post-write re-read, no whole-run
// fingerprint accumulation (the old fps slice was the merge window's largest
// RSS transient).
func mergeSealedRuns(runs []*sealedRun, path string) (*sealedRun, []*sealedRun, error) {
	paths := make([]string, len(runs))
	expected := 0
	for i, r := range runs {
		paths[i] = r.path
		expected += r.terms
	}
	rt := newRunRouting(expected) // upper bound on the union's term count
	// Stream the merged output straight to disk: the merged run can cover
	// most of the chunk's terms (~GBs late-chunk), and buffering it whole was
	// the acceptance run's only RSS spike.
	rw, err := runspill.NewRunWriter(path)
	if err != nil {
		return nil, nil, err
	}
	emit := func(term events.TermKey, ids []uint32) error {
		rt.observe(term, rw.Written())
		return rw.Append(term, ids)
	}
	if err := runspill.MergeRuns(paths, emit); err != nil {
		_ = rw.Close()
		_ = os.Remove(path)
		return nil, nil, err
	}
	payloadLen := rw.Written()
	if err := rw.Close(); err != nil {
		return nil, nil, err
	}
	merged, err := rt.open(path, payloadLen)
	if err != nil {
		return nil, nil, err
	}
	return merged, runs, nil
}

// openSealedRun drains a PRE-EXISTING run file (verifying its CRC), building
// the bloom + fences + open file handle for lookups. Warmup-only: it is the
// crash-recovery trust anchor for files that survived a restart — routing
// state for those is always rebuilt from the verified file, never trusted
// from elsewhere. Freshly WRITTEN runs (seal/merge) build routing in the
// write pass instead (runRouting) and are not re-read.
func openSealedRun(path string) (*sealedRun, error) {
	r, err := runspill.OpenRun(path)
	if err != nil {
		return nil, err
	}
	var (
		nTerms int
		fences []fence
		off    int64
	)
	// First pass metadata: term count for bloom sizing requires a drain, so
	// collect fences+offsets in the same pass and blooms in a second cheap
	// pass over the recorded fps.
	var fps []uint64
	for {
		term, ids, nerr := r.Next()
		if errors.Is(nerr, io.EOF) {
			break
		}
		if nerr != nil {
			_ = r.Close()
			return nil, nerr
		}
		if nTerms%fenceEvery == 0 {
			fences = append(fences, fence{term: term, off: off})
		}
		off += int64(events.TermPostingsLen(ids))
		fps = append(fps, fp64(term))
		nTerms++
	}
	if err := r.Close(); err != nil {
		return nil, err
	}
	fences = append(fences, fence{off: off}) // end sentinel: off = payload length
	bloom := newBloom(max(nTerms, 1))
	for _, fp := range fps {
		bloom.add(fp)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &sealedRun{path: path, bloom: bloom, fences: fences, terms: nTerms, file: f}, nil
}

// lookup probes the run for one term: bloom reject → fence window → pread →
// linear decode with full-term verification. Concurrent-safe (ReadAt).
func (r *sealedRun) lookup(term events.TermKey) ([]uint32, error) {
	if !r.bloom.mayContain(fp64(term)) {
		return nil, nil
	}
	// Last fence with fence.term <= term (fences[0].off == 0 always).
	i := sort.Search(len(r.fences)-1, func(k int) bool {
		return bytes.Compare(r.fences[k].term[:], term[:]) > 0
	})
	if i == 0 {
		return nil, nil // term sorts before the first record
	}
	start, end := r.fences[i-1].off, r.fences[i].off
	buf := make([]byte, end-start)
	// Fence offsets are payload-relative; the payload begins after the run
	// header (runspill.HeaderLen keeps this pread aligned with the framing).
	if _, err := r.file.ReadAt(buf, runspill.HeaderLen+start); err != nil {
		return nil, fmt.Errorf("events: hotindex run pread %s: %w", r.path, err)
	}
	for len(buf) > 0 {
		c := bytes.Compare(buf[:16], term[:])
		recLen := events.PackedRecordLen(buf)
		if c == 0 {
			return decodeRecordIDs(buf[:recLen])
		}
		if c > 0 {
			return nil, nil // sorted: passed the slot, absent
		}
		buf = buf[recLen:]
	}
	return nil, nil
}

func (r *sealedRun) close() {
	if r.file != nil {
		_ = r.file.Close()
	}
}

// ─────────────────────────── warmup ───────────────────────────

// OpenHotIndex rebuilds the engine from its manifest after a restart: every
// manifest-listed run is re-opened (drain-verified — corruption is a loud
// failure, never auto-healed), unreferenced files in dir are swept as
// orphans, and the caller then replays the un-sealed tail of packed rows
// (ledgers past the sealed frontier) through ApplyLedger. The dense overlay
// starts empty and self-heals: a firehose term re-promotes on its first
// ledger, backfilling its history from window+runs.
func OpenHotIndex(dir string, manifest manifestStore) (*HotIndex, uint32, error) {
	h, err := NewHotIndex(dir, manifest)
	if err != nil {
		return nil, 0, err
	}
	names, lastSealed, err := manifest.GetRuns()
	if err != nil {
		return nil, 0, fmt.Errorf("events: hotindex manifest read: %w", err)
	}
	referenced := make(map[string]bool, len(names))
	runs := make([]*sealedRun, 0, len(names))
	for _, name := range names {
		referenced[name] = true
		r, oerr := openSealedRun(filepath.Join(dir, name))
		if oerr != nil {
			for _, r2 := range runs {
				r2.close()
			}
			return nil, 0, fmt.Errorf("events: hotindex: manifest run %s: %w", name, oerr)
		}
		runs = append(runs, r)
	}
	// Orphan sweep: files present but unreferenced are garbage by definition
	// (crash between rename and manifest, or between manifest and delete).
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, err
	}
	for _, e := range entries {
		if !e.IsDir() && !referenced[e.Name()] {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	h.view.Store(&hotIndexView{runs: runs})
	// Resume the name sequence past every live run so a future seal can never
	// overwrite one (numeric suffixes are monotone within each prefix).
	maxSeq := 0
	for _, name := range names {
		var n int
		if _, serr := fmt.Sscanf(name, "seal-%06d.run", &n); serr == nil && n > maxSeq {
			maxSeq = n
		}
		if _, serr := fmt.Sscanf(name, "merge-%06d.run", &n); serr == nil && n > maxSeq {
			maxSeq = n
		}
	}
	h.sealSeq = maxSeq + 1
	return h, lastSealed, nil
}
