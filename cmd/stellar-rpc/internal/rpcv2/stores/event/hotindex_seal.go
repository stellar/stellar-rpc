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

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/bloom"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event/runspill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/internal/runset"
)

// ─────────────────────────── sealing ───────────────────────────

// startSeal kicks the background job: fold the given window rows into one
// run and, if that would push the live-run count past maxLiveRuns, merge
// everything into one. Writer goroutine only; h.sealInFlight guards the
// single-job invariant.
func (h *HotIndex) startSeal(rows []windowRow) {
	runsSnapshot := h.view.Load().runs
	// Read on the writer goroutine, like runsSnapshot: the job below closes
	// over no mutable HotIndex state but h.fsyncDir (fixed at construction).
	secret := h.secret
	sealed := len(rows)
	// Both run names follow runset.NextSealSeq's grammar (<prefix>-%06d.run,
	// the prefixes OpenHotIndex resumes from); changing either shape silently
	// breaks the seal-sequence resume.
	name := filepath.Join(h.dir, fmt.Sprintf("seal-%06d.run", h.sealSeq))
	h.sealSeq++
	h.sealInFlight = true
	go func() {
		res := sealResult{rows: sealed, lastSeq: rows[sealed-1].seq}
		res.run, res.err = sealWindow(rows, name, secret, h.fsyncDir)
		if res.err == nil && len(runsSnapshot)+1 > h.maxRuns {
			merged, obsolete, merr := mergeSealedRuns(
				append(append([]*sealedRun{}, runsSnapshot...), res.run),
				filepath.Join(h.dir, fmt.Sprintf("merge-%06d.run", h.sealSeq)), // NextSealSeq's grammar
				h.fsyncDir)
			if merr != nil {
				// The sealed run just became garbage — dispose of it here,
				// while it is known to be unpublished (see sealResult:
				// errors carry no resources).
				res.run.close()
				_ = os.Remove(res.run.path)
				res.run = nil
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
	// A merge fold's run replaces ALL live runs; a plain seal joins them.
	live := old.runs
	if res.replaceAll {
		live = nil
	}
	// A failed Publish disposes of the un-listed run itself (errors carry no
	// resources); the obsolete inputs stay untouched — they are still live
	// in the view and listed by the manifest.
	if err := runset.Publish(h.manifest, live, res.run, res.lastSeq); err != nil {
		return fmt.Errorf("events: hotindex manifest: %w", err)
	}
	runs := append(append([]*sealedRun{}, live...), res.run)
	h.view.Store(&hotIndexView{rows: old.rows[res.rows:], runs: runs})
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

// runRouting accumulates a run's in-RAM routing state (bloom + fences +
// record count) record by record in one pass. The seal and merge feed it in
// the SAME pass that writes the file — freshly written runs are trusted
// without a post-write re-read (owner-accepted: RocksDB/file-level integrity
// plus the corruption gates cover it) — and warmup (openSealedRun) feeds it
// from the drain-verify of pre-existing files, which remains the
// crash-recovery trust anchor.
//
// The bloom is sized up front so fingerprints stream straight into it and no
// whole-run fp accumulation is ever retained: seals size from the window
// map's exact term count, merges from the inputs' summed counts — an upper
// bound on the union, which bloom.New's power-of-two rounding absorbs (an
// over-sized bloom only lowers the false-positive rate) — and warmup from
// the header's validated record count.
type runRouting struct {
	bloom bloom.Filter
	fb    fenceBuilder
}

func newRunRouting(expectedTerms int) *runRouting {
	return &runRouting{
		bloom: bloom.New(max(expectedTerms, 1)),
		// Cadence fences + sentinel; byte-cap extras may tail-grow. Matters
		// at merge scale (~1M fences), where regrowing from nil churned
		// ~100MB of transient allocation.
		fb: fenceBuilder{fences: make([]fence, 0, expectedTerms/fenceEvery+2)},
	}
}

// observe records one written record: term, at payload-relative offset off.
// Must be called once per record, in emission order, with the writer's
// pre-Append offset.
func (rt *runRouting) observe(term TermKey, off int64) {
	rt.fb.observe(term, off)
	rt.bloom.Add(fp64(term))
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
		fences: rt.fb.finish(payloadLen),
		terms:  rt.fb.records,
		file:   f,
	}, nil
}

// openDurable is the seal's and the merge's shared post-Commit epilogue: the
// fsyncDir dirent barrier first — the hand-back's next stop is the manifest
// write that names the run (see HotIndex.fsyncDir) — then the open handle for
// lookups. Either failure unlinks the committed artifact rather than
// stranding it until the next warmup's orphan sweep, and neither leaves a
// handle open, so an error carries no resources (sealResult contract).
// Warmup opens through rt.open instead: those files are already durable.
func (rt *runRouting) openDurable(
	path string, payloadLen int64, fsyncDir func(string) error,
) (*sealedRun, error) {
	if err := fsyncDir(filepath.Dir(path)); err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	run, err := rt.open(path, payloadLen)
	if err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	return run, nil
}

// fenceBuilder accumulates a run's fence array under two caps: fenceEvery
// records (bounds a probe's decode walk) and fenceSpanBytes of payload
// (bounds a probe's pread). A record of fenceSpanBytes or more is fenced on
// both sides into a window of its own, so a term that sorts beside a dense
// term's multi-MB record never preads it. Seal, merge, and warmup all build
// fences through this one type — spacing cannot drift between a freshly
// written run and its reopened form. Single-use: the fences finish returns
// may share the builder's backing array, so drop the builder afterwards.
type fenceBuilder struct {
	fences       []fence
	lastFenceOff int64
	prevTerm     TermKey
	prevOff      int64
	records      int
}

func (fb *fenceBuilder) observe(term TermKey, off int64) {
	fb.isolatePrev(off)
	if fb.records%fenceEvery == 0 || off-fb.lastFenceOff >= fenceSpanBytes {
		fb.put(term, off)
	}
	fb.prevTerm, fb.prevOff = term, off
	fb.records++
}

// isolatePrev retroactively fences the just-ended record (end is its
// exclusive boundary) when it was oversized, so the window holding it holds
// nothing else.
func (fb *fenceBuilder) isolatePrev(end int64) {
	if fb.records > 0 && end-fb.prevOff >= fenceSpanBytes {
		fb.put(fb.prevTerm, fb.prevOff)
	}
}

// put appends a fence at off unless one is already there — the single
// dedupe point for every placement rule.
func (fb *fenceBuilder) put(term TermKey, off int64) {
	if len(fb.fences) > 0 && fb.lastFenceOff == off {
		return
	}
	fb.fences = append(fb.fences, fence{term: term, off: off})
	fb.lastFenceOff = off
}

// finish isolates an oversized final record, then closes the array with the
// end sentinel at payloadLen.
func (fb *fenceBuilder) finish(payloadLen int64) []fence {
	fb.isolatePrev(payloadLen)
	return append(fb.fences, fence{off: payloadLen})
}

// foldBlindedRow decodes ONE packed index row and unions its postings into a
// blinded window map, returning how many DECODED bytes the window grew by
// (16-byte key + 4 bytes per id — what the map actually holds, which the
// varint-encoded row length undercounts several-fold on dense rows).
//
// This is the blind-at-seal step itself, written once for its two callers:
// the seal, which folds a whole window of rows and ignores the byte count,
// and the freeze's tail scan, which folds rows until the count crosses its
// window cap (cold_freeze.go). The in-place merge those two feed is only
// correct because the seal's key and the freeze tail's key are the same
// function of the same term — so they are the same expression.
func foldBlindedRow(window map[TermKey][]uint32, row []byte, secret [stores.SecretLen]byte) (int, error) {
	added := 0
	if err := DecodePackedRow(row, func(term TermKey, ids []uint32) {
		bk := blindTerm(secret, term)
		window[bk] = append(window[bk], ids...)
		added += 16 + 4*len(ids)
	}); err != nil {
		return 0, err
	}
	return added, nil
}

// sealWindow folds window rows into one run file, building its in-RAM
// routing state in the same pass as the write, and hands back through
// openDurable — so the run's dirent is durable before the caller ever sees
// it. Runs on the background goroutine over immutable inputs.
//
// The fold is where a term stops being raw and becomes the blinded routing
// key the cold index is built on (blind at seal): the packed rows keep their
// raw terms — they are what warmup replays and what the freeze's tail scan
// reads — while everything downstream of the fold (the run's records, its
// fences, its bloom, and the cold index the freeze merges them into) is
// keyed by BlindKey(secret, term). Blinding at the fold, rather than at the
// write, keeps writeSortedRun key-agnostic: the freeze's tail windows blind
// at their own insert and stream through the same writer.
func sealWindow(
	rows []windowRow, path string, secret [stores.SecretLen]byte, fsyncDir func(string) error,
) (*sealedRun, error) {
	window := make(map[TermKey][]uint32, 1<<15)
	for i := range rows {
		if _, err := foldBlindedRow(window, rows[i].bytes, secret); err != nil {
			return nil, err
		}
	}
	rt := newRunRouting(len(window)) // exact: one record per window term
	payloadLen, err := writeSortedRun(window, path, rt.observe)
	if err != nil {
		return nil, err
	}
	return rt.openDurable(path, payloadLen, fsyncDir)
}

// writeSortedRun streams a folded (key → ids) window to path as one
// key-sorted run file — no whole-payload buffer — and returns the payload
// length. observe, when non-nil, sees each record's key and payload-relative
// offset just before it is written (one-pass routing-state construction).
// Shared by the seal and by the events freeze's window flushes (observe=nil),
// so the fold-and-stream shape has one implementation.
//
// Deliberately key-AGNOSTIC: both callers hand it a window that is already
// keyed the way the run must store it (blinded — the seal blinds in its
// fold, the freeze's tail scan at its insert), so record order here is
// exactly the caller's key order and nothing re-keys behind their back.
func writeSortedRun(
	window map[TermKey][]uint32, path string, observe func(term TermKey, off int64),
) (int64, error) {
	terms := make([]TermKey, 0, len(window))
	for k := range window {
		terms = append(terms, k)
	}
	slices.SortFunc(terms, func(a, b TermKey) int { return bytes.Compare(a[:], b[:]) })
	rw, err := runspill.NewRunWriter(path)
	if err != nil {
		return 0, err
	}
	defer rw.Close()
	for _, t := range terms {
		if observe != nil {
			observe(t, rw.Written())
		}
		if err := rw.Append(t, window[t]); err != nil {
			return 0, err
		}
	}
	return rw.Written(), rw.Commit()
}

// mergeSealedRuns merges live runs into one new run file (union semantics —
// runspill.MergeRuns), returning it plus the now-obsolete inputs; like
// sealWindow it hands back through openDurable's dirent barrier. Routing
// state is built as records stream out — no post-write re-read, no whole-run
// fingerprint accumulation (the old fps slice was the merge window's largest
// RSS transient).
func mergeSealedRuns(
	runs []*sealedRun, path string, fsyncDir func(string) error,
) (*sealedRun, []*sealedRun, error) {
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
	defer rw.Close()
	emit := func(rawTerm [16]byte, ids []uint32) error {
		term := TermKey(rawTerm)
		rt.observe(term, rw.Written())
		return rw.Append(term, ids)
	}
	if err := runspill.MergeRuns(paths, emit); err != nil {
		return nil, nil, err
	}
	payloadLen := rw.Written()
	if err := rw.Commit(); err != nil {
		return nil, nil, err
	}
	merged, err := rt.openDurable(path, payloadLen, fsyncDir)
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
	// The header's record count (validated at open, cross-checked by the
	// drain) sizes the routing state up front — the same one-pass runRouting
	// shape the write side uses, here fed from the verified drain.
	rt := newRunRouting(int(r.Records())) //nolint:gosec // bounded by payload/minRecordBytes at open
	for {
		off := r.Offset()
		term, _, nerr := r.Next()
		if errors.Is(nerr, io.EOF) {
			break
		}
		if nerr != nil {
			_ = r.Close()
			return nil, nerr
		}
		rt.observe(term, off)
	}
	end := r.Offset()
	if err := r.Close(); err != nil {
		return nil, err
	}
	return rt.open(path, end) // end = payload length
}

// lookup probes the run for one term: bloom reject → fence window → pread →
// linear decode with full-term verification. Concurrent-safe (ReadAt).
func (r *sealedRun) lookup(term TermKey) ([]uint32, error) {
	if !r.bloom.MayContain(fp64(term)) {
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
		recLen := PackedRecordLen(buf)
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

// runset.Run adapters: the publish protocol addresses a sealed run only by
// path and disposal.
func (r *sealedRun) RunPath() string { return r.path }
func (r *sealedRun) CloseRun()       { r.close() }

// ─────────────────────────── warmup ───────────────────────────

// OpenHotIndex rebuilds the engine from its manifest after a restart: every
// manifest-listed run is re-opened (drain-verified — corruption is a loud
// failure, never auto-healed), unreferenced files in dir are swept as
// orphans, and the caller then replays the un-sealed tail of packed rows
// (ledgers past the sealed frontier) through ApplyLedger. Sealed runs are
// adopted AS THEY ARE — already blinded under the chunk DB's adopted secret
// (hot_store.go), which is the same secret passed here — while the replayed
// rows are raw, exactly as they were live. The dense overlay
// starts empty and self-heals: a firehose term re-promotes on its first
// ledger, backfilling its history from window+runs.
func OpenHotIndex(
	dir string, manifest runset.Manifest, secret [stores.SecretLen]byte,
) (*HotIndex, uint32, error) {
	h, err := NewHotIndex(dir, manifest, secret)
	if err != nil {
		return nil, 0, err
	}
	names, lastSealed, err := manifest.GetRuns()
	if err != nil {
		return nil, 0, fmt.Errorf("events: hotindex manifest read: %w", err)
	}
	runs := make([]*sealedRun, 0, len(names))
	// Cleanup is exit-invariant: any return before the caller takes ownership
	// releases every run opened so far, however the open failed.
	opened := false
	defer func() {
		if !opened {
			closeRuns(runs)
		}
	}()
	for _, name := range names {
		r, oerr := openSealedRun(filepath.Join(dir, name))
		if oerr != nil {
			return nil, 0, fmt.Errorf("events: hotindex: manifest run %s: %w", name, oerr)
		}
		runs = append(runs, r)
	}
	if err := runset.SweepOrphans(dir, names); err != nil {
		return nil, 0, err
	}
	h.view.Store(&hotIndexView{runs: runs})
	// Both run-name prefixes this engine writes (startSeal).
	h.sealSeq = runset.NextSealSeq(names, "seal", "merge")
	opened = true
	return h, lastSealed, nil
}

func closeRuns(runs []*sealedRun) {
	for _, r := range runs {
		r.close()
	}
}
