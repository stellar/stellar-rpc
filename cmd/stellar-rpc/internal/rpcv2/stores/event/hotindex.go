package event

// hotindex.go — the Sorted-Run Tier: the hot chunk's term index with a
// hard-bounded live set, replacing the O(unique terms) in-memory mirror for
// sparse terms (design: ~/bench-artifacts/mirror-memory-design.md).
//
// Structure (all published through ONE atomic view pointer):
//
//   - WINDOW: the last ≤windowLedgers ledgers' packed index rows, RETAINED
//     as-is (IngestLedgerToBatch already allocates each row for the commit
//     batch; keeping the slice is free) in a ring, plus one pointerless
//     "accel" pair of arrays per row (sorted fp64 + record offset) built by a
//     single linear scan — rows are term-sorted, so the accel is sorted by
//     construction.
//   - RUNS: immutable flat files beside the chunk DB (runspill format: full
//     16-byte terms, CRC-framed), sealed from the window every windowLedgers
//     ledgers on a background goroutine, each with an in-RAM bloom + fence
//     array (sidecar-derived; rebuildable by draining the run). A single-level
//     merge caps live runs at maxLiveRuns.
//   - DENSE OVERLAY: terms promoted by a per-ledger admission policy keep
//     their roaring bitmaps in the existing events.ConcurrentBitmaps (with
//     its tail-delta optimization) — popular-term queries stay memory-fast.
//     Dense postings ALSO flow into rows/runs (set-union dedupes), so the
//     overlay is rebuildable and runs are self-contained.
//
// Reads are exact: fp64 hits verify the full 16-byte term before IDs count.
// Ingest-side work per ledger is O(row bytes) with two pointerless
// allocations — no per-term map or bitmap mutation for sparse terms.
//
// Concurrency: single-writer (the ingest goroutine calls ApplyLedger; seals
// and merges run on ONE background goroutine owned here); readers Load the
// view and use immutable state lock-free. The publish-run-before-trim-window
// ordering guarantees a term is always findable in window ∪ runs.

import (
	"bytes"
	"cmp"
	"fmt"
	"os"
	"slices"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
)

const (
	// windowLedgers is the seal cadence: rows retained before a background
	// seal folds them into a run. 256 ledgers ≈ 2.5min at 600ms cadence.
	windowLedgers = 256

	// maxLiveRuns triggers the single-level merge: more than this many live
	// runs would push rare-term probes past the cold-query budget.
	maxLiveRuns = 8

	// densePromoteWindowCount promotes a term to the dense overlay when ONE
	// ledger brings it at least this many events. Firehose terms (every
	// event) promote on their first ledger; the long tail never does —
	// exactness for it comes from window+runs. Deliberately conservative:
	// a missed promotion costs read speed on that term, never correctness.
	densePromoteWindowCount = 32

	// fenceEvery is the record granularity of a run's fence array: one
	// (term, offset) fence per fenceEvery records bounds a probe's pread +
	// decode span.
	fenceEvery = 64
)

// hotIndexView is the immutable read view: everything a query needs, swapped
// atomically as ledgers apply, seals publish, and merges land.
type hotIndexView struct {
	// rows[i] is the i-th UNSEALED ledger row (oldest first): the retained
	// packed-row bytes and its accel arrays. Never mutated after publish.
	rows []windowRow
	// runs are the live sealed runs, oldest first.
	runs []*sealedRun
}

// windowRow is one retained per-ledger packed row plus its accel: fps and
// offs are parallel arrays, sorted by fp (row order), mapping each term's
// fp64 to its record offset in bytes.
type windowRow struct {
	seq   uint32 // the ledger this row belongs to
	bytes []byte
	fps   []uint64
	offs  []uint32
}

// sealedRun is one live run file with its in-RAM routing state and an open
// handle for concurrent ReadAt lookups.
type sealedRun struct {
	path   string
	bloom  bloomFilter
	fences []fence // sorted by term; one per fenceEvery records + final end sentinel
	file   *os.File
}

// fence marks a record boundary: the first term at offset off (byte offset
// into the run file's payload region).
type fence struct {
	term events.TermKey
	off  int64
}

// HotIndex is the engine. One per open hot chunk (read-write opens).
type HotIndex struct {
	dir     string // run-file directory (inside the chunk's DB dir)
	overlay *events.ConcurrentBitmaps
	view    atomic.Pointer[hotIndexView]

	// Writer-owned state (single-writer contract). sealEvery/maxRuns default
	// to the package consts; tests shrink them to exercise seals and merges
	// with small inputs.
	sealEvery       int
	maxRuns         int
	ledgersInWindow int
	sealSeq         int
	pendingSeal     chan sealResult // capacity 1: at most one seal in flight
	sealInFlight    bool
	manifest        manifestStore

	// sealArmed gates the seal trigger. The engine starts DISARMED so an
	// open's warmup replay is pure in-memory reconstruction — no run files,
	// no manifest writes — and the index's durable state cannot change
	// before verifyChunkConsistency has judged the open. A failed open is
	// then identically retryable, and the tripwire's inputs can never be
	// moved by the very open it is guarding. warmup arms via ArmSealing
	// only after verification passes.
	sealArmed bool

	// retired holds run handles displaced by a merge fold. They are NOT
	// closed at fold time: lock-free readers may still hold the previous
	// view and probe them, so the handles stay open (their files already
	// unlinked) until Close, when no reader can remain. Writer-owned.
	retired []*sealedRun

	// closed makes Close a one-shot: it drains the in-flight seal (if any)
	// and releases every run handle this index still owns.
	closed sync.Once
}

// sealResult is the background sealer's hand-back.
type sealResult struct {
	run        *sealedRun
	rows       int    // number of window rows the seal covered
	lastSeq    uint32 // highest ledger the seal covered (manifest frontier)
	replaceAll bool   // run replaces ALL live runs (a merge happened)
	obsolete   []*sealedRun
	err        error
}

// manifestStore persists which runs are live — the crash-recovery authority.
// Implemented over the chunk's RocksDB by the integration layer; faked in
// tests.
type manifestStore interface {
	// PutRuns atomically replaces the live-run list and records the highest
	// sealed ledger (warmup replays packed rows PAST it).
	PutRuns(names []string, lastSealed uint32) error
	// GetRuns returns the live-run list and the sealed frontier (zero when
	// nothing sealed).
	GetRuns() ([]string, uint32, error)
}

// NewHotIndex creates the engine for a fresh chunk. dir is created.
func NewHotIndex(dir string, manifest manifestStore) (*HotIndex, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("events: hotindex mkdir %s: %w", dir, err)
	}
	h := &HotIndex{
		dir:         dir,
		overlay:     events.NewConcurrentBitmapsFromBitmaps(events.Bitmaps{}),
		pendingSeal: make(chan sealResult, 1),
		manifest:    manifest,
		sealEvery:   windowLedgers,
		maxRuns:     maxLiveRuns,
	}
	h.view.Store(&hotIndexView{})
	return h, nil
}

// ArmSealing enables the background seal machinery (see sealArmed): sealing
// rights follow validation, the same rule the catalog applies to artifact
// state. The first armed ApplyLedger that finds the window over-full seals
// ALL of it (startSeal covers the whole window), so a backlog replayed while
// disarmed drains in one background seal — no separate drain path. Writer
// goroutine only.
func (h *HotIndex) ArmSealing() { h.sealArmed = true }

// fp64 is the accel/bloom fingerprint of a term.
func fp64(term events.TermKey) uint64 { return xxhash.Sum64(term[:]) }

// ApplyLedger ingests one committed ledger's index state: rowBytes is the
// SAME packed-row slice the commit batch carried (retained, not copied —
// the caller must not reuse it), runs the per-term view the write path
// already built. runs is BORROWED — consumed synchronously, never retained
// (the overlay copies every ID it keeps), so callers may back it with
// reused arenas. Writer goroutine only.
func (h *HotIndex) ApplyLedger(seq uint32, rowBytes []byte, runs termRuns) error {
	// Fold any completed seal FIRST so its run becomes visible before the
	// window trims past its ledgers on this call's publish.
	if err := h.reapSeal(false); err != nil {
		return err
	}

	// Dense admission + overlay feed: promoted terms get their IDs in RAM.
	// Per-term decisions are independent, so iterating byte-sorted runs
	// instead of the retired map changes nothing — the overlay-equivalence
	// differential in termsort_test.go pins it.
	for r := range runs.terms {
		term := runs.terms[r]
		ids := runs.run(r)
		if h.overlay.Has(term) {
			h.overlay.AddTo(term, ids...)
		} else if len(ids) >= densePromoteWindowCount {
			// Late promotion: backfill the term's earlier IDs from the
			// current view (window+runs) so the overlay is complete, then
			// add this ledger's.
			prior, err := h.lookupSparse(h.view.Load(), term)
			if err != nil {
				return fmt.Errorf("events: hotindex promote %x: %w", term, err)
			}
			h.overlay.AddTo(term, prior...)
			h.overlay.AddTo(term, ids...)
		}
	}

	row := buildWindowRow(rowBytes, len(runs.terms))
	row.seq = seq
	old := h.view.Load()
	rows := make([]windowRow, 0, len(old.rows)+1)
	rows = append(rows, old.rows...)
	rows = append(rows, row)
	h.view.Store(&hotIndexView{rows: rows, runs: old.runs})
	h.ledgersInWindow++

	if h.sealArmed && h.ledgersInWindow >= h.sealEvery && !h.sealInFlight {
		h.startSeal(rows)
	}
	return nil
}

// buildWindowRow scans a packed row once, emitting the accel arrays. The row
// is term-sorted, so fps comes out sorted (fp64 is not order-preserving, so
// we sort the pair arrays — ~24k entries, microseconds). nTerms sizes the
// scratch (the caller's run count is exact); this runs post-commit on
// the ingest goroutine, inside per-ledger latency.
func buildWindowRow(rowBytes []byte, nTerms int) windowRow {
	type pair struct {
		fp  uint64
		off uint32
	}
	pairs := make([]pair, 0, nTerms)
	off := 0
	rest := rowBytes
	for len(rest) > 0 {
		var term events.TermKey
		copy(term[:], rest[:16])
		pairs = append(pairs, pair{fp: fp64(term), off: uint32(off)})
		adv := events.PackedRecordLen(rest)
		rest = rest[adv:]
		off += adv
	}
	slices.SortFunc(pairs, func(a, b pair) int { return cmp.Compare(a.fp, b.fp) })
	row := windowRow{bytes: rowBytes, fps: make([]uint64, len(pairs)), offs: make([]uint32, len(pairs))}
	for i, p := range pairs {
		row.fps[i] = p.fp
		row.offs[i] = p.off
	}
	return row
}

// Get returns the term's postings (the zero value if absent): overlay fast
// path, else window+runs. Concurrent-safe.
//
// Neither path materializes an id list into a bitmap.
func (h *HotIndex) Get(term events.TermKey) (events.Postings, error) {
	if post, err := h.overlay.Get(term); err != nil || post.Present() {
		return post, err
	}
	ids, err := h.lookupSparse(h.view.Load(), term)
	if err != nil {
		return events.Postings{}, err
	}
	return events.IDPostings(ids), nil
}

// decodeRecordIDs decodes ONE packed record's ID list.
func decodeRecordIDs(rec []byte) ([]uint32, error) {
	var out []uint32
	err := events.DecodePackedRow(rec[:events.PackedRecordLen(rec)], func(_ events.TermKey, ids []uint32) {
		out = append(out, ids...)
	})
	return out, err
}

// dedupAscendingIDs dedups an ascending-with-possible-overlap ID list (a
// dense term's postings appear in both a run and the overlay path never hits
// here; window/run overlap cannot happen — but keep reads defensive).
func dedupAscendingIDs(ids []uint32) []uint32 {
	return slices.Compact(ids)
}

// Close drains any in-flight seal (discarding its result — warmup rebuilds
// deterministically) and releases every run handle this index still owns:
// the live view's runs, the handles retired by merge folds (kept open for
// lock-free readers), and a drained seal's freshly opened run. Without this,
// every chunk close leaked up to maxRuns open fds whose unlinked files —
// multi-GB after a late-chunk merge — stayed pinned on disk until process
// exit. One-shot; runs after ingestion and readers have stopped.
func (h *HotIndex) Close() {
	h.closed.Do(func() {
		if h.sealInFlight {
			res := <-h.pendingSeal
			h.sealInFlight = false
			if res.err == nil {
				res.run.close()
				for _, r := range res.obsolete {
					r.close()
				}
			}
		}
		for _, r := range h.view.Load().runs {
			r.close()
		}
		for _, r := range h.retired {
			r.close()
		}
		h.retired = nil
	})
}

// lookupSparse collects a term's IDs from the view's runs (oldest first)
// then window rows (oldest first) — ascending ID order overall, since runs
// hold strictly older ledgers than the window.
func (h *HotIndex) lookupSparse(v *hotIndexView, term events.TermKey) ([]uint32, error) {
	var out []uint32
	fp := fp64(term)
	for _, r := range v.runs {
		ids, err := r.lookup(term)
		if err != nil {
			return nil, err
		}
		out = append(out, ids...)
	}
	for i := range v.rows {
		row := &v.rows[i]
		// Binary search the accel for fp; verify the full term at each hit
		// (fp64 collisions are possible, exactness is not negotiable).
		j := sort.Search(len(row.fps), func(k int) bool { return row.fps[k] >= fp })
		for ; j < len(row.fps) && row.fps[j] == fp; j++ {
			rec := row.bytes[row.offs[j]:]
			if !bytes.Equal(rec[:16], term[:]) {
				continue
			}
			ids, err := decodeRecordIDs(rec)
			if err != nil {
				return nil, err
			}
			out = append(out, ids...)
		}
	}
	return dedupAscendingIDs(out), nil
}
