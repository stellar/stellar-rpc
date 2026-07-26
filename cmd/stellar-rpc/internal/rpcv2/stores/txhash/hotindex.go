package txhash

// hotindex.go — the packed-row hot tier: the hot chunk's (tx hash → ledger
// seq) index with a hard-bounded live set, replacing the one-Put-per-hash
// mirror CF (design: ~/bench-artifacts/txhash-onerow-design.md). A
// deliberate simplification-clone of the events Sorted-Run Tier
// (event/hotindex.go) minus its three hardest parts — no dense overlay, no
// merge tier, no retired handles: a txhash probe is a single-hit
// bloom-routed point lookup, not a per-term union, so runs never need
// displacing.
//
// Structure (all published through ONE atomic view pointer):
//
//   - WINDOW: the last ≤sealEvery ledgers' packed rows, RETAINED as-is (the
//     commit batch already allocates each row; keeping the slice is free).
//     A row is one ledger's full 32-byte hashes, sorted, concatenated —
//     directly binary-searchable, no accel arrays.
//   - RUNS: immutable flat files beside the chunk DB (TXHRUN01 format:
//     36-byte hash‖seq records, hash-sorted, CRC64-framed), sealed from the
//     window every sealEvery ledgers on a background goroutine, each with
//     an in-RAM bloom + page ladder rebuilt from the verified file bytes.
//
// Reads are exact: every route ends in a full 32-byte hash compare before a
// sequence is returned. The row chain is DENSE — one row per ledger, empty
// value for tx-less ledgers — so the CF is self-validating: a gap is
// corruption, never ambiguity.
//
// Concurrency: single-writer (the ingest goroutine calls ApplyRow; seals run
// on ONE background goroutine owned here); readers Load the view and use
// immutable state lock-free. The publish-run-before-trim-window ordering
// guarantees a hash is always findable in window ∪ runs.

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// windowLedgers is the seal cadence: rows retained before a background seal
// folds them into a run. 256 ledgers ≈ 2.5min at 600ms cadence — the same
// cadence the events tier landed with.
const windowLedgers = 256

// hotIndexView is the immutable read view: everything a probe needs, swapped
// atomically as ledgers apply and seals publish.
type hotIndexView struct {
	// rows[i] is the i-th UNSEALED ledger row (oldest first): the retained
	// packed-row bytes. Never mutated after publish.
	rows []windowRow
	// runs are the live sealed runs, oldest first.
	runs []*sealedRun
}

// windowRow is one retained per-ledger packed row: the ledger it belongs to
// and its EncodeRow bytes (32-byte stride, strictly ascending).
type windowRow struct {
	seq   uint32
	bytes []byte
}

// sealedRun is one live run file with its in-RAM routing state and an open
// handle for concurrent ReadAt lookups. first/last is the ledger range the
// run covers; records is the drain-verified payload record count (it bounds
// the final page's pread).
type sealedRun struct {
	path    string
	bloom   bloomFilter
	ladder  [][ladderKeyLen]byte // first 16B of each page's first record
	file    *os.File
	first   uint32
	last    uint32
	records int
}

// HotIndex is the engine. One per open hot chunk (read-write opens).
type HotIndex struct {
	dir  string // run-file directory (inside the chunk's DB dir)
	view atomic.Pointer[hotIndexView]

	// Writer-owned state (single-writer contract). sealEvery defaults to
	// windowLedgers; tests shrink it to exercise seals with small inputs.
	sealEvery       int
	ledgersInWindow int
	sealSeq         int
	pendingSeal     chan sealResult // capacity 1: at most one seal in flight
	sealInFlight    bool
	manifest        manifestStore

	// sealArmed gates the seal trigger. The engine starts DISARMED so an
	// open's warmup replay is pure in-memory reconstruction — no run files,
	// no manifest writes — and the index's durable state cannot change
	// before the open has been judged valid. A failed open is then
	// identically retryable, and the open tripwire's inputs can never be
	// moved by the very open it is guarding. Warmup arms via ArmSealing
	// only after verification passes.
	sealArmed bool

	// expectedNext pins the dense row chain: ApplyRow accepts exactly this
	// sequence, loudly rejecting anything else. Zero = unanchored (a fresh
	// NewHotIndex engine's first row anchors the chain; OpenHotIndex anchors
	// at max(lastSealed+1, chunk first ledger)).
	expectedNext uint32

	// closed makes Close a one-shot: it drains the in-flight seal (if any)
	// and releases every run handle this index still owns.
	closed sync.Once
}

// sealResult is the background sealer's hand-back.
type sealResult struct {
	run     *sealedRun
	rows    int    // number of window rows the seal covered
	lastSeq uint32 // highest ledger the seal covered (manifest frontier)
	err     error
}

// NewHotIndex creates the engine for a fresh chunk. dir is created.
func NewHotIndex(dir string, manifest manifestStore) (*HotIndex, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("txhash: hotindex mkdir %s: %w", dir, err)
	}
	h := &HotIndex{
		dir:         dir,
		pendingSeal: make(chan sealResult, 1),
		manifest:    manifest,
		sealEvery:   windowLedgers,
	}
	h.view.Store(&hotIndexView{})
	return h, nil
}

// ArmSealing enables the background seal machinery (see sealArmed): sealing
// rights follow validation, the same rule the events engine applies. The
// first armed ApplyRow that finds the window over-full seals ALL of it
// (startSeal covers the whole window), so a backlog replayed while disarmed
// drains in one background seal — no separate drain path. Writer goroutine
// only.
func (h *HotIndex) ArmSealing() { h.sealArmed = true }

// ApplyRow ingests one committed ledger's packed row: rowBytes is the SAME
// slice the commit batch carried (retained, not copied — the caller must not
// reuse it), count its hash count. The row must be EncodeRow/validateRow
// clean — both feeding paths (write, warmup replay) validate before
// applying. Writer goroutine only.
func (h *HotIndex) ApplyRow(seq uint32, rowBytes []byte, count int) error {
	// Fold any completed seal FIRST so its run becomes visible before the
	// window trims past its ledgers on this call's publish.
	if err := h.reapSeal(false); err != nil {
		return err
	}
	if len(rowBytes) != count*rowHashLen {
		return fmt.Errorf("txhash: hotindex row %d: %d bytes for %d hashes", seq, len(rowBytes), count)
	}
	if seq == 0 || (h.expectedNext != 0 && seq != h.expectedNext) {
		return fmt.Errorf("txhash: hotindex row %d breaks the dense chain (want %d)", seq, h.expectedNext)
	}

	old := h.view.Load()
	rows := make([]windowRow, 0, len(old.rows)+1)
	rows = append(rows, old.rows...)
	rows = append(rows, windowRow{seq: seq, bytes: rowBytes})
	h.view.Store(&hotIndexView{rows: rows, runs: old.runs})
	h.expectedNext = seq + 1
	h.ledgersInWindow++

	if h.sealArmed && h.ledgersInWindow >= h.sealEvery && !h.sealInFlight {
		h.startSeal(rows)
	}
	return nil
}

// Get returns the ledger holding hash, or stores.ErrNotFound. Window rows
// are probed newest→oldest, then runs newest→oldest — recent transactions
// are the hot hits. Correctness is order-independent: a hash lives in one
// ledger, and for the legal cross-ledger duplicate any containing ledger is
// a correct answer (GetTransaction verifies against the ledger itself).
// Concurrent-safe.
func (h *HotIndex) Get(hash [32]byte) (uint32, error) {
	v := h.view.Load()
	for i := len(v.rows) - 1; i >= 0; i-- {
		if rowContains(v.rows[i].bytes, hash) {
			return v.rows[i].seq, nil
		}
	}
	for i := len(v.runs) - 1; i >= 0; i-- {
		seq, ok, err := v.runs[i].lookup(hash)
		if err != nil {
			return 0, err
		}
		if ok {
			return seq, nil
		}
	}
	return 0, stores.ErrNotFound
}

// Close drains any in-flight seal (discarding its result — warmup rebuilds
// deterministically) and releases every run handle this index still owns.
// Runs are never displaced (no merge tier), so the live view's handles are
// all there are. One-shot; runs after ingestion and readers have stopped.
func (h *HotIndex) Close() {
	h.closed.Do(func() {
		if h.sealInFlight {
			res := <-h.pendingSeal
			h.sealInFlight = false
			if res.err == nil {
				res.run.close()
			}
		}
		for _, r := range h.view.Load().runs {
			r.close()
		}
	})
}

// fp64 is the bloom fingerprint of a hash.
func fp64(hash [32]byte) uint64 { return xxhash.Sum64(hash[:]) }

// rowContains binary-searches one packed row (32-byte stride, strictly
// ascending) for hash.
func rowContains(row []byte, hash [32]byte) bool {
	n := len(row) / rowHashLen
	i := sort.Search(n, func(k int) bool {
		return bytes.Compare(row[k*rowHashLen:(k+1)*rowHashLen], hash[:]) >= 0
	})
	return i < n && bytes.Equal(row[i*rowHashLen:(i+1)*rowHashLen], hash[:])
}
