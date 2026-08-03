package bench

import (
	"bufio"
	"fmt"
	"os"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

// hotTraceHeader is the per-ledger trace CSV's header row: one row per
// ingested ledger, wall-clock stamped, with every phase duration — the
// row-level data the aggregated hot.csv percentiles are built from. wall_ns
// is Unix nanoseconds at the ledger's PhaseExtract signal (the burst's first),
// so rows can be correlated against external timelines (RocksDB LOG flush
// events, iostat samples). pace_lag_ns is 0 on unpaced runs.
const hotTraceHeader = "seq,wall_ns,extract_ns,ledgers_ns,txhash_ns,events_ns,commit_ns,apply_ns,total_ns,pace_lag_ns"

// hotTrace streams one CSV row per ingested ledger to --trace. The csvSink
// feeds it under the sink's own mutex (recordPhase from HotPhase, writeRow
// from LastCommitted), so it adds no locking of its own. The production hot
// loop is a single goroutine emitting strict per-ledger phase bursts
// (extract → … → apply, then LastCommitted), which is what makes the
// pending-row accumulation correct.
type hotTrace struct {
	f *os.File
	w *bufio.Writer

	// pending accumulates the current ledger's phase durations; wallStart
	// stamps the burst's first signal. complete latches on PhaseApply — the
	// burst's terminal phase. PhaseApply is also emitted when the apply
	// itself fails, but the ingest loop then aborts before LastCommitted,
	// so writeRow still never attributes a partial burst to a committed seq.
	pending   [hotchunk.NumPhases]time.Duration
	wallStart time.Time
	complete  bool

	// err records the first write failure; tracing stops reporting rows
	// after it (close surfaces it once, the run itself is never failed).
	err error
}

// newHotTrace creates (truncates) path and writes the header row.
func newHotTrace(path string) (*hotTrace, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create --trace %s: %w", path, err)
	}
	w := bufio.NewWriterSize(f, 1<<16)
	if _, err := fmt.Fprintln(w, hotTraceHeader); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("write --trace header: %w", err)
	}
	return &hotTrace{f: f, w: w}, nil
}

// recordPhase folds one phase signal into the pending row. PhaseExtract (the
// burst's first signal) resets the row and stamps its wall time; PhaseApply
// (terminal) marks it complete for writeRow.
func (t *hotTrace) recordPhase(phase hotchunk.Phase, d time.Duration) {
	if phase == hotchunk.PhaseExtract {
		t.pending = [hotchunk.NumPhases]time.Duration{}
		t.wallStart = time.Now()
		t.complete = false
	}
	t.pending[phase] = d
	if phase == hotchunk.PhaseApply {
		t.complete = true
	}
}

// writeRow emits the pending row as ledger seq's trace line and resets it.
// A row whose burst never completed (a failed ledger's partial burst, or a
// LastCommitted with no burst at all) is dropped — aggregates already carry
// partial-phase samples; the trace carries only whole ledgers.
func (t *hotTrace) writeRow(seq uint32, paceLag time.Duration) {
	if !t.complete {
		return
	}
	t.complete = false
	if t.err != nil {
		return
	}
	var total time.Duration
	for _, d := range t.pending {
		total += d
	}
	_, err := fmt.Fprintf(t.w, "%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
		seq, t.wallStart.UnixNano(),
		t.pending[hotchunk.PhaseExtract].Nanoseconds(),
		t.pending[hotchunk.PhaseLedgers].Nanoseconds(),
		t.pending[hotchunk.PhaseTxhash].Nanoseconds(),
		t.pending[hotchunk.PhaseEvents].Nanoseconds(),
		t.pending[hotchunk.PhaseCommit].Nanoseconds(),
		t.pending[hotchunk.PhaseApply].Nanoseconds(),
		total.Nanoseconds(), paceLag.Nanoseconds())
	if err != nil {
		t.err = fmt.Errorf("write --trace row seq %d: %w", seq, err)
	}
}

// close flushes and closes the trace file, returning the first error the
// trace hit (a mid-run write failure, a flush failure, or the close itself).
func (t *hotTrace) close() error {
	ferr := t.w.Flush()
	cerr := t.f.Close()
	switch {
	case t.err != nil:
		return t.err
	case ferr != nil:
		return fmt.Errorf("flush --trace: %w", ferr)
	default:
		return cerr
	}
}
