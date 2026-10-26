package bench

import (
	"bufio"
	"fmt"
	"os"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

// traceFile is the per-ledger trace a hot run writes with --trace.
const traceFile = "trace.csv"

// traceHeader is the trace's header row: one row per ingested ledger with
// every phase's duration. wall_ns is the Unix time of the ledger's extract
// phase, so a slow ledger can be matched against other timelines, such as
// RocksDB's LOG. pace_lag_ns is 0 on an unpaced run.
const traceHeader = "seq,wall_ns,extract_ns,ledgers_ns,txhash_ns,events_ns,commit_ns,apply_ns,total_ns,pace_lag_ns"

// hotTrace writes one trace row per ledger. The csvSink calls it under its
// own mutex: recordPhase from HotPhase, then writeRow from LastCommitted,
// which the hot loop calls after each ledger's phases.
type hotTrace struct {
	f *os.File
	w *bufio.Writer

	// pending holds the current ledger's phases; complete is set by its apply
	// phase, so a ledger that failed earlier writes no row.
	pending  [hotchunk.NumPhases]time.Duration
	start    time.Time
	complete bool

	// err is the first write failure; the trace stops there, and close
	// reports it.
	err error
}

func newHotTrace(path string) (*hotTrace, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", path, err)
	}
	w := bufio.NewWriterSize(f, 1<<16)
	if _, err := fmt.Fprintln(w, traceHeader); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("write %s: %w", path, err)
	}
	return &hotTrace{f: f, w: w}, nil
}

func (t *hotTrace) recordPhase(phase hotchunk.Phase, d time.Duration) {
	if phase == hotchunk.PhaseExtract {
		t.pending = [hotchunk.NumPhases]time.Duration{}
		t.start = time.Now()
		t.complete = false
	}
	t.pending[phase] = d
	if phase == hotchunk.PhaseApply {
		t.complete = true
	}
}

func (t *hotTrace) writeRow(seq uint32, paceLag time.Duration) {
	if !t.complete || t.err != nil {
		return
	}
	t.complete = false
	var total time.Duration
	for _, d := range t.pending {
		total += d
	}
	_, err := fmt.Fprintf(t.w, "%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
		seq, t.start.UnixNano(),
		t.pending[hotchunk.PhaseExtract].Nanoseconds(),
		t.pending[hotchunk.PhaseLedgers].Nanoseconds(),
		t.pending[hotchunk.PhaseTxhash].Nanoseconds(),
		t.pending[hotchunk.PhaseEvents].Nanoseconds(),
		t.pending[hotchunk.PhaseCommit].Nanoseconds(),
		t.pending[hotchunk.PhaseApply].Nanoseconds(),
		total.Nanoseconds(), paceLag.Nanoseconds())
	if err != nil {
		t.err = fmt.Errorf("write %s row for ledger %d: %w", traceFile, seq, err)
	}
}

// close flushes and closes the trace, returning its first error.
func (t *hotTrace) close() error {
	ferr := t.w.Flush()
	cerr := t.f.Close()
	switch {
	case t.err != nil:
		return t.err
	case ferr != nil:
		return fmt.Errorf("flush %s: %w", traceFile, ferr)
	default:
		return cerr
	}
}
