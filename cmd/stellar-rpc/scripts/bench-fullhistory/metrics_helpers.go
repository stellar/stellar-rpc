package main

// Shared CSV-emit + stdout-summary helpers for the ingest collectors.
// Every collector emits the same per-stage CSV shape:
//
//	stage,n,n_items,total_ns,p50_ns,p90_ns,p99_ns,max_ns
//
// where n is the number of non-zero samples (empty-ledger samples are
// excluded from the percentile distribution to match the old per-type
// benches), and n_items is the data-type-specific count (tx hashes,
// event payloads, or 1 for ledgers).

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// stageRow is one CSV row's input. durs already filtered to non-zero
// samples by the caller; items is the data-type total across the
// entire run (sum of Items field on the underlying samples).
type stageRow struct {
	name  string
	durs  []time.Duration
	items int
}

// writeStageCSV writes the per-stage aggregation CSV for a collector.
// Rows whose duration slice is empty are suppressed (zero-total stages
// don't render).
func writeStageCSV(outDir, filenamePrefix string, rows []stageRow) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", outDir, err)
	}
	path := filepath.Join(outDir, filenamePrefix+".csv")
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()

	if _, err := fmt.Fprintln(f, "stage,n,n_items,total_ns,p50_ns,p90_ns,p99_ns,max_ns"); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	for _, r := range rows {
		if len(r.durs) == 0 {
			continue
		}
		s := computeStats(r.durs)
		if _, err := fmt.Fprintf(f, "%s,%d,%d,%d,%d,%d,%d,%d\n",
			r.name, len(r.durs), r.items, s.total.Nanoseconds(),
			s.p50.Nanoseconds(), s.p90.Nanoseconds(), s.p99.Nanoseconds(), s.maxv.Nanoseconds(),
		); err != nil {
			return fmt.Errorf("write row %s: %w", r.name, err)
		}
	}
	return nil
}

// printStageSummary prints one labeled percentile line to w. nSamples
// is the total sample count INCLUDING zero (empty-ledger) samples,
// for context; percentiles are computed over the non-zero subset.
func printStageSummary(w io.Writer, label string, durs []time.Duration, nSamples int) {
	if len(durs) == 0 {
		fmt.Fprintf(w, "  %-32s n=%d (all empty)\n", label, nSamples)
		return
	}
	s := computeStats(durs)
	fmt.Fprintf(w, "  %-32s n=%-5d total=%-10s p50=%-10s p90=%-10s p99=%-10s max=%-10s\n",
		label, len(durs),
		s.total.Round(time.Microsecond),
		s.p50.Round(time.Microsecond),
		s.p90.Round(time.Microsecond),
		s.p99.Round(time.Microsecond),
		s.maxv.Round(time.Microsecond),
	)
}

// printChunkScalar renders a per-chunk scalar field (commit, finish,
// sort, write_bin, prepare_range). When the slice is empty it prints
// nothing; with one entry it prints a single value; with more than
// one it prints the percentile distribution across chunks.
func printChunkScalar(w io.Writer, label string, durs []time.Duration) {
	switch len(durs) {
	case 0:
		return
	case 1:
		fmt.Fprintf(w, "  %-32s = %s\n", label, durs[0].Round(time.Microsecond))
		return
	default:
		printStageSummary(w, label, durs, len(durs))
	}
}

// printNamedStage prints the row named csvName from rows under label,
// reusing the durations the collector already filtered for its CSV so
// PrintSummary and WriteCSV share one filtering pass. nSamples is the
// full sample count (incl. empty) for the "(all empty)" context. A
// name not present in rows prints nothing.
func printNamedStage(w io.Writer, label string, rows []stageRow, csvName string, nSamples int) {
	for i := range rows {
		if rows[i].name == csvName {
			printStageSummary(w, label, rows[i].durs, nSamples)
			return
		}
	}
}

// stageRowsTotal sums the durations across every stage row — the
// per-ledger component of a collector's in-pipeline time.
func stageRowsTotal(rows []stageRow) time.Duration {
	var total time.Duration
	for i := range rows {
		total += sumDur(rows[i].durs)
	}
	return total
}

// printThroughput prints the wall-rate and in-pipeline-rate lines that
// the old benches' summary blocks ended with. wall is the driver's
// per-loop wall time for this run; inPipeline is the per-ingester
// sum-of-stage time (mode-invariant under --parallel).
func printThroughput(w io.Writer, label string, totalItems int, wall, inPipeline time.Duration) {
	if totalItems == 0 || wall == 0 || inPipeline == 0 {
		return
	}
	wallRate := float64(totalItems) / wall.Seconds()
	pipeRate := float64(totalItems) / inPipeline.Seconds()
	fmt.Fprintf(w, "  %-32s wall=%-10s (%6.0f items/s end-to-end)\n", label, wall.Round(time.Millisecond), wallRate)
	fmt.Fprintf(w, "  %-32s in-pipeline=%-10s (%6.0f items/s extract+write)\n", label, inPipeline.Round(time.Millisecond), pipeRate)
}
