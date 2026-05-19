package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

// Shared scaffolding for the (n × workers) grid benchmark. Both
// cold-ledgers and hot-ledgers use the same flag surface (--n,
// --workers as comma-lists; --iters; --seed; --out) and emit the same
// CSV schema; the helpers here own that boilerplate.

// parseIntList parses "1,2,4,8" → [1,2,4,8]. Returns an error if the
// list is empty or any element fails to parse. Whitespace around
// elements and trailing commas are tolerated.
func parseIntList(s string) ([]int, error) {
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("bad int %q: %w", p, err)
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil, errors.New("empty list")
	}
	return out, nil
}

// validateGridFlags enforces the shared bounds on --n and --workers:
// each value must be >= 1, and each --n must fit within a single
// chunk's ledger capacity. Calls fatal on the first violation.
func validateGridFlags(logger *supportlog.Entry, nList, workersList []int) {
	for _, n := range nList {
		if n < 1 {
			fatal(logger, "--n values must be >= 1, got %d", n)
		}
		if uint32(n) > ledgersPerChunk {
			fatal(logger, "--n=%d exceeds single-chunk capacity %d", n, ledgersPerChunk)
		}
	}
	for _, w := range workersList {
		if w < 1 {
			fatal(logger, "--workers values must be >= 1, got %d", w)
		}
	}
}

// gridCSVHeader is the column set every (n × workers) read-bench
// emits, so post-processing tools can treat all read benches uniformly.
const gridCSVHeader = "n,workers,iters,p50_ms,p90_ms,p99_ms,max_ms,ops_per_sec,errors"

// createCSV creates <outDir>/<name>.csv (overwriting if present),
// writes the given header line, and returns the open file plus its
// path. The caller is responsible for closing it. Used by every bench
// subcommand that emits a CSV; the header string is the only thing
// that varies across them.
func createCSV(outDir, name, header string) (*os.File, string, error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, "", fmt.Errorf("mkdir %s: %w", outDir, err)
	}
	csvPath := filepath.Join(outDir, name+".csv")
	f, err := os.Create(csvPath)
	if err != nil {
		return nil, "", fmt.Errorf("create %s: %w", csvPath, err)
	}
	if _, err := fmt.Fprintln(f, header); err != nil {
		f.Close()
		return nil, "", fmt.Errorf("write csv header: %w", err)
	}
	return f, csvPath, nil
}

// printGridHeader prints the column header for the per-cell table
// emitted to stdout while a grid runs.
func printGridHeader() {
	fmt.Printf("\n%-8s %-9s %-7s %-9s %-9s %-9s %-10s\n",
		"n", "workers", "iters", "p50_ms", "p99_ms", "max_ms", "ops/sec")
	fmt.Println(strings.Repeat("-", 70))
}

// formatCellOutput emits one row of the per-cell table to stdout and
// one CSV row to csvF, returning the sweepRow needed for the
// saturation summary.
func formatCellOutput(n, workers int, res concurrentResult, csvF *os.File) sweepRow {
	s := res.stats
	p50ms := float64(s.p50.Microseconds()) / 1000.0
	p90ms := float64(s.p90.Microseconds()) / 1000.0
	p99ms := float64(s.p99.Microseconds()) / 1000.0
	maxms := float64(s.maxv.Microseconds()) / 1000.0
	fmt.Printf("%-8d %-9d %-7d %-9.2f %-9.2f %-9.2f %-10.0f\n",
		n, workers, s.n, p50ms, p99ms, maxms, s.opsPerSec)
	fmt.Fprintf(csvF, "%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%.1f,%d\n",
		n, workers, s.n, p50ms, p90ms, p99ms, maxms, s.opsPerSec, res.totalErrs)
	return sweepRow{n, workers, p50ms, p99ms, s.opsPerSec}
}

// runBenchGrid iterates the (n × workers) cross-product, calls runFn
// for each cell, prints rows to stdout, writes them to csvF, and (when
// there's more than one --workers value) prints a saturation summary
// at the end.
func runBenchGrid(
	csvF *os.File,
	nList, workersList []int,
	runFn func(n, workers int) concurrentResult,
) {
	var rows []sweepRow
	for _, n := range nList {
		for _, w := range workersList {
			rows = append(rows, formatCellOutput(n, w, runFn(n, w), csvF))
		}
		fmt.Println()
	}
	if len(workersList) > 1 {
		fmt.Println("Saturation summary (highest ops/sec per n):")
		reportSaturation(rows)
	}
}

// sweepRow is one cell's summary line, retained for the saturation
// table printed at the end of a multi-worker grid run.
type sweepRow struct {
	n, workers    int
	p50, p99, ops float64
}

// reportSaturation prints, per n value, the worker count with peak
// ops/sec plus p50/p99 at that point.
func reportSaturation(rows []sweepRow) {
	byN := map[int][]sweepRow{}
	for _, r := range rows {
		byN[r.n] = append(byN[r.n], r)
	}
	ns := make([]int, 0, len(byN))
	for k := range byN {
		ns = append(ns, k)
	}
	sort.Ints(ns)
	fmt.Printf("%-10s %-12s %-10s %-10s %-12s\n",
		"n", "peak_workers", "peak_ops/s", "p50@peak", "p99@peak")
	for _, n := range ns {
		rs := byN[n]
		best := rs[0]
		for _, r := range rs[1:] {
			if r.ops > best.ops {
				best = r
			}
		}
		fmt.Printf("%-10d %-12d %-10.0f %-10.2f %-12.2f\n",
			n, best.workers, best.ops, best.p50, best.p99)
	}
}
