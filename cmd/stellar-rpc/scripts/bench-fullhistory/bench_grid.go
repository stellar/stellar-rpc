package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

// Shared scaffolding for the 1D `--workers` sweep that every read
// bench (cold + hot, all data types) now uses. Each bench parses
// --workers as a comma-list, loops runConcurrentSweep once per
// worker count, prints one stdout row per cell + a saturation line
// at the end, and writes one summary CSV row per cell.
//
// parseIntList and createCSV are also used by the few benches that
// pre-existed the 1D convention (build-txhash-index, events benches).

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

// validateWorkersList enforces --workers >= 1. Calls fatal on the
// first violation.
func validateWorkersList(logger *supportlog.Entry, workersList []int) {
	for _, w := range workersList {
		if w < 1 {
			fatal(logger, "--workers values must be >= 1, got %d", w)
		}
	}
}

// sweepCSVHeader is the summary-row schema every read bench emits to
// its `<bench>-sweep.csv`: one row per worker count.
const sweepCSVHeader = "workers,iters,p50_ms,p90_ms,p99_ms,max_ms,ops_per_sec,errors"

// createCSV creates <outDir>/<name>.csv (overwriting if present),
// writes the given header line, and returns the open file plus its
// path. The caller is responsible for closing it.
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

// printSweepHeader prints the column header for the per-workers
// table emitted to stdout during a sweep.
func printSweepHeader() {
	fmt.Printf("\n%-9s %-7s %-9s %-9s %-9s %-9s %-10s %-7s\n",
		"workers", "iters", "p50_ms", "p90_ms", "p99_ms", "max_ms", "ops/sec", "errors")
	fmt.Println(strings.Repeat("-", 80))
}

// printSweepRow prints one cell's stdout row and writes its summary
// CSV row. csvF must be opened with sweepCSVHeader.
func printSweepRow(workers int, res concurrentResult, csvF *os.File) {
	s := res.stats
	p50ms := float64(s.p50.Microseconds()) / 1000.0
	p90ms := float64(s.p90.Microseconds()) / 1000.0
	p99ms := float64(s.p99.Microseconds()) / 1000.0
	maxms := float64(s.maxv.Microseconds()) / 1000.0
	fmt.Printf("%-9d %-7d %-9.2f %-9.2f %-9.2f %-9.2f %-10.0f %-7d\n",
		workers, s.n, p50ms, p90ms, p99ms, maxms, s.opsPerSec, res.totalErrs)
	fmt.Fprintf(csvF, "%d,%d,%.3f,%.3f,%.3f,%.3f,%.1f,%d\n",
		workers, s.n, p50ms, p90ms, p99ms, maxms, s.opsPerSec, res.totalErrs)
}

// reportSaturation finds the workers value with peak ops/sec across
// the sweep and prints it. Only meaningful when the sweep covers
// more than one worker count.
func reportSaturation(workersList []int, results []concurrentResult) {
	if len(results) < 2 {
		return
	}
	bestIdx := 0
	for i := 1; i < len(results); i++ {
		if results[i].stats.opsPerSec > results[bestIdx].stats.opsPerSec {
			bestIdx = i
		}
	}
	best := results[bestIdx].stats
	p50ms := float64(best.p50.Microseconds()) / 1000.0
	p99ms := float64(best.p99.Microseconds()) / 1000.0
	fmt.Printf("Peak: workers=%d ops/s=%.0f p50=%.2fms p99=%.2fms\n",
		workersList[bestIdx], best.opsPerSec, p50ms, p99ms)
}
