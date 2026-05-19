package main

import (
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// cmdLedgerRangeConcurrencySweep runs a grid of (workers x page-size)
// scenarios over the cold ledger store. Per iteration: pick a random
// chunk in [chunk-lo, chunk-hi], evict its packfile, open a fresh
// ColdStoreReader, read N consecutive ledgers starting at a random
// in-chunk position, close. Reports p50 / p99 / ops-per-second per
// (workers, n) cell so the saturation knee is visible.
func cmdLedgerRangeConcurrencySweep() {
	fs := flag.NewFlagSet("ledger-range-concurrency-sweep", flag.ExitOnError)
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold", "cold-store root")
	chunkLo := fs.Uint("chunk-lo", 4999, "low chunk id (inclusive)")
	chunkHi := fs.Uint("chunk-hi", 5050, "high chunk id (inclusive)")
	workersCSV := fs.String("workers", "1,2,4,8,16,24,32,48,64", "comma-separated worker counts to sweep")
	pageSizesCSV := fs.String("page-sizes", "1,10,20", "comma-separated --n values to sweep")
	itersPerWorker := fs.Int("iters", 60, "iterations per worker per scenario")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	if *chunkHi < *chunkLo {
		fatal(logger, "chunk-hi (%d) < chunk-lo (%d)", *chunkHi, *chunkLo)
	}
	chunkSpan := uint32(*chunkHi - *chunkLo + 1)

	workers, err := parseIntList(*workersCSV)
	if err != nil {
		fatal(logger, "parse --workers: %v", err)
	}
	pageSizes, err := parseIntList(*pageSizesCSV)
	if err != nil {
		fatal(logger, "parse --page-sizes: %v", err)
	}

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal(logger, "mkdir %s: %v", *outDir, err)
	}
	summaryPath := filepath.Join(*outDir, "ledger-range-concurrency-sweep.csv")
	summaryF, ferr := os.Create(summaryPath)
	if ferr != nil {
		fatal(logger, "create %s: %v", summaryPath, ferr)
	}
	defer summaryF.Close()
	fmt.Fprintln(summaryF, "page_size,workers,iters,p50_ms,p90_ms,p99_ms,max_ms,ops_per_sec,errors")

	logger.Infof("sweep workers=%v page_sizes=%v iters=%d chunk_span=%d",
		workers, pageSizes, *itersPerWorker, chunkSpan)
	fmt.Printf("\n%-8s %-9s %-7s %-9s %-9s %-9s %-10s\n",
		"page", "workers", "n", "p50_ms", "p99_ms", "max_ms", "ops/sec")
	fmt.Println(strings.Repeat("-", 70))

	var rows []sweepRow

	for _, n := range pageSizes {
		for _, w := range workers {
			res := runColdConcurrent(logger, *coldDir, uint32(*chunkLo), chunkSpan,
				w, *itersPerWorker, *seed, rangeWorkload(n))
			s := res.stats
			p50ms := float64(s.p50.Microseconds()) / 1000.0
			p90ms := float64(s.p90.Microseconds()) / 1000.0
			p99ms := float64(s.p99.Microseconds()) / 1000.0
			maxms := float64(s.maxv.Microseconds()) / 1000.0
			fmt.Printf("%-8d %-9d %-7d %-9.2f %-9.2f %-9.2f %-10.0f\n",
				n, w, s.n, p50ms, p99ms, maxms, s.opsPerSec)
			fmt.Fprintf(summaryF, "%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%.1f,%d\n",
				n, w, s.n, p50ms, p90ms, p99ms, maxms, s.opsPerSec, res.totalErrs)
			rows = append(rows, sweepRow{n, w, p50ms, p99ms, s.opsPerSec})
		}
		fmt.Println()
	}

	fmt.Println("\nSaturation summary (highest ops/sec per page size):")
	reportSaturation(rows)
	logger.Infof("wrote %s", summaryPath)
}

// rangeWorkload returns a coldWorkload that reads n consecutive ledgers
// starting at a random in-chunk position.
func rangeWorkload(n int) coldWorkload {
	startSpan := ledgersPerChunk - uint32(n) + 1
	return func(r *ledger.ColdStoreReader, rng *rand.Rand, c uint32) error {
		start := chunkFirstLedger(c) + rng.Uint32N(startSpan)
		end := start + uint32(n) - 1
		seen := 0
		var payloadErr error
		err := coldAdapter{r}.iterateRange(start, end, func(_ uint32, b []byte) bool {
			if len(b) == 0 {
				payloadErr = errors.New("empty payload")
				return false
			}
			seen++
			return true
		})
		if err != nil {
			return err
		}
		if payloadErr != nil {
			return payloadErr
		}
		if seen != n {
			return fmt.Errorf("got %d ledgers, expected %d", seen, n)
		}
		return nil
	}
}

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

type sweepRow struct {
	pageSize, workers int
	p50, p99, ops     float64
}

// reportSaturation prints, per page-size, the worker count with peak
// ops/sec plus p50/p99 at that point.
func reportSaturation(rows []sweepRow) {
	byPage := map[int][]sweepRow{}
	for _, r := range rows {
		byPage[r.pageSize] = append(byPage[r.pageSize], r)
	}
	pages := make([]int, 0, len(byPage))
	for k := range byPage {
		pages = append(pages, k)
	}
	sort.Ints(pages)
	fmt.Printf("%-10s %-12s %-10s %-10s %-12s\n",
		"page_size", "peak_workers", "peak_ops/s", "p50@peak", "p99@peak")
	for _, ps := range pages {
		rs := byPage[ps]
		best := rs[0]
		for _, r := range rs[1:] {
			if r.ops > best.ops {
				best = r
			}
		}
		fmt.Printf("%-10d %-12d %-10.0f %-10.2f %-12.2f\n",
			ps, best.workers, best.ops, best.p50, best.p99)
	}
}
