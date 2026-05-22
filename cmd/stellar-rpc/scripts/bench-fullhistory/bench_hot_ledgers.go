package main

import (
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// hotWarmupIters — fixed warmup count for RocksDB block-cache priming
// in the hot-ledgers grid sweep. Hardcoded rather than exposed as a
// flag because hot-tier warmup is rarely a knob the bench user wants
// to tune, and the previous default (100) is what generated the
// reference numbers in bench-out/summary.csv.
const hotWarmupIters = 100

// hotWarmupSharedIters — default --warmup for hot query benches
// (hot-txpage, hot-txhash, hot-events). Lower than hotWarmupIters
// because per-iter work is dominated by lookup+scan, not raw
// block-cache fetches, so block-cache priming saturates faster. 20
// is empirically enough to flatten the warmup tail on a fresh open.
const hotWarmupSharedIters = 20

// cmdHotLedgers benches hot-store (RocksDB) ledger reads. One
// HotStore handle is opened at startup and shared across all workers
// — matching production usage where the server keeps one long-lived
// RocksDB handle.
//
// --chunk=N is required because the HotStore API exposes no FirstSeq /
// LastSeq for auto-discovery; the chunk ID drives the sampling range
// via chunkFirstLedger / chunkLastLedger. The store is expected to
// have been populated by `hot-ledgers-ingest --chunk=N` (which uses
// the same chunk semantics).
func cmdHotLedgers() {
	fs := flag.NewFlagSet("hot-ledgers", flag.ExitOnError)
	hotDir := fs.String("hot-dir", "", "hot-store (RocksDB) path (required)")
	chunk := fs.Uint("chunk", 0, "chunk ID whose ledgers are in the store (required)")
	nCSV := fs.String("n", "1", "ledgers per read; comma-list (e.g. 1,10,20)")
	workersCSV := fs.String("workers", "1", "parallel workers; comma-list (e.g. 1,4,16)")
	iters := fs.Int("iters", 60, "iterations per worker per cell")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	if *hotDir == "" {
		fatal(logger, "--hot-dir is required")
	}
	if *chunk == 0 {
		fatal(logger, "--chunk is required (the chunk ID whose ledgers are in the HotStore)")
	}
	chunkID := uint32(*chunk)

	nList, err := parseIntList(*nCSV)
	if err != nil {
		fatal(logger, "parse --n: %v", err)
	}
	workersList, err := parseIntList(*workersCSV)
	if err != nil {
		fatal(logger, "parse --workers: %v", err)
	}
	validateGridFlags(logger, nList, workersList)

	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)

	h, err := ledger.OpenHotStore(*hotDir, logger)
	if err != nil {
		fatal(logger, "OpenHotStore %s: %v", *hotDir, err)
	}
	defer h.Close()

	if _, err := h.GetLedgerRaw(first); err != nil {
		fatal(logger, "hot store missing seq %d (run hot-ledgers-ingest --chunk=%d first?): %v", first, chunkID, err)
	}
	if _, err := h.GetLedgerRaw(last); err != nil {
		fatal(logger, "hot store missing seq %d (partial seed?): %v", last, err)
	}

	csvF, csvPath, err := createCSV(*outDir, "hot-ledgers", gridCSVHeader)
	if err != nil {
		fatal(logger, "%v", err)
	}
	defer csvF.Close()

	logger.Infof("hot-ledgers dir=%s chunk=%d seqs=[%d,%d] n=%v workers=%v iters=%d warmup=%d",
		*hotDir, chunkID, first, last, nList, workersList, *iters, hotWarmupIters)
	printGridHeader()

	runBenchGrid(csvF, nList, workersList, func(n, w int) concurrentResult {
		return runHotConcurrent(h, first, last, w, *iters, *seed, n)
	})

	logger.Infof("wrote %s", csvPath)
}

// runHotConcurrent runs `workers` goroutines reading from the shared
// HotStore. Each iteration picks a random start seq in [first, last-n+1]
// and reads n consecutive ledgers (GetLedgerRaw for n=1, IterateLedgers
// for n>1, mirroring the old --tier=hot point-vs-range split). Each
// worker runs hotWarmupIters discarded iterations first to warm
// RocksDB's block cache. Returns concurrentResult — same shape as
// runColdConcurrent.
func runHotConcurrent(
	h *ledger.HotStore,
	first, last uint32,
	workers, itersPerWorker int,
	baseSeed int64,
	n int,
) concurrentResult {
	startSpan := last - first - uint32(n) + 2

	type workerResult struct {
		durs []time.Duration
		errs int
	}
	results := make([]workerResult, workers)

	readOnce := func(rng *rand.Rand) (time.Duration, error) {
		start := first + rng.Uint32N(startSpan)
		if n == 1 {
			t0 := time.Now()
			raw, err := h.GetLedgerRaw(start)
			d := time.Since(t0)
			if err != nil {
				return d, err
			}
			if len(raw) == 0 {
				return d, errors.New("empty payload")
			}
			return d, nil
		}
		end := start + uint32(n) - 1
		t0 := time.Now()
		seen := 0
		for entry, err := range h.IterateLedgers(start, end) {
			if err != nil {
				return time.Since(t0), err
			}
			if len(entry.Bytes) == 0 {
				return time.Since(t0), errors.New("empty payload")
			}
			seen++
		}
		d := time.Since(t0)
		if seen != n {
			return d, fmt.Errorf("got %d ledgers, expected %d", seen, n)
		}
		return d, nil
	}

	var wg sync.WaitGroup
	wg.Add(workers)
	tStart := time.Now()
	for wID := range workers {
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(
				uint64(baseSeed)+uint64(id),
				uint64(baseSeed*7919)+uint64(id),
			))
			for range hotWarmupIters {
				_, _ = readOnce(rng)
			}

			durs := make([]time.Duration, 0, itersPerWorker)
			var errs int
			for range itersPerWorker {
				d, err := readOnce(rng)
				if err != nil {
					errs++
					continue
				}
				durs = append(durs, d)
			}
			results[id] = workerResult{durs: durs, errs: errs}
		}(wID)
	}
	wg.Wait()
	wall := time.Since(tStart)

	all := make([]time.Duration, 0, workers*itersPerWorker)
	var totalErrs int
	for _, r := range results {
		all = append(all, r.durs...)
		totalErrs += r.errs
	}
	stats := computeStats(all)
	// Override computeStats's sum-of-durations ops/sec with wall-time
	// throughput: at workers>1 the durations overlap, so 1/mean_latency
	// would overstate throughput; wall-time is the rate a caller actually
	// observes. (Also folds in warmup overhead, which is honest.)
	stats.opsPerSec = float64(len(all)) / wall.Seconds()
	return concurrentResult{stats: stats, totalErrs: totalErrs, durs: all}
}
