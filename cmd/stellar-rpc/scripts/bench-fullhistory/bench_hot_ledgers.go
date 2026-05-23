package main

import (
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// hotWarmupIters — fixed warmup count for RocksDB block-cache priming
// in the hot-ledgers sweep. Hardcoded rather than exposed as a flag
// because hot-tier warmup is rarely a knob the bench user wants to
// tune, and the previous default (100) is what generated the reference
// numbers in bench-out/summary.csv.
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
// have been populated by `hot-ingest --types=ledgers --chunk=N` (which
// uses the same chunk semantics).
func cmdHotLedgers() {
	fs := flag.NewFlagSet("hot-ledgers", flag.ExitOnError)
	hotDir := fs.String("hot-dir", "", "hot-store (RocksDB) path (required)")
	chunk := fs.Uint("chunk", 0, "chunk ID whose ledgers are in the store (required)")
	n := fs.Int("n", 20, "ledgers per read (production page size)")
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
	if *n < 1 {
		fatal(logger, "--n must be >= 1, got %d", *n)
	}
	if uint32(*n) > ledgersPerChunk {
		fatal(logger, "--n=%d exceeds single-chunk capacity %d", *n, ledgersPerChunk)
	}
	chunkID := uint32(*chunk)

	workersList, err := parseIntList(*workersCSV)
	if err != nil {
		fatal(logger, "parse --workers: %v", err)
	}
	validateWorkersList(logger, workersList)

	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)

	h, err := ledger.OpenHotStore(*hotDir, logger)
	if err != nil {
		fatal(logger, "OpenHotStore %s: %v", *hotDir, err)
	}
	defer h.Close()

	if _, err := h.GetLedgerRaw(first); err != nil {
		fatal(logger, "hot store missing seq %d (run hot-ingest --types=ledgers --chunk=%d first?): %v", first, chunkID, err)
	}
	if _, err := h.GetLedgerRaw(last); err != nil {
		fatal(logger, "hot store missing seq %d (partial seed?): %v", last, err)
	}

	csvF, csvPath, err := createCSV(*outDir, "hot-ledgers", sweepCSVHeader)
	if err != nil {
		fatal(logger, "%v", err)
	}
	defer csvF.Close()

	logger.Infof("hot-ledgers dir=%s chunk=%d seqs=[%d,%d] n=%d workers=%v iters=%d warmup=%d",
		*hotDir, chunkID, first, last, *n, workersList, *iters, hotWarmupIters)
	printSweepHeader()

	op := hotRangeOp(h, first, last, *n)

	results := make([]concurrentResult, 0, len(workersList))
	for _, w := range workersList {
		res := runConcurrentSweepWithWarmup(w, hotWarmupIters, *iters, *seed, op)
		printSweepRow(w, res, csvF)
		results = append(results, res)
	}
	reportSaturation(workersList, results)

	logger.Infof("wrote %s", csvPath)
}

// hotRangeOp returns a per-iter closure that reads n consecutive
// ledgers from the shared HotStore starting at a random seq in
// [first, last-n+1]. Uses GetLedgerRaw for n=1 and IterateLedgers for
// n>1.
func hotRangeOp(
	h *ledger.HotStore,
	first, last uint32,
	n int,
) iterOp {
	startSpan := last - first - uint32(n) + 2
	return func(rng *rand.Rand, _ bool) (time.Duration, error) {
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
}
