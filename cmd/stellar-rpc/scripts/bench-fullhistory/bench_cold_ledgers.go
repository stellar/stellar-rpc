package main

import (
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// cmdColdLedgers benches cold-storage ledger reads. Each iteration
// picks a random chunk, evicts its packfile from the OS page cache,
// opens a fresh ColdReader, reads n consecutive ledgers from a
// random in-chunk position, and closes. --workers is the concurrency
// sweep axis; --n (ledgers per read) is a single value chosen to
// represent the production page size.
func cmdColdLedgers() {
	fs := flag.NewFlagSet("cold-ledgers", flag.ExitOnError)
	coldDir := fs.String("cold-dir", "", "cold-store root (required); expects bucketed layout <cold-dir>/<5-digit-bucket>/<8-digit-chunk>.pack")
	flagLo := fs.Uint("chunk-lo", 0, "inclusive lower chunk ID (0 = auto-discover from --cold-dir; set with --chunk-hi to constrain)")
	flagHi := fs.Uint("chunk-hi", 0, "inclusive upper chunk ID (0 = auto-discover; set with --chunk-lo to constrain)")
	n := fs.Int("n", 20, "ledgers per read (production page size)")
	workersCSV := fs.String("workers", "1", "parallel workers; comma-list (e.g. 1,4,16)")
	iters := fs.Int("iters", 60, "iterations per worker per cell")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	if *coldDir == "" {
		fatal(logger, "--cold-dir is required")
	}
	if *n < 1 {
		fatal(logger, "--n must be >= 1, got %d", *n)
	}
	if uint32(*n) > ledgersPerChunk {
		fatal(logger, "--n=%d exceeds single-chunk capacity %d", *n, ledgersPerChunk)
	}

	workersList, err := parseIntList(*workersCSV)
	if err != nil {
		fatal(logger, "parse --workers: %v", err)
	}
	validateWorkersList(logger, workersList)

	chunkLo, chunkHi, err := resolveLedgerChunkRange(*coldDir, uint32(*flagLo), uint32(*flagHi))
	if err != nil {
		fatal(logger, "resolve chunk range in %s: %v", *coldDir, err)
	}
	chunkSpan := chunkHi - chunkLo + 1

	csvF, csvPath, err := createCSV(*outDir, "cold-ledgers", sweepCSVHeader)
	if err != nil {
		fatal(logger, "%v", err)
	}
	defer csvF.Close()

	logger.Infof("cold-ledgers chunks=[%d,%d] (%d files) n=%d workers=%v iters=%d",
		chunkLo, chunkHi, chunkSpan, *n, workersList, *iters)
	printSweepHeader()

	op := coldRangeOp(logger, *coldDir, chunkLo, chunkSpan, *n)

	results := make([]concurrentResult, 0, len(workersList))
	for _, w := range workersList {
		res := runConcurrentSweep(w, *iters, *seed, op)
		printSweepRow(w, res, csvF)
		results = append(results, res)
	}
	reportSaturation(workersList, results)

	logger.Infof("wrote %s", csvPath)
}

// coldRangeOp returns a per-iter closure that picks a random chunk in
// [chunkLo, chunkLo+chunkSpan), evicts its packfile, opens a fresh
// ColdReader, reads n consecutive ledgers from a random in-chunk
// start, and closes. The timer covers OpenColdReader through the
// last successful entry; close is deferred and not timed.
func coldRangeOp(
	logger *supportlog.Entry,
	coldDir string,
	chunkLo, chunkSpan uint32,
	n int,
) iterOp {
	startSpan := ledgersPerChunk - uint32(n) + 1
	return func(rng *rand.Rand, _ bool) (time.Duration, error) {
		c := chunkLo + rng.Uint32N(chunkSpan)
		path := packPath(coldDir, c)

		if err := evictFile(path); err != nil {
			logger.WithError(err).Warnf("evict %s", path)
			return 0, err
		}

		t0 := time.Now()
		r, err := ledger.OpenColdReader(path)
		if err != nil {
			return 0, err
		}
		defer r.Close()

		start := chunkFirstLedger(c) + rng.Uint32N(startSpan)
		end := start + uint32(n) - 1
		seen := 0
		for entry, ierr := range r.IterateLedgers(start, end) {
			if ierr != nil {
				return 0, ierr
			}
			if len(entry.Bytes) == 0 {
				return 0, errors.New("empty payload")
			}
			seen++
		}
		if seen != n {
			return 0, fmt.Errorf("got %d ledgers, expected %d", seen, n)
		}
		return time.Since(t0), nil
	}
}

// resolveLedgerChunkRange returns the [lo, hi] chunk-id range a cold
// bench should use. If both flagLo and flagHi are zero, falls back to
// discoverChunks(coldDir) — the "use everything on disk" default. If
// either flag is set, both must be set and form a valid range, and
// every chunk in the range must have a .pack on disk.
//
// Operators bench-tuning specific chunk subsets, comparing chunk
// ranges, or sharing a disk with concurrent backfill producers
// should use the explicit flags so runs are reproducible.
func resolveLedgerChunkRange(coldDir string, flagLo, flagHi uint32) (uint32, uint32, error) {
	if flagLo == 0 && flagHi == 0 {
		return discoverChunks(coldDir)
	}
	if flagLo == 0 || flagHi == 0 {
		return 0, 0, errors.New("--chunk-lo and --chunk-hi must both be set, or neither")
	}
	if flagHi < flagLo {
		return 0, 0, fmt.Errorf("--chunk-hi=%d < --chunk-lo=%d", flagHi, flagLo)
	}
	for c := flagLo; c <= flagHi; c++ {
		if _, err := os.Stat(packPath(coldDir, c)); err != nil {
			return 0, 0, fmt.Errorf("packfile missing for chunk %d: %w", c, err)
		}
	}
	return flagLo, flagHi, nil
}

// discoverChunks scans <dir>/<bucket>/*.pack and returns the inclusive
// chunk-id range. Errors if no packfiles are found or chunk IDs are
// non-contiguous (gaps would break random chunk picking, which uses
// chunkLo + rand_in_span).
func discoverChunks(dir string) (lo, hi uint32, err error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*", "*.pack"))
	if err != nil {
		return 0, 0, err
	}
	if len(matches) == 0 {
		return 0, 0, fmt.Errorf("no .pack files under %s (expected bucketed layout: <dir>/<5-digit-bucket>/<8-digit-chunk>.pack)", dir)
	}
	chunks := make([]uint32, 0, len(matches))
	for _, m := range matches {
		base := strings.TrimSuffix(filepath.Base(m), ".pack")
		id, perr := strconv.ParseUint(base, 10, 32)
		if perr != nil {
			continue
		}
		chunks = append(chunks, uint32(id))
	}
	if len(chunks) == 0 {
		return 0, 0, fmt.Errorf("no .pack files with parseable chunk-id names under %s", dir)
	}
	slices.Sort(chunks)
	for i := 1; i < len(chunks); i++ {
		if chunks[i] != chunks[i-1]+1 {
			return 0, 0, fmt.Errorf("non-contiguous chunks: gap between %d and %d (random chunk picking needs a contiguous range)", chunks[i-1], chunks[i])
		}
	}
	return chunks[0], chunks[len(chunks)-1], nil
}
