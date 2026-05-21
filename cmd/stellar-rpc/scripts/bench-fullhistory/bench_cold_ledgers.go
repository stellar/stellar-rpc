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

// cmdColdLedgers benches cold-storage ledger reads. Each iteration
// picks a random chunk, evicts its packfile from the OS page cache,
// opens a fresh ColdStoreReader, reads n consecutive ledgers from a
// random in-chunk position, and closes. --n and --workers are
// comma-lists; the run produces a grid over (n, workers).
func cmdColdLedgers() {
	fs := flag.NewFlagSet("cold-ledgers", flag.ExitOnError)
	coldDir := fs.String("cold-dir", "", "cold-store root (required); expects bucketed layout <cold-dir>/<5-digit-bucket>/<8-digit-chunk>.pack")
	nCSV := fs.String("n", "1", "ledgers per read; comma-list (e.g. 1,10,20)")
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

	nList, err := parseIntList(*nCSV)
	if err != nil {
		fatal(logger, "parse --n: %v", err)
	}
	workersList, err := parseIntList(*workersCSV)
	if err != nil {
		fatal(logger, "parse --workers: %v", err)
	}
	validateGridFlags(logger, nList, workersList)

	chunkLo, chunkHi, err := discoverChunks(*coldDir)
	if err != nil {
		fatal(logger, "discover chunks in %s: %v", *coldDir, err)
	}
	chunkSpan := chunkHi - chunkLo + 1

	csvF, csvPath, err := createCSV(*outDir, "cold-ledgers", gridCSVHeader)
	if err != nil {
		fatal(logger, "%v", err)
	}
	defer csvF.Close()

	logger.Infof("cold-ledgers chunks=[%d,%d] (%d files) n=%v workers=%v iters=%d",
		chunkLo, chunkHi, chunkSpan, nList, workersList, *iters)
	printGridHeader()

	runBenchGrid(csvF, nList, workersList, func(n, w int) concurrentResult {
		return runColdConcurrent(logger, *coldDir, chunkLo, chunkSpan,
			w, *iters, *seed, rangeWorkload(n))
	})

	logger.Infof("wrote %s", csvPath)
}

// rangeWorkload returns a coldWorkload that reads n consecutive
// ledgers from a random in-chunk start.
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

// discoverChunks scans <dir>/<bucket>/*.pack and returns the inclusive
// chunk-id range. Errors if no packfiles are found or chunk IDs are
// non-contiguous (gaps would break random chunk picking in the runner,
// which uses chunkLo + rand_in_span).
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
	sort.Slice(chunks, func(i, j int) bool { return chunks[i] < chunks[j] })
	for i := 1; i < len(chunks); i++ {
		if chunks[i] != chunks[i-1]+1 {
			return 0, 0, fmt.Errorf("non-contiguous chunks: gap between %d and %d (random chunk picking needs a contiguous range)", chunks[i-1], chunks[i])
		}
	}
	return chunks[0], chunks[len(chunks)-1], nil
}
