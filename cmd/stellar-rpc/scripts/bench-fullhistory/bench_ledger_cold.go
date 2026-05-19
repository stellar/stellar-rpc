package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

// cmdLedgerPointColdOpen measures full cold-open + cold-read latency.
// Each iteration:
//  1. picks a random chunk in [chunk-lo, chunk-hi] and a random seq inside it,
//  2. drops that packfile's pages from the page cache (FADV_DONTNEED),
//  3. opens a fresh ColdStoreReader, GetLedgerRaw, Close — all timed.
//
// Spread across chunks prevents accidental warm reuse. Chunk span
// should comfortably exceed iters * any reasonable repeat factor;
// repeats are not de-duped (the RNG can pick the same chunk twice).
func cmdLedgerPointColdOpen() {
	fs := flag.NewFlagSet("ledger-point-cold-open", flag.ExitOnError)
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold", "cold-store root")
	chunkLo := fs.Uint("chunk-lo", 1000, "low chunk id (inclusive)")
	chunkHi := fs.Uint("chunk-hi", 5000, "high chunk id (inclusive)")
	iters := fs.Int("iters", 200, "number of iterations")
	seed := fs.Int64("seed", 1, "RNG seed")
	checkResidency := fs.Bool("check-residency", false, "after each eviction, mincore-verify resident pages == 0")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)
	dec := zstd.NewDecompressor()

	if *chunkHi < *chunkLo {
		fatal(logger, "chunk-hi (%d) < chunk-lo (%d)", *chunkHi, *chunkLo)
	}
	chunkSpan := uint32(*chunkHi - *chunkLo + 1)

	rng := rand.New(rand.NewPCG(uint64(*seed), uint64(*seed*7919)))

	logger.Infof("ledger-point-cold-open chunks=[%d,%d] iters=%d", *chunkLo, *chunkHi, *iters)

	durs := make([]time.Duration, 0, *iters)
	var residencyWarns int
	for i := 0; i < *iters; i++ {
		c := uint32(*chunkLo) + rng.Uint32N(chunkSpan)
		first := chunkFirstLedger(c)
		seq := first + rng.Uint32N(ledgersPerChunk)
		path := packPath(*coldDir, c)

		if err := evictFile(path); err != nil {
			fatal(logger, "evict %s: %v", path, err)
		}

		if *checkResidency {
			res, tot, rerr := residency(path)
			if rerr != nil {
				logger.WithError(rerr).Warnf("residency check failed for %s", path)
			} else if res > 0 {
				residencyWarns++
				if residencyWarns <= 5 {
					logger.Warnf("eviction left %d/%d pages resident in %s", res, tot, path)
				}
			}
		}

		t0 := time.Now()
		r, err := ledger.NewColdStoreReader(path, dec)
		if err != nil {
			fatal(logger, "open %s: %v", path, err)
		}
		raw, err := r.GetLedgerRaw(seq)
		d := time.Since(t0)
		_ = r.Close()

		if err != nil {
			fatal(logger, "GetLedgerRaw(%d): %v", seq, err)
		}
		if len(raw) == 0 {
			fatal(logger, "empty payload seq=%d", seq)
		}
		durs = append(durs, d)
	}

	if residencyWarns > 0 {
		logger.Warnf("residency check: %d/%d iters had non-zero resident pages after eviction",
			residencyWarns, *iters)
	}

	stats := computeStats(durs)
	fmt.Println(stats.line("ledger-point-cold-open"))

	csv := filepath.Join(*outDir, "ledger-point-cold-open.csv")
	if err := writeCSV(csv, durs); err != nil {
		logger.WithError(err).Warnf("could not write CSV %s", csv)
	} else {
		logger.Infof("wrote %s", csv)
	}
}

// cmdLedgerPointConcurrentCold runs N workers concurrently. Each worker
// loops: pick a random chunk in [chunk-lo, chunk-hi], evict it from
// pagecache, open a fresh ColdStoreReader, read one random seq, close.
//
// Workers use independent RNG streams so they don't synchronize. Two
// workers picking the same chunk in the same epoch can race — one may
// see warm cache populated by the other. With chunkSpan >> workers
// this is rare.
func cmdLedgerPointConcurrentCold() {
	fs := flag.NewFlagSet("ledger-point-concurrent-cold", flag.ExitOnError)
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold", "cold-store root")
	chunkLo := fs.Uint("chunk-lo", 1000, "low chunk id (inclusive)")
	chunkHi := fs.Uint("chunk-hi", 5000, "high chunk id (inclusive)")
	workers := fs.Int("workers", 8, "number of concurrent workers")
	itersPerWorker := fs.Int("iters", 100, "iterations per worker")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	if *chunkHi < *chunkLo {
		fatal(logger, "chunk-hi (%d) < chunk-lo (%d)", *chunkHi, *chunkLo)
	}
	if *workers < 1 {
		fatal(logger, "--workers must be >= 1")
	}
	chunkSpan := uint32(*chunkHi - *chunkLo + 1)

	op := func(r *ledger.ColdStoreReader, rng *rand.Rand, c uint32) error {
		seq := chunkFirstLedger(c) + rng.Uint32N(ledgersPerChunk)
		raw, err := r.GetLedgerRaw(seq)
		if err != nil {
			return err
		}
		if len(raw) == 0 {
			return fmt.Errorf("empty payload seq=%d", seq)
		}
		return nil
	}

	res := runColdConcurrent(logger, *coldDir, uint32(*chunkLo), chunkSpan,
		*workers, *itersPerWorker, *seed, op)

	fmt.Println(res.stats.line(fmt.Sprintf("ledger-point-concurrent-cold w=%d", *workers)))
	logger.Infof("ops=%d errs=%d agg=%.0f ops/s",
		res.stats.n, res.totalErrs, res.stats.opsPerSec)

	csv := filepath.Join(*outDir, fmt.Sprintf("ledger-point-concurrent-cold-w%d.csv", *workers))
	if err := writeCSV(csv, res.durs); err != nil {
		logger.WithError(err).Warnf("could not write CSV %s", csv)
	} else {
		logger.Infof("wrote %s", csv)
	}
}
