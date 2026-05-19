package main

import (
	"errors"
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

// reader is the common surface both HotStore and ColdStoreReader expose
// for the ledger benchmarks.
type ledgerReader interface {
	GetLedgerRaw(seq uint32) ([]byte, error)
	Close() error
}

// rangeReader extends ledgerReader with the iterator used by ledger-range.
type rangeReader interface {
	ledgerReader
	// iterate calls fn for each ledger in [start, end] inclusive.
	// fn returns false to stop early.
	iterateRange(start, end uint32, fn func(seq uint32, bytes []byte) bool) error
}

// hotAdapter wraps *ledger.HotStore as a rangeReader.
type hotAdapter struct{ *ledger.HotStore }

func (h hotAdapter) iterateRange(start, end uint32, fn func(uint32, []byte) bool) error {
	for entry, err := range h.IterateLedgers(start, end) {
		if err != nil {
			return err
		}
		if !fn(entry.Seq, entry.Bytes) {
			return nil
		}
	}
	return nil
}

// coldAdapter wraps *ledger.ColdStoreReader as a rangeReader.
type coldAdapter struct{ *ledger.ColdStoreReader }

func (c coldAdapter) iterateRange(start, end uint32, fn func(uint32, []byte) bool) error {
	for entry, err := range c.IterateLedgers(start, end) {
		if err != nil {
			return err
		}
		if !fn(entry.Seq, entry.Bytes) {
			return nil
		}
	}
	return nil
}

// openReader returns a tier-appropriate reader plus the [first, last]
// inclusive seq range it covers. For cold, this is the chunk's full
// range. For hot, we assume the store was seeded for the same chunk
// (the harness verifies by probing first and last).
func openReader(
	logger *supportlog.Entry,
	tier, coldDir, hotDir string,
	chunkID uint32,
	dec *zstd.Decompressor,
) (rangeReader, uint32, uint32, error) {
	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)
	switch tier {
	case "hot":
		h, err := ledger.NewHotStore(hotDir, logger)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("NewHotStore: %w", err)
		}
		// Probe to confirm the chunk is present.
		if _, err := h.GetLedgerRaw(first); err != nil {
			h.Close()
			return nil, 0, 0, fmt.Errorf("hot store missing seq %d (run seed-hot first?): %w", first, err)
		}
		if _, err := h.GetLedgerRaw(last); err != nil {
			h.Close()
			return nil, 0, 0, fmt.Errorf("hot store missing seq %d (partial seed?): %w", last, err)
		}
		return hotAdapter{h}, first, last, nil

	case "cold":
		path := packPath(coldDir, chunkID)
		c, err := ledger.NewColdStoreReader(path, dec)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("NewColdStoreReader %s: %w", path, err)
		}
		f, ferr := c.FirstSeq()
		l, lerr := c.LastSeq()
		if ferr != nil || lerr != nil {
			c.Close()
			return nil, 0, 0, fmt.Errorf("cold seq probe: %w %w", ferr, lerr)
		}
		if f != first || l != last {
			c.Close()
			return nil, 0, 0, fmt.Errorf("cold range mismatch: got [%d,%d], expected [%d,%d]", f, l, first, last)
		}
		return coldAdapter{c}, first, last, nil

	default:
		return nil, 0, 0, fmt.Errorf("unknown --tier=%q (want hot|cold)", tier)
	}
}

// cmdLedgerPoint benches random GetLedgerRaw(seq) lookups.
func cmdLedgerPoint() {
	fs := flag.NewFlagSet("ledger-point", flag.ExitOnError)
	tier := fs.String("tier", "cold", "storage tier: hot|cold")
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold", "cold-store root")
	hotDir := fs.String("hot-dir", "/mnt/nvme/disk2/ledgers/hot", "hot-store dir")
	chunk := fs.Uint("chunk", 5000, "chunk to use as source of seqs")
	iters := fs.Int("iters", 1000, "number of point lookups")
	warmup := fs.Int("warmup", 100, "warm-up lookups (not counted)")
	seed := fs.Int64("seed", 1, "RNG seed for seq selection")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)
	dec := zstd.NewDecompressor()

	r, first, last, err := openReader(logger, *tier, *coldDir, *hotDir, uint32(*chunk), dec)
	if err != nil {
		fatal(logger, "open reader: %v", err)
	}
	defer r.Close()

	rng := rand.New(rand.NewPCG(uint64(*seed), uint64(*seed*7919)))
	span := last - first + 1

	// Warm-up — touches a different seq distribution to fill any caches.
	for i := 0; i < *warmup; i++ {
		seq := first + rng.Uint32N(span)
		_, err := r.GetLedgerRaw(seq)
		if err != nil {
			fatal(logger, "warmup GetLedgerRaw(%d): %v", seq, err)
		}
	}

	logger.Infof("ledger-point tier=%s chunk=%d iters=%d span=[%d,%d]",
		*tier, *chunk, *iters, first, last)

	durs := make([]time.Duration, 0, *iters)
	for i := 0; i < *iters; i++ {
		seq := first + rng.Uint32N(span)
		t0 := time.Now()
		raw, err := r.GetLedgerRaw(seq)
		d := time.Since(t0)
		if err != nil {
			fatal(logger, "GetLedgerRaw(%d): %v", seq, err)
		}
		if len(raw) == 0 {
			fatal(logger, "empty payload for seq=%d", seq)
		}
		durs = append(durs, d)
	}

	stats := computeStats(durs)
	fmt.Println(stats.line(fmt.Sprintf("ledger-point %s", *tier)))

	csv := filepath.Join(*outDir, fmt.Sprintf("ledger-point-%s.csv", *tier))
	if err := writeCSV(csv, durs); err != nil {
		logger.WithError(err).Warnf("could not write CSV %s", csv)
	} else {
		logger.Infof("wrote %s", csv)
	}
}

// cmdLedgerRange benches IterateLedgers across N consecutive ledgers.
// Each iteration picks a fresh random window of width N inside the chunk.
func cmdLedgerRange() {
	fs := flag.NewFlagSet("ledger-range", flag.ExitOnError)
	tier := fs.String("tier", "cold", "storage tier: hot|cold")
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold", "cold-store root")
	hotDir := fs.String("hot-dir", "/mnt/nvme/disk2/ledgers/hot", "hot-store dir")
	chunk := fs.Uint("chunk", 5000, "chunk to use as source")
	n := fs.Int("n", 100, "ledgers per range")
	iters := fs.Int("iters", 100, "number of ranges")
	warmup := fs.Int("warmup", 5, "warm-up ranges (not counted)")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)
	dec := zstd.NewDecompressor()

	if *n < 1 {
		fatal(logger, "--n must be >= 1")
	}
	if uint32(*n) > ledgersPerChunk {
		fatal(logger, "--n=%d exceeds single-chunk capacity %d", *n, ledgersPerChunk)
	}

	r, first, last, err := openReader(logger, *tier, *coldDir, *hotDir, uint32(*chunk), dec)
	if err != nil {
		fatal(logger, "open reader: %v", err)
	}
	defer r.Close()

	rng := rand.New(rand.NewPCG(uint64(*seed), uint64(*seed*7919)))
	// Valid start range: first .. last-(n-1).
	startSpan := last - first - uint32(*n) + 2
	if startSpan == 0 {
		fatal(logger, "no valid start position (n too large?)")
	}

	pick := func() (uint32, uint32) {
		start := first + rng.Uint32N(startSpan)
		end := start + uint32(*n) - 1
		return start, end
	}

	for i := 0; i < *warmup; i++ {
		start, end := pick()
		seen := 0
		if err := r.iterateRange(start, end, func(seq uint32, b []byte) bool {
			if len(b) == 0 {
				fatal(logger, "warmup empty payload at %d", seq)
			}
			seen++
			return true
		}); err != nil {
			fatal(logger, "warmup iterate: %v", err)
		}
		if seen != *n {
			fatal(logger, "warmup seen=%d, expected %d", seen, *n)
		}
	}

	logger.Infof("ledger-range tier=%s chunk=%d n=%d iters=%d", *tier, *chunk, *n, *iters)

	durs := make([]time.Duration, 0, *iters)
	for i := 0; i < *iters; i++ {
		start, end := pick()
		t0 := time.Now()
		seen := 0
		var iterErr error
		err := r.iterateRange(start, end, func(seq uint32, b []byte) bool {
			if len(b) == 0 {
				iterErr = errors.New("empty payload")
				return false
			}
			seen++
			return true
		})
		d := time.Since(t0)
		if err != nil {
			fatal(logger, "iterate range [%d,%d]: %v", start, end, err)
		}
		if iterErr != nil {
			fatal(logger, "iterate range [%d,%d]: %v", start, end, iterErr)
		}
		if seen != *n {
			fatal(logger, "got %d ledgers, expected %d", seen, *n)
		}
		durs = append(durs, d)
	}

	stats := computeStats(durs)
	fmt.Println(stats.line(fmt.Sprintf("ledger-range%s-n%d", "/"+*tier, *n)))

	csv := filepath.Join(*outDir, fmt.Sprintf("ledger-range-%s-n%d.csv", *tier, *n))
	if err := writeCSV(csv, durs); err != nil {
		logger.WithError(err).Warnf("could not write CSV %s", csv)
	} else {
		logger.Infof("wrote %s", csv)
	}
}
