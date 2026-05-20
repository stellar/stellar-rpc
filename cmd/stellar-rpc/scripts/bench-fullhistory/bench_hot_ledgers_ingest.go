package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// cmdHotLedgersIngest benches per-ledger ingestion into a fresh
// HotStore. All ledgers from the specified cold-store chunk are
// streamed in one at a time and timed individually.
//
// The timer covers `HotStore.AddLedgers(entry)` only — the cold-side
// iteration that supplies the bytes runs outside it. AddLedgers's
// single-entry path is Store.Put with WriteOptions.Sync=true, so each
// call WAL-fsyncs before returning (durability per ledger).
//
// Output: one CSV row per ledger (seq, latency_ns). Summary stats are
// printed to stdout. There is no grid here — this is a single-stream
// measurement, no --n / --workers / --iters.
func cmdHotLedgersIngest() {
	fs := flag.NewFlagSet("hot-ledgers-ingest", flag.ExitOnError)
	coldDir := fs.String("cold-dir", "", "source cold-store dir (required)")
	chunk := fs.Int64("chunk", -1, "source chunk; bench ingests all its ledgers (required)")
	hotDir := fs.String("hot-dir", "", "fresh HotStore destination dir (required; must be empty or absent)")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	if *coldDir == "" {
		fatal(logger, "--cold-dir is required")
	}
	if *chunk < 0 {
		fatal(logger, "--chunk is required")
	}
	if *chunk > int64(^uint32(0)) {
		fatal(logger, "--chunk=%d exceeds uint32", *chunk)
	}
	if *hotDir == "" {
		fatal(logger, "--hot-dir is required")
	}
	chunkID := uint32(*chunk)

	// Refuse to write into a non-empty dir — preserves the "fresh ingestion"
	// premise of the metric. Missing dir is fine; NewHotStore creates it.
	if entries, err := os.ReadDir(*hotDir); err == nil && len(entries) > 0 {
		fatal(logger, "--hot-dir=%s is not empty; pick a fresh path or remove its contents", *hotDir)
	}

	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)

	src := packPath(*coldDir, chunkID)
	if _, err := os.Stat(src); err != nil {
		fatal(logger, "cold pack missing: %s: %v", src, err)
	}

	cold, err := ledger.NewColdStoreReader(src)
	if err != nil {
		fatal(logger, "NewColdStoreReader %s: %v", src, err)
	}
	defer cold.Close()

	if err := os.MkdirAll(filepath.Dir(*hotDir), 0o755); err != nil {
		fatal(logger, "mkdir parent of %s: %v", *hotDir, err)
	}
	hot, err := ledger.NewHotStore(*hotDir, logger)
	if err != nil {
		fatal(logger, "NewHotStore %s: %v", *hotDir, err)
	}
	defer hot.Close()

	csvF, csvPath, err := createCSV(*outDir, "hot-ledgers-ingest", "seq,latency_ns")
	if err != nil {
		fatal(logger, "%v", err)
	}
	defer csvF.Close()

	logger.Infof("hot-ledgers-ingest cold=%s chunk=%d hot=%s seqs=[%d,%d]",
		*coldDir, chunkID, *hotDir, first, last)

	durs := make([]time.Duration, 0, last-first+1)
	for entry, iterErr := range cold.IterateLedgers(first, last) {
		if iterErr != nil {
			fatal(logger, "cold iterate at seq %d: %v", entry.Seq, iterErr)
		}
		e := ledger.Entry{Seq: entry.Seq, Bytes: entry.Bytes}

		t0 := time.Now()
		if err := hot.AddLedgers(e); err != nil {
			fatal(logger, "AddLedgers(seq=%d): %v", e.Seq, err)
		}
		d := time.Since(t0)

		durs = append(durs, d)
		fmt.Fprintf(csvF, "%d,%d\n", e.Seq, d.Nanoseconds())
	}

	if len(durs) == 0 {
		fatal(logger, "no ledgers ingested (source chunk empty?)")
	}

	stats := computeStats(durs)
	// Synchronous single-ledger writes don't overlap, so the
	// sum-of-durations ops/sec from computeStats matches wall-clock
	// throughput. Use it as-is.
	fmt.Println()
	fmt.Printf("hot-ledgers-ingest n=%-5d p50=%-9s p90=%-9s p99=%-9s max=%-9s ops/s=%.0f\n",
		stats.n,
		stats.p50.Round(time.Microsecond),
		stats.p90.Round(time.Microsecond),
		stats.p99.Round(time.Microsecond),
		stats.maxv.Round(time.Microsecond),
		stats.opsPerSec,
	)
	logger.Infof("wrote %s", csvPath)
}
