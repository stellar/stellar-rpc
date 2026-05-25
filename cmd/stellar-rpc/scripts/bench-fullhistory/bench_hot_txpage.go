package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// cmdHotTxPage benches "fetch a page of N transactions starting
// from a random in-chunk cursor" against the hot tier with hot-tier
// methodology: one shared HotStore handle for the run, RocksDB
// block-cache warmup before timed iters.
//
// Per-iter CSV columns mirror cold-txpage minus open_ns (the reader
// is shared, not per-iter). A workers column is added so concurrent-
// sweep rows from different worker counts can be filtered after the
// fact.
func cmdHotTxPage() {
	fs := flag.NewFlagSet("hot-txpage", flag.ExitOnError)
	hotDir := fs.String("hot-dir", "/mnt/nvme/disk2/ledgers/hot-5000", "hot ledger store dir")
	chunk := fs.Uint("chunk", 5000, "chunk to use")
	page := fs.Int("page-size", 20, "transactions per page")
	iters := fs.Int("iters", 200, "number of timed pages per worker")
	workersCSV := fs.String("query-concurrency", "1", "concurrent in-flight queries; comma-list sweep (e.g. 1,4,16)")
	warmup := fs.Int("warmup", hotWarmupSharedIters, "warm-up pages per worker (RocksDB block-cache priming; not counted)")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	if *page < 1 {
		fatal(logger, "--page-size must be >= 1")
	}
	workersList, err := parseIntList(*workersCSV)
	if err != nil {
		fatal(logger, "parse --query-concurrency: %v", err)
	}
	validateWorkersList(logger, workersList)

	chunkID := uint32(*chunk)
	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)

	h, err := ledger.OpenHotStore(*hotDir, logger)
	if err != nil {
		fatal(logger, "OpenHotStore %s: %v", *hotDir, err)
	}
	defer h.Close()

	infos, totalTx := preflightTxCountsHot(logger, h, first, last)
	if totalTx < *page {
		fatal(logger, "hot store has only %d txs but page-size=%d", totalTx, *page)
	}
	logger.Infof("hot-txpage chunk=%d page=%d iters=%d workers=%v warmup=%d (preflight: %d ledgers, %d total tx, avg %.1f/ledger)",
		chunkID, *page, *iters, workersList, *warmup, len(infos), totalTx, float64(totalTx)/float64(len(infos)))

	// Per-iter detail CSV. Workers column lets a post-processor split
	// by worker count.
	detailPath := filepath.Join(*outDir, fmt.Sprintf("hot-txpage-%d.csv", *page))
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal(logger, "mkdir %s: %v", *outDir, err)
	}
	detailF, err := os.Create(detailPath)
	if err != nil {
		fatal(logger, "create CSV %s: %v", detailPath, err)
	}
	defer detailF.Close()
	if _, err := fmt.Fprintln(detailF, "query_concurrency,cursor_seq,cursor_tx,n_ledgers,fetch_ns,decode_ns,scan_ns,total_ns"); err != nil {
		fatal(logger, "write CSV header: %v", err)
	}

	// Summary CSV (one row per worker count).
	summaryF, summaryPath, err := createCSV(*outDir, fmt.Sprintf("hot-txpage-%d-sweep", *page), sweepCSVHeader)
	if err != nil {
		fatal(logger, "%v", err)
	}
	defer summaryF.Close()

	printSweepHeader()

	var csvMu sync.Mutex
	results := make([]concurrentResult, 0, len(workersList))
	for _, w := range workersList {
		op := hotTxPageOp(h, infos, *page, w, detailF, &csvMu)
		res := runConcurrentSweepWithWarmup(w, *warmup, *iters, *seed, op)
		printSweepRow(w, res, summaryF)
		results = append(results, res)
	}
	reportSaturation(workersList, results)

	logger.Infof("wrote %s and %s", detailPath, summaryPath)
}

// hotTxPageOp returns a per-iter closure that picks a random cursor,
// walks a page of `page` transactions across the shared HotStore,
// and writes one row to detailF (under csvMu). Returns the total
// walk duration as the headline measurement.
func hotTxPageOp(
	h *ledger.HotStore,
	infos []ledgerTxCount,
	page, workers int,
	detailF *os.File,
	csvMu *sync.Mutex,
) iterOp {
	return func(rng *rand.Rand, measured bool) (time.Duration, error) {
		li, ti := pickCursor(infos, page, rng)
		fetchNs, decodeNs, scanNs, nLedgers, got, walkErr := walkPagePhased(h.GetLedgerRaw, infos, li, ti, page)
		if walkErr != nil {
			return 0, walkErr
		}
		if got != page {
			return 0, fmt.Errorf("short read: got %d, want %d", got, page)
		}
		totalNs := fetchNs + decodeNs + scanNs

		if measured {
			csvMu.Lock()
			_, err := fmt.Fprintf(detailF, "%d,%d,%d,%d,%d,%d,%d,%d\n",
				workers,
				infos[li].seq, ti, nLedgers,
				fetchNs.Nanoseconds(), decodeNs.Nanoseconds(),
				scanNs.Nanoseconds(), totalNs.Nanoseconds())
			csvMu.Unlock()
			if err != nil {
				return totalNs, err
			}
		}
		return totalNs, nil
	}
}

// preflightTxCountsHot walks the hot store's [first, last] range
// once at startup to populate (seq, txCount) per ledger.
func preflightTxCountsHot(logger *supportlog.Entry, h *ledger.HotStore, first, last uint32) ([]ledgerTxCount, int) {
	logger.Infof("preflight: scanning hot store [%d,%d] for tx counts...", first, last)
	infos := make([]ledgerTxCount, 0, ledgersPerChunk)
	totalTx := 0
	for entry, err := range h.IterateLedgers(first, last) {
		if err != nil {
			fatal(logger, "preflight IterateLedgers: %v", err)
		}
		var lcm goxdr.LedgerCloseMeta
		if err := lcm.UnmarshalBinary(entry.Bytes); err != nil {
			fatal(logger, "preflight UnmarshalBinary seq=%d: %v", entry.Seq, err)
		}
		ct := lcm.CountTransactions()
		infos = append(infos, ledgerTxCount{seq: entry.Seq, txCount: ct})
		totalTx += ct
	}
	return infos, totalTx
}
