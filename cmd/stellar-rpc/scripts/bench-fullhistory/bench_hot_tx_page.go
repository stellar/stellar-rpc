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
	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// cmdHotTxPage benches "fetch a page of N transactions starting
// from a random in-chunk cursor" against the hot tier with hot-tier
// methodology: one shared HotStore handle for the run, RocksDB
// block-cache warmup before timed iters.
//
// CSV columns mirror cold-tx-page minus the open_ns column (the
// reader is shared, not per-iter).
func cmdHotTxPage() {
	fs := flag.NewFlagSet("hot-tx-page", flag.ExitOnError)
	hotDir := fs.String("hot-dir", "/mnt/nvme/disk2/ledgers/hot-5000", "hot ledger store dir")
	chunk := fs.Uint("chunk", 5000, "chunk to use")
	page := fs.Int("page-size", 20, "transactions per page")
	iters := fs.Int("iters", 200, "number of timed pages")
	warmup := fs.Int("warmup", hotWarmupSharedIters, "warm-up pages (RocksDB block-cache priming; not counted)")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	if *page < 1 {
		fatal(logger, "--page-size must be >= 1")
	}
	chunkID := uint32(*chunk)
	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)

	h, err := ledger.NewHotStore(*hotDir, logger)
	if err != nil {
		fatal(logger, "NewHotStore %s: %v", *hotDir, err)
	}
	defer h.Close()

	infos, totalTx := preflightTxCountsHot(logger, h, first, last)
	if totalTx < *page {
		fatal(logger, "hot store has only %d txs but page-size=%d", totalTx, *page)
	}
	logger.Infof("hot-tx-page chunk=%d page=%d iters=%d warmup=%d (preflight: %d ledgers, %d total tx, avg %.1f/ledger)",
		chunkID, *page, *iters, *warmup, len(infos), totalTx, float64(totalTx)/float64(len(infos)))

	rng := rand.New(rand.NewPCG(uint64(*seed), uint64(*seed*7919)))
	pick := makeCursorPicker(infos, *page, rng)

	for i := range *warmup {
		li, ti := pick()
		_, _, _, _, got, walkErr := walkPagePhased(h.GetLedgerRaw, infos, li, ti, *page)
		if walkErr != nil {
			fatal(logger, "warmup %d: %v", i, walkErr)
		}
		if got != *page {
			fatal(logger, "warmup %d short read: got %d, want %d", i, got, *page)
		}
	}

	csvPath := filepath.Join(*outDir, fmt.Sprintf("hot-tx-page-%d.csv", *page))
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal(logger, "mkdir %s: %v", *outDir, err)
	}
	csvF, err := os.Create(csvPath)
	if err != nil {
		fatal(logger, "create CSV %s: %v", csvPath, err)
	}
	defer csvF.Close()
	if _, err := fmt.Fprintln(csvF, "cursor_seq,cursor_tx,n_ledgers,fetch_ns,decode_ns,scan_ns,total_ns"); err != nil {
		fatal(logger, "write CSV header: %v", err)
	}

	totals := make([]time.Duration, 0, *iters)
	for i := range *iters {
		li, ti := pick()
		fetchNs, decodeNs, scanNs, nLedgers, got, walkErr := walkPagePhased(h.GetLedgerRaw, infos, li, ti, *page)
		if walkErr != nil {
			fatal(logger, "iter %d walk: %v", i, walkErr)
		}
		if got != *page {
			fatal(logger, "iter %d short read: got %d, want %d", i, got, *page)
		}

		totalNs := fetchNs + decodeNs + scanNs
		totals = append(totals, totalNs)
		if _, err := fmt.Fprintf(csvF, "%d,%d,%d,%d,%d,%d,%d\n",
			infos[li].seq, ti, nLedgers,
			fetchNs.Nanoseconds(), decodeNs.Nanoseconds(),
			scanNs.Nanoseconds(), totalNs.Nanoseconds()); err != nil {
			fatal(logger, "iter %d write CSV: %v", i, err)
		}
	}

	stats := computeStats(totals)
	fmt.Println(stats.line(fmt.Sprintf("hot-tx-page-%d", *page)))
	logger.Infof("wrote %s", csvPath)
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
