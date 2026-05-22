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

// cmdColdTxPage benches "fetch a page of N transactions starting
// from a random in-chunk cursor" against the cold tier with
// cold-cache methodology: per iter evict the packfile from page
// cache, open a fresh ColdReader, walk forward across ledgers
// until the page is filled, close.
//
// Per-iter CSV row:
//
//	cursor_seq    starting ledger seq for the page
//	cursor_tx     starting tx index within cursor_seq
//	n_ledgers     ledgers actually touched while filling the page
//	open_ns       OpenColdReader
//	fetch_ns      total GetLedgerRaw time across all touched ledgers
//	decode_ns     total UnmarshalBinary time
//	scan_ns       per-tx Hash + ResultPair touch
//	total_ns      sum of the above (close deferred)
//
// The chunk's per-ledger tx counts are computed once at startup with
// a temporary reader (amortized; not part of the timed loop) so the
// pick-a-cursor helper can guarantee at least N txs ahead.
func cmdColdTxPage() {
	fs := flag.NewFlagSet("cold-txpage", flag.ExitOnError)
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold", "cold-store root")
	chunk := fs.Uint("chunk", 5000, "chunk to use")
	page := fs.Int("page-size", 20, "transactions per page")
	iters := fs.Int("iters", 200, "number of timed pages")
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
	packFile := packPath(*coldDir, chunkID)

	infos, totalTx := preflightTxCounts(logger, packFile, first, last)
	if totalTx < *page {
		fatal(logger, "chunk has only %d txs but page-size=%d", totalTx, *page)
	}
	logger.Infof("cold-txpage chunk=%d page=%d iters=%d (preflight: %d ledgers, %d total tx, avg %.1f/ledger)",
		chunkID, *page, *iters, len(infos), totalTx, float64(totalTx)/float64(len(infos)))

	rng := rand.New(rand.NewPCG(uint64(*seed), uint64(*seed*7919)))
	pick := makeCursorPicker(infos, *page, rng)

	csvPath := filepath.Join(*outDir, fmt.Sprintf("cold-txpage-%d.csv", *page))
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal(logger, "mkdir %s: %v", *outDir, err)
	}
	csvF, err := os.Create(csvPath)
	if err != nil {
		fatal(logger, "create CSV %s: %v", csvPath, err)
	}
	defer csvF.Close()
	if _, err := fmt.Fprintln(csvF, "cursor_seq,cursor_tx,n_ledgers,open_ns,fetch_ns,decode_ns,scan_ns,total_ns"); err != nil {
		fatal(logger, "write CSV header: %v", err)
	}

	totals := make([]time.Duration, 0, *iters)
	for i := range *iters {
		li, ti := pick()

		if err := evictFile(packFile); err != nil {
			fatal(logger, "iter %d evict: %v", i, err)
		}

		t0 := time.Now()
		cr, err := ledger.OpenColdReader(packFile)
		openNs := time.Since(t0)
		if err != nil {
			fatal(logger, "iter %d OpenColdReader: %v", i, err)
		}

		fetchNs, decodeNs, scanNs, nLedgers, got, walkErr := walkPagePhased(cr.GetLedgerRaw, infos, li, ti, *page)
		cr.Close()
		if walkErr != nil {
			fatal(logger, "iter %d walk: %v", i, walkErr)
		}
		if got != *page {
			fatal(logger, "iter %d short read: got %d, want %d", i, got, *page)
		}

		totalNs := openNs + fetchNs + decodeNs + scanNs
		totals = append(totals, totalNs)
		if _, err := fmt.Fprintf(csvF, "%d,%d,%d,%d,%d,%d,%d,%d\n",
			infos[li].seq, ti, nLedgers,
			openNs.Nanoseconds(), fetchNs.Nanoseconds(),
			decodeNs.Nanoseconds(), scanNs.Nanoseconds(),
			totalNs.Nanoseconds()); err != nil {
			fatal(logger, "iter %d write CSV: %v", i, err)
		}
	}

	stats := computeStats(totals)
	fmt.Println(stats.line(fmt.Sprintf("cold-txpage-%d", *page)))
	logger.Infof("wrote %s", csvPath)
}

// ledgerTxCount captures a (seq, tx_count) pair for one ledger in the
// chunk, used by the cursor picker to guarantee at least N txs ahead.
type ledgerTxCount struct {
	seq     uint32
	txCount int
}

// preflightTxCounts opens a one-shot ColdReader, walks the
// chunk's [first, last] range, and returns (seq, txCount) per ledger
// plus the total tx count. Amortized at startup; not part of the
// per-iter timed loop.
func preflightTxCounts(logger *supportlog.Entry, packFile string, first, last uint32) ([]ledgerTxCount, int) {
	logger.Infof("preflight: scanning %s for tx counts...", packFile)
	r, err := ledger.OpenColdReader(packFile)
	if err != nil {
		fatal(logger, "preflight OpenColdReader: %v", err)
	}
	defer r.Close()

	infos := make([]ledgerTxCount, 0, ledgersPerChunk)
	totalTx := 0
	for entry, err := range r.IterateLedgers(first, last) {
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

// makeCursorPicker returns a closure that yields a random
// (ledgerIdx, txIdx) cursor with at least `page` txs ahead of it.
func makeCursorPicker(infos []ledgerTxCount, page int, rng *rand.Rand) func() (int, int) {
	return func() (int, int) {
		for {
			i := rng.IntN(len(infos))
			if infos[i].txCount == 0 {
				continue
			}
			j := rng.IntN(infos[i].txCount)
			ahead := infos[i].txCount - j
			for k := i + 1; k < len(infos) && ahead < page; k++ {
				ahead += infos[k].txCount
			}
			if ahead >= page {
				return i, j
			}
		}
	}
}

// walkPagePhased reads ledgers starting from (ledgerIdx, txIdx) and
// emits one tx at a time, tracking per-phase totals across the walk:
//
//	fetch  = sum of getLedger() call times
//	decode = sum of UnmarshalBinary times
//	scan   = sum of per-tx Hash + ResultPair touches
//	nLed   = number of distinct ledgers touched
//	got    = txs emitted
//
// Stops when `wanted` txs have been emitted or the chunk is exhausted.
func walkPagePhased(
	getLedger func(uint32) ([]byte, error),
	infos []ledgerTxCount,
	ledgerIdx, txIdx, wanted int,
) (fetch, decode, scan time.Duration, nLed, got int, err error) {
	remaining := wanted
	for i := ledgerIdx; i < len(infos) && remaining > 0; i++ {
		t0 := time.Now()
		raw, gerr := getLedger(infos[i].seq)
		fetch += time.Since(t0)
		if gerr != nil {
			err = gerr
			return
		}
		nLed++

		t1 := time.Now()
		var lcm goxdr.LedgerCloseMeta
		if uerr := lcm.UnmarshalBinary(raw); uerr != nil {
			decode += time.Since(t1)
			err = uerr
			return
		}
		decode += time.Since(t1)

		t2 := time.Now()
		nTx := lcm.CountTransactions()
		startIdx := 0
		if i == ledgerIdx {
			startIdx = txIdx
		}
		for j := startIdx; j < nTx && remaining > 0; j++ {
			_ = lcm.TransactionHash(j)
			_ = lcm.TransactionResultPair(j)
			got++
			remaining--
		}
		scan += time.Since(t2)
	}
	return
}
