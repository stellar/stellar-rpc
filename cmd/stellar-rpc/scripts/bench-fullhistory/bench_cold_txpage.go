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

// cmdColdTxPage benches "fetch a page of N transactions starting
// from a random in-chunk cursor" against the cold tier with cold-
// cache methodology. Multi-chunk: per iter pick a random chunk, evict
// that chunk's packfile, open a fresh ColdReader, walk forward
// within the chunk until the page is filled, close. Pages are
// constrained to fit within a single chunk (the cursor picker only
// returns starts with `page` txs ahead within that chunk).
//
// Per-iter CSV row:
//
//	workers       worker-count cell this iter belongs to
//	chunk         ID randomly picked for this iter
//	cursor_seq    starting ledger seq for the page
//	cursor_tx     starting tx index within cursor_seq
//	n_ledgers     ledgers actually touched while filling the page
//	open_ns       OpenColdReader
//	fetch_ns      total GetLedgerRaw time across all touched ledgers
//	decode_ns     total UnmarshalBinary time
//	scan_ns       per-tx Hash + ResultPair touch
//	total_ns      sum (close deferred, not timed)
func cmdColdTxPage() {
	fs := flag.NewFlagSet("cold-txpage", flag.ExitOnError)
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold", "cold-store root")
	flagLo := fs.Uint("chunk-lo", 0, "inclusive lower chunk ID (0 = auto-discover from --cold-dir; set with --chunk-hi to constrain)")
	flagHi := fs.Uint("chunk-hi", 0, "inclusive upper chunk ID (0 = auto-discover; set with --chunk-lo to constrain)")
	page := fs.Int("page-size", 20, "transactions per page")
	iters := fs.Int("iters", 200, "number of timed pages per worker")
	workersCSV := fs.String("query-concurrency", "1", "concurrent in-flight queries; comma-list sweep (e.g. 1,4,16)")
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

	chunkLo, chunkHi, err := resolveLedgerChunkRange(*coldDir, uint32(*flagLo), uint32(*flagHi))
	if err != nil {
		fatal(logger, "resolve chunk range in %s: %v", *coldDir, err)
	}

	// Preflight ALL chunks once. With ~10 chunks this is fast; with
	// hundreds it's slow-but-one-shot — amortized into setup, not
	// charged against the timed loop.
	preflights := preflightAllChunks(logger, *coldDir, chunkLo, chunkHi, *page)
	if len(preflights) == 0 {
		fatal(logger, "no chunk has %d txs (raise --page-size or extend chunk range)", *page)
	}

	var totalTx int
	for _, cp := range preflights {
		totalTx += cp.totalTx
	}
	logger.Infof("cold-txpage chunks=[%d,%d] usable=%d page=%d iters=%d workers=%v totalTx=%d",
		chunkLo, chunkHi, len(preflights), *page, *iters, workersList, totalTx)

	detailPath := filepath.Join(*outDir, fmt.Sprintf("cold-txpage-%d.csv", *page))
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal(logger, "mkdir %s: %v", *outDir, err)
	}
	detailF, err := os.Create(detailPath)
	if err != nil {
		fatal(logger, "create CSV %s: %v", detailPath, err)
	}
	defer detailF.Close()
	if _, err := fmt.Fprintln(detailF,
		"query_concurrency,chunk,cursor_seq,cursor_tx,n_ledgers,open_ns,fetch_ns,decode_ns,scan_ns,total_ns"); err != nil {
		fatal(logger, "write CSV header: %v", err)
	}

	summaryF, summaryPath, err := createCSV(*outDir, fmt.Sprintf("cold-txpage-%d-sweep", *page), sweepCSVHeader)
	if err != nil {
		fatal(logger, "%v", err)
	}
	defer summaryF.Close()

	printSweepHeader()

	var csvMu sync.Mutex
	results := make([]concurrentResult, 0, len(workersList))
	for _, w := range workersList {
		op := coldTxPageOp(*coldDir, preflights, *page, w, detailF, &csvMu)
		res := runConcurrentSweep(w, *iters, *seed, op)
		printSweepRow(w, res, summaryF)
		results = append(results, res)
	}
	reportSaturation(workersList, results)

	logger.Infof("wrote %s and %s", detailPath, summaryPath)
}

// chunkPreflight caches per-chunk tx counts so the timed loop can
// pick cursors without re-reading the chunk.
type chunkPreflight struct {
	chunkID uint32
	infos   []ledgerTxCount
	totalTx int
}

// preflightAllChunks scans every chunk in [lo, hi], collecting per-
// ledger tx counts. Chunks with totalTx < pageSize are dropped (the
// cursor picker requires `page` txs ahead inside the chunk).
func preflightAllChunks(logger *supportlog.Entry, coldDir string, lo, hi uint32, pageSize int) []chunkPreflight {
	logger.Infof("preflight: scanning chunks [%d,%d] for tx counts...", lo, hi)
	out := make([]chunkPreflight, 0, hi-lo+1)
	for c := lo; c <= hi; c++ {
		packFile := packPath(coldDir, c)
		first := chunkFirstLedger(c)
		last := chunkLastLedger(c)
		infos, total := preflightTxCounts(logger, packFile, first, last)
		if total < pageSize {
			continue
		}
		out = append(out, chunkPreflight{chunkID: c, infos: infos, totalTx: total})
	}
	return out
}

// coldTxPageOp returns a per-iter closure: pick a random chunk from
// the preflight set, evict its packfile, open a fresh ColdReader,
// pick a cursor with `page` txs ahead, walk the page, close, write
// CSV row.
func coldTxPageOp(
	coldDir string,
	preflights []chunkPreflight,
	page, workers int,
	detailF *os.File,
	csvMu *sync.Mutex,
) iterOp {
	return func(rng *rand.Rand, measured bool) (time.Duration, error) {
		cp := preflights[rng.IntN(len(preflights))]
		packFile := packPath(coldDir, cp.chunkID)

		if err := evictFile(packFile); err != nil {
			return 0, fmt.Errorf("evict %s: %w", packFile, err)
		}

		t0 := time.Now()
		cr, err := ledger.OpenColdReader(packFile)
		openNs := time.Since(t0)
		if err != nil {
			return 0, fmt.Errorf("OpenColdReader %s: %w", packFile, err)
		}
		defer cr.Close()

		li, ti := pickCursor(cp.infos, page, rng)
		fetchNs, decodeNs, scanNs, nLedgers, got, walkErr := walkPagePhased(cr.GetLedgerRaw, cp.infos, li, ti, page)
		if walkErr != nil {
			return 0, fmt.Errorf("walk: %w", walkErr)
		}
		if got != page {
			return 0, fmt.Errorf("short read: got %d, want %d", got, page)
		}

		totalNs := openNs + fetchNs + decodeNs + scanNs

		if measured {
			csvMu.Lock()
			_, werr := fmt.Fprintf(detailF, "%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
				workers, cp.chunkID,
				cp.infos[li].seq, ti, nLedgers,
				openNs.Nanoseconds(), fetchNs.Nanoseconds(),
				decodeNs.Nanoseconds(), scanNs.Nanoseconds(),
				totalNs.Nanoseconds())
			csvMu.Unlock()
			if werr != nil {
				return totalNs, werr
			}
		}
		return totalNs, nil
	}
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
	r, err := ledger.OpenColdReader(packFile)
	if err != nil {
		fatal(logger, "preflight OpenColdReader %s: %v", packFile, err)
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

// pickCursor returns a random (ledgerIdx, txIdx) cursor with at
// least `page` txs ahead of it in `infos`. Stateless across calls —
// the caller supplies the rng — so the same `infos` slice is safe to
// share across concurrent workers.
func pickCursor(infos []ledgerTxCount, page int, rng *rand.Rand) (int, int) {
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
