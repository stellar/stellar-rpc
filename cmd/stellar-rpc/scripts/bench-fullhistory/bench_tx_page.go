package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	goxdr "github.com/stellar/go-stellar-sdk/xdr"
)

// cmdTxPage benches "give me a page of N transactions starting from
// cursor (seq, txIdx)". Each iteration picks a random valid cursor
// inside the chunk and walks forward via the tier's reader until the
// page is full. Stays within one chunk per the goal-doc scope
// carve-out (no multi-chunk iterator).
func cmdTxPage() {
	fs := flag.NewFlagSet("tx-page", flag.ExitOnError)
	tier := fs.String("tier", "cold", "storage tier: hot|cold")
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold", "cold-store root")
	hotDir := fs.String("hot-dir", "/mnt/nvme/disk2/ledgers/hot", "hot-store dir")
	chunk := fs.Uint("chunk", 5000, "chunk to use")
	page := fs.Int("page-size", 20, "transactions per page")
	iters := fs.Int("iters", 200, "number of pages")
	warmup := fs.Int("warmup", 5, "warm-up pages (not counted)")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	if *page < 1 {
		fatal(logger, "--page-size must be >= 1")
	}
	if uint64(*chunk) > math.MaxUint32 {
		fatal(logger, "--chunk=%d exceeds uint32", *chunk)
	}

	r, first, last, err := openReader(logger, *tier, *coldDir, *hotDir, uint32(*chunk))
	if err != nil {
		fatal(logger, "open reader: %v", err)
	}
	defer r.Close()

	// Build a tx-per-ledger map from the chunk once, so we know how
	// far we may need to walk to fill a page. Decode each ledger,
	// record its tx count. This is amortized across all pages.
	logger.Infof("tx-page tier=%s chunk=%d page=%d iters=%d — preflight scanning chunk for tx counts...",
		*tier, *chunk, *page, *iters)

	type ledgerInfo struct {
		seq     uint32
		txCount int
	}
	infos := make([]ledgerInfo, 0, ledgersPerChunk)
	totalTx := 0
	if err := r.iterateRange(first, last, func(seq uint32, b []byte) bool {
		var lcm goxdr.LedgerCloseMeta
		if err := lcm.UnmarshalBinary(b); err != nil {
			fatal(logger, "preflight UnmarshalBinary seq=%d: %v", seq, err)
		}
		ct := lcm.CountTransactions()
		infos = append(infos, ledgerInfo{seq: seq, txCount: ct})
		totalTx += ct
		return true
	}); err != nil {
		fatal(logger, "preflight iterate: %v", err)
	}
	logger.Infof("preflight done: %d ledgers, %d total tx (avg %.1f/ledger)",
		len(infos), totalTx, float64(totalTx)/float64(len(infos)))

	if totalTx < *page {
		fatal(logger, "chunk has only %d txs but page-size=%d", totalTx, *page)
	}

	// Helper: walk from cursor (ledgerIdx, txIdx) emitting one tx at
	// a time. Returns when `wanted` tx have been emitted or we run
	// off the chunk.
	walkPage := func(ledgerIdx int, txIdx int) (got int, err error) {
		remaining := *page
		for i := ledgerIdx; i < len(infos) && remaining > 0; i++ {
			raw, gerr := r.GetLedgerRaw(infos[i].seq)
			if gerr != nil {
				return got, gerr
			}
			var lcm goxdr.LedgerCloseMeta
			if uerr := lcm.UnmarshalBinary(raw); uerr != nil {
				return got, uerr
			}
			nTx := lcm.CountTransactions()
			startIdx := 0
			if i == ledgerIdx {
				startIdx = txIdx
			}
			for j := startIdx; j < nTx && remaining > 0; j++ {
				// Touch the per-tx hash + result-pair so the bench
				// exercises the same decode work a real page would.
				_ = lcm.TransactionHash(j)
				_ = lcm.TransactionResultPair(j)
				got++
				remaining--
			}
		}
		return got, nil
	}

	rng := rand.New(rand.NewPCG(uint64(*seed), uint64(*seed*7919)))

	// Random start cursor that leaves at least *page txs ahead.
	pick := func() (int, int) {
		for {
			i := rng.IntN(len(infos))
			if infos[i].txCount == 0 {
				continue
			}
			j := rng.IntN(infos[i].txCount)
			// Quick feasibility: count remaining tx >= page.
			ahead := infos[i].txCount - j
			for k := i + 1; k < len(infos) && ahead < *page; k++ {
				ahead += infos[k].txCount
			}
			if ahead >= *page {
				return i, j
			}
		}
	}

	for i := 0; i < *warmup; i++ {
		li, ti := pick()
		got, werr := walkPage(li, ti)
		if werr != nil {
			fatal(logger, "warmup walk: %v", werr)
		}
		if got != *page {
			fatal(logger, "warmup short read: got %d, want %d", got, *page)
		}
	}

	durs := make([]time.Duration, 0, *iters)
	for i := 0; i < *iters; i++ {
		li, ti := pick()
		t0 := time.Now()
		got, werr := walkPage(li, ti)
		d := time.Since(t0)
		if werr != nil {
			fatal(logger, "walk: %v", werr)
		}
		if got != *page {
			fatal(logger, "short read: got %d, want %d", got, *page)
		}
		durs = append(durs, d)
	}

	stats := computeStats(durs)
	fmt.Println(stats.line(fmt.Sprintf("tx-page-%s-%d", *tier, *page)))

	csv := filepath.Join(*outDir, fmt.Sprintf("tx-page-%s-%d.csv", *tier, *page))
	if err := writeCSV(csv, durs); err != nil {
		logger.WithError(err).Warnf("could not write CSV %s", csv)
	} else {
		logger.Infof("wrote %s", csv)
	}
}
