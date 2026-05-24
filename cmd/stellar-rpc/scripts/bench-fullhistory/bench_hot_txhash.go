package main

import (
	"encoding/hex"
	"errors"
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

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// cmdHotTxHash benches getTransaction(hash) end-to-end against the
// hot tier with hot-tier methodology: one shared HotStore + txhash
// HotStore handle for the whole run, RocksDB block-cache warmup
// before the timed iters.
//
// The hash pool is sampled from the cold pack at startup (cheaper
// and tier-independent — we just need known-valid hashes inside the
// chunk's seq window).
//
// Per-iter CSV row decomposes the call chain:
//
//	workers        worker-count cell this iter belongs to (sweep axis)
//	lookup_ns      txhash hot store Get(hash) → ledgerSeq
//	fetch_ns       ledger hot store GetLedgerRaw(seq)
//	scan_ns        locate the matching tx in the LCM
//	materialize_ns build db.Transaction
//	total_ns       sum
//
// --xdr-views (default false) toggles the same scan+materialize split
// as cold-txhash; CSV filename gets "-xdrviews" or "-roundtrip"
// suffix so paired runs don't overwrite each other.
func cmdHotTxHash() {
	fs := flag.NewFlagSet("hot-txhash", flag.ExitOnError)
	hotDir := fs.String("hot-dir", "/mnt/nvme/disk2/ledgers/hot-5000", "hot ledger store dir")
	txHotDir := fs.String("txhash-hot", "/mnt/nvme/disk2/ledgers/txhash-hot",
		"hot txhash store dir")
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold",
		"cold-store root (used at startup to sample known-valid hashes; not on the timed path)")
	chunk := fs.Uint("chunk", 5000, "chunk to use")
	iters := fs.Int("iters", 1000, "number of timed lookups per worker")
	workersCSV := fs.String("query-concurrency", "1", "concurrent in-flight queries; comma-list sweep (e.g. 1,4,16)")
	warmup := fs.Int("warmup", hotWarmupSharedIters, "warm-up lookups per worker (RocksDB block-cache priming)")
	sampleLedgers := fs.Int("sample-ledgers", 100,
		"number of random ledgers to sample for the hash pool (~300 hashes each)")
	missRate := fs.Float64("miss-rate", 0.0,
		"fraction of iters that look up a random non-existent hash (range [0,1]). "+
			"is_miss column in CSV is 1 for those iters. Hot store returns ErrNotFound "+
			"deterministically (no fingerprint false positives) so misses skip fetch/scan/materialize.")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	xdrViews := fs.Bool("xdr-views", false,
		"use XDR views to scan + materialize. false = lcm.UnmarshalBinary + db.ParseTransaction round-trip.")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	if *missRate < 0 || *missRate > 1 {
		fatal(logger, "--miss-rate=%v out of range [0,1]", *missRate)
	}
	workersList, err := parseIntList(*workersCSV)
	if err != nil {
		fatal(logger, "parse --query-concurrency: %v", err)
	}
	validateWorkersList(logger, workersList)

	chunkID := uint32(*chunk)
	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)

	txh, err := txhash.OpenHotStore(*txHotDir, logger)
	if err != nil {
		fatal(logger, "txhash OpenHotStore %s: %v", *txHotDir, err)
	}
	defer txh.Close()

	lh, err := ledger.OpenHotStore(*hotDir, logger)
	if err != nil {
		fatal(logger, "ledger OpenHotStore %s: %v", *hotDir, err)
	}
	defer lh.Close()

	sampleRNG := rand.New(rand.NewPCG(uint64(*seed), uint64(*seed*7919)))
	hashes, err := sampleHashesFromCold(*coldDir, chunkID, first, last, *sampleLedgers, sampleRNG)
	if err != nil {
		fatal(logger, "sample hashes: %v", err)
	}
	if len(hashes) == 0 {
		fatal(logger, "no hashes sampled (chunk has no tx?)")
	}
	logger.Infof("hot-txhash chunk=%d iters=%d workers=%v warmup=%d sampled %d hashes xdr-views=%v",
		chunkID, *iters, workersList, *warmup, len(hashes), *xdrViews)

	suffix := "-roundtrip"
	if *xdrViews {
		suffix = "-xdrviews"
	}
	detailPath := filepath.Join(*outDir, "hot-txhash"+suffix+".csv")
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal(logger, "mkdir %s: %v", *outDir, err)
	}
	detailF, err := os.Create(detailPath)
	if err != nil {
		fatal(logger, "create CSV %s: %v", detailPath, err)
	}
	defer detailF.Close()
	if _, err := fmt.Fprintln(detailF, "query_concurrency,hash,seq,is_miss,lookup_ns,fetch_ns,scan_ns,materialize_ns,total_ns"); err != nil {
		fatal(logger, "write CSV header: %v", err)
	}

	summaryF, summaryPath, err := createCSV(*outDir, "hot-txhash"+suffix+"-sweep", sweepCSVHeader)
	if err != nil {
		fatal(logger, "%v", err)
	}
	defer summaryF.Close()

	printSweepHeader()

	var csvMu sync.Mutex
	results := make([]concurrentResult, 0, len(workersList))
	for _, w := range workersList {
		hits := make([]time.Duration, 0, *iters)
		misses := make([]time.Duration, 0, *iters)
		op := hotTxHashOp(lh, txh, hashes, *missRate, *xdrViews, w, detailF, &csvMu, &hits, &misses)
		res := runConcurrentSweepWithWarmup(w, *warmup, *iters, *seed, op)
		printSweepRow(w, res, summaryF)
		if len(hits) > 0 {
			fmt.Println(computeStats(hits).line(fmt.Sprintf("  workers=%d hit", w)))
		}
		if len(misses) > 0 {
			fmt.Println(computeStats(misses).line(fmt.Sprintf("  workers=%d miss", w)))
		}
		results = append(results, res)
	}
	reportSaturation(workersList, results)

	logger.Infof("wrote %s and %s", detailPath, summaryPath)
}

// hotTxHashOp returns a per-iter closure that picks a hash (hit or
// miss per missRate), runs the full lookup + fetch + scan + materialize
// chain through the shared hot stores, writes one detail-CSV row, and
// appends the total to hits or misses for post-sweep split stats.
func hotTxHashOp(
	lh *ledger.HotStore,
	txh *txhash.HotStore,
	hashes [][32]byte,
	missRate float64,
	xdrViews bool,
	workers int,
	detailF *os.File,
	csvMu *sync.Mutex,
	hits, misses *[]time.Duration,
) iterOp {
	return func(rng *rand.Rand, measured bool) (time.Duration, error) {
		hash := pickHashOrMiss(rng, hashes, missRate)
		lookupNs, fetchNs, scanNs, matNs, seq, isMiss, err := hotTxHashLookup(lh, txh, hash, xdrViews)
		if err != nil {
			return 0, err
		}
		totalNs := lookupNs + fetchNs + scanNs + matNs

		if !measured {
			return totalNs, nil
		}
		missFlag := 0
		if isMiss {
			missFlag = 1
		}

		csvMu.Lock()
		if isMiss {
			*misses = append(*misses, totalNs)
		} else {
			*hits = append(*hits, totalNs)
		}
		_, werr := fmt.Fprintf(detailF, "%d,%x,%d,%d,%d,%d,%d,%d,%d\n",
			workers, hash[:8], seq, missFlag,
			lookupNs.Nanoseconds(), fetchNs.Nanoseconds(),
			scanNs.Nanoseconds(), matNs.Nanoseconds(), totalNs.Nanoseconds())
		csvMu.Unlock()
		if werr != nil {
			return totalNs, werr
		}
		return totalNs, nil
	}
}

// hotTxHashLookup runs the full hot-side getTransaction(hash) chain.
// isMiss=true with err=nil indicates a clean miss (ErrNotFound); the
// fetch/scan/materialize stages are skipped.
func hotTxHashLookup(
	lh *ledger.HotStore,
	txh *txhash.HotStore,
	hash [32]byte,
	xdrViews bool,
) (lookup, fetch, scan, mat time.Duration, seq uint32, isMiss bool, err error) {
	t1 := time.Now()
	seq, err = txh.Lookup(hash)
	lookup = time.Since(t1)
	if errors.Is(err, stores.ErrNotFound) {
		err = nil
		isMiss = true
		return
	}
	if err != nil {
		return
	}

	t2 := time.Now()
	raw, gerr := lh.GetLedgerRaw(seq)
	fetch = time.Since(t2)
	if gerr != nil {
		err = gerr
		return
	}

	if xdrViews {
		t3 := time.Now()
		applyIdx, ferr := findTxByHashView(raw, hash)
		scan = time.Since(t3)
		if ferr != nil {
			err = ferr
			return
		}
		if applyIdx < 0 {
			err = errors.New("hash not found")
			return
		}

		t4 := time.Now()
		tx, merr := materializeViews(raw, applyIdx)
		mat = time.Since(t4)
		if merr != nil {
			err = merr
			return
		}
		// Sanity-check the materialized tx so the compiler can't DCE
		// the per-field byte-slicing into `_`.
		if tx.TransactionHash[:16] != hex.EncodeToString(hash[:8]) {
			err = fmt.Errorf("view hash mismatch: got %s want %x", tx.TransactionHash, hash[:8])
		}
		return
	}

	t3 := time.Now()
	var lcm goxdr.LedgerCloseMeta
	if uerr := lcm.UnmarshalBinary(raw); uerr != nil {
		err = uerr
		return
	}
	scan = time.Since(t3)

	t4 := time.Now()
	tx, _, merr := materializeRoundtripFromLCM(lcm, hash, pubnetPassphrase)
	mat = time.Since(t4)
	if merr != nil {
		err = merr
		return
	}
	if tx.TransactionHash[:16] != hex.EncodeToString(hash[:8]) {
		err = fmt.Errorf("roundtrip hash mismatch: got %s want %x", tx.TransactionHash, hash[:8])
	}
	return
}

// pickHashOrMiss returns a hit-pool hash (most of the time) or a
// fresh random 32-byte hash when missRate triggers a miss. Stateless
// across calls — same rng-per-call shape as pickCursor.
func pickHashOrMiss(rng *rand.Rand, hashes [][32]byte, missRate float64) [32]byte {
	if missRate > 0 && rng.Float64() < missRate {
		var h [32]byte
		for j := 0; j < 32; j += 8 {
			v := rng.Uint64()
			for k := 0; k < 8 && j+k < 32; k++ {
				h[j+k] = byte(v >> (8 * k))
			}
		}
		return h
	}
	return hashes[rng.IntN(len(hashes))]
}
