package main

import (
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
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
//	lookup_ns      txhash hot store Get(hash) → ledgerSeq
//	fetch_ns       ledger hot store GetLedgerRaw(seq)
//	scan_ns        locate the matching tx in the LCM
//	materialize_ns build db.Transaction
//	total_ns       sum
//
// --xdr-views (default true) toggles the same scan+materialize split
// as cold-tx-hash; CSV filename gets "-xdrviews" or "-roundtrip"
// suffix so paired runs don't overwrite each other.
func cmdHotTxHash() {
	fs := flag.NewFlagSet("hot-tx-hash", flag.ExitOnError)
	hotDir := fs.String("hot-dir", "/mnt/nvme/disk2/ledgers/hot-5000", "hot ledger store dir")
	txHotDir := fs.String("txhash-hot", "/mnt/nvme/disk2/ledgers/txhash-hot",
		"hot txhash store dir")
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold",
		"cold-store root (used at startup to sample known-valid hashes; not on the timed path)")
	chunk := fs.Uint("chunk", 5000, "chunk to use")
	iters := fs.Int("iters", 1000, "number of timed lookups")
	warmup := fs.Int("warmup", hotWarmupSharedIters, "warm-up lookups (RocksDB block-cache priming)")
	sampleLedgers := fs.Int("sample-ledgers", 100,
		"number of random ledgers to sample for the hash pool (~300 hashes each)")
	missRate := fs.Float64("miss-rate", 0.0,
		"fraction of iters that look up a random non-existent hash (range [0,1]). "+
			"is_miss column in CSV is 1 for those iters. Hot store returns ErrNotFound "+
			"deterministically (no fingerprint false positives) so misses skip fetch/scan/materialize.")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	xdrViews := fs.Bool("xdr-views", true,
		"use XDR views to scan + materialize. false = lcm.UnmarshalBinary + db.ParseTransaction round-trip.")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	if *missRate < 0 || *missRate > 1 {
		fatal(logger, "--miss-rate=%v out of range [0,1]", *missRate)
	}
	chunkID := uint32(*chunk)
	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)

	txh, err := txhash.NewHotStore(*txHotDir, logger)
	if err != nil {
		fatal(logger, "txhash NewHotStore %s: %v", *txHotDir, err)
	}
	defer txh.Close()

	lh, err := ledger.NewHotStore(*hotDir, logger)
	if err != nil {
		fatal(logger, "ledger NewHotStore %s: %v", *hotDir, err)
	}
	defer lh.Close()

	rng := rand.New(rand.NewPCG(uint64(*seed), uint64(*seed*7919)))
	hashes, err := sampleHashesFromCold(*coldDir, chunkID, first, last, *sampleLedgers, rng)
	if err != nil {
		fatal(logger, "sample hashes: %v", err)
	}
	if len(hashes) == 0 {
		fatal(logger, "no hashes sampled (chunk has no tx?)")
	}
	logger.Infof("hot-tx-hash chunk=%d iters=%d warmup=%d sampled %d hashes xdr-views=%v",
		chunkID, *iters, *warmup, len(hashes), *xdrViews)

	// doOne returns isMiss=true when the lookup misses (clean
	// ErrNotFound) — the timed fetch/scan/materialize don't run. err is
	// reserved for actual failures (bad reader state etc.).
	doOne := func(hash [32]byte) (lookup, fetch, scan, mat time.Duration, seq uint32, isMiss bool, err error) {
		t1 := time.Now()
		seq, err = txh.Get(hash)
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

		if *xdrViews {
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
			// Sanity-check the materialized tx so the compiler can't
			// DCE the per-field byte-slicing into `_`.
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

	pickHash := func() ([32]byte, bool) {
		if *missRate > 0 && rng.Float64() < *missRate {
			var h [32]byte
			for j := 0; j < 32; j += 8 {
				v := rng.Uint64()
				for k := 0; k < 8 && j+k < 32; k++ {
					h[j+k] = byte(v >> (8 * k))
				}
			}
			return h, true
		}
		return hashes[rng.IntN(len(hashes))], false
	}

	// Warmup uses only hits (regardless of --miss-rate) so the
	// block-cache and lookup paths get exercised for the timed work.
	for i := range *warmup {
		hash := hashes[rng.IntN(len(hashes))]
		if _, _, _, _, _, _, werr := doOne(hash); werr != nil {
			fatal(logger, "warmup %d: %v", i, werr)
		}
	}

	suffix := "-roundtrip"
	if *xdrViews {
		suffix = "-xdrviews"
	}
	csvPath := filepath.Join(*outDir, "hot-tx-hash"+suffix+".csv")
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal(logger, "mkdir %s: %v", *outDir, err)
	}
	csvF, err := os.Create(csvPath)
	if err != nil {
		fatal(logger, "create CSV %s: %v", csvPath, err)
	}
	defer csvF.Close()
	if _, err := fmt.Fprintln(csvF, "hash,seq,is_miss,lookup_ns,fetch_ns,scan_ns,materialize_ns,total_ns"); err != nil {
		fatal(logger, "write CSV header: %v", err)
	}

	totals := make([]time.Duration, 0, *iters)
	missTotals := make([]time.Duration, 0, *iters)
	for i := range *iters {
		hash, _ := pickHash()
		lookupNs, fetchNs, scanNs, matNs, seq, isMiss, derr := doOne(hash)
		if derr != nil {
			fatal(logger, "iter %d: %v", i, derr)
		}
		totalNs := lookupNs + fetchNs + scanNs + matNs
		if isMiss {
			missTotals = append(missTotals, totalNs)
		} else {
			totals = append(totals, totalNs)
		}
		missFlag := 0
		if isMiss {
			missFlag = 1
		}
		if _, err := fmt.Fprintf(csvF, "%x,%d,%d,%d,%d,%d,%d,%d\n",
			hash[:8], seq, missFlag,
			lookupNs.Nanoseconds(), fetchNs.Nanoseconds(),
			scanNs.Nanoseconds(), matNs.Nanoseconds(), totalNs.Nanoseconds()); err != nil {
			fatal(logger, "iter %d write CSV: %v", i, err)
		}
	}

	hitStats := computeStats(totals)
	fmt.Println(hitStats.line(fmt.Sprintf("hot-tx-hash%s hit", suffix)))
	if len(missTotals) > 0 {
		missStats := computeStats(missTotals)
		fmt.Println(missStats.line(fmt.Sprintf("hot-tx-hash%s miss", suffix)))
	}
	logger.Infof("wrote %s (hits=%d misses=%d)", csvPath, len(totals), len(missTotals))
}
