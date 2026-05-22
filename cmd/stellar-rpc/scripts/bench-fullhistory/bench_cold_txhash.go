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

// cmdColdTxHash benches getTransaction(hash) end-to-end against the
// cold tier with cold-cache methodology: every iteration evicts both
// the chunk's packfile and the MPHF from the OS page cache, opens a
// fresh ColdReader + txhash.ColdReader (MPHF), runs the lookup,
// and closes. The MPHF eviction matters because its on-disk layout is
// mmap-backed — without eviction, the first Lookup faults pages in
// once and they stay warm forever, making lookup_ns ~µs steady-state
// instead of the cold-fault cost a real first request pays.
//
// Per-iter CSV row decomposes the call chain so callers can see where
// time goes:
//
//	mphf_open_ns   txhash.OpenColdReader (file open + mmap setup)
//	pack_open_ns   OpenColdReader (parse pack header)
//	lookup_ns      mph.Lookup(hash) → ledgerSeq (includes MPHF page faults)
//	fetch_ns       cr.GetLedgerRaw(seq) (pack read + zstd decode)
//	scan_ns        locate the matching tx in the LCM
//	materialize_ns build db.Transaction (envelope/result/meta/events)
//	total_ns       sum of the above (closes are deferred, not timed)
//
// --xdr-views (default false) toggles the scan+materialize between an
// XDR-view path (cheap walk + slice-from-raw for Result and Meta) and
// the production-shape path (lcm.UnmarshalBinary + ingest reader +
// db.ParseTransaction's MarshalBinary-each-field round-trip). Output
// filename gets a "-xdrviews" suffix on the view path so paired runs
// don't overwrite each other.
func cmdColdTxHash() {
	fs := flag.NewFlagSet("cold-txhash", flag.ExitOnError)
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold", "cold-store root for ledger reads")
	txColdMPHF := fs.String("txhash-cold-mphf", "/mnt/nvme/disk2/ledgers/txhash-cold/00005000.idx",
		"cold txhash streamhash MPHF .idx")
	chunk := fs.Uint("chunk", 5000, "chunk to use")
	iters := fs.Int("iters", 1000, "number of timed lookups")
	sampleLedgers := fs.Int("sample-ledgers", 100,
		"number of random ledgers to sample for the hash pool (~300 hashes each)")
	missRate := fs.Float64("miss-rate", 0.0,
		"fraction of iters that look up a random non-existent hash (range [0,1]). "+
			"is_miss column in CSV is 1 for those iters. Misses skip fetch/scan/materialize "+
			"and only pay open + lookup. The MPHF's 1-byte fingerprint rejects most misses at "+
			"~255/256; the ~1/256 false positives that survive get re-classified here when "+
			"the in-LCM scan fails to find the hash.")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	xdrViews := fs.Bool("xdr-views", false,
		"use XDR views to scan + materialize (slice Result/Meta from raw via .Raw()). "+
			"false = lcm.UnmarshalBinary + db.ParseTransaction round-trip.")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	if *missRate < 0 || *missRate > 1 {
		fatal(logger, "--miss-rate=%v out of range [0,1]", *missRate)
	}
	chunkID := uint32(*chunk)
	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)

	// Validate the MPHF path early so we don't fatal mid-loop after
	// thousands of evictions. The actual handle is opened per-iter.
	if _, err := os.Stat(*txColdMPHF); err != nil {
		fatal(logger, "txhash MPHF missing: %s: %v", *txColdMPHF, err)
	}

	rng := rand.New(rand.NewPCG(uint64(*seed), uint64(*seed*7919)))
	// Sample hashes via a temporary MPHF + pack reader (out of the
	// timed loop). This warms the page cache for both files; the
	// per-iter evictFile calls below clear them before each measurement.
	hashes, err := sampleHashesFromCold(*coldDir, chunkID, first, last, *sampleLedgers, rng)
	if err != nil {
		fatal(logger, "sample hashes: %v", err)
	}
	if len(hashes) == 0 {
		fatal(logger, "no hashes sampled (chunk has no tx?)")
	}
	logger.Infof("cold-txhash chunk=%d iters=%d sampled %d hashes xdr-views=%v miss-rate=%.3f",
		chunkID, *iters, len(hashes), *xdrViews, *missRate)

	// pickHash returns (hash, isMiss). When isMiss is true the hash is
	// a fresh 32 random bytes that almost certainly isn't in the MPHF
	// (255/256 of these are rejected by the 1-byte fingerprint).
	pickHash := func() ([32]byte, bool) {
		if *missRate > 0 && rng.Float64() < *missRate {
			var h [32]byte
			// rand/v2: derive four uint64s and copy into the hash buffer.
			binPut := func(off int, v uint64) {
				for j := range 8 {
					h[off+j] = byte(v >> (8 * j))
				}
			}
			binPut(0, rng.Uint64())
			binPut(8, rng.Uint64())
			binPut(16, rng.Uint64())
			binPut(24, rng.Uint64())
			return h, true
		}
		return hashes[rng.IntN(len(hashes))], false
	}

	suffix := "-roundtrip"
	if *xdrViews {
		suffix = "-xdrviews"
	}
	csvPath := filepath.Join(*outDir, "cold-txhash"+suffix+".csv")
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal(logger, "mkdir %s: %v", *outDir, err)
	}
	csvF, err := os.Create(csvPath)
	if err != nil {
		fatal(logger, "create CSV %s: %v", csvPath, err)
	}
	defer csvF.Close()
	if _, err := fmt.Fprintln(csvF, "hash,seq,is_miss,mphf_open_ns,pack_open_ns,lookup_ns,fetch_ns,scan_ns,materialize_ns,total_ns"); err != nil {
		fatal(logger, "write CSV header: %v", err)
	}

	packFile := packPath(*coldDir, chunkID)
	totals := make([]time.Duration, 0, *iters)
	missTotals := make([]time.Duration, 0, *iters)

	for i := range *iters {
		hash, expectMiss := pickHash()

		// Evict both files. The MPHF mmap pages stay warm forever once
		// touched, so without this every iter would measure warm-MPHF
		// lookup cost instead of the cold-page-fault cost a real first
		// request pays.
		if err := evictFile(packFile); err != nil {
			fatal(logger, "iter %d evict pack: %v", i, err)
		}
		if err := evictFile(*txColdMPHF); err != nil {
			fatal(logger, "iter %d evict mphf: %v", i, err)
		}

		t0 := time.Now()
		mph, err := txhash.OpenColdReader(*txColdMPHF)
		mphfOpenNs := time.Since(t0)
		if err != nil {
			fatal(logger, "iter %d OpenColdReader: %v", i, err)
		}

		t1 := time.Now()
		cr, err := ledger.OpenColdReader(packFile)
		packOpenNs := time.Since(t1)
		if err != nil {
			mph.Close()
			fatal(logger, "iter %d OpenColdReader: %v", i, err)
		}

		t2 := time.Now()
		seq, err := mph.Lookup(hash)
		lookupNs := time.Since(t2)
		isMiss := false
		var fetchNs, scanNs, matNs time.Duration

		switch {
		case errors.Is(err, stores.ErrNotFound):
			// Clean miss: MPHF rejected the hash (1-byte fingerprint
			// caught it). No fetch/scan/materialize work to do.
			isMiss = true
		case err != nil:
			cr.Close()
			mph.Close()
			fatal(logger, "iter %d mph.Lookup: %v", i, err)
		case seq < first || seq > last:
			// MPHF fingerprint false-positive on an unseen hash:
			// returned a ledger outside the chunk window. Treat as a
			// miss; no fetch (the seq isn't in our pack).
			isMiss = true
		}
		// expectMiss is informational — the reported is_miss reflects
		// what actually happened, not what was requested (a fingerprint
		// false positive can flip either way).
		_ = expectMiss

		var raw []byte
		if !isMiss {
			t3a := time.Now()
			raw, err = cr.GetLedgerRaw(seq)
			fetchNs = time.Since(t3a)
			if err != nil {
				cr.Close()
				mph.Close()
				fatal(logger, "iter %d GetLedgerRaw(%d): %v", i, seq, err)
			}
		}

		switch {
		case isMiss:
			// already classified, nothing to scan or materialize
		case *xdrViews:
			t3 := time.Now()
			applyIdx, ferr := findTxByHashView(raw, hash)
			scanNs = time.Since(t3)
			if ferr != nil {
				cr.Close()
				mph.Close()
				fatal(logger, "iter %d findTxByHashView: %v", i, ferr)
			}
			if applyIdx < 0 {
				// Fingerprint false positive that survived Lookup but
				// the in-LCM hash scan found no match. Classify as miss.
				isMiss = true
				break
			}
			t4 := time.Now()
			tx, merr := materializeViews(raw, applyIdx)
			matNs = time.Since(t4)
			if merr != nil {
				cr.Close()
				mph.Close()
				fatal(logger, "iter %d materializeViews: %v", i, merr)
			}
			// Sanity-check the materialized tx so the compiler can't
			// DCE the per-field byte-slicing into `_`. Hash agreement
			// is the cheapest available check.
			if tx.TransactionHash[:16] != hex.EncodeToString(hash[:8]) {
				cr.Close()
				mph.Close()
				fatal(logger, "iter %d view hash mismatch: got %s want %x", i, tx.TransactionHash, hash[:8])
			}
		default:
			t3 := time.Now()
			var lcm goxdr.LedgerCloseMeta
			if uerr := lcm.UnmarshalBinary(raw); uerr != nil {
				cr.Close()
				mph.Close()
				fatal(logger, "iter %d UnmarshalBinary: %v", i, uerr)
			}
			scanNs = time.Since(t3)

			t4 := time.Now()
			tx, _, merr := materializeRoundtripFromLCM(lcm, hash, pubnetPassphrase)
			matNs = time.Since(t4)
			if merr != nil {
				// Round-trip path's only "hash not found" signal is the
				// formatted error from materializeRoundtripFromLCM. A
				// fingerprint false positive lands here.
				isMiss = true
				break
			}
			if tx.TransactionHash[:16] != hex.EncodeToString(hash[:8]) {
				cr.Close()
				mph.Close()
				fatal(logger, "iter %d roundtrip hash mismatch: got %s want %x", i, tx.TransactionHash, hash[:8])
			}
		}

		cr.Close()
		mph.Close()

		totalNs := mphfOpenNs + packOpenNs + lookupNs + fetchNs + scanNs + matNs
		if isMiss {
			missTotals = append(missTotals, totalNs)
		} else {
			totals = append(totals, totalNs)
		}
		missFlag := 0
		if isMiss {
			missFlag = 1
		}
		if _, err := fmt.Fprintf(csvF, "%x,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
			hash[:8], seq, missFlag,
			mphfOpenNs.Nanoseconds(), packOpenNs.Nanoseconds(),
			lookupNs.Nanoseconds(), fetchNs.Nanoseconds(),
			scanNs.Nanoseconds(), matNs.Nanoseconds(), totalNs.Nanoseconds()); err != nil {
			fatal(logger, "iter %d write CSV: %v", i, err)
		}
	}

	hitStats := computeStats(totals)
	fmt.Println(hitStats.line(fmt.Sprintf("cold-txhash%s hit", suffix)))
	if len(missTotals) > 0 {
		missStats := computeStats(missTotals)
		fmt.Println(missStats.line(fmt.Sprintf("cold-txhash%s miss", suffix)))
	}
	logger.Infof("wrote %s (hits=%d misses=%d)", csvPath, len(totals), len(missTotals))
}
