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

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// cmdColdTxHash benches getTransaction(hash) end-to-end against the
// cold tier. Multi-chunk: the MPHF advertises the chunk range it
// covers (via CoveredRange), hashes are sampled at startup from exactly
// those chunks' packs, and per-iter eviction targets the chunk pack the
// lookup resolves to.
//
// By default the MPHF is opened once at startup and kept page-cache
// warm across all iters and workers (modeling a long-lived server),
// so the default run does NOT measure MPHF cold-fault latency — the
// index mmap stays resident for the whole run, on every code path.
//
// --evict-mphf opts into a cold-MPHF measurement: per iter the index
// is evicted from the page cache, re-opened (fresh mmap, timed as
// mphf_open_ns), queried once (cold-faulting its bucket/pilot/payload
// pages), then closed at iter end. The close-per-iter is load-bearing:
// FADV_DONTNEED skips still-mapped pages, so the index must be
// munmaped before the next iter's evict can actually drop it. This
// mode is single-worker only (--query-concurrency=1) — one shared idx
// file under concurrency would have workers evicting each other's
// just-faulted pages.
//
// Per-iter CSV columns:
//
//	workers        worker-count cell this iter belongs to
//	chunk          ID the looked-up hash resolved to
//	hash           first 8 bytes of looked-up hash
//	seq            ledger seq the MPHF returned (0 on miss)
//	is_miss        1 if MPHF returned ErrNotFound or fingerprint FP
//	mphf_open_ns   OpenColdReader on the index (0 unless --evict-mphf)
//	lookup_ns      mph.Lookup(hash)
//	pack_open_ns   OpenColdReader on the resolved chunk
//	fetch_ns       cr.GetLedgerRaw(seq)
//	scan_ns        locate the matching tx in the LCM
//	materialize_ns build db.Transaction
//	total_ns       sum
//
// --xdr-views toggles the scan+materialize between view path and
// production-shape round-trip.
func cmdColdTxHash() {
	fs := flag.NewFlagSet("cold-txhash", flag.ExitOnError)
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold", "cold-store root for ledger reads")
	txColdMPHF := fs.String("txhash-cold-mphf", "/mnt/nvme/disk2/ledgers/txhash-cold/00005000.idx",
		"cold txhash streamhash MPHF .idx (opened once, kept warm across workers)")
	iters := fs.Int("iters", 1000, "number of timed lookups per worker")
	workersCSV := fs.String("query-concurrency", "1", "concurrent in-flight queries; comma-list sweep (e.g. 1,4,16)")
	sampleLedgers := fs.Int("sample-ledgers", 100,
		"number of random ledgers PER CHUNK to sample for the hash pool (~300 hashes each)")
	missRate := fs.Float64("miss-rate", 0.0,
		"fraction of iters that look up a random non-existent hash (range [0,1]). "+
			"is_miss column in CSV is 1 for those iters. Misses skip fetch/scan/materialize "+
			"and only pay lookup_ns. The MPHF's 1-byte fingerprint rejects most misses at "+
			"~255/256; the ~1/256 false positives that survive get re-classified here when "+
			"the in-LCM scan fails to find the hash.")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	xdrViews := fs.Bool("xdr-views", false,
		"use XDR views to scan + materialize (slice Result/Meta from raw via .Raw()). "+
			"false = lcm.UnmarshalBinary + db.ParseTransaction round-trip.")
	evictMPHF := fs.Bool("evict-mphf", false,
		"measure a COLD MPHF: per iter, evict the .idx from the page cache, open a fresh "+
			"streamhash mmap (timed as mphf_open_ns), do one lookup (cold-faults the bucket/pilot/"+
			"payload pages), then close the mmap at iter end. Requires --query-concurrency=1 — the "+
			"single shared idx file means concurrent workers would evict each other's just-faulted "+
			"pages. Default false keeps the index opened once and page-cache-warm across the run.")
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

	if *evictMPHF && (len(workersList) != 1 || workersList[0] != 1) {
		fatal(logger, "--evict-mphf requires --query-concurrency=1 (cold MPHF measurement is "+
			"single-threaded; concurrent workers share one idx mmap and evict each other's faulted pages)")
	}

	// Evict the index from the page cache before opening it so the run
	// starts cold regardless of what a prior run left resident. This
	// matters even when --evict-mphf=false: a full-history index is far
	// larger than RAM and uniform-random txhash lookups have no hot
	// subset, so the realistic steady state is "index not resident."
	// Evicting up front removes residual page-cache state as a hidden
	// variable. Must run before OpenColdReader mmaps the file —
	// FADV_DONTNEED skips still-mapped pages. Best-effort: a missing or
	// unreadable file surfaces authoritatively at OpenColdReader below.
	if err := evictFile(*txColdMPHF); err != nil {
		logger.Warnf("startup evict of %s failed (continuing): %v", *txColdMPHF, err)
	}

	// Open the MPHF once. With --evict-mphf=false, pages fault in and
	// accumulate across workers over the run (starting cold per the
	// eviction above); per-iter eviction of this shared mmap was
	// meaningful single-threaded but is fundamentally racy under
	// workers>1, which is why --evict-mphf re-opens per iter instead.
	mph, err := txhash.OpenColdReader(*txColdMPHF)
	if err != nil {
		fatal(logger, "OpenColdReader %s: %v", *txColdMPHF, err)
	}
	defer mph.Close()

	// The MPHF advertises the [minLedger, maxLedger] it was built over,
	// so the bench learns its chunk coverage straight from the index — no
	// directory glob and no hash-probe to discover the range. Map the
	// ledger range to chunk IDs and sample from exactly those packs.
	//
	// This assumes the MPHF covers a contiguous chunk run with every pack
	// present (the normal cold-ingest case). Unlike the old probe, a gap
	// or a missing pack is NOT silently skipped: a missing pack fatals at
	// the sample step below, and a sparse MPHF would sample gap chunks
	// whose lookups miss (skewing hit/miss). Both are non-issues for a
	// contiguous build.
	minLedger, maxLedger := mph.CoveredRange()
	if minLedger < chunk.FirstLedgerSeq {
		fatal(logger, "MPHF %s reports minLedger %d below genesis %d — corrupt coverage metadata?",
			*txColdMPHF, minLedger, chunk.FirstLedgerSeq)
	}
	chunkLo := uint32(chunk.IDFromLedger(minLedger))
	chunkHi := uint32(chunk.IDFromLedger(maxLedger))
	covered := make([]uint32, 0, chunkHi-chunkLo+1)
	for c := chunkLo; c <= chunkHi; c++ {
		covered = append(covered, c)
	}

	sampleRNG := rand.New(rand.NewPCG(uint64(*seed), uint64(*seed*7919)))
	hashes, err := sampleHashesFromColdChunks(*coldDir, covered, *sampleLedgers, sampleRNG)
	if err != nil {
		fatal(logger, "sample hashes (MPHF covers chunks [%d,%d]): %v", chunkLo, chunkHi, err)
	}
	if len(hashes) == 0 {
		fatal(logger, "no hashes sampled (chunks have no tx?)")
	}
	logger.Infof("cold-txhash mphf-covers=chunks[%d,%d] seqs=[%d,%d] iters=%d workers=%v sampled %d hashes xdr-views=%v miss-rate=%.3f",
		chunkLo, chunkHi, minLedger, maxLedger, *iters, workersList, len(hashes), *xdrViews, *missRate)

	suffix := "-roundtrip"
	if *xdrViews {
		suffix = "-xdrviews"
	}
	detailPath := filepath.Join(*outDir, "cold-txhash"+suffix+".csv")
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal(logger, "mkdir %s: %v", *outDir, err)
	}
	detailF, err := os.Create(detailPath)
	if err != nil {
		fatal(logger, "create CSV %s: %v", detailPath, err)
	}
	defer detailF.Close()
	if _, err := fmt.Fprintln(detailF,
		"query_concurrency,chunk,hash,seq,is_miss,mphf_open_ns,lookup_ns,pack_open_ns,fetch_ns,scan_ns,materialize_ns,total_ns"); err != nil {
		fatal(logger, "write CSV header: %v", err)
	}

	summaryF, summaryPath, err := createCSV(*outDir, "cold-txhash"+suffix+"-sweep", sweepCSVHeader)
	if err != nil {
		fatal(logger, "%v", err)
	}
	defer summaryF.Close()

	printSweepHeader()

	// In evict mode the timed loop re-opens the MPHF per iter; release
	// the startup mmap (used above for coverage probing + hash sampling)
	// so the first iter's FADV_DONTNEED can actually drop the idx pages.
	// Close is idempotent, so the deferred Close above is a no-op.
	if *evictMPHF {
		if err := mph.Close(); err != nil {
			fatal(logger, "close startup mphf: %v", err)
		}
	}

	var csvMu sync.Mutex
	results := make([]concurrentResult, 0, len(workersList))
	for _, w := range workersList {
		hits := make([]time.Duration, 0, *iters)
		misses := make([]time.Duration, 0, *iters)
		op := coldTxHashOp(mph, *coldDir, *txColdMPHF, *evictMPHF, hashes, *missRate, *xdrViews, w, detailF, &csvMu, &hits, &misses)
		res := runConcurrentSweep(w, *iters, *seed, op)
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

// coldTxHashOp returns a per-iter closure: pick a hash (hit or miss),
// MPHF Lookup, evict the resolved chunk's pack, open a fresh
// ColdReader, fetch+scan+materialize, close, write CSV row.
func coldTxHashOp(
	mph *txhash.ColdReader,
	coldDir string,
	idxPath string,
	evictMPHF bool,
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

		// Cold-MPHF path: drop the index from the page cache, open a
		// fresh mmap (timed), and close it at iter end via defer. The
		// evict only drops pages because the prior iter already munmaped
		// (FADV_DONTNEED skips still-mapped pages), so close-per-iter is
		// what makes the eviction real. Single-worker only (enforced in
		// cmdColdTxHash).
		activeMph := mph
		var mphfOpenNs time.Duration
		if evictMPHF {
			if eerr := evictFile(idxPath); eerr != nil {
				return 0, fmt.Errorf("evict mphf %s: %w", idxPath, eerr)
			}
			tOpen := time.Now()
			local, oerr := txhash.OpenColdReader(idxPath)
			mphfOpenNs = time.Since(tOpen)
			if oerr != nil {
				return 0, fmt.Errorf("OpenColdReader mphf %s: %w", idxPath, oerr)
			}
			defer local.Close()
			activeMph = local
		}

		t0 := time.Now()
		seq, lerr := activeMph.Lookup(hash)
		lookupNs := time.Since(t0)

		isMiss := false
		var chunkID uint32
		var packOpenNs, fetchNs, scanNs, matNs time.Duration

		switch {
		case errors.Is(lerr, stores.ErrNotFound):
			isMiss = true
		case lerr != nil:
			return 0, fmt.Errorf("mph.Lookup: %w", lerr)
		default:
			chunkID = uint32(chunk.IDFromLedger(seq))
			packFile := packPath(coldDir, chunkID)
			if eerr := evictFile(packFile); eerr != nil {
				return 0, fmt.Errorf("evict %s: %w", packFile, eerr)
			}

			t1 := time.Now()
			cr, oerr := ledger.OpenColdReader(packFile)
			packOpenNs = time.Since(t1)
			if oerr != nil {
				return 0, fmt.Errorf("OpenColdReader %s: %w", packFile, oerr)
			}
			defer cr.Close()

			t2 := time.Now()
			raw, gerr := cr.GetLedgerRaw(seq)
			fetchNs = time.Since(t2)
			if gerr != nil {
				return 0, fmt.Errorf("GetLedgerRaw(%d): %w", seq, gerr)
			}

			if xdrViews {
				t3 := time.Now()
				applyIdx, ferr := findTxByHashView(raw, hash)
				scanNs = time.Since(t3)
				if ferr != nil {
					return 0, fmt.Errorf("findTxByHashView: %w", ferr)
				}
				if applyIdx < 0 {
					isMiss = true
				} else {
					t4 := time.Now()
					tx, merr := materializeViews(raw, applyIdx)
					matNs = time.Since(t4)
					if merr != nil {
						return 0, fmt.Errorf("materializeViews: %w", merr)
					}
					if tx.TransactionHash[:16] != hex.EncodeToString(hash[:8]) {
						return 0, fmt.Errorf("view hash mismatch: got %s want %x", tx.TransactionHash, hash[:8])
					}
				}
			} else {
				t3 := time.Now()
				var lcm goxdr.LedgerCloseMeta
				if uerr := lcm.UnmarshalBinary(raw); uerr != nil {
					return 0, fmt.Errorf("UnmarshalBinary: %w", uerr)
				}
				scanNs = time.Since(t3)

				t4 := time.Now()
				tx, _, merr := materializeRoundtripFromLCM(lcm, hash, pubnetPassphrase)
				matNs = time.Since(t4)
				if merr != nil {
					// Round-trip's "hash not found" signal lands as a
					// formatted error from materializeRoundtripFromLCM —
					// a fingerprint false positive ends up here.
					isMiss = true
				} else if tx.TransactionHash[:16] != hex.EncodeToString(hash[:8]) {
					return 0, fmt.Errorf("roundtrip hash mismatch: got %s want %x", tx.TransactionHash, hash[:8])
				}
			}
		}

		totalNs := mphfOpenNs + lookupNs + packOpenNs + fetchNs + scanNs + matNs
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
		_, werr := fmt.Fprintf(detailF, "%d,%d,%x,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
			workers, chunkID, hash[:8], seq, missFlag,
			mphfOpenNs.Nanoseconds(), lookupNs.Nanoseconds(), packOpenNs.Nanoseconds(),
			fetchNs.Nanoseconds(), scanNs.Nanoseconds(),
			matNs.Nanoseconds(), totalNs.Nanoseconds())
		csvMu.Unlock()
		if werr != nil {
			return totalNs, werr
		}
		return totalNs, nil
	}
}

// sampleHashesFromColdChunks calls sampleHashesFromCold for each
// chunk ID in the given list and returns the union of sampled hashes.
// Sampling is read-only and not part of the timed path.
func sampleHashesFromColdChunks(
	coldDir string,
	chunks []uint32,
	perChunkLedgers int,
	rng *rand.Rand,
) ([][32]byte, error) {
	var pool [][32]byte
	for _, c := range chunks {
		first := chunkFirstLedger(c)
		last := chunkLastLedger(c)
		hashes, err := sampleHashesFromCold(coldDir, c, first, last, perChunkLedgers, rng)
		if err != nil {
			return nil, fmt.Errorf("chunk %d: %w", c, err)
		}
		pool = append(pool, hashes...)
	}
	return pool, nil
}
