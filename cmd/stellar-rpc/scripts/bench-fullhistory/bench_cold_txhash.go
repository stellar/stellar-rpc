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
// cold tier. Multi-chunk: hashes are sampled at startup from every
// chunk discovered under --cold-dir, the MPHF is opened once at
// startup and kept warm across all iters and workers, and per-iter
// eviction targets only the chunk pack the lookup resolves to.
//
// What this does NOT measure: MPHF cold-fault latency. The mmap is
// opened once before timing starts and stays page-cache resident
// for the duration of the run, on every code path (workers=1 and
// workers>1 alike). If you need MPHF cold-fault numbers, use a
// separate single-shot measurement that evicts + re-opens per call.
//
// Per-iter CSV columns:
//
//	workers        worker-count cell this iter belongs to
//	chunk          ID the looked-up hash resolved to
//	hash           first 8 bytes of looked-up hash
//	seq            ledger seq the MPHF returned (0 on miss)
//	is_miss        1 if MPHF returned ErrNotFound or fingerprint FP
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
	flagLo := fs.Uint("chunk-lo", 0, "inclusive lower chunk ID (0 = auto-discover, then probe-narrow to MPHF coverage)")
	flagHi := fs.Uint("chunk-hi", 0, "inclusive upper chunk ID (0 = auto-discover, then probe-narrow to MPHF coverage)")
	probeHashes := fs.Int("mphf-probe-hashes", 32,
		"number of hashes per chunk sampled at startup to detect MPHF coverage. The MPHF's UserMetadata only "+
			"embeds MinLedger, not the upper bound — to know which chunks the MPHF actually covers, we sample "+
			"hashes from each chunk's pack and check whether mph.Lookup resolves any of them back to a seq in "+
			"that chunk. A probe sampled from an UNCOVERED chunk can still fingerprint-FP into the covered "+
			"set: per-probe FP rate is ~1/256 (1-byte fingerprint), and the FP-lands-in-this-chunk rate is "+
			"~LedgersPerChunk/totalMPHFLedgers. For a 10-chunk MPHF that's ~4e-4 per probe, so any-probe-FP "+
			"per chunk is 1-(1-4e-4)^N. With N=32 the false-coverage rate per uncovered chunk is ~1.3% — "+
			"acceptable; raise this flag if your MPHF covers far fewer chunks (per-probe FP rate scales up).")
	txColdMPHF := fs.String("txhash-cold-mphf", "/mnt/nvme/disk2/ledgers/txhash-cold/00005000.idx",
		"cold txhash streamhash MPHF .idx (opened once, kept warm across workers)")
	iters := fs.Int("iters", 1000, "number of timed lookups per worker")
	workersCSV := fs.String("workers", "1", "parallel workers; comma-list (e.g. 1,4,16)")
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
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	if *missRate < 0 || *missRate > 1 {
		fatal(logger, "--miss-rate=%v out of range [0,1]", *missRate)
	}
	workersList, err := parseIntList(*workersCSV)
	if err != nil {
		fatal(logger, "parse --workers: %v", err)
	}
	validateWorkersList(logger, workersList)

	chunkLo, chunkHi, err := resolveLedgerChunkRange(*coldDir, uint32(*flagLo), uint32(*flagHi))
	if err != nil {
		fatal(logger, "resolve chunk range in %s: %v", *coldDir, err)
	}

	// Open the MPHF once. Pages stay warm across workers; per-iter
	// eviction of the MPHF was meaningful single-threaded but is
	// fundamentally racy under workers>1.
	mph, err := txhash.OpenColdReader(*txColdMPHF)
	if err != nil {
		fatal(logger, "OpenColdReader %s: %v", *txColdMPHF, err)
	}
	defer mph.Close()

	// Probe-narrow the chunk range to what the MPHF actually covers.
	// Sampling hashes from chunks the MPHF doesn't cover would corrupt
	// the hit/miss distribution: every "hit" lookup from an uncovered
	// chunk either misses outright or resolves to a fingerprint false
	// positive, neither of which represents real bench latency.
	sampleRNG := rand.New(rand.NewPCG(uint64(*seed), uint64(*seed*7919)))
	covered, dropped, err := probeMPHFCoverage(logger, *coldDir, mph, chunkLo, chunkHi, *probeHashes, sampleRNG)
	if err != nil {
		fatal(logger, "probe MPHF coverage: %v", err)
	}
	if len(covered) == 0 {
		fatal(logger, "MPHF %s covers none of the chunks in [%d, %d] — wrong --txhash-cold-mphf or wrong chunk range?",
			*txColdMPHF, chunkLo, chunkHi)
	}
	if dropped > 0 {
		logger.Warnf("MPHF coverage probe: dropped %d chunks not covered by %s (use --chunk-lo/--chunk-hi to silence)",
			dropped, *txColdMPHF)
	}

	hashes, err := sampleHashesFromColdChunks(*coldDir, covered, *sampleLedgers, sampleRNG)
	if err != nil {
		fatal(logger, "sample hashes: %v", err)
	}
	if len(hashes) == 0 {
		fatal(logger, "no hashes sampled (chunks have no tx?)")
	}
	logger.Infof("cold-txhash mphf-covers=%d-chunks iters=%d workers=%v sampled %d hashes xdr-views=%v miss-rate=%.3f",
		len(covered), *iters, workersList, len(hashes), *xdrViews, *missRate)

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
		"workers,chunk,hash,seq,is_miss,lookup_ns,pack_open_ns,fetch_ns,scan_ns,materialize_ns,total_ns"); err != nil {
		fatal(logger, "write CSV header: %v", err)
	}

	summaryF, summaryPath, err := createCSV(*outDir, "cold-txhash"+suffix+"-sweep", sweepCSVHeader)
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
		op := coldTxHashOp(mph, *coldDir, hashes, *missRate, *xdrViews, w, detailF, &csvMu, &hits, &misses)
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

		t0 := time.Now()
		seq, lerr := mph.Lookup(hash)
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

		totalNs := lookupNs + packOpenNs + fetchNs + scanNs + matNs
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
		_, werr := fmt.Fprintf(detailF, "%d,%d,%x,%d,%d,%d,%d,%d,%d,%d,%d\n",
			workers, chunkID, hash[:8], seq, missFlag,
			lookupNs.Nanoseconds(), packOpenNs.Nanoseconds(),
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

// probeMPHFCoverage walks [lo, hi] and returns the subset of chunks
// the MPHF actually covers. For each chunk it samples `probeCount`
// hashes from the pack, looks each up against the MPHF, and counts
// the chunk as covered iff ANY probe resolves back to a ledger seq
// inside the chunk.
//
// Per-probe FP rate (probing a hash NOT in the MPHF's build set, but
// the FP-resolved seq landing inside the chunk we're probing) is
// ~(1/256) × (LedgersPerChunk / totalMPHFLedgers). For an N-chunk
// MPHF this is ~1/(256N) per probe. With K probes the per-chunk
// false-coverage rate is 1 - (1 - 1/(256N))^K ≈ K/(256N) for small
// K. With default K=32 and N=10 chunks, ~1.3% per uncovered chunk —
// acceptable; raise probeCount if the MPHF is small (low N).
//
// Returns the covered chunk IDs (in ascending order) and a count of
// dropped chunks.
func probeMPHFCoverage(
	logger *supportlog.Entry,
	coldDir string,
	mph *txhash.ColdReader,
	lo, hi uint32,
	probeCount int,
	rng *rand.Rand,
) ([]uint32, int, error) {
	// probeSampleLedgers — read this many random ledgers from each
	// chunk to assemble a candidate hash pool, then slice to
	// probeCount hashes. 3 ledgers × ~hundreds of tx is plenty even
	// when individual ledgers are sparse; the slice cap is what the
	// math comment assumes K is.
	const probeSampleLedgers = 3

	covered := make([]uint32, 0, hi-lo+1)
	dropped := 0
	for c := lo; c <= hi; c++ {
		first := chunkFirstLedger(c)
		last := chunkLastLedger(c)
		candidates, err := sampleHashesFromCold(coldDir, c, first, last, probeSampleLedgers, rng)
		if err != nil {
			return nil, 0, fmt.Errorf("probe chunk %d: %w", c, err)
		}
		if len(candidates) == 0 {
			logger.Debugf("chunk %d: no probe hashes available, treating as uncovered", c)
			dropped++
			continue
		}
		if len(candidates) > probeCount {
			candidates = candidates[:probeCount]
		}
		isCovered := false
		for _, h := range candidates {
			seq, lerr := mph.Lookup(h)
			if lerr != nil {
				continue
			}
			if seq >= first && seq <= last {
				isCovered = true
				break
			}
		}
		if isCovered {
			covered = append(covered, c)
		} else {
			dropped++
		}
	}
	return covered, dropped, nil
}
