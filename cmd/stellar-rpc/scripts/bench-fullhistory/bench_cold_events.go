package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
)

// cmdColdEvents benches eventstore.Query against the cold tier with
// cold-cache methodology. Multi-chunk: at startup every chunk
// discovered under --cold-events-dir gets its own corpus (built from
// that chunk's events). Per iter, a chunk is picked at random, its
// three pack files (events.pack + index.pack + index.hash) are
// evicted from the OS page cache, a fresh ColdReader is opened, one
// query is dispatched, and the reader is closed.
//
// Per-iter CSV row:
//
//	workers         worker-count cell this iter belongs to
//	chunk           ID randomly picked for this iter
//	n_filters       K (filters per request)
//	n_unique_terms  unique-term count after partition (≤15)
//	open_ns         OpenColdReader
//	query_ns        eventstore.Query end-to-end
//	n_events        events returned (capped by MaxEvents)
//	total_ns        open_ns + query_ns
func cmdColdEvents() {
	fs := flag.NewFlagSet("cold-events", flag.ExitOnError)
	bucketsSpec := fs.String("buckets", "",
		"comma-separated K values (filters-per-request) for the corpus (default 1,2,3,5,8,12,15)")
	coldDir := fs.String("cold-events-dir",
		"/mnt/nvme/disk2/ledgers/events-cold", "cold eventstore bucket dir")
	flagLo := fs.Uint("chunk-lo", 0, "inclusive lower chunk ID (0 = auto-discover from --cold-events-dir; set with --chunk-hi to constrain)")
	flagHi := fs.Uint("chunk-hi", 0, "inclusive upper chunk ID (0 = auto-discover; set with --chunk-lo to constrain)")
	iters := fs.Int("iters", 500, "number of timed iterations per worker")
	workersCSV := fs.String("query-concurrency", "1", "concurrent in-flight queries; comma-list sweep (e.g. 1,4,16)")
	maxFetch := fs.Int("max-fetch", 1000,
		"MaxEvents (pagination limit) baked into each query")
	seed := fs.Int64("seed", 1, "RNG seed (drives chunk shuffle + per-chunk corpus shuffle)")
	xdrViews := fs.Bool("xdr-views", false,
		"Skip ContractEvent.UnmarshalBinary in FetchEvents; alias raw bytes into Payload.ContractEventBytes "+
			"and run the post-filter via xdr.ContractEventView. Symmetric to the ingest --xdr-views flag.")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	workersList, err := parseIntList(*workersCSV)
	if err != nil {
		fatal(logger, "parse --query-concurrency: %v", err)
	}
	validateWorkersList(logger, workersList)

	buckets, err := parseBuckets(*bucketsSpec)
	if err != nil {
		fatal(logger, "parse -buckets: %v", err)
	}

	ctx := context.Background()
	corpora := buildColdEventsCorpora(ctx, logger, *coldDir, buckets, *maxFetch,
		uint32(*flagLo), uint32(*flagHi))
	if len(corpora) == 0 {
		fatal(logger, "no usable chunks under %s", *coldDir)
	}
	logger.Infof("cold-events chunks=%d buckets=%s iters=%d workers=%v xdr-views=%v",
		len(corpora), intListString(buckets), *iters, workersList, *xdrViews)

	csvName := "cold-events-query"
	if *xdrViews {
		csvName = "cold-events-query-xdrviews"
	}
	detailF, detailPath, err := createCSV(*outDir, csvName,
		"query_concurrency,chunk,n_filters,n_unique_terms,open_ns,query_ns,n_events,total_ns")
	if err != nil {
		fatal(logger, "%v", err)
	}
	defer detailF.Close()

	summaryF, summaryPath, err := createCSV(*outDir, csvName+"-sweep", sweepCSVHeader)
	if err != nil {
		fatal(logger, "%v", err)
	}
	defer summaryF.Close()

	printSweepHeader()

	var csvMu sync.Mutex
	results := make([]concurrentResult, 0, len(workersList))
	for _, w := range workersList {
		byClass := map[string][]time.Duration{}
		allIters := make([]time.Duration, 0, *iters)
		op := coldEventsOp(ctx, *coldDir, corpora, *xdrViews, w, detailF, &csvMu, byClass, &allIters)
		res := runConcurrentSweep(w, *iters, *seed, op)
		printSweepRow(w, res, summaryF)
		fmt.Printf("  workers=%d:\n", w)
		printBenchStats(fmt.Sprintf("    cold-events-query w=%d", w), byClass, allIters)
		results = append(results, res)
	}
	reportSaturation(workersList, results)

	logger.Infof("wrote %s and %s", detailPath, summaryPath)
}

// chunkCorpus pairs a chunk ID with its built corpus and its set of
// per-chunk eviction targets (events.pack + index.pack + index.hash).
type chunkCorpus struct {
	chunkID      chunk.ID
	corpus       *corpus
	evictTargets []string
}

// buildColdEventsCorpora discovers chunks under coldDir whose events
// files are present, opens each in turn to build its corpus, closes
// the reader, and returns the per-chunk corpora. Runs synchronously
// at startup and is not part of the timed loop.
//
// If flagLo and flagHi are both zero, every chunk discovered in the
// directory is used. Otherwise both must be set and the chunk set is
// restricted to [flagLo, flagHi]; chunks in that range whose files
// are missing fatal.
func buildColdEventsCorpora(
	ctx context.Context, logger *supportlog.Entry,
	coldDir string, buckets []int, maxFetch int,
	flagLo, flagHi uint32,
) []chunkCorpus {
	if (flagLo == 0) != (flagHi == 0) {
		fatal(logger, "--chunk-lo and --chunk-hi must both be set, or neither")
	}
	var chunkIDs []chunk.ID
	if flagLo > 0 {
		if flagHi < flagLo {
			fatal(logger, "--chunk-hi=%d < --chunk-lo=%d", flagHi, flagLo)
		}
		for c := flagLo; c <= flagHi; c++ {
			cid := chunk.ID(c)
			// All three files are required — eventstore.OpenColdReader
			// fails if any is missing, and we want the explicit-range
			// path to surface that as a fatal here rather than as a
			// per-chunk Warnf-skip later (which the discovery path uses).
			for _, name := range []string{
				eventstore.EventsPackName(cid),
				eventstore.IndexPackName(cid),
				eventstore.IndexHashName(cid),
			} {
				p := filepath.Join(coldDir, name)
				if _, err := os.Stat(p); err != nil {
					fatal(logger, "missing %s for chunk %d: %v", name, c, err)
				}
			}
			chunkIDs = append(chunkIDs, cid)
		}
	} else {
		// Discovery: a chunk is "usable" if all three files exist.
		matches, err := filepath.Glob(filepath.Join(coldDir, "*events*.pack"))
		if err != nil {
			fatal(logger, "glob %s: %v", coldDir, err)
		}
		seen := map[uint32]struct{}{}
		for _, m := range matches {
			base := filepath.Base(m)
			// eventstore.EventsPackName is "<chunkID:08d>-events.pack" —
			// require exactly 8 decimal digits followed by "-events.pack"
			// so a name like "5000-events.pack" (truncated chunk-id) is
			// rejected rather than silently parsed as 5000.
			if !strings.HasSuffix(base, "-events.pack") || len(base) < 8 {
				continue
			}
			id, perr := strconv.ParseUint(base[:8], 10, 32)
			if perr != nil {
				continue
			}
			if base[8] != '-' {
				continue
			}
			if _, dup := seen[uint32(id)]; dup {
				continue
			}
			seen[uint32(id)] = struct{}{}
			chunkIDs = append(chunkIDs, chunk.ID(id))
		}
		slices.Sort(chunkIDs)
	}

	logger.Infof("cold-events: discovered %d candidate chunks under %s, building corpora...",
		len(chunkIDs), coldDir)
	out := make([]chunkCorpus, 0, len(chunkIDs))
	for _, cid := range chunkIDs {
		scanReader, err := eventstore.OpenColdReader(cid, coldDir, eventstore.ColdReaderOptions{})
		if err != nil {
			logger.Warnf("skip chunk %d: open: %v", cid, err)
			continue
		}
		c, err := newCorpus(ctx, logger, scanReader, buckets, maxFetch)
		scanReader.Close()
		if err != nil {
			logger.Warnf("skip chunk %d: corpus: %v", cid, err)
			continue
		}
		out = append(out, chunkCorpus{
			chunkID: cid,
			corpus:  c,
			evictTargets: []string{
				filepath.Join(coldDir, eventstore.EventsPackName(cid)),
				filepath.Join(coldDir, eventstore.IndexPackName(cid)),
				filepath.Join(coldDir, eventstore.IndexHashName(cid)),
			},
		})
	}
	return out
}

// coldEventsOp returns a per-iter closure: pick a random chunk, evict
// its three event files, open a fresh ColdReader, draw a request
// from that chunk's corpus, query, close, write CSV row.
func coldEventsOp(
	ctx context.Context,
	coldDir string,
	corpora []chunkCorpus,
	xdrViews bool,
	workers int,
	detailF *os.File,
	csvMu *sync.Mutex,
	byClass map[string][]time.Duration,
	allIters *[]time.Duration,
) iterOp {
	return func(rng *rand.Rand, measured bool) (time.Duration, error) {
		cc := corpora[rng.IntN(len(corpora))]
		for _, p := range cc.evictTargets {
			if err := evictFile(p); err != nil {
				return 0, fmt.Errorf("evict %s: %w", p, err)
			}
		}

		req := cc.corpus.Next(rng)

		t0 := time.Now()
		reader, oerr := eventstore.OpenColdReader(cc.chunkID, coldDir,
			eventstore.ColdReaderOptions{UseXDRViews: xdrViews})
		openNs := time.Since(t0)
		if oerr != nil {
			return 0, fmt.Errorf("OpenColdReader chunk=%d: %w", cc.chunkID, oerr)
		}
		defer reader.Close()

		t1 := time.Now()
		out, qerr := eventstore.Query(ctx, reader, req.filters, req.opts)
		queryNs := time.Since(t1)
		if qerr != nil {
			return 0, fmt.Errorf("query %s chunk=%d: %w", req.label, cc.chunkID, qerr)
		}
		totalNs := openNs + queryNs

		if !measured {
			return totalNs, nil
		}
		csvMu.Lock()
		byClass[req.label] = append(byClass[req.label], totalNs)
		*allIters = append(*allIters, totalNs)
		_, werr := fmt.Fprintf(detailF, "%d,%d,%d,%d,%d,%d,%d,%d\n",
			workers, cc.chunkID,
			req.k, req.nUniqueTerms,
			openNs.Nanoseconds(), queryNs.Nanoseconds(),
			len(out), totalNs.Nanoseconds())
		csvMu.Unlock()
		if werr != nil {
			return totalNs, werr
		}
		return totalNs, nil
	}
}

// printBenchStats emits per-K and aggregate latency lines, grouped
// by the corpus iterator's per-iter K label. Empty buckets are
// skipped; the aggregate always prints. latencyStats.line omits
// ops/s on purpose — these are sub-summary breakdowns of a
// concurrent sweep cell, and the cell-level wall-clock ops/s is
// printed separately by printSweepRow.
func printBenchStats(
	label string, byClass map[string][]time.Duration, allIters []time.Duration,
) {
	keys := make([]string, 0, len(byClass))
	for k := range byClass {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Println("# Per-class")
	for _, k := range keys {
		fmt.Println(computeStats(byClass[k]).line(label + k))
	}
	fmt.Println("# Aggregate")
	fmt.Println(computeStats(allIters).line(label))
}
