package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
)

// cmdHotEvents benches eventstore.Query against the hot tier with
// hot-tier methodology: one shared HotStore, --warmup queries per
// worker before timing. See bench_cold_events.go for the auto-corpus
// shape (scan-once → 15 high-volume terms → round-robin K-filter
// partition per iter).
//
// Per-iter CSV row (no open_ns — reader is shared):
//
//	workers,n_filters,n_unique_terms,query_ns,n_events,total_ns
//
// total_ns equals query_ns and is kept as a column for symmetry
// with the cold bench's CSV.
func cmdHotEvents() {
	fs := flag.NewFlagSet("hot-events", flag.ExitOnError)
	bucketsSpec := fs.String("buckets", "",
		"comma-separated K values (filters-per-request) for the corpus (default 1,2,3,5,8,12,15)")
	hotDir := fs.String("hot-dir",
		"/mnt/nvme/disk2/ledgers/events-hot", "hot eventstore dir")
	chunkN := fs.Uint("chunk", 5000, "chunk ID")
	iters := fs.Int("iters", 500, "number of timed iterations per worker")
	workersCSV := fs.String("query-concurrency", "1", "concurrent in-flight queries; comma-list sweep (e.g. 1,4,16)")
	warmup := fs.Int("warmup", hotWarmupSharedIters,
		"warm-up iterations per worker (not counted)")
	maxFetch := fs.Int("max-fetch", 1000,
		"MaxEvents (pagination limit) baked into each query")
	seed := fs.Int64("seed", 1, "RNG seed (drives corpus shuffle + K-bucket selection)")
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

	chunkID := chunk.ID(uint32(*chunkN))
	ctx := context.Background()

	buckets, err := parseBuckets(*bucketsSpec)
	if err != nil {
		fatal(logger, "parse -buckets: %v", err)
	}
	reader, oerr := eventstore.OpenHotStore(*hotDir, chunkID, logger,
		eventstore.WithXDRViews(*xdrViews))
	if oerr != nil {
		fatal(logger, "OpenHotStore: %v", oerr)
	}
	defer reader.Close()
	c, err := newCorpus(ctx, logger, reader, buckets, *maxFetch)
	if err != nil {
		fatal(logger, "corpus: %v", err)
	}
	logger.Infof("hot-events source=auto-corpus(chunk=%d,buckets=%s,seed=%d) iters=%d workers=%v warmup=%d xdr-views=%v",
		chunkID, intListString(buckets), *seed, *iters, workersList, *warmup, *xdrViews)

	csvName := "hot-events-query"
	if *xdrViews {
		csvName = "hot-events-query-xdrviews"
	}
	detailF, detailPath, err := createCSV(*outDir, csvName,
		"query_concurrency,n_filters,n_unique_terms,query_ns,n_events,total_ns")
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
		// Per-worker-count: collect per-K stats so the printed summary
		// can still report by request class. The closure appends to
		// byClass + allIters under csvMu.
		byClass := map[string][]time.Duration{}
		allIters := make([]time.Duration, 0, *iters)
		op := hotEventsOp(ctx, reader, c, w, detailF, &csvMu, byClass, &allIters)
		res := runConcurrentSweepWithWarmup(w, *warmup, *iters, *seed, op)
		printSweepRow(w, res, summaryF)
		fmt.Printf("  workers=%d:\n", w)
		printBenchStats(fmt.Sprintf("    hot-events-query w=%d", w), byClass, allIters)
		results = append(results, res)
	}
	reportSaturation(workersList, results)

	logger.Infof("wrote %s and %s", detailPath, summaryPath)
}

// hotEventsOp returns a per-iter closure that draws one request from
// the corpus using the worker's rng, runs the query against the
// shared HotStore, and writes one detail row. Per-K stats are
// recorded for the post-sweep summary print.
func hotEventsOp(
	ctx context.Context,
	reader *eventstore.HotStore,
	c *corpus,
	workers int,
	detailF *os.File,
	csvMu *sync.Mutex,
	byClass map[string][]time.Duration,
	allIters *[]time.Duration,
) iterOp {
	return func(rng *rand.Rand, measured bool) (time.Duration, error) {
		req := c.Next(rng)

		t0 := time.Now()
		out, qerr := eventstore.Query(ctx, reader, req.filters, req.opts)
		queryNs := time.Since(t0)
		if qerr != nil {
			return queryNs, qerr
		}

		if !measured {
			return queryNs, nil
		}
		csvMu.Lock()
		byClass[req.label] = append(byClass[req.label], queryNs)
		*allIters = append(*allIters, queryNs)
		_, werr := fmt.Fprintf(detailF, "%d,%d,%d,%d,%d,%d\n",
			workers, req.k, req.nUniqueTerms,
			queryNs.Nanoseconds(), len(out), queryNs.Nanoseconds())
		csvMu.Unlock()
		if werr != nil {
			return queryNs, werr
		}
		return queryNs, nil
	}
}
