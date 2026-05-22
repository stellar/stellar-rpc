package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
)

// cmdHotEvents benches eventstore.Query against the hot tier with
// hot-tier methodology: one shared HotStore, --warmup queries before
// timing. See bench_cold_events.go for the auto-corpus shape
// (scan-once → 15 high-volume terms → round-robin K-filter
// partition per iter).
//
// Per-iter CSV row (no open_ns — reader is shared):
//
//	n_filters,n_unique_terms,query_ns,n_events,total_ns
//
// total_ns equals query_ns and is kept as a column for symmetry
// with the cold bench's CSV.
//

func cmdHotEvents() {
	fs := flag.NewFlagSet("hot-events", flag.ExitOnError)
	bucketsSpec := fs.String("buckets", "",
		"comma-separated K values (filters-per-request) for the corpus (default 1,2,3,5,8,12,15)")
	hotDir := fs.String("hot-dir",
		"/mnt/nvme/disk2/ledgers/events-hot", "hot eventstore dir")
	chunkN := fs.Uint("chunk", 5000, "chunk ID")
	iters := fs.Int("iters", 500, "number of timed iterations")
	warmup := fs.Int("warmup", hotWarmupSharedIters,
		"warm-up iterations (not counted)")
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

	chunkID := chunk.ID(uint32(*chunkN))
	ctx := context.Background()

	// Single reader serves both corpus build and timed iterations.
	// The corpus picker (corpus.go::scanForTopTerms) is mode-agnostic
	// — it reads topics via xdr.ContractEventView when the payload
	// arrives as a view (useXDRViews=true) and via
	// ContractEvent.Body.V0.Topics otherwise. No second reader, no
	// double warmup.
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
	c, err := newCorpus(ctx, logger, reader, buckets, *maxFetch, *seed)
	if err != nil {
		fatal(logger, "corpus: %v", err)
	}
	logger.Infof("hot-events source=auto-corpus(chunk=%d,buckets=%s,seed=%d) iters=%d warmup=%d xdr-views=%v",
		chunkID, intListString(buckets), *seed, *iters, *warmup, *xdrViews)

	for i := range *warmup {
		req := c.Next()
		if _, werr := eventstore.Query(ctx, reader, req.filters, req.opts); werr != nil {
			fatal(logger, "warmup %d query %s: %v", i, req.label, werr)
		}
	}

	csvName := "hot-events-query"
	if *xdrViews {
		csvName = "hot-events-query-xdrviews"
	}
	csvF, csvPath, err := createCSV(*outDir, csvName,
		"n_filters,n_unique_terms,query_ns,n_events,total_ns")
	if err != nil {
		fatal(logger, "%v", err)
	}
	defer csvF.Close()

	byClass := map[string][]time.Duration{}
	allIters := make([]time.Duration, 0, *iters)

	for i := range *iters {
		req := c.Next()

		t0 := time.Now()
		out, qerr := eventstore.Query(ctx, reader, req.filters, req.opts)
		queryNs := time.Since(t0)
		if qerr != nil {
			fatal(logger, "iter %d query %s: %v", i, req.label, qerr)
		}

		byClass[req.label] = append(byClass[req.label], queryNs)
		allIters = append(allIters, queryNs)

		if _, err := fmt.Fprintf(csvF, "%d,%d,%d,%d,%d\n",
			req.k, req.nUniqueTerms,
			queryNs.Nanoseconds(),
			len(out), queryNs.Nanoseconds()); err != nil {
			fatal(logger, "iter %d write CSV: %v", i, err)
		}
	}

	printBenchStats("hot-events-query", byClass, allIters)
	logger.Infof("wrote %s", csvPath)
}
