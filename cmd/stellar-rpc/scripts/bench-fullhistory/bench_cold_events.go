package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
)

// cmdColdEvents benches eventstore.Query against the cold tier with
// cold-cache methodology: per iter the chunk's three pack files
// (events.pack + index.pack + index.hash) are evicted from the OS
// page cache, a fresh ColdReader is opened, one query is dispatched,
// and the reader is closed.
//
// The request corpus is auto-generated from (chunk, seed): a one-shot
// scan picks 15 high-volume terms (3 contracts + top 12 topic values
// from their 4-topic events), and each iter shuffles them into a
// K-filter partition via corpus.Next (see corpus.go). Reproducible
// from (chunk, seed); no input files required.
//
// Per-iter CSV row:
//
//	n_filters       K (filters per request)
//	n_unique_terms  unique-term count after partition (≤15; max = 15)
//	open_ns         OpenColdReader
//	query_ns        eventstore.Query end-to-end
//	n_events        events returned (capped by MaxEvents)
//	total_ns        open_ns + query_ns
//
// Per-class and aggregate latency stats are printed at the end.
//

func cmdColdEvents() {
	fs := flag.NewFlagSet("cold-events", flag.ExitOnError)
	bucketsSpec := fs.String("buckets", "",
		"comma-separated K values (filters-per-request) for the corpus (default 1,2,3,5,8,12,15)")
	coldDir := fs.String("cold-events-dir",
		"/mnt/nvme/disk2/ledgers/events-cold", "cold eventstore bucket dir")
	chunkN := fs.Uint("chunk", 5000, "chunk ID")
	iters := fs.Int("iters", 500, "number of timed iterations")
	maxFetch := fs.Int("max-fetch", 1000,
		"MaxEvents (pagination limit) baked into each query")
	seed := fs.Int64("seed", 1, "RNG seed (drives corpus shuffle + K-bucket selection)")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	chunkID := chunk.ID(uint32(*chunkN))
	ctx := context.Background()

	c, sourceLabel := newColdEventsCorpus(ctx, logger, *bucketsSpec, chunkID, *coldDir, *maxFetch, *seed)
	logger.Infof("cold-events source=%s chunk=%d iters=%d", sourceLabel, chunkID, *iters)

	csvF, csvPath, err := createCSV(*outDir, "cold-events-query",
		"n_filters,n_unique_terms,open_ns,query_ns,n_events,total_ns")
	if err != nil {
		fatal(logger, "%v", err)
	}
	defer csvF.Close()

	evictTargets := []string{
		filepath.Join(*coldDir, eventstore.EventsPackName(chunkID)),
		filepath.Join(*coldDir, eventstore.IndexPackName(chunkID)),
		filepath.Join(*coldDir, eventstore.IndexHashName(chunkID)),
	}

	byClass := map[string][]time.Duration{}
	allIters := make([]time.Duration, 0, *iters)

	for i := range *iters {
		for _, p := range evictTargets {
			if err := evictFile(p); err != nil {
				fatal(logger, "iter %d evict %s: %v", i, p, err)
			}
		}

		req := c.Next()

		t0 := time.Now()
		reader, oerr := eventstore.OpenColdReader(chunkID, *coldDir, eventstore.ColdReaderOptions{})
		openNs := time.Since(t0)
		if oerr != nil {
			fatal(logger, "iter %d OpenColdReader: %v", i, oerr)
		}

		t1 := time.Now()
		out, qerr := eventstore.Query(ctx, reader, req.filters, req.opts)
		queryNs := time.Since(t1)
		reader.Close()
		if qerr != nil {
			fatal(logger, "iter %d query %s: %v", i, req.label, qerr)
		}

		totalNs := openNs + queryNs
		byClass[req.label] = append(byClass[req.label], totalNs)
		allIters = append(allIters, totalNs)

		if _, err := fmt.Fprintf(csvF, "%d,%d,%d,%d,%d,%d\n",
			req.k, req.nUniqueTerms,
			openNs.Nanoseconds(), queryNs.Nanoseconds(),
			len(out), totalNs.Nanoseconds()); err != nil {
			fatal(logger, "iter %d write CSV: %v", i, err)
		}
	}

	printBenchStats("cold-events-query", byClass, allIters)
	logger.Infof("wrote %s", csvPath)
}

// newColdEventsCorpus opens a one-shot ColdReader to scan the chunk,
// picks the term universe, and returns a corpus iterator plus a
// human-readable source label (logged once at startup for
// run-traceability). The scanner is closed before the bench's
// per-iter eviction loop starts; chunk constants survive in the
// returned corpus.
func newColdEventsCorpus(
	ctx context.Context, logger *supportlog.Entry, bucketsSpec string,
	chunkID chunk.ID, coldDir string, maxFetch int, seed int64,
) (*corpus, string) {
	buckets, err := parseBuckets(bucketsSpec)
	if err != nil {
		fatal(logger, "parse -buckets: %v", err)
	}
	scanReader, err := eventstore.OpenColdReader(chunkID, coldDir, eventstore.ColdReaderOptions{})
	if err != nil {
		fatal(logger, "open scan reader: %v", err)
	}
	c, err := newCorpus(ctx, logger, scanReader, buckets, maxFetch, seed)
	scanReader.Close()
	if err != nil {
		fatal(logger, "corpus: %v", err)
	}
	return c, fmt.Sprintf("auto-corpus(chunk=%d,buckets=%s,seed=%d)",
		chunkID, intListString(buckets), seed)
}

// printBenchStats emits per-K and aggregate latency lines, grouped
// by the corpus iterator's per-iter K label. Empty buckets are
// skipped; the aggregate always prints.
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
