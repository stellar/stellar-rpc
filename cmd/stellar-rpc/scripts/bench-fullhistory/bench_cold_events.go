package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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
// Request source has two modes:
//   - Default (auto-corpus): a one-shot scan picks 15 high-volume
//     terms from the chunk (3 contracts × top 3 topic values per
//     position); each iter generates a fresh K-filter partition via
//     corpus.Next. See corpus.go.
//   - Override (-queries <file>): hand-authored JSON corpus; each
//     iter draws a request uniformly. See query_corpus.go.
//
// Per-iter CSV row:
//
//	n_filters       K (filters per request)
//	n_unique_terms  unique-term count after dedupe (≤15, max-cost = 15)
//	open_ns         OpenColdReader
//	query_ns        eventstore.Query end-to-end
//	n_events        events returned (capped by MaxEvents)
//	total_ns        open_ns + query_ns
//
// Per-class and aggregate latency stats are printed at the end.
//
//nolint:funlen // sequential pipeline: parse flags → set up source → per-iter loop → stats
func cmdColdEvents() {
	fs := flag.NewFlagSet("cold-events", flag.ExitOnError)
	queriesPath := fs.String("queries", "",
		"JSON queries file (optional; default auto-corpus from -chunk + -seed)")
	bucketsSpec := fs.String("buckets", "",
		"comma-separated K values for auto-corpus (default 1,2,3,5,8,12,15)")
	coldDir := fs.String("cold-events-dir",
		"/mnt/nvme/disk2/ledgers/events-cold", "cold eventstore bucket dir")
	chunkN := fs.Uint("chunk", 5000, "chunk ID")
	iters := fs.Int("iters", 500, "number of timed iterations")
	maxFetch := fs.Int("max-fetch", 1000,
		"MaxEvents (pagination limit) baked into each query")
	seed := fs.Int64("seed", 1, "RNG seed (drives query selection / corpus generation)")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	chunkID := chunk.ID(uint32(*chunkN))
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(uint64(*seed), uint64(*seed*7919))) //nolint:gosec

	nextRequest, sourceLabel := newColdRequestSource(
		ctx, logger, *queriesPath, *bucketsSpec, chunkID, *coldDir, *maxFetch, *seed, rng,
	)
	logger.Infof("cold-events source=%s chunk=%d iters=%d", sourceLabel, chunkID, *iters)

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal(logger, "mkdir %s: %v", *outDir, err)
	}
	csvPath := filepath.Join(*outDir, "cold-events-query.csv")
	csvF, err := os.Create(csvPath) //nolint:gosec // bench output
	if err != nil {
		fatal(logger, "create CSV %s: %v", csvPath, err)
	}
	defer csvF.Close()
	if _, err := fmt.Fprintln(csvF,
		"n_filters,n_unique_terms,open_ns,query_ns,n_events,total_ns"); err != nil {
		fatal(logger, "write CSV header: %v", err)
	}

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

		req := nextRequest()

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

// newColdRequestSource initialises either an auto-corpus or
// JSON-corpus request source for the cold-events bench, depending
// on whether -queries was set. Returns the per-iter request
// generator and a label identifying the source (logged once at
// startup for run-traceability).
func newColdRequestSource(
	ctx context.Context, logger *supportlog.Entry,
	queriesPath, bucketsSpec string,
	chunkID chunk.ID, coldDir string, maxFetch int, seed int64,
	rng *rand.Rand,
) (func() generatedRequest, string) {
	if queriesPath != "" {
		return newJSONRequestSource(logger, queriesPath, maxFetch, rng), "json:" + queriesPath
	}
	buckets, err := parseBuckets(bucketsSpec)
	if err != nil {
		fatal(logger, "parse -buckets: %v", err)
	}
	// One-shot scan reader: opened only to feed the picker, then
	// closed. The bench loop's per-iter eviction handles cold-cache
	// state regardless of what the scan touched.
	scanReader, err := eventstore.OpenColdReader(chunkID, coldDir, eventstore.ColdReaderOptions{})
	if err != nil {
		fatal(logger, "open scan reader: %v", err)
	}
	c, err := newCorpus(ctx, logger, scanReader, buckets, maxFetch, seed)
	scanReader.Close()
	if err != nil {
		fatal(logger, "corpus: %v", err)
	}
	return c.Next, fmt.Sprintf("auto-corpus(chunk=%d,buckets=%s,seed=%d)",
		chunkID, intListString(buckets), seed)
}

// newJSONRequestSource wraps the JSON-loaded queries[] as a
// generatedRequest source. Per-iter unique-term count is computed
// at queryPrep time and reused here so the CSV's n_unique_terms
// column stays meaningful on the JSON path.
func newJSONRequestSource(
	logger *supportlog.Entry, queriesPath string, maxFetch int, rng *rand.Rand,
) func() generatedRequest {
	reqs, err := loadQueries(queriesPath)
	if err != nil {
		fatal(logger, "load queries: %v", err)
	}
	queries, err := prepareQueries(reqs, maxFetch)
	if err != nil {
		fatal(logger, "prepare queries: %v", err)
	}
	return func() generatedRequest {
		q := &queries[rng.IntN(len(queries))]
		return generatedRequest{
			filters:      q.filters,
			opts:         q.opts,
			k:            len(q.filters),
			nUniqueTerms: q.nUniqueTerms,
			label:        q.label(),
		}
	}
}

// printBenchStats emits per-class and aggregate latency lines.
// byClass groups iters by source-defined demux key (K-bucket for
// auto-corpus, JSON-entry label for -queries). Empty buckets are
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

// intListString formats a []int as "1,2,3" without the default
// `%v` slice formatting's space-separated brackets. Used in source
// labels logged at startup.
func intListString(xs []int) string {
	parts := make([]string, len(xs))
	for i, x := range xs {
		parts[i] = strconv.Itoa(x)
	}
	return "[" + strings.Join(parts, ",") + "]"
}
