package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
)

// cmdColdEvents benches eventstore.Query against the cold tier with
// cold-cache methodology: per iter the chunk's three pack files
// (events.pack + index.pack + index.hash) are evicted from the OS
// page cache, a fresh ColdReader is opened, one query from the JSON
// corpus is dispatched, and the reader is closed.
//
// Per-iter CSV row:
//
//	query_idx   index into the JSON corpus (CSV demux key)
//	n_filters   number of filters in this query
//	open_ns     OpenColdReader
//	query_ns    eventstore.Query end-to-end (Lookup+intersect+fetch)
//	n_events    events returned
//	total_ns    open_ns + query_ns
//
// Per-query and aggregate latency stats are printed at the end.
//
//nolint:funlen // sequential pipeline: parse flags → load queries → per-iter loop → stats
func cmdColdEvents() {
	fs := flag.NewFlagSet("cold-events", flag.ExitOnError)
	queriesPath := fs.String("queries", "",
		"JSON queries file (required); see query_corpus.go for shape")
	coldDir := fs.String("cold-events-dir",
		"/mnt/nvme/disk2/ledgers/events-cold", "cold eventstore bucket dir")
	chunkN := fs.Uint("chunk", 5000, "chunk ID")
	iters := fs.Int("iters", 500, "number of timed iterations")
	maxFetch := fs.Int("max-fetch", 1000,
		"default MaxEvents when a JSON query omits maxEvents")
	seed := fs.Int64("seed", 1, "RNG seed (drives random query selection)")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	if *queriesPath == "" {
		fatal(logger, "-queries is required")
	}

	chunkID := chunk.ID(uint32(*chunkN))

	reqs, err := loadQueries(*queriesPath)
	if err != nil {
		fatal(logger, "load queries: %v", err)
	}
	queries, err := prepareQueries(reqs, *maxFetch)
	if err != nil {
		fatal(logger, "prepare queries: %v", err)
	}
	logger.Infof("cold-events queries=%d chunk=%d iters=%d", len(queries), chunkID, *iters)

	rng := rand.New(rand.NewPCG(uint64(*seed), uint64(*seed*7919))) //nolint:gosec
	ctx := context.Background()

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal(logger, "mkdir %s: %v", *outDir, err)
	}
	csvPath := filepath.Join(*outDir, "cold-events-query.csv")
	csvF, err := os.Create(csvPath)
	if err != nil {
		fatal(logger, "create CSV %s: %v", csvPath, err)
	}
	defer csvF.Close()
	if _, err := fmt.Fprintln(csvF,
		"query_idx,n_filters,open_ns,query_ns,n_events,total_ns"); err != nil {
		fatal(logger, "write CSV header: %v", err)
	}

	// Per-iter eviction targets: the three files OpenColdReader opens.
	evictTargets := []string{
		filepath.Join(*coldDir, eventstore.EventsPackName(chunkID)),
		filepath.Join(*coldDir, eventstore.IndexPackName(chunkID)),
		filepath.Join(*coldDir, eventstore.IndexHashName(chunkID)),
	}

	perQuery := make(map[int][]time.Duration, len(queries))
	allIters := make([]time.Duration, 0, *iters)

	for i := range *iters {
		for _, p := range evictTargets {
			if err := evictFile(p); err != nil {
				fatal(logger, "iter %d evict %s: %v", i, p, err)
			}
		}

		q := &queries[rng.IntN(len(queries))]

		t0 := time.Now()
		reader, oerr := eventstore.OpenColdReader(chunkID, *coldDir, eventstore.ColdReaderOptions{})
		openNs := time.Since(t0)
		if oerr != nil {
			fatal(logger, "iter %d OpenColdReader: %v", i, oerr)
		}

		t1 := time.Now()
		out, qerr := eventstore.Query(ctx, reader, q.filters, q.opts)
		queryNs := time.Since(t1)
		reader.Close()
		if qerr != nil {
			fatal(logger, "iter %d query%s: %v", i, q.label(), qerr)
		}

		totalNs := openNs + queryNs
		perQuery[q.idx] = append(perQuery[q.idx], totalNs)
		allIters = append(allIters, totalNs)

		if _, err := fmt.Fprintf(csvF, "%d,%d,%d,%d,%d,%d\n",
			q.idx, len(q.filters),
			openNs.Nanoseconds(), queryNs.Nanoseconds(),
			len(out), totalNs.Nanoseconds()); err != nil {
			fatal(logger, "iter %d write CSV: %v", i, err)
		}
	}

	printQueryStats("cold-events-query", queries, perQuery, allIters)
	logger.Infof("wrote %s", csvPath)
}

// printQueryStats emits per-query and aggregate latency lines in the
// format computeStats produces. Per-query lines are emitted in JSON
// array order so the operator can read the breakdown alongside the
// JSON file. Queries never selected during the run are skipped (the
// RNG happens not to draw them at low iter counts).
func printQueryStats(
	label string,
	queries []preparedQuery,
	perQuery map[int][]time.Duration,
	allIters []time.Duration,
) {
	fmt.Println("# Per-query")
	for i := range queries {
		dur := perQuery[i]
		if len(dur) == 0 {
			continue
		}
		fmt.Println(computeStats(dur).line(label + queries[i].label()))
	}
	fmt.Println("# Aggregate")
	fmt.Println(computeStats(allIters).line(label))
}
