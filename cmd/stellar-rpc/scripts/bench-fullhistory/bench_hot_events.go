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

// cmdHotEvents benches eventstore.Query against the hot tier with
// hot-tier methodology: one shared HotStore reader, --warmup queries
// before timing.
//
// Per-iter CSV row (no open_ns — reader is shared):
//
//	query_idx,n_filters,query_ns,n_events,total_ns
//
// total_ns is equal to query_ns and kept as a column for symmetry
// with the cold bench's CSV.
//
//nolint:funlen // sequential pipeline: parse flags → load queries → per-iter loop → stats
func cmdHotEvents() {
	fs := flag.NewFlagSet("hot-events", flag.ExitOnError)
	queriesPath := fs.String("queries", "",
		"JSON queries file (required); see query_corpus.go for shape")
	hotDir := fs.String("hot-events-dir",
		"/mnt/nvme/disk2/ledgers/events-hot", "hot eventstore dir")
	chunkN := fs.Uint("chunk", 5000, "chunk ID")
	iters := fs.Int("iters", 500, "number of timed iterations")
	warmup := fs.Int("warmup", hotWarmupSharedIters,
		"warm-up iterations (not counted)")
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
	logger.Infof("hot-events queries=%d chunk=%d iters=%d warmup=%d",
		len(queries), chunkID, *iters, *warmup)

	reader, oerr := eventstore.OpenHotStore(*hotDir, chunkID, logger)
	if oerr != nil {
		fatal(logger, "OpenHotStore: %v", oerr)
	}
	defer reader.Close()

	rng := rand.New(rand.NewPCG(uint64(*seed), uint64(*seed*7919))) //nolint:gosec
	ctx := context.Background()

	for i := range *warmup {
		q := &queries[rng.IntN(len(queries))]
		if _, werr := eventstore.Query(ctx, reader, q.filters, q.opts); werr != nil {
			fatal(logger, "warmup %d query%s: %v", i, q.label(), werr)
		}
	}

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal(logger, "mkdir %s: %v", *outDir, err)
	}
	csvPath := filepath.Join(*outDir, "hot-events-query.csv")
	csvF, err := os.Create(csvPath)
	if err != nil {
		fatal(logger, "create CSV %s: %v", csvPath, err)
	}
	defer csvF.Close()
	if _, err := fmt.Fprintln(csvF,
		"query_idx,n_filters,query_ns,n_events,total_ns"); err != nil {
		fatal(logger, "write CSV header: %v", err)
	}

	perQuery := make(map[int][]time.Duration, len(queries))
	allIters := make([]time.Duration, 0, *iters)

	for i := range *iters {
		q := &queries[rng.IntN(len(queries))]

		t0 := time.Now()
		out, qerr := eventstore.Query(ctx, reader, q.filters, q.opts)
		queryNs := time.Since(t0)
		if qerr != nil {
			fatal(logger, "iter %d query%s: %v", i, q.label(), qerr)
		}

		perQuery[q.idx] = append(perQuery[q.idx], queryNs)
		allIters = append(allIters, queryNs)

		if _, err := fmt.Fprintf(csvF, "%d,%d,%d,%d,%d\n",
			q.idx, len(q.filters),
			queryNs.Nanoseconds(),
			len(out), queryNs.Nanoseconds()); err != nil {
			fatal(logger, "iter %d write CSV: %v", i, err)
		}
	}

	printQueryStats("hot-events-query", queries, perQuery, allIters)
	logger.Infof("wrote %s", csvPath)
}
