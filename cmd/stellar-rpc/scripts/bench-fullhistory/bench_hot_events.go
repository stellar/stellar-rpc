package main

import (
	"context"
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

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
)

// cmdHotEvents benches event filter scenarios against the hot tier
// with hot-tier methodology: one shared HotStore reader, warmup
// before the timed iters.
//
// CSV columns mirror cold-events minus open_ns.
func cmdHotEvents() {
	fs := flag.NewFlagSet("hot-events", flag.ExitOnError)
	scenario := fs.String("scenario", "contract", "scenario: no-filter|contract|topic|both")
	hotDir := fs.String("hot-events-dir", "/mnt/nvme/disk2/ledgers/events-hot", "hot eventstore dir")
	corpus := fs.String("corpus", "/mnt/nvme/disk2/ledgers/events-corpus.json", "term corpus path")
	chunkN := fs.Uint("chunk", 5000, "chunk ID")
	iters := fs.Int("iters", 500, "number of timed iterations")
	warmup := fs.Int("warmup", hotWarmupSharedIters, "warm-up iterations (not counted)")
	rangeLedgers := fs.Int("range-ledgers", 50, "ledgers per fetch in no-filter scenario")
	maxFetch := fs.Int("max-fetch", 1000, "cap on per-iter eventIDs fed to FetchEvents")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	chunkID := chunk.ID(uint32(*chunkN))

	tc, err := loadTermCorpus(*corpus)
	if err != nil {
		fatal(logger, "load corpus: %v", err)
	}
	logger.Infof("corpus: contracts=%d topic0=%d topic1=%d (chunk=%d)",
		len(tc.ContractIDs), len(tc.Topic0), len(tc.Topic1), tc.ChunkID)

	reader, oerr := eventstore.OpenHotStore(*hotDir, chunkID, logger)
	if oerr != nil {
		fatal(logger, "OpenHotStore: %v", oerr)
	}
	defer reader.Close()

	rng := rand.New(rand.NewPCG(uint64(*seed), uint64(*seed*7919)))
	ctx := context.Background()

	pickTerm := func(keys []string) (events.TermKey, error) {
		if len(keys) == 0 {
			return events.TermKey{}, errors.New("empty corpus slice")
		}
		i := rng.IntN(len(keys))
		raw, err := hex.DecodeString(keys[i])
		if err != nil || len(raw) != 16 {
			return events.TermKey{}, fmt.Errorf("bad hex key %q: %w", keys[i], err)
		}
		var k events.TermKey
		copy(k[:], raw)
		return k, nil
	}

	for i := range *warmup {
		if _, _, _, werr := runEventsScenario(ctx, reader, *scenario, tc, rng, *rangeLedgers, *maxFetch, pickTerm); werr != nil {
			fatal(logger, "warmup %d: %v", i, werr)
		}
	}

	csvPath := filepath.Join(*outDir, fmt.Sprintf("hot-events-%s.csv", *scenario))
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal(logger, "mkdir %s: %v", *outDir, err)
	}
	csvF, err := os.Create(csvPath)
	if err != nil {
		fatal(logger, "create CSV %s: %v", csvPath, err)
	}
	defer csvF.Close()
	if _, err := fmt.Fprintln(csvF, "filter_ns,fetch_ns,n_events,total_ns"); err != nil {
		fatal(logger, "write CSV header: %v", err)
	}

	totals := make([]time.Duration, 0, *iters)
	for i := range *iters {
		filterNs, fetchNs, nEvents, ferr := runEventsScenario(
			ctx, reader, *scenario, tc, rng, *rangeLedgers, *maxFetch, pickTerm)
		if ferr != nil {
			fatal(logger, "iter %d: %v", i, ferr)
		}
		totalNs := filterNs + fetchNs
		totals = append(totals, totalNs)
		if _, err := fmt.Fprintf(csvF, "%d,%d,%d,%d\n",
			filterNs.Nanoseconds(), fetchNs.Nanoseconds(),
			nEvents, totalNs.Nanoseconds()); err != nil {
			fatal(logger, "iter %d write CSV: %v", i, err)
		}
	}

	stats := computeStats(totals)
	fmt.Println(stats.line("hot-events-" + *scenario))
	logger.Infof("wrote %s", csvPath)
}
