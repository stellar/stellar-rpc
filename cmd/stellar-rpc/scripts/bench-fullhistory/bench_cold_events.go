package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
)

// cmdColdEvents benches the four event filter scenarios against the
// cold tier with cold-cache methodology: per iter evict the chunk's
// three pack files from the OS page cache (events pack + index pack
// + index hash), open a fresh ColdReader, run the filter+fetch, close.
// Each chunk has exactly three files because the eventstore writer
// produces one packfile per (chunk, kind) — see EventsPackName /
// IndexPackName / IndexHashName.
//
// Per-iter CSV row decomposes:
//
//	open_ns          OpenColdReader
//	filter_ns        Lookup(term) building the candidate bitmap (no-filter: Offsets walk)
//	fetch_ns         FetchEvents(ids)
//	n_events         events returned
//	total_ns         sum (close deferred)
func cmdColdEvents() {
	fs := flag.NewFlagSet("cold-events", flag.ExitOnError)
	scenario := fs.String("scenario", "contract", "scenario: no-filter|contract|topic|both")
	coldDir := fs.String("cold-events-dir", "/mnt/nvme/disk2/ledgers/events-cold", "cold eventstore bucket dir")
	corpus := fs.String("corpus", "/mnt/nvme/disk2/ledgers/events-corpus.json", "term corpus path")
	chunkN := fs.Uint("chunk", 5000, "chunk ID")
	iters := fs.Int("iters", 500, "number of timed iterations")
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

	rng := rand.New(rand.NewPCG(uint64(*seed), uint64(*seed*7919)))
	ctx := context.Background()

	csvPath := filepath.Join(*outDir, fmt.Sprintf("cold-events-%s.csv", *scenario))
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal(logger, "mkdir %s: %v", *outDir, err)
	}
	csvF, err := os.Create(csvPath)
	if err != nil {
		fatal(logger, "create CSV %s: %v", csvPath, err)
	}
	defer csvF.Close()
	if _, err := fmt.Fprintln(csvF, "open_ns,filter_ns,fetch_ns,n_events,total_ns"); err != nil {
		fatal(logger, "write CSV header: %v", err)
	}

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

	// Per-iter eviction targets: the three files OpenColdReader opens.
	evictTargets := []string{
		filepath.Join(*coldDir, eventstore.EventsPackName(chunkID)),
		filepath.Join(*coldDir, eventstore.IndexPackName(chunkID)),
		filepath.Join(*coldDir, eventstore.IndexHashName(chunkID)),
	}

	totals := make([]time.Duration, 0, *iters)
	for i := range *iters {
		for _, p := range evictTargets {
			if err := evictFile(p); err != nil {
				fatal(logger, "iter %d evict %s: %v", i, p, err)
			}
		}

		t0 := time.Now()
		reader, oerr := eventstore.OpenColdReader(chunkID, *coldDir, eventstore.ColdReaderOptions{})
		openNs := time.Since(t0)
		if oerr != nil {
			fatal(logger, "iter %d OpenColdReader: %v", i, oerr)
		}

		filterNs, fetchNs, nEvents, ferr := runEventsScenario(
			ctx, reader, *scenario, tc, rng, *rangeLedgers, *maxFetch, pickTerm)
		reader.Close()
		if ferr != nil {
			fatal(logger, "iter %d: %v", i, ferr)
		}

		totalNs := openNs + filterNs + fetchNs
		totals = append(totals, totalNs)
		if _, err := fmt.Fprintf(csvF, "%d,%d,%d,%d,%d\n",
			openNs.Nanoseconds(), filterNs.Nanoseconds(),
			fetchNs.Nanoseconds(), nEvents, totalNs.Nanoseconds()); err != nil {
			fatal(logger, "iter %d write CSV: %v", i, err)
		}
	}

	stats := computeStats(totals)
	fmt.Println(stats.line("cold-events-" + *scenario))
	logger.Infof("wrote %s", csvPath)
}

// runEventsScenario executes one filter+fetch iteration and returns
// per-phase timings. Shared with cmdHotEvents so the same scenario
// branches apply to both tiers.
func runEventsScenario(
	ctx context.Context,
	reader eventstore.Reader,
	scenario string,
	tc *termCorpus,
	rng *rand.Rand,
	rangeLedgers, maxFetch int,
	pickTerm func([]string) (events.TermKey, error),
) (filterNs, fetchNs time.Duration, nEvents int, err error) {
	switch scenario {
	case "no-filter":
		t0 := time.Now()
		ofs, oerr := reader.Offsets()
		if oerr != nil {
			return 0, 0, 0, fmt.Errorf("offsets: %w", oerr)
		}
		ledgerCount := tc.LastLedger - tc.FirstLedger + 1
		if uint32(rangeLedgers) > ledgerCount {
			return 0, 0, 0, errors.New("range-ledgers > chunk")
		}
		startLedger := tc.FirstLedger + rng.Uint32N(ledgerCount-uint32(rangeLedgers))
		endLedger := startLedger + uint32(rangeLedgers) - 1
		firstID, _, ferr := ofs.EventIDs(startLedger)
		if ferr != nil {
			return 0, 0, 0, fmt.Errorf("EventIDs start: %w", ferr)
		}
		_, lastID, lerr := ofs.EventIDs(endLedger)
		if lerr != nil {
			return 0, 0, 0, fmt.Errorf("EventIDs end: %w", lerr)
		}
		filterNs = time.Since(t0)
		if lastID <= firstID {
			return filterNs, 0, 0, nil
		}
		count := min(int(lastID-firstID), maxFetch)
		ids := make([]uint32, count)
		for i := range ids {
			ids[i] = firstID + uint32(i)
		}
		t1 := time.Now()
		ps, ferr2 := reader.FetchEvents(ctx, ids)
		fetchNs = time.Since(t1)
		if ferr2 != nil {
			return filterNs, fetchNs, 0, fmt.Errorf("FetchEvents: %w", ferr2)
		}
		return filterNs, fetchNs, len(ps), nil

	case "contract", "topic":
		var keys []string
		if scenario == "contract" {
			keys = tc.ContractIDs
		} else {
			keys = tc.Topic0
		}
		k, kerr := pickTerm(keys)
		if kerr != nil {
			return 0, 0, 0, kerr
		}
		t0 := time.Now()
		bm, lerr := reader.Lookup(k)
		filterNs = time.Since(t0)
		if lerr != nil {
			if errors.Is(lerr, eventstore.ErrTermNotFound) {
				return filterNs, 0, 0, nil
			}
			return filterNs, 0, 0, fmt.Errorf("lookup: %w", lerr)
		}
		ids := bitmapToSortedIDs(bm, maxFetch)
		t1 := time.Now()
		ps, ferr := reader.FetchEvents(ctx, ids)
		fetchNs = time.Since(t1)
		if ferr != nil {
			return filterNs, fetchNs, 0, fmt.Errorf("FetchEvents: %w", ferr)
		}
		return filterNs, fetchNs, len(ps), nil

	case "both":
		kc, kerr := pickTerm(tc.ContractIDs)
		if kerr != nil {
			return 0, 0, 0, kerr
		}
		kt, kerr := pickTerm(tc.Topic0)
		if kerr != nil {
			return 0, 0, 0, kerr
		}
		t0 := time.Now()
		bmC, lerr := reader.Lookup(kc)
		if lerr != nil {
			filterNs = time.Since(t0)
			if errors.Is(lerr, eventstore.ErrTermNotFound) {
				return filterNs, 0, 0, nil
			}
			return filterNs, 0, 0, fmt.Errorf("lookup contract: %w", lerr)
		}
		bmT, terr := reader.Lookup(kt)
		if terr != nil {
			filterNs = time.Since(t0)
			if errors.Is(terr, eventstore.ErrTermNotFound) {
				return filterNs, 0, 0, nil
			}
			return filterNs, 0, 0, fmt.Errorf("lookup topic: %w", terr)
		}
		bm := roaring.And(bmC, bmT)
		filterNs = time.Since(t0)
		ids := bitmapToSortedIDs(bm, maxFetch)
		t1 := time.Now()
		ps, ferr := reader.FetchEvents(ctx, ids)
		fetchNs = time.Since(t1)
		if ferr != nil {
			return filterNs, fetchNs, 0, fmt.Errorf("FetchEvents: %w", ferr)
		}
		return filterNs, fetchNs, len(ps), nil

	default:
		return 0, 0, 0, fmt.Errorf("unknown --scenario=%q", scenario)
	}
}

func bitmapToSortedIDs(bm *roaring.Bitmap, capacity int) []uint32 {
	if bm == nil {
		return nil
	}
	out := make([]uint32, 0, min(int(bm.GetCardinality()), capacity))
	it := bm.Iterator()
	for it.HasNext() && len(out) < capacity {
		out = append(out, it.Next())
	}
	return out
}

func loadTermCorpus(path string) (*termCorpus, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var tc termCorpus
	if err := json.Unmarshal(data, &tc); err != nil {
		return nil, err
	}
	return &tc, nil
}
