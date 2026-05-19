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

// cmdEventsBench benches the four filter scenarios across hot/cold:
//   - no-filter:  ledger-range fetch (random N-ledger window via Offsets)
//   - contract:   Lookup(contractID) → FetchEvents
//   - topic:      Lookup(topic0) → FetchEvents
//   - both:       intersect(Lookup(contractID), Lookup(topic0)) → FetchEvents
//
// Per iteration: pick the term/range, run, time it end-to-end.
func cmdEventsBench() {
	fs := flag.NewFlagSet("events", flag.ExitOnError)
	tier := fs.String("tier", "cold", "storage tier: hot|cold")
	scenario := fs.String("scenario", "contract", "scenario: no-filter|contract|topic|both")
	hotDir := fs.String("hot-events-dir", "/mnt/nvme/disk2/ledgers/events-hot", "hot eventstore dir")
	coldDir := fs.String("cold-events-dir", "/mnt/nvme/disk2/ledgers/events-cold", "cold eventstore bucket dir")
	corpus := fs.String("corpus", "/mnt/nvme/disk2/ledgers/events-corpus.json", "term corpus path")
	chunkN := fs.Uint("chunk", 5000, "chunk ID")
	iters := fs.Int("iters", 500, "number of iterations")
	warmup := fs.Int("warmup", 20, "warm-up iterations")
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

	// Open the reader for the requested tier.
	var reader eventstore.Reader
	switch *tier {
	case "hot":
		r, oerr := eventstore.OpenHotStore(*hotDir, chunkID, logger)
		if oerr != nil {
			fatal(logger, "OpenHotStore: %v", oerr)
		}
		reader = r
	case "cold":
		r, oerr := eventstore.OpenColdReader(chunkID, *coldDir, eventstore.ColdReaderOptions{})
		if oerr != nil {
			fatal(logger, "OpenColdReader: %v", oerr)
		}
		reader = r
	default:
		fatal(logger, "unknown --tier=%q", *tier)
	}
	defer reader.Close()

	rng := rand.New(rand.NewPCG(uint64(*seed), uint64(*seed*7919)))
	ctx := context.Background()

	// pickTerm decodes a hex term key from the corpus.
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

	// One end-to-end query: returns the number of events fetched.
	doOne := func() (int, error) {
		switch *scenario {
		case "no-filter":
			// Pick a random N-ledger window inside the chunk; fetch
			// every event in that window via Offsets + sequential
			// IDs. This mirrors getEvents with no filters across a
			// ledger range.
			ofs := reader.Offsets()
			ledgerCount := tc.LastLedger - tc.FirstLedger + 1
			if uint32(*rangeLedgers) > ledgerCount {
				return 0, fmt.Errorf("range-ledgers > chunk")
			}
			startLedger := tc.FirstLedger + rng.Uint32N(ledgerCount-uint32(*rangeLedgers))
			endLedger := startLedger + uint32(*rangeLedgers) - 1
			firstID, _, ferr := ofs.EventIDs(startLedger)
			if ferr != nil {
				return 0, fmt.Errorf("EventIDs start: %w", ferr)
			}
			_, lastID, lerr := ofs.EventIDs(endLedger)
			if lerr != nil {
				return 0, fmt.Errorf("EventIDs end: %w", lerr)
			}
			if lastID <= firstID {
				return 0, nil
			}
			count := int(lastID - firstID)
			if count > *maxFetch {
				count = *maxFetch
			}
			ids := make([]uint32, count)
			for i := range ids {
				ids[i] = firstID + uint32(i)
			}
			ps, ferr2 := reader.FetchEvents(ctx, ids)
			if ferr2 != nil {
				return 0, fmt.Errorf("FetchEvents: %w", ferr2)
			}
			return len(ps), nil

		case "contract":
			k, kerr := pickTerm(tc.ContractIDs)
			if kerr != nil {
				return 0, kerr
			}
			bm, lerr := reader.Lookup(k)
			if lerr != nil {
				if errors.Is(lerr, eventstore.ErrTermNotFound) {
					return 0, nil
				}
				return 0, fmt.Errorf("Lookup: %w", lerr)
			}
			ids := bitmapToSortedIDs(bm, *maxFetch)
			ps, ferr := reader.FetchEvents(ctx, ids)
			if ferr != nil {
				return 0, fmt.Errorf("FetchEvents: %w", ferr)
			}
			return len(ps), nil

		case "topic":
			k, kerr := pickTerm(tc.Topic0)
			if kerr != nil {
				return 0, kerr
			}
			bm, lerr := reader.Lookup(k)
			if lerr != nil {
				if errors.Is(lerr, eventstore.ErrTermNotFound) {
					return 0, nil
				}
				return 0, fmt.Errorf("Lookup: %w", lerr)
			}
			ids := bitmapToSortedIDs(bm, *maxFetch)
			ps, ferr := reader.FetchEvents(ctx, ids)
			if ferr != nil {
				return 0, fmt.Errorf("FetchEvents: %w", ferr)
			}
			return len(ps), nil

		case "both":
			kc, kerr := pickTerm(tc.ContractIDs)
			if kerr != nil {
				return 0, kerr
			}
			kt, kerr := pickTerm(tc.Topic0)
			if kerr != nil {
				return 0, kerr
			}
			bmC, lerr := reader.Lookup(kc)
			if lerr != nil {
				if errors.Is(lerr, eventstore.ErrTermNotFound) {
					return 0, nil
				}
				return 0, fmt.Errorf("Lookup contract: %w", lerr)
			}
			bmT, terr := reader.Lookup(kt)
			if terr != nil {
				if errors.Is(terr, eventstore.ErrTermNotFound) {
					return 0, nil
				}
				return 0, fmt.Errorf("Lookup topic: %w", terr)
			}
			bm := roaring.And(bmC, bmT)
			ids := bitmapToSortedIDs(bm, *maxFetch)
			ps, ferr := reader.FetchEvents(ctx, ids)
			if ferr != nil {
				return 0, fmt.Errorf("FetchEvents: %w", ferr)
			}
			return len(ps), nil

		default:
			return 0, fmt.Errorf("unknown --scenario=%q", *scenario)
		}
	}

	// Warm up
	var warmupHits int
	for i := 0; i < *warmup; i++ {
		n, werr := doOne()
		if werr != nil {
			fatal(logger, "warmup: %v", werr)
		}
		warmupHits += n
	}

	logger.Infof("events tier=%s scenario=%s chunk=%d iters=%d (warmup %d, hit=%d)",
		*tier, *scenario, uint32(chunkID), *iters, *warmup, warmupHits)

	durs := make([]time.Duration, 0, *iters)
	totalFetched := 0
	for i := 0; i < *iters; i++ {
		t0 := time.Now()
		n, werr := doOne()
		d := time.Since(t0)
		if werr != nil {
			fatal(logger, "iter %d: %v", i, werr)
		}
		totalFetched += n
		durs = append(durs, d)
	}

	stats := computeStats(durs)
	avgFetched := float64(totalFetched) / float64(*iters)
	fmt.Println(stats.line(fmt.Sprintf("events-%s-%s (avg=%.1f events/iter)", *tier, *scenario, avgFetched)))

	csv := filepath.Join(*outDir, fmt.Sprintf("events-%s-%s.csv", *tier, *scenario))
	if err := writeCSV(csv, durs); err != nil {
		logger.WithError(err).Warnf("could not write CSV %s", csv)
	} else {
		logger.Infof("wrote %s", csv)
	}
}

func bitmapToSortedIDs(bm *roaring.Bitmap, cap_ int) []uint32 {
	if bm == nil {
		return nil
	}
	out := make([]uint32, 0, min(int(bm.GetCardinality()), cap_))
	it := bm.Iterator()
	for it.HasNext() && len(out) < cap_ {
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
