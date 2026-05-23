package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// cmdColdIngest is the CLI entry point for the unified cold ingest
// benchmark. Same exit-code pattern as cmdHotIngest.
func cmdColdIngest() int {
	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	ctx, deps, cleanup, err := buildColdDeps(logger)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		logger.Errorf("setup: %v", err)
		return 1
	}

	if err := runCold(ctx, logger, deps); err != nil {
		logger.Errorf("%v", err)
		return 1
	}
	return 0
}

// coldDeps mirrors hotDeps but holds the cold-ingester slice (typed
// ColdIngester so the Finalize-after-loop step compiles without
// dispatch) plus the cold-specific output root.
type coldDeps struct {
	backend      ledgerbackend.LedgerBackend
	chunkID      chunk.ID
	xdrViews     bool
	parallel     bool
	ings         []ColdIngester
	ledColl      *LedgerCollector
	thxColl      *TxhashCollector
	evtColl      *EventsCollector
	outDir       string
	mode         string
	cpuProfile   string
	memProfile   string
	prepareRange time.Duration // measured by openSourceBackend
}

func buildColdDeps(logger *supportlog.Entry) (context.Context, coldDeps, func(), error) {
	fs := flag.NewFlagSet("cold-ingest", flag.ExitOnError)
	typesArg := fs.String("types", "", "comma-separated subset of ledgers,events,txhash (required)")
	source := fs.String("source", "pack", "ledger source: pack | bsb")
	coldDir := fs.String("cold-dir", "", "source cold-store dir (required iff --source=pack)")
	bucketPath := fs.String("bucket-path", "sdf-ledger-close-meta/v1/ledgers/pubnet",
		"GCS destination_bucket_path (used iff --source=bsb)")
	bsbBufferSize := fs.Uint("bsb-buffer-size", 5000, "BSB prefetch buffer depth")
	bsbNumWorkers := fs.Uint("bsb-num-workers", 50, "BSB download workers")
	retryLimit := fs.Uint("retry-limit", 3, "BSB retry attempts on transient backend failure")
	retryWait := fs.Duration("retry-wait", 5*time.Second, "BSB delay between retry attempts")
	chunkArg := fs.Uint("chunk", 0, "chunk ID to ingest (required)")
	coldOutDir := fs.String("cold-out-dir", "",
		"output root for cold artifacts (required; per-type subdirs ledgers/, events/, txhash/ created under it)")
	ledgersConcurrency := fs.Int("ledgers-packfile-concurrency", 4,
		"ledgers ColdWriter parallel zstd encoder workers")
	ledgersBytesPerSync := fs.Int("ledgers-bytes-per-sync", 0,
		"ledgers writer non-blocking writeback granularity in bytes (0 disables)")
	eventsConcurrency := fs.Int("events-packfile-concurrency", 4,
		"events ColdWriter parallel zstd encoder workers")
	eventsBytesPerSync := fs.Int("events-bytes-per-sync", 0,
		"events writer non-blocking writeback granularity in bytes (0 disables)")
	xdrViews := fs.Bool("xdr-views", false,
		"decode via XDR views (zero-copy) instead of UnmarshalBinary + struct walk")
	parallel := fs.Bool("parallel", false,
		"run enabled ingesters concurrently per ledger via errgroup. "+
			"NOTE: per-stage timings under --parallel reflect per-goroutine wall time including "+
			"scheduler contention and are not directly comparable to serial-mode timings.")
	cpuProfile := fs.String("cpuprofile", "", "write a Go CPU profile to PATH")
	memProfile := fs.String("memprofile", "", "write a Go heap profile to PATH")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	enabled, err := parseTypes(*typesArg, []string{"ledgers", "events", "txhash"})
	if err != nil {
		return nil, coldDeps{}, nil, err
	}
	if *chunkArg == 0 {
		return nil, coldDeps{}, nil, errors.New("--chunk is required")
	}
	if *coldOutDir == "" {
		return nil, coldDeps{}, nil, errors.New("--cold-out-dir is required")
	}
	chunkID := chunk.ID(uint32(*chunkArg))
	mode := modeString(*xdrViews)

	// Refuse to overwrite the input pack when re-packing ledgers locally.
	if *source == "pack" && enabled["ledgers"] {
		if samePath(*coldDir, filepath.Join(*coldOutDir, "ledgers")) {
			return nil, coldDeps{}, nil, fmt.Errorf("--cold-out-dir/ledgers must differ from --cold-dir (%s)", *coldDir)
		}
	}

	// Per-type cold subdirs must each be empty/absent.
	subdirs := map[string]string{
		"ledgers": filepath.Join(*coldOutDir, "ledgers"),
		"events":  filepath.Join(*coldOutDir, "events"),
		"txhash":  filepath.Join(*coldOutDir, "txhash"),
	}
	for t := range enabled {
		if entries, derr := os.ReadDir(subdirs[t]); derr == nil && len(entries) > 0 {
			return nil, coldDeps{}, nil, fmt.Errorf("%s subdir %s is not empty; pick a fresh path", t, subdirs[t])
		}
		if err := os.MkdirAll(subdirs[t], 0o755); err != nil {
			return nil, coldDeps{}, nil, fmt.Errorf("mkdir %s: %w", subdirs[t], err)
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	backend, srcCleanup, prepareDur, err := openSourceBackend(ctx, *source, *coldDir, *bucketPath,
		*bsbBufferSize, *bsbNumWorkers, *retryLimit, *retryWait, chunkID)
	if err != nil {
		cancel()
		return nil, coldDeps{}, nil, fmt.Errorf("open source (%s): %w", *source, err)
	}

	n := int(chunkID.LastLedger() - chunkID.FirstLedger() + 1)
	var (
		ings    []ColdIngester
		ledColl *LedgerCollector
		thxColl *TxhashCollector
		evtColl *EventsCollector
	)
	closeOnErr := func() {
		for _, ing := range ings {
			_ = ing.Close()
		}
		srcCleanup()
		cancel()
	}
	for _, t := range canonicalIngestTypes() {
		if !enabled[t] {
			continue
		}
		switch t {
		case "ledgers":
			ledColl = NewLedgerCollector(n)
			ing, ierr := NewLedgersCold(ledColl, subdirs["ledgers"], chunkID, LedgersColdOpts{
				Concurrency: *ledgersConcurrency, BytesPerSync: *ledgersBytesPerSync,
			})
			if ierr != nil {
				closeOnErr()
				return nil, coldDeps{}, nil, fmt.Errorf("open LedgersCold: %w", ierr)
			}
			ings = append(ings, ing)
		case "events":
			evtColl = NewEventsCollector(n)
			ing, ierr := NewEventsCold(evtColl, subdirs["events"], chunkID, EventsColdOpts{
				Concurrency: *eventsConcurrency, BytesPerSync: *eventsBytesPerSync,
			}, *xdrViews)
			if ierr != nil {
				closeOnErr()
				return nil, coldDeps{}, nil, fmt.Errorf("open EventsCold: %w", ierr)
			}
			ings = append(ings, ing)
		case "txhash":
			thxColl = NewTxhashCollector(n)
			ing, ierr := NewTxhashCold(thxColl, subdirs["txhash"], chunkID, *xdrViews)
			if ierr != nil {
				closeOnErr()
				return nil, coldDeps{}, nil, fmt.Errorf("open TxhashCold: %w", ierr)
			}
			ings = append(ings, ing)
		}
	}

	deps := coldDeps{
		backend: backend, chunkID: chunkID,
		xdrViews: *xdrViews, parallel: *parallel,
		ings: ings, ledColl: ledColl, thxColl: thxColl, evtColl: evtColl,
		outDir: *outDir, mode: mode,
		cpuProfile: *cpuProfile, memProfile: *memProfile,
		prepareRange: prepareDur,
	}
	cleanup := func() {
		srcCleanup()
		cancel()
	}

	logger.Infof("cold-ingest source=%s types=%s chunk=%d cold-out=%s mode=%s parallel=%v",
		*source, *typesArg, uint32(chunkID), *coldOutDir, mode, *parallel)
	if enabled["txhash"] {
		logger.Infof("txhash phase-1 .bin output: %s (feed to build-txhash-index --in-dir)", subdirs["txhash"])
	}
	return ctx, deps, cleanup, nil
}

// runCold is the test-friendly cold driver loop. Same shape as runHot
// but with []ColdIngester + a Finalize phase after the ledger loop +
// a one-time PrepareRange timing into driverMetrics.
func runCold(ctx context.Context, logger *supportlog.Entry, d coldDeps) (err error) {
	if stop := maybeStartCPUProfile(logger, d.cpuProfile); stop != nil {
		defer stop()
	}

	for _, ing := range d.ings {
		defer func() {
			if cerr := ing.Close(); cerr != nil {
				err = errors.Join(err, fmt.Errorf("close: %w", cerr))
			}
		}()
	}
	if d.memProfile != "" {
		defer writeMemProfile(logger, d.memProfile)
	}

	first, last := d.chunkID.FirstLedger(), d.chunkID.LastLedger()
	n := int(last - first + 1)
	needsLCM := !d.xdrViews && (d.thxColl != nil || d.evtColl != nil)
	dm := newDriverMetrics(n, needsLCM)
	dm.prepareRange = d.prepareRange

	loopStart := time.Now()
	for seq := first; seq <= last; seq++ {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		l, lerr := readLedger(ctx, d.backend, seq, d.xdrViews, needsLCM, dm)
		if lerr != nil {
			return lerr
		}

		fanStart := time.Now()
		if d.parallel {
			g, gctx := errgroup.WithContext(ctx)
			for _, ing := range d.ings {
				g.Go(func() error { return ing.Ingest(gctx, l) })
			}
			if werr := g.Wait(); werr != nil {
				return werr
			}
		} else {
			for _, ing := range d.ings {
				if ierr := ing.Ingest(ctx, l); ierr != nil {
					return ierr
				}
			}
		}
		dm.fanOutPerLedger = append(dm.fanOutPerLedger, time.Since(fanStart))
		// totalPerLedger intentionally NOT recorded for cold: the
		// per-ledger window doesn't include per-chunk Finalize
		// (Commit / Finish+WriteColdIndex / sort+write_bin), so it
		// would mislead anyone interpreting it as "total cold cost
		// per ledger". Cold's finalize cost is reported as a scalar
		// per data type instead.
	}

	// Finalize phase — explicit, not deferred. Errors here mean the
	// chunk's cold artifact didn't durably land.
	for _, ing := range d.ings {
		if ferr := ing.Finalize(ctx); ferr != nil {
			return fmt.Errorf("finalize: %w", ferr)
		}
	}

	// Wall includes prepareRange + per-ledger loop + finalize (folded
	// into the loop window since Finalize runs before time.Since).
	wall := dm.prepareRange + time.Since(loopStart)

	if rerr := reportCold(logger, d, dm, wall); rerr != nil {
		return fmt.Errorf("report: %w", rerr)
	}
	return nil
}

func reportCold(logger *supportlog.Entry, d coldDeps, dm *driverMetrics, wall time.Duration) error {
	w := os.Stdout
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Per-stage percentile summary:")

	if d.ledColl != nil {
		d.ledColl.PrintSummary("cold", w)
		printThroughput(w, "cold.ledgers", len(d.ledColl.samples), wall, d.ledColl.InPipelineTime())
		if cerr := d.ledColl.WriteCSV(d.outDir, "cold-ledgers-"+d.mode); cerr != nil {
			return cerr
		}
		// Parity line matching the old cold-ledgers-ingest CSV semantics.
		blocked := dm.prepareRange + sumDur(dm.readBlocked)
		writerTotal := sumDur(extractLedgerWrites(d.ledColl))
		// Note: this "total" is per-type (ledger writer + commit), not
		// the chunk-level wall the old single-purpose bench printed.
		// Rename intentional — different semantics.
		fmt.Fprintf(w,
			"  cold.ledgers parity: prepare_range=%s writer_total=%s commit=%s blocked=%s\n",
			dm.prepareRange.Round(time.Microsecond),
			writerTotal.Round(time.Microsecond),
			d.ledColl.commit.Round(time.Microsecond),
			blocked.Round(time.Microsecond),
		)
	}
	if d.thxColl != nil {
		d.thxColl.PrintSummary("cold", w)
		printThroughput(w, "cold.txhash", d.thxColl.TotalItems(), wall, d.thxColl.InPipelineTime())
		if cerr := d.thxColl.WriteCSV(d.outDir, "cold-txhash-"+d.mode); cerr != nil {
			return cerr
		}
	}
	if d.evtColl != nil {
		d.evtColl.PrintSummary("cold", w)
		printThroughput(w, "cold.events", d.evtColl.TotalItems(), wall, d.evtColl.InPipelineTime())
		if cerr := d.evtColl.WriteCSV(d.outDir, "cold-events-"+d.mode); cerr != nil {
			return cerr
		}
	}
	if err := dm.report(w, d.outDir, "cold-driver-"+d.mode); err != nil {
		return err
	}
	logger.Infof("wrote CSVs to %s", d.outDir)
	return nil
}

// extractLedgerWrites returns the Write field across all samples.
// Helper for the cold-ledgers parity line.
func extractLedgerWrites(c *LedgerCollector) []time.Duration {
	out := make([]time.Duration, len(c.samples))
	for i, s := range c.samples {
		out[i] = s.Write
	}
	return out
}

// samePath returns true when a and b resolve to the same directory.
// Used to refuse re-packing a local cold pack in place. Falls back to
// abs-path string compare when stat fails (the empty-dir guard later
// in setup will catch any collision missed here).
func samePath(a, b string) bool {
	ai, aerr := os.Stat(a)
	bi, berr := os.Stat(b)
	if aerr == nil && berr == nil {
		return os.SameFile(ai, bi)
	}
	absA, _ := filepath.Abs(a)
	absB, _ := filepath.Abs(b)
	return absA == absB
}
