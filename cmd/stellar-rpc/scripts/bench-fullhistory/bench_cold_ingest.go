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

// coldDeps is the run-wide configuration handed to the cold driver.
// Unlike hotDeps it does NOT pre-build ingesters or collectors — those
// are built per-chunk inside the chunk-worker goroutine because each
// chunk needs fresh per-chunk state (ColdWriter, TxhashCold accumulator,
// EventsCold mirror+offsets). Backends are opened per-chunk inside each
// worker too (a pack reader or a BSB session), so nothing here is shared
// across workers — BSB's single sequential cursor cannot be.
type coldDeps struct {
	source       string
	coldDir      string
	bucketPath   string
	startChunk   chunk.ID
	numChunks    int
	chunkWorkers int
	outRoot      string // --cold-out-dir; per-type subdirs live underneath
	subdirs      map[string]string
	enabled      map[string]bool
	xdrViews     bool
	parallel     bool
	mode         string
	outDir       string // --out (CSV destination)
	cpuProfile   string
	memProfile   string
	ledgersOpts  LedgersColdOpts
	eventsOpts   EventsColdOpts
	bsbOpts      BSBOpts // BufferedStorageBackend tuning; one session per chunk when --source=bsb
}

func buildColdDeps(logger *supportlog.Entry) (context.Context, coldDeps, func(), error) {
	fs := flag.NewFlagSet("cold-ingest", flag.ExitOnError)
	typesArg := fs.String("types", "", "comma-separated subset of ledgers,events,txhash (required)")
	source := fs.String("source", sourcePack, "ledger source: pack | bsb")
	coldDir := fs.String("cold-dir", "", "source cold-store dir (required iff --source=pack)")
	bucketPath := fs.String("bucket-path", "sdf-ledger-close-meta/v1/ledgers/pubnet",
		"GCS destination_bucket_path (used iff --source=bsb)")
	bsbBufferSize := fs.Uint("bsb-buffer-size", 5000,
		"BSB prefetch buffer depth, PER chunk worker (total buffered ledgers ≈ this × --chunk-workers)")
	bsbNumWorkers := fs.Uint("bsb-num-workers", 50,
		"BSB download workers, PER chunk worker (total GCS download concurrency ≈ this × --chunk-workers)")
	retryLimit := fs.Uint("retry-limit", 3, "BSB retry attempts on transient backend failure")
	retryWait := fs.Duration("retry-wait", 5*time.Second, "BSB delay between retry attempts")
	chunkArg := fs.Uint("chunk", 0, "first chunk ID to ingest (required)")
	numChunks := fs.Int("num-chunks", 1, "how many consecutive chunks to ingest starting at --chunk")
	chunkWorkers := fs.Int("chunk-workers", 1,
		"how many chunks to run concurrently (clamped to <= --num-chunks). "+
			"NOTE: with --parallel, total goroutines ≈ 3 * chunk-workers; "+
			"sizing close to GOMAXPROCS avoids scheduler thrashing.")
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
		"within each chunk, run enabled ingesters concurrently per ledger via errgroup. "+
			"Independent of --chunk-workers (which fans across chunks).")
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
	if *numChunks < 1 {
		return nil, coldDeps{}, nil, fmt.Errorf("--num-chunks must be >= 1, got %d", *numChunks)
	}
	if *chunkWorkers < 1 {
		return nil, coldDeps{}, nil, fmt.Errorf("--chunk-workers must be >= 1, got %d", *chunkWorkers)
	}
	if *chunkWorkers > *numChunks {
		logger.Infof("--chunk-workers=%d > --num-chunks=%d; clamping to %d",
			*chunkWorkers, *numChunks, *numChunks)
		*chunkWorkers = *numChunks
	}
	startChunk := chunk.ID(uint32(*chunkArg))
	mode := modeString(*xdrViews)

	if *source == sourcePack && enabled["ledgers"] {
		if samePath(*coldDir, filepath.Join(*coldOutDir, "ledgers")) {
			return nil, coldDeps{}, nil, fmt.Errorf("--cold-out-dir/ledgers must differ from --cold-dir (%s)", *coldDir)
		}
	}

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

	// Validate pack sources up-front so we don't waste any work
	// discovering a missing pack mid-run.
	if *source == sourcePack {
		for i := range *numChunks {
			cid := startChunk + chunk.ID(uint32(i))
			path := packPath(*coldDir, uint32(cid))
			if _, err := os.Stat(path); err != nil {
				return nil, coldDeps{}, nil, fmt.Errorf("missing source pack: %s: %w", path, err)
			}
		}
	}

	// For --source=bsb, validate the bucket up front; each chunk worker
	// opens its own BSB session inside runOneChunkCold (BSB's single
	// sequential cursor can't be shared across concurrent workers).
	if *source == sourceBSB && *bucketPath == "" {
		return nil, coldDeps{}, nil, errors.New("--bucket-path is required when --source=bsb")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	deps := coldDeps{
		source: *source, coldDir: *coldDir, bucketPath: *bucketPath,
		startChunk: startChunk, numChunks: *numChunks, chunkWorkers: *chunkWorkers,
		outRoot: *coldOutDir, subdirs: subdirs, enabled: enabled,
		xdrViews: *xdrViews, parallel: *parallel, mode: mode,
		outDir:     *outDir,
		cpuProfile: *cpuProfile, memProfile: *memProfile,
		ledgersOpts: LedgersColdOpts{Concurrency: *ledgersConcurrency, BytesPerSync: *ledgersBytesPerSync},
		eventsOpts:  EventsColdOpts{Concurrency: *eventsConcurrency, BytesPerSync: *eventsBytesPerSync},
		bsbOpts: BSBOpts{
			BufferSize: *bsbBufferSize, NumWorkers: *bsbNumWorkers,
			RetryLimit: *retryLimit, RetryWait: *retryWait,
		},
	}
	cleanup := func() { cancel() }

	logger.Infof("cold-ingest source=%s types=%s start-chunk=%d num-chunks=%d chunk-workers=%d cold-out=%s mode=%s parallel=%v",
		*source, *typesArg, uint32(startChunk), *numChunks, *chunkWorkers, *coldOutDir, mode, *parallel)
	if enabled["txhash"] {
		logger.Infof("txhash phase-1 .bin output: %s (feed to build-txhash-index --in-dir)", subdirs["txhash"])
	}
	return ctx, deps, cleanup, nil
}

// chunkResult is what a per-chunk worker returns to the driver after
// it finishes its chunk's ingest. The driver merges these into the
// run-wide aggregate collectors before reporting.
type chunkResult struct {
	wall    time.Duration
	ledColl *LedgerCollector // nil if !enabled["ledgers"]
	thxColl *TxhashCollector // nil if !enabled["txhash"]
	evtColl *EventsCollector // nil if !enabled["events"]
	dm      *driverMetrics   // per-chunk metrics (read_blocked, lcm_decode, fan_out)
}

// runCold orchestrates the multi-chunk cold ingest. Spawns up to
// d.chunkWorkers goroutines that each process one chunk via
// runOneChunkCold, then merges per-chunk collectors into the
// aggregate report.
func runCold(ctx context.Context, logger *supportlog.Entry, d coldDeps) (err error) {
	if stop := maybeStartCPUProfile(logger, d.cpuProfile); stop != nil {
		defer stop()
	}
	if d.memProfile != "" {
		defer writeMemProfile(logger, d.memProfile)
	}

	results := make([]*chunkResult, d.numChunks)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(d.chunkWorkers)

	loopStart := time.Now()
	for i := range d.numChunks {
		chunkID := d.startChunk + chunk.ID(uint32(i))
		g.Go(func() error {
			r, rerr := runOneChunkCold(gctx, d, chunkID)
			if rerr != nil {
				return fmt.Errorf("chunk %d: %w", uint32(chunkID), rerr)
			}
			results[i] = r
			return nil
		})
	}
	if werr := g.Wait(); werr != nil {
		return werr
	}
	totalWall := time.Since(loopStart)

	// Merge per-chunk collectors + driver metrics into run-wide
	// aggregates. Workers are now joined, so single-goroutine merge.
	aggLed, aggThx, aggEvt := mergeChunkCollectors(d.enabled, results)
	aggDM := mergeDriverMetrics(d, results)

	return reportCold(logger, d, aggLed, aggThx, aggEvt, aggDM, totalWall)
}

// runOneChunkCold processes a single chunk: opens its own backend (a
// per-chunk pack reader or BSB session), opens per-type ingesters +
// collectors, runs the per-ledger loop (with --parallel for ingester
// fan-out within the chunk), finalizes the per-chunk artifacts, and
// returns the chunk's collectors + driver metrics.
func runOneChunkCold(ctx context.Context, d coldDeps, chunkID chunk.ID) (_ *chunkResult, err error) {
	chunkStart := time.Now()

	// Acquire this chunk's ledger stream. Each chunk gets its own INDEPENDENT
	// stream so chunk workers run fully in parallel, and the stream owns its own
	// setup + teardown (no separate prepare/close to manage here).
	stream, oerr := openChunkStream(d.source, d.coldDir, d.bucketPath, d.bsbOpts, chunkID)
	if oerr != nil {
		return nil, oerr
	}

	// Build per-chunk ingesters + collectors.
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	n := int(last - first + 1)
	var (
		ledColl *LedgerCollector
		thxColl *TxhashCollector
		evtColl *EventsCollector
		ings    []ColdIngester
	)
	for _, t := range canonicalIngestTypes() {
		if !d.enabled[t] {
			continue
		}
		switch t {
		case "ledgers":
			ledColl = NewLedgerCollector(n)
			ing, ierr := NewLedgersCold(ledColl, d.subdirs["ledgers"], chunkID, d.ledgersOpts)
			if ierr != nil {
				return nil, fmt.Errorf("open LedgersCold: %w", ierr)
			}
			ings = append(ings, ing)
		case "events":
			evtColl = NewEventsCollector(n)
			ing, ierr := NewEventsCold(evtColl, d.subdirs["events"], chunkID, d.eventsOpts, d.xdrViews)
			if ierr != nil {
				return nil, fmt.Errorf("open EventsCold: %w", ierr)
			}
			ings = append(ings, ing)
		case "txhash":
			thxColl = NewTxhashCollector(n)
			ing, ierr := NewTxhashCold(thxColl, d.subdirs["txhash"], chunkID, d.xdrViews)
			if ierr != nil {
				return nil, fmt.Errorf("open TxhashCold: %w", ierr)
			}
			ings = append(ings, ing)
		}
	}
	// Defer ingester Close (idempotent; cleans up partial cold packs
	// on the failure path before Finalize landed).
	for _, ing := range ings {
		defer func() {
			if cerr := ing.Close(); cerr != nil {
				err = errors.Join(err, fmt.Errorf("close: %w", cerr))
			}
		}()
	}

	// Per-chunk driver metrics. Pre-sized; no contention since each
	// worker owns its own dm.
	needsLCM := !d.xdrViews && (thxColl != nil || evtColl != nil)
	dm := newDriverMetrics(n, needsLCM)

	// Per-ledger loop, streaming raw bytes from the backend. raw is BORROWED
	// (valid only until the next yield); each ingester copies what it retains,
	// and in --parallel mode pg.Wait() ensures all ingesters are done with the
	// ledger before the next yield. readBlocked measures the wait between yields.
	seq := first
	tRead := time.Now()
	for raw, serr := range stream.RawLedgers(ctx, ledgerbackend.BoundedRange(first, last)) {
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		if serr != nil {
			return nil, fmt.Errorf("RawLedgers(%d): %w", seq, serr)
		}
		dm.readBlocked = append(dm.readBlocked, time.Since(tRead))
		l, lerr := buildLedger(seq, raw, d.xdrViews, needsLCM, dm)
		if lerr != nil {
			return nil, lerr
		}

		fanStart := time.Now()
		if d.parallel {
			pg, pgctx := errgroup.WithContext(ctx)
			for _, ing := range ings {
				pg.Go(func() error { return ing.Ingest(pgctx, l) })
			}
			if werr := pg.Wait(); werr != nil {
				return nil, werr
			}
		} else {
			for _, ing := range ings {
				if ierr := ing.Ingest(ctx, l); ierr != nil {
					return nil, ierr
				}
			}
		}
		dm.fanOutPerLedger = append(dm.fanOutPerLedger, time.Since(fanStart))
		seq++
		tRead = time.Now()
	}

	// Finalize (commit/finish+writeColdIndex/sort+write_bin).
	for _, ing := range ings {
		if ferr := ing.Finalize(ctx); ferr != nil {
			return nil, fmt.Errorf("finalize: %w", ferr)
		}
	}

	return &chunkResult{
		wall:    time.Since(chunkStart),
		ledColl: ledColl,
		thxColl: thxColl,
		evtColl: evtColl,
		dm:      dm,
	}, nil
}

// mergeChunkCollectors concatenates per-chunk samples and appends per-chunk
// scalars into a single run-wide collector per enabled data type.
func mergeChunkCollectors(enabled map[string]bool, results []*chunkResult) (*LedgerCollector, *TxhashCollector, *EventsCollector) {
	var (
		led *LedgerCollector
		thx *TxhashCollector
		evt *EventsCollector
	)
	if enabled["ledgers"] {
		led = NewLedgerCollector(0)
	}
	if enabled["txhash"] {
		thx = NewTxhashCollector(0)
	}
	if enabled["events"] {
		evt = NewEventsCollector(0)
	}
	for _, r := range results {
		if r == nil {
			continue
		}
		if led != nil && r.ledColl != nil {
			led.Merge(r.ledColl)
		}
		if thx != nil && r.thxColl != nil {
			thx.Merge(r.thxColl)
		}
		if evt != nil && r.evtColl != nil {
			evt.Merge(r.evtColl)
		}
	}
	return led, thx, evt
}

// mergeDriverMetrics concatenates per-chunk per-ledger samples and
// records each chunk's wall into the aggregate.
func mergeDriverMetrics(d coldDeps, results []*chunkResult) *driverMetrics {
	agg := &driverMetrics{
		chunkWall: make([]time.Duration, 0, d.numChunks),
	}
	for _, r := range results {
		if r == nil {
			continue
		}
		agg.readBlocked = append(agg.readBlocked, r.dm.readBlocked...)
		agg.fanOutPerLedger = append(agg.fanOutPerLedger, r.dm.fanOutPerLedger...)
		if r.dm.lcmDecode != nil {
			agg.lcmDecode = append(agg.lcmDecode, r.dm.lcmDecode...)
		}
		agg.chunkWall = append(agg.chunkWall, r.wall)
	}
	return agg
}

// reportCold prints per-stage percentile summaries + per-chunk wall
// distribution + total wall + writes per-type and driver CSVs.
//

func reportCold(
	logger *supportlog.Entry, d coldDeps,
	ledColl *LedgerCollector, thxColl *TxhashCollector, evtColl *EventsCollector,
	dm *driverMetrics, totalWall time.Duration,
) error {
	w := os.Stdout
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Per-stage percentile summary:")

	if ledColl != nil {
		ledColl.PrintSummary("cold", w)
		printThroughput(w, "cold.ledgers", len(ledColl.samples), totalWall, ledColl.InPipelineTime())
		if cerr := ledColl.WriteCSV(d.outDir, "cold-ledgers-"+d.mode); cerr != nil {
			return cerr
		}
		// Parity line matching the old cold-ledgers-ingest CSV semantics.
		// Prepare latency is no longer broken out (the stream owns
		// preparation); it now folds into the first read_blocked sample.
		blocked := sumDur(dm.readBlocked)
		writerTotal := sumDur(ledColl.Writes())
		fmt.Fprintf(w,
			"  cold.ledgers parity: writer_total=%s commit=%s blocked=%s\n",
			writerTotal.Round(time.Microsecond),
			sumDur(ledColl.commit).Round(time.Microsecond),
			blocked.Round(time.Microsecond),
		)
	}
	if thxColl != nil {
		thxColl.PrintSummary("cold", w)
		printThroughput(w, "cold.txhash", thxColl.TotalItems(), totalWall, thxColl.InPipelineTime())
		if cerr := thxColl.WriteCSV(d.outDir, "cold-txhash-"+d.mode); cerr != nil {
			return cerr
		}
	}
	if evtColl != nil {
		evtColl.PrintSummary("cold", w)
		printThroughput(w, "cold.events", evtColl.TotalItems(), totalWall, evtColl.InPipelineTime())
		if cerr := evtColl.WriteCSV(d.outDir, "cold-events-"+d.mode); cerr != nil {
			return cerr
		}
	}
	if err := dm.report(w, d.outDir, "cold-driver-"+d.mode); err != nil {
		return err
	}

	// Total wall line — wall of the whole chunk loop (per-chunk backend
	// opens + PrepareRange happen inside it). Effective concurrency =
	// sum(chunkWall)/totalWall.
	sumChunkWall := sumDur(dm.chunkWall)
	if d.numChunks > 1 && totalWall > 0 {
		fmt.Fprintf(w, "  total wall                       = %s   (sum(chunk_wall)/total = %.2f×)\n",
			totalWall.Round(time.Millisecond),
			float64(sumChunkWall)/float64(totalWall),
		)
	} else {
		fmt.Fprintf(w, "  total wall                       = %s\n", totalWall.Round(time.Millisecond))
	}
	logger.Infof("wrote CSVs to %s", d.outDir)
	return nil
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
