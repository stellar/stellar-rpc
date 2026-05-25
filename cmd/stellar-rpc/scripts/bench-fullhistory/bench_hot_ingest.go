package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// cmdHotIngest is the CLI entry point for the unified hot ingest
// benchmark. Returns the process exit code; the dispatch in main()
// calls os.Exit(rc). Returning instead of os.Exit-ing inline lets all
// deferred cleanup (BSB datastore Close, ingester Closes, memprofile
// write) run on every exit path.
func cmdHotIngest() int {
	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	ctx, deps, cleanup, err := buildHotDeps(logger)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		logger.Errorf("setup: %v", err)
		return 1
	}

	if err := runHot(ctx, logger, deps); err != nil {
		logger.Errorf("%v", err)
		return 1
	}
	return 0
}

// hotDeps is the fully-resolved configuration the hot driver runs on.
// Built once from CLI flags + opened resources, then passed to runHot
// so the driver loop is testable in isolation with a fake backend.
type hotDeps struct {
	stream     ledgerbackend.LedgerStream
	chunkID    chunk.ID
	xdrViews   bool
	parallel   bool
	ings       []Ingester
	ledColl    *LedgerCollector
	thxColl    *TxhashCollector
	evtColl    *EventsCollector
	outDir     string
	mode       string // "view" or "parsed"
	cpuProfile string
	memProfile string
}

// buildHotDeps parses flags, opens the source backend, opens the
// per-type hot stores, and returns the assembled hotDeps plus a
// cleanup func that releases the source backend (the ingester Closes
// are deferred inside runHot so they unwind on every return path).
func buildHotDeps(logger *supportlog.Entry) (context.Context, hotDeps, func(), error) {
	fs := flag.NewFlagSet("hot-ingest", flag.ExitOnError)
	typesArg := fs.String("types", "", "comma-separated subset of ledgers,txhash,events (required)")
	source := fs.String("source", sourcePack, "ledger source: pack | bsb")
	coldDir := fs.String("cold-dir", "", "source cold-store dir (required iff --source=pack)")
	bucketPath := fs.String("bucket-path", "sdf-ledger-close-meta/v1/ledgers/pubnet",
		"GCS destination_bucket_path (used iff --source=bsb)")
	bsbBufferSize := fs.Uint("bsb-buffer-size", 5000, "BSB prefetch buffer depth")
	bsbNumWorkers := fs.Uint("bsb-num-workers", 50, "BSB download workers")
	retryLimit := fs.Uint("retry-limit", 3, "BSB retry attempts on transient backend failure")
	retryWait := fs.Duration("retry-wait", 5*time.Second, "BSB delay between retry attempts")
	chunkArg := fs.Uint("chunk", 0, "chunk ID to ingest (required)")
	hotDir := fs.String("hot-dir", "", "fresh hot-store root (required; per-type subdirs created under it)")
	xdrViews := fs.Bool("xdr-views", false,
		"decode via XDR views (zero-copy) instead of UnmarshalBinary + struct walk")
	parallel := fs.Bool("parallel", false,
		"run enabled ingesters concurrently per ledger via errgroup. "+
			"NOTE: per-stage timings under --parallel reflect per-goroutine wall time including "+
			"scheduler contention and are not directly comparable to serial-mode timings.")
	cpuProfile := fs.String("cpuprofile", "", "write a Go CPU profile to PATH")
	memProfile := fs.String("memprofile", "", "write a Go heap profile to PATH (taken at run end before stores close)")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	enabled, err := parseTypes(*typesArg, []string{"ledgers", "txhash", "events"})
	if err != nil {
		return nil, hotDeps{}, nil, err
	}
	if *chunkArg == 0 {
		return nil, hotDeps{}, nil, errors.New("--chunk is required")
	}
	if *hotDir == "" {
		return nil, hotDeps{}, nil, errors.New("--hot-dir is required")
	}
	chunkID := chunk.ID(uint32(*chunkArg))
	mode := modeString(*xdrViews)

	// Enforce empty/absent per-type subdir under --hot-dir.
	subdirs := map[string]string{
		"ledgers": filepath.Join(*hotDir, "ledgers"),
		"txhash":  filepath.Join(*hotDir, "txhash"),
		"events":  filepath.Join(*hotDir, "events"),
	}
	for t := range enabled {
		if entries, derr := os.ReadDir(subdirs[t]); derr == nil && len(entries) > 0 {
			return nil, hotDeps{}, nil, fmt.Errorf("%s subdir %s is not empty; pick a fresh path", t, subdirs[t])
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	// Open the single-chunk ledger stream. Hot ingest is single-chunk; the
	// stream owns its own setup + teardown, so there is nothing to close here
	// beyond the ingesters.
	stream, err := openChunkStream(*source, *coldDir, *bucketPath,
		BSBOpts{BufferSize: *bsbBufferSize, NumWorkers: *bsbNumWorkers, RetryLimit: *retryLimit, RetryWait: *retryWait},
		chunkID)
	if err != nil {
		cancel()
		return nil, hotDeps{}, nil, fmt.Errorf("open source (%s): %w", *source, err)
	}

	// Open per-type hot ingesters + their collectors.
	n := int(chunkID.LastLedger() - chunkID.FirstLedger() + 1)
	var (
		ings    []Ingester
		ledColl *LedgerCollector
		thxColl *TxhashCollector
		evtColl *EventsCollector
	)
	closeOnErr := func() {
		for _, ing := range ings {
			_ = ing.Close()
		}
		cancel()
	}
	for _, t := range canonicalIngestTypes() {
		if !enabled[t] {
			continue
		}
		switch t {
		case "ledgers":
			ledColl = NewLedgerCollector(n)
			ing, ierr := NewLedgersHot(ledColl, subdirs["ledgers"], logger)
			if ierr != nil {
				closeOnErr()
				return nil, hotDeps{}, nil, fmt.Errorf("open LedgersHot: %w", ierr)
			}
			ings = append(ings, ing)
		case "txhash":
			thxColl = NewTxhashCollector(n)
			ing, ierr := NewTxhashHot(thxColl, subdirs["txhash"], logger, *xdrViews)
			if ierr != nil {
				closeOnErr()
				return nil, hotDeps{}, nil, fmt.Errorf("open TxhashHot: %w", ierr)
			}
			ings = append(ings, ing)
		case "events":
			evtColl = NewEventsCollector(n)
			ing, ierr := NewEventsHot(evtColl, subdirs["events"], chunkID, logger, *xdrViews)
			if ierr != nil {
				closeOnErr()
				return nil, hotDeps{}, nil, fmt.Errorf("open EventsHot: %w", ierr)
			}
			ings = append(ings, ing)
		}
	}

	deps := hotDeps{
		stream: stream, chunkID: chunkID,
		xdrViews: *xdrViews, parallel: *parallel,
		ings: ings, ledColl: ledColl, thxColl: thxColl, evtColl: evtColl,
		outDir: *outDir, mode: mode,
		cpuProfile: *cpuProfile, memProfile: *memProfile,
	}

	cleanup := func() {
		// ingester Closes happen inside runHot's defer chain; the stream owns
		// its own teardown, so there's nothing else to close here.
		cancel()
	}
	return ctx, deps, cleanup, nil
}

// runHot is the test-friendly driver loop. Takes a fully-built hotDeps;
// performs the per-ledger fan-out (serial or parallel), then reports
// per-type summaries + driver-level metrics. Errors from Ingest fail
// the run fast; Close errors are joined into the returned error via
// errors.Join in the deferred Close.
func runHot(ctx context.Context, logger *supportlog.Entry, d hotDeps) (err error) {
	if stop := maybeStartCPUProfile(logger, d.cpuProfile); stop != nil {
		defer stop()
	}

	// Defer order matters: close ingesters first (releases stores +
	// removes any partial files for cold writers), then write
	// memprofile last so the heap snapshot is taken BEFORE allocations
	// are freed by Close. defer is LIFO, so register memprofile LAST.
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

	logger.Infof("hot-ingest chunk=%d mode=%s parallel=%v out=%s",
		uint32(d.chunkID), d.mode, d.parallel, d.outDir)

	first, last := d.chunkID.FirstLedger(), d.chunkID.LastLedger()
	n := int(last - first + 1)
	needsLCM := !d.xdrViews && (d.thxColl != nil || d.evtColl != nil)
	dm := newDriverMetrics(n, needsLCM)
	loopStart := time.Now()
	// Stream raw bytes from the backend. raw is BORROWED (valid only until the
	// next yield); each ingester copies what it retains, and in --parallel mode
	// g.Wait() ensures all ingesters are done before the next yield. readBlocked
	// measures the wait between yields; totalPerLedger covers read + build + fan.
	seq := first
	tRead := time.Now()
	for raw, serr := range d.stream.RawLedgers(ctx, ledgerbackend.BoundedRange(first, last)) {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if serr != nil {
			return fmt.Errorf("RawLedgers(%d): %w", seq, serr)
		}
		dm.readBlocked = append(dm.readBlocked, time.Since(tRead))
		l, lerr := buildLedger(seq, raw, d.xdrViews, needsLCM, dm)
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
		dm.totalPerLedger = append(dm.totalPerLedger, time.Since(tRead))
		seq++
		tRead = time.Now()
	}

	// Wall time is the per-ledger loop, which now includes range preparation
	// (the stream prepares on the first pull). Throughput is computed
	// against this total to give an honest end-to-end rate.
	wall := time.Since(loopStart)

	if err := reportHot(logger, d, dm, wall); err != nil {
		return fmt.Errorf("report: %w", err)
	}
	return nil
}

// reportHot prints summaries to stdout and writes per-type + driver
// CSVs. wall is the elapsed driver-loop time (end-to-end); inPipe is
// the sum of fanOutPerLedger (per-ingester work, excluding source
// read + decode).
func reportHot(logger *supportlog.Entry, d hotDeps, dm *driverMetrics, wall time.Duration) error {
	w := os.Stdout
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Per-stage percentile summary:")

	if d.ledColl != nil {
		d.ledColl.PrintSummary("hot", w)
		printThroughput(w, "hot.ledgers", len(d.ledColl.samples), wall, d.ledColl.InPipelineTime())
		if cerr := d.ledColl.WriteCSV(d.outDir, "hot-ledgers-"+d.mode); cerr != nil {
			return cerr
		}
	}
	if d.thxColl != nil {
		d.thxColl.PrintSummary("hot", w)
		printThroughput(w, "hot.txhash", d.thxColl.TotalItems(), wall, d.thxColl.InPipelineTime())
		if cerr := d.thxColl.WriteCSV(d.outDir, "hot-txhash-"+d.mode); cerr != nil {
			return cerr
		}
	}
	if d.evtColl != nil {
		d.evtColl.PrintSummary("hot", w)
		printThroughput(w, "hot.events", d.evtColl.TotalItems(), wall, d.evtColl.InPipelineTime())
		if cerr := d.evtColl.WriteCSV(d.outDir, "hot-events-"+d.mode); cerr != nil {
			return cerr
		}
	}
	if err := dm.report(w, d.outDir, "hot-driver-"+d.mode); err != nil {
		return err
	}
	logger.Infof("wrote CSVs to %s", d.outDir)
	return nil
}

// ───────────────────────── driver metrics ─────────────────────────

// driverMetrics is the unexported, driver-scoped metric set. It holds
// per-ledger costs that don't belong to any single ingester — source
// read time, the (parsed-only) UnmarshalBinary cost, the per-ledger
// fan-out wall time, and the full per-ledger wall time (read + decode
// + fan-out summed into a single sample so users can see per-ledger
// total latency distribution without joining across stages).
type driverMetrics struct {
	readBlocked     []time.Duration
	lcmDecode       []time.Duration // populated iff parsed mode
	fanOutPerLedger []time.Duration
	totalPerLedger  []time.Duration // wall time per ledger including source read + decode + fan-out (hot only)
	chunkWall       []time.Duration // wall time per chunk; cold-only, len == numChunks
}

func newDriverMetrics(n int, needsLCM bool) *driverMetrics {
	dm := &driverMetrics{
		readBlocked:     make([]time.Duration, 0, n),
		fanOutPerLedger: make([]time.Duration, 0, n),
		totalPerLedger:  make([]time.Duration, 0, n),
	}
	if needsLCM {
		dm.lcmDecode = make([]time.Duration, 0, n)
	}
	return dm
}

func (dm *driverMetrics) report(w io.Writer, outDir, filenamePrefix string) error {
	printStageSummary(w, "driver.read_blocked", filterNonzero(dm.readBlocked), len(dm.readBlocked))
	if dm.lcmDecode != nil {
		printStageSummary(w, "driver.lcm_decode", filterNonzero(dm.lcmDecode), len(dm.lcmDecode))
	}
	printStageSummary(w, "driver.fan_out", filterNonzero(dm.fanOutPerLedger), len(dm.fanOutPerLedger))
	if len(dm.totalPerLedger) > 0 {
		printStageSummary(w, "driver.total_per_ledger", filterNonzero(dm.totalPerLedger), len(dm.totalPerLedger))
	}
	if len(dm.chunkWall) > 0 {
		printChunkScalar(w, "driver.chunk_wall", dm.chunkWall)
	}
	rows := []stageRow{
		{name: "read_blocked", durs: filterNonzero(dm.readBlocked), items: len(dm.readBlocked)},
		{name: "fan_out_per_ledger", durs: filterNonzero(dm.fanOutPerLedger), items: len(dm.fanOutPerLedger)},
	}
	if len(dm.totalPerLedger) > 0 {
		rows = append(rows, stageRow{name: "total_per_ledger", durs: filterNonzero(dm.totalPerLedger), items: len(dm.totalPerLedger)})
	}
	if dm.lcmDecode != nil {
		rows = append(rows, stageRow{name: "lcm_decode", durs: filterNonzero(dm.lcmDecode), items: len(dm.lcmDecode)})
	}
	if len(dm.chunkWall) > 0 {
		rows = append(rows, stageRow{name: "chunk_wall", durs: dm.chunkWall, items: len(dm.chunkWall)})
	}
	return writeStageCSV(outDir, filenamePrefix, rows)
}

// ───────────────────────── helpers ─────────────────────────

// buildLedger turns one ledger's raw bytes (streamed from the backend) into a
// Ledger and, in parsed mode when at least one ingester actually needs the
// parsed struct, unmarshals it once for sharing across all ingesters. When the
// only enabled type is "ledgers" (which writes raw bytes verbatim and ignores
// any LCM), the decode is skipped.
//
// raw is BORROWED from the stream's RawLedgers and valid only for the
// current iteration step; the resulting Ledger (and any view over raw) must be
// fully consumed before the next stream yield, which the driver guarantees.
func buildLedger(seq uint32, raw []byte, xdrViews, needsLCM bool, dm *driverMetrics) (Ledger, error) {
	if xdrViews || !needsLCM {
		return newViewLedger(seq, raw), nil
	}

	td := time.Now()
	var lcm xdr.LedgerCloseMeta
	if uerr := lcm.UnmarshalBinary(raw); uerr != nil {
		return Ledger{}, fmt.Errorf("UnmarshalBinary seq %d: %w", seq, uerr)
	}
	dm.lcmDecode = append(dm.lcmDecode, time.Since(td))
	return newParsedLedger(seq, raw, &lcm), nil
}

// modeString turns the --xdr-views bool into the filename + summary
// label suffix: "view" or "parsed".
func modeString(xdrViews bool) string {
	if xdrViews {
		return "view"
	}
	return "parsed"
}

// parseTypes splits a comma-separated --types list and validates each
// against the allowed set. Returns the set of selected types.
func parseTypes(raw string, allowed []string) (map[string]bool, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("--types is required (subset of %s)", strings.Join(allowed, ","))
	}
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, a := range allowed {
		allowedSet[a] = struct{}{}
	}
	out := make(map[string]bool)
	for t := range strings.SplitSeq(raw, ",") {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if _, ok := allowedSet[t]; !ok {
			return nil, fmt.Errorf("--types: unknown type %q (allowed: %s)", t, strings.Join(allowed, ","))
		}
		out[t] = true
	}
	if len(out) == 0 {
		return nil, errors.New("--types: empty list")
	}
	return out, nil
}

// canonicalIngestTypes is the construction-order list used by both
// drivers. Determines the order ingesters appear in the per-ledger
// fan-out and the order summary blocks are printed.
func canonicalIngestTypes() []string { return []string{"ledgers", "txhash", "events"} }

// sumDur sums a slice of durations.
func sumDur(durs []time.Duration) time.Duration {
	var s time.Duration
	for _, d := range durs {
		s += d
	}
	return s
}

// filterNonzero returns the subset of durs that are strictly > 0.
// Used to exclude empty-ledger samples from percentile distributions
// (matches old per-type bench semantics).
func filterNonzero(durs []time.Duration) []time.Duration {
	out := make([]time.Duration, 0, len(durs))
	for _, d := range durs {
		if d > 0 {
			out = append(out, d)
		}
	}
	return out
}

// ───────────────────────── profiling ─────────────────────────

// maybeStartCPUProfile opens the profile file and starts CPU profiling.
// Returns nil if no path was supplied. The returned func is the
// teardown the caller should defer.
func maybeStartCPUProfile(logger *supportlog.Entry, path string) func() {
	if path == "" {
		return nil
	}
	f, err := os.Create(path)
	if err != nil {
		logger.Errorf("cpuprofile: create %s: %v", path, err)
		return nil
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		logger.Errorf("cpuprofile: start: %v", err)
		return nil
	}
	return func() {
		pprof.StopCPUProfile()
		_ = f.Close()
	}
}

// writeMemProfile takes a heap snapshot and writes it to path. Calls
// runtime.GC() first so the dump reflects the live set rather than
// post-GC noise.
func writeMemProfile(logger *supportlog.Entry, path string) {
	f, err := os.Create(path)
	if err != nil {
		logger.Errorf("memprofile: create %s: %v", path, err)
		return
	}
	defer f.Close()
	runtime.GC()
	if err := pprof.WriteHeapProfile(f); err != nil {
		logger.Errorf("memprofile: write: %v", err)
	}
}
