package bench

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/adapters"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

// Serving a prepared dataset over HTTP, for read benchmarks against a fixed
// corpus.
//
// The problem this solves is that a cold artifact tree is not self-describing.
// Routing reads nothing but catalog keys — ReadView.resolveTier serves a chunk
// only when its key says "frozen" (cold) or "ready" with a published handle
// (hot) — and the catalog never lists a directory. So a pack tree with no
// catalog serves nothing, however complete it is on disk. That is the state
// every published dataset is in: `bench-ingest cold` writes its artifacts to
// --cold-out-dir but builds its catalog in a temp dir it deletes on the way out
// (openScratchCatalog), so the artifacts outlive the only record of them.
//
// ADOPTION is the fix: walk the artifacts that exist, then write the keys that
// make them reachable, into a catalog that persists. It is the inverse of the
// one-write protocol — instead of marking then writing a file, it observes a
// file then marks it — and it is sound for the same reason the protocol is: a
// "frozen" key asserts the file is complete and durable, which for an artifact
// some earlier run already fsynced is true.
//
// This never fabricates data. Every key it writes names a file it stat'ed
// first, and a chunk missing its ledger pack fails the command rather than
// being advertised as servable.

// optionalChunk is a chunk ID that may be absent.
//
// The zero value is ABSENT, deliberately. A bare chunk.ID field cannot express
// "no chunk" — chunk 0 is a real chunk — so a sentinel like -1 has to be
// carried in a signed field, and then any struct literal that omits the field
// silently means "chunk 0". That is a trap worth closing at the type: with this
// type, forgetting the field can only ever mean "not set".
type optionalChunk struct {
	id  chunk.ID
	set bool
}

// optionalChunkFrom converts the CLI's signed form, where any negative value
// means absent.
func optionalChunkFrom(v int64) optionalChunk {
	if v < 0 {
		return optionalChunk{}
	}
	return optionalChunk{id: chunk.ID(v), set: true}
}

func (o optionalChunk) get() (chunk.ID, bool) { return o.id, o.set }
func (o optionalChunk) present() bool         { return o.set }

// serveOptions configures one bench-serve run.
type serveOptions struct {
	// ColdRoot is the cold artifact root: the tree holding ledgers/, events/
	// and txhash/, laid out by geometry.NewLayout. A published dataset's pack
	// root and `bench-ingest cold`'s --cold-out-dir are both this shape.
	ColdRoot string

	// HotRoot is the same value `bench-ingest hot` takes for --hot-dir: the
	// per-chunk DBs live one level below it, at <HotRoot>/hot/{chunk:08d}. The
	// two commands must read one flag the same way, or a run that hands the hot
	// leg's own --hot-dir straight over finds nothing. Empty means no hot tier:
	// cold chunks alone are served.
	HotRoot string

	// CatalogDir is where the adopted catalog is created. Unlike the ingest
	// benchmarks' scratch catalog this one PERSISTS — it is the artifact that
	// makes the dataset servable, so a second run over the same dataset reuses
	// it instead of re-adopting.
	CatalogDir string

	// StartChunk and NumChunks give the cold chunk range to adopt,
	// [StartChunk, StartChunk+NumChunks).
	StartChunk chunk.ID
	NumChunks  int

	// HotChunk names a pre-built hot DB under HotRoot to serve as the hot tier.
	// `bench-ingest hot` leaves exactly this: a finished DB whose catalog was
	// thrown away. It is adopted read-write (never through the create bracket,
	// which would wipe it).
	HotChunk optionalChunk

	// LatestLedger is the newest ledger reads may serve. Zero derives it from
	// the highest adopted chunk. It must be set for anything to be servable at
	// all: every range is gated against it (ReadView.ClampRange), so a registry
	// left at zero answers nothing.
	LatestLedger uint32

	// Endpoint is the host:port the read server binds.
	Endpoint string

	// NetworkPassphrase is what transaction hashes are computed against. It
	// must match the passphrase the dataset was generated under, or every
	// getTransaction lookup misses.
	NetworkPassphrase string

	// Source is the ledger source the replay leg reads from. Used only when
	// ReplayChunk is set.
	Source sourceConfig

	// ReplayChunk is the chunk to ingest live while serving; absent means a
	// static run. It turns the run into the reads-under-ingest-load
	// measurement: the daemon's real ingestion loop writes this chunk while
	// reads are served from the adopted cold chunks beside it.
	//
	// Its hot DB is created FRESH — the production open bracket wipes any
	// leftover dir — so this must not name a chunk whose DB is wanted.
	ReplayChunk optionalChunk

	// ReplayLedgers caps how many ledgers are replayed from the chunk's start
	// (0 = the whole chunk). A cap keeps a smoke run short without changing
	// what each ledger costs.
	ReplayLedgers uint32

	// CloseInterval paces the replay to a steady-state close cadence, as in
	// `bench-ingest hot`. It is what makes the ingest load realistic instead of
	// a catch-up burst, and what makes the replay last long enough to blast
	// against: 10,000 ledgers at 600ms is about 1h40m. Zero replays
	// back-to-back, which finishes fast and measures reads against a saturating
	// writer instead.
	CloseInterval time.Duration

	// OutDir receives the replay's CSV report.
	OutDir string
}

// replaying reports whether the run has a live ingest leg.
func (o serveOptions) replaying() bool { return o.ReplayChunk.present() }

func (o serveOptions) validate() error {
	switch {
	case o.ColdRoot == "":
		return errors.New("--cold-dir is required")
	case o.CatalogDir == "":
		return errors.New("--catalog-dir is required")
	case o.Endpoint == "":
		return errors.New("--endpoint is required")
	case o.NetworkPassphrase == "":
		return errors.New("--network-passphrase is required")
	case o.NumChunks < 1:
		return fmt.Errorf("--num-chunks must be >= 1, got %d", o.NumChunks)
	}
	if end := uint64(o.StartChunk) + uint64(o.NumChunks) - 1; end > uint64(maxChunkID) {
		return fmt.Errorf("--start-chunk=%d with --num-chunks=%d ends at chunk %d, past the last valid chunk ID %d",
			uint32(o.StartChunk), o.NumChunks, end, uint32(maxChunkID))
	}
	if hot, ok := o.HotChunk.get(); ok {
		if o.HotRoot == "" {
			return errors.New("--hot-chunk needs --hot-dir")
		}
		if hot > maxChunkID {
			return fmt.Errorf("--hot-chunk=%s is past the last valid chunk ID %d", hot, uint32(maxChunkID))
		}
	}
	return o.validateReplay()
}

// validateReplay checks the live-ingest leg. The two hot modes are mutually
// exclusive: --hot-chunk adopts a finished DB read-write, --replay-chunk creates
// one fresh and writes it, and allowing both would let a run silently replay
// into the DB it was asked to preserve.
func (o serveOptions) validateReplay() error {
	if !o.replaying() {
		return nil
	}
	replay, _ := o.ReplayChunk.get()
	switch {
	case o.HotChunk.present():
		return errors.New("--replay-chunk and --hot-chunk are exclusive: " +
			"the first creates a fresh hot DB, the second serves an existing one")
	case o.HotRoot == "":
		return errors.New("--replay-chunk needs --hot-dir")
	case o.OutDir == "":
		return errors.New("--replay-chunk needs --out for the replay's CSV report")
	case replay > maxChunkID:
		return fmt.Errorf("--replay-chunk=%s is past the last valid chunk ID %d", replay, uint32(maxChunkID))
	case o.CloseInterval < 0:
		return fmt.Errorf("--close-interval must be >= 0, got %s", o.CloseInterval)
	}
	if err := o.Source.validate(); err != nil {
		return err
	}
	// The replay must extend the served history, not overwrite it: a chunk
	// inside the adopted cold range already has frozen artifacts, and cold wins
	// the tier decision, so its replayed ledgers would be written and then
	// never read.
	if replay >= o.StartChunk && replay <= o.lastColdChunk() {
		return fmt.Errorf("--replay-chunk=%s is inside the adopted cold range [%s, %s]; "+
			"cold wins the tier decision, so the replayed ledgers would never be served",
			replay, o.StartChunk, o.lastColdChunk())
	}
	return nil
}

// lastColdChunk is the highest chunk in the adopted cold range.
func (o serveOptions) lastColdChunk() chunk.ID {
	//nolint:gosec // validate() proved StartChunk+NumChunks-1 <= maxChunkID
	return o.StartChunk + chunk.ID(uint32(o.NumChunks-1))
}

// highestChunk is the highest chunk the run serves from either tier — the
// anchor for the derived latest ledger and the serving frontier.
func (o serveOptions) highestChunk() chunk.ID {
	highest := o.lastColdChunk()
	if hot, ok := o.HotChunk.get(); ok && hot > highest {
		highest = hot
	}
	return highest
}

// layout binds the cold trees to ColdRoot and the hot tree to HotRoot, so a
// dataset dir and a hot dir on different filesystems can be served together.
// The catalog is its own root: it is derived state, not part of the dataset.
func (o serveOptions) layout() geometry.Layout {
	cold := geometry.NewLayout(o.ColdRoot)
	// No hot tier: derive the hot root under the catalog rather than the
	// dataset, so nothing can write into a read-only dataset dir.
	hotBase := o.HotRoot
	if hotBase == "" {
		hotBase = o.CatalogDir
	}
	return geometry.NewLayoutFromRoots(
		filepath.Join(o.CatalogDir, "rocksdb"),
		// Same derivation bench-ingest hot applies to its --hot-dir, so one
		// value works in both commands.
		geometry.NewLayout(hotBase).HotRoot(),
		cold.LedgersRoot(),
		cold.EventsRoot(),
		cold.TxHashRawRoot(),
		cold.TxHashIndexRoot(),
	)
}

// runServe adopts the dataset into a persistent catalog, builds a serving
// registry over it, and serves reads until the context is canceled.
func runServe(ctx context.Context, logger *supportlog.Entry, opts serveOptions) error {
	if err := opts.validate(); err != nil {
		return err
	}
	layout := opts.layout()
	if err := os.MkdirAll(opts.CatalogDir, 0o755); err != nil {
		return fmt.Errorf("create --catalog-dir %s: %w", opts.CatalogDir, err)
	}
	txLayout, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	if err != nil {
		return err
	}
	cat, err := catalog.Open(layout.CatalogPath(), layout, txLayout, logger)
	if err != nil {
		return fmt.Errorf("open catalog at %s: %w", layout.CatalogPath(), err)
	}
	defer func() { _ = cat.Close() }()

	reg, err := buildServingRegistry(cat, logger, opts)
	if err != nil {
		return err
	}
	defer reg.Close()

	if !opts.replaying() {
		if err := seedCloseTimes(reg); err != nil {
			return err
		}
		return rpcv2.BenchServeReads(ctx, opts.serveConfig(reg, logger))
	}
	return runServeWithReplay(ctx, logger, cat, reg, opts)
}

// seedCloseTimes stamps both servable-window edges' close times before the port
// binds, as the daemon's startup does (adapters.SeedCloseTimes, called from
// run() before ServeReads). Without it the first request that reports a window
// edge pays one point read per edge — a cold packfile open in the common case —
// and a read benchmark would charge that one-off cost to whichever request
// happened to land first.
//
// Two orderings are load-bearing, which is why this is called per serve path
// rather than at the end of buildServingRegistry. It must run AFTER some hot
// chunk is ready: it acquires a read view, and an empty ready scan is
// ErrNoReadyHotChunk. A static run has the frontier marker by then; a replay
// run has no ready chunk until BenchOpenReplayChunk, so it seeds there. And it
// must run BEFORE a replay leg starts, because it writes the latest-ledger
// stamp itself — racing a leg that already advanced the tip would move the
// served tip backwards.
func seedCloseTimes(reg *query.Registry) error {
	if err := adapters.SeedCloseTimes(reg); err != nil {
		return fmt.Errorf("seed window close times: %w", err)
	}
	return nil
}

func (o serveOptions) serveConfig(reg *query.Registry, logger *supportlog.Entry) rpcv2.BenchServeConfig {
	return rpcv2.BenchServeConfig{
		Endpoint:          o.Endpoint,
		NetworkPassphrase: o.NetworkPassphrase,
		Registry:          reg,
		Logger:            logger,
		RetentionWindow:   0, // full history: nothing is pruned
	}
}

// runServeWithReplay serves reads while the daemon's ingestion loop writes the
// replay chunk — the reads-under-ingest-load measurement.
//
// The hot DB opens BEFORE the port binds, matching the daemon's startup order:
// the ready key that open writes is what read-view acquisition derives its
// frontier from, so binding first would leave a window where every read fails.
//
// The replay finishing does NOT end the run. A load generator is usually still
// blasting, and cutting the server down mid-run would corrupt its numbers with
// connection errors; worse, it would silently turn the tail of the measurement
// into a static run. So the leg logs loudly and serving continues until the
// operator interrupts. Size --close-interval so the replay outlasts the blast.
func runServeWithReplay(
	ctx context.Context, logger *supportlog.Entry,
	cat *catalog.Catalog, reg *query.Registry, opts serveOptions,
) error {
	if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
		return fmt.Errorf("create --out dir %s: %w", opts.OutDir, err)
	}
	backend, release, err := openSource(ctx, opts.Source)
	if err != nil {
		return err
	}
	defer release()

	replayChunk, _ := opts.ReplayChunk.get()
	first, last := replayChunk.FirstLedger(), replayChunk.LastLedger()
	// Overflow-safe cap: compare against the chunk's span rather than adding a
	// flag-supplied count to a ledger sequence.
	if span := last - first + 1; opts.ReplayLedgers > 0 && opts.ReplayLedgers < span {
		last = first + opts.ReplayLedgers - 1
	}
	hotDB, err := rpcv2.BenchOpenReplayChunk(cat, first, reg, logger)
	if err != nil {
		return err
	}
	// The replay chunk's ready key now exists, so a read view can be acquired;
	// the leg has not started, so nothing races the latest stamp.
	if err := seedCloseTimes(reg); err != nil {
		return err
	}
	logger.WithField("chunk", replayChunk.String()).
		WithField("ledgers", fmt.Sprintf("%d-%d", first, last)).
		WithField("close_interval", opts.CloseInterval.String()).
		Info("bench-serve: replaying into the hot tier while serving")

	sink := newCSVSink()
	stream, schedule := buildHotStream(backend, first, last, opts.CloseInterval)
	sink.schedule = schedule

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return rpcv2.BenchServeReads(gctx, opts.serveConfig(reg, logger))
	})
	g.Go(func() error {
		if err := runReplayLeg(gctx, logger, sink, opts, rpcv2.BenchReplayConfig{
			Stream:   stream,
			Resume:   first,
			HotDB:    hotDB,
			Catalog:  cat,
			Registry: reg,
			Logger:   logger,
			Metrics:  sink,
			Sink:     sink,
		}, last); err != nil {
			return err
		}
		return keepServingAfterReplay(gctx, cat, reg, logger, first)
	})
	return g.Wait()
}

// keepServingAfterReplay reopens the replayed chunk so reads survive the leg
// ending.
//
// The ingestion loop closes its write handle on the way out — correct for the
// daemon, where the loop stopping means the process is going down. Here serving
// deliberately outlives the leg, and the registry still points at that closed
// handle, so every read of the replayed chunk would fail as
// temporarily-unavailable. Reopening republishes a live handle over the same
// now-complete DB; the close flushed it, so nothing is missing.
func keepServingAfterReplay(
	ctx context.Context, cat *catalog.Catalog, reg *query.Registry,
	logger *supportlog.Entry, resume uint32,
) error {
	if ctx.Err() != nil {
		return nil // interrupted: the run is ending, not continuing
	}
	if _, err := rpcv2.BenchOpenReplayChunk(cat, resume, reg, logger); err != nil {
		return fmt.Errorf("reopen replayed chunk for continued serving: %w", err)
	}
	logger.Warn("bench-serve: replay finished; STILL SERVING the now-static dataset. " +
		"Reads from here on are NOT under ingest load — stop the load run, or interrupt to exit.")
	<-ctx.Done()
	return nil
}

// runReplayLeg runs the ingest leg and writes its CSV report. A replay that
// ends short of `last` is reported as an error: it means the source ran dry, so
// the ingest load stopped partway and the read numbers cover a window nobody
// intended.
func runReplayLeg(
	ctx context.Context, logger *supportlog.Entry, sink *csvSink,
	opts serveOptions, cfg rpcv2.BenchReplayConfig, last uint32,
) error {
	start := time.Now()
	err := rpcv2.BenchReplayIntoRegistry(ctx, cfg)
	// VmHWM never decreases, so a failed leg's partial CSV still gets the row.
	recordPeakRSS(logger, sink, readPeakRSS)
	// An interrupt is the expected way a serve run ends; the leg being cut
	// short by it is not a failure.
	if err == nil && ctx.Err() == nil && sink.lastCommittedSeq() != last {
		err = fmt.Errorf("replay stream ended at seq %d, expected through %d", sink.lastCommittedSeq(), last)
	}
	if err != nil {
		writePartialCSVs(logger, sink, opts.OutDir)
		return err
	}
	sink.observe(fileDriver, driverRunWall, time.Since(start), int(last-cfg.Resume+1))
	sink.logSummary(logger)
	written, werr := sink.writeCSVs(opts.OutDir)
	if werr != nil {
		return werr
	}
	logger.Infof("wrote %d CSVs to %s", len(written), opts.OutDir)
	return nil
}

// buildServingRegistry adopts the dataset's artifacts into cat and returns a
// registry serving them. The caller closes the registry.
func buildServingRegistry(
	cat *catalog.Catalog, logger *supportlog.Entry, opts serveOptions,
) (*query.Registry, error) {
	first, last := opts.StartChunk, opts.lastColdChunk()
	if err := adoptColdChunks(cat, logger, first, last); err != nil {
		return nil, err
	}
	if err := adoptTxHashIndexes(cat, logger, first, last); err != nil {
		return nil, err
	}

	// Retention is full history anchored at the first adopted chunk, so the
	// servable window starts exactly where the dataset does.
	reg := query.NewRegistry(cat, geometry.NewRetention(0, first))
	if err := adoptHotChunk(cat, reg, logger, opts); err != nil {
		reg.Close()
		return nil, err
	}
	// A replay needs no frontier marker: the chunk it ingests becomes the ready
	// live chunk itself, which is exactly what the marker stands in for. Adding
	// one above it would also claim the still-ingesting chunk as complete.
	if !opts.replaying() {
		if err := markServingFrontier(cat, logger, opts.highestChunk()+1); err != nil {
			reg.Close()
			return nil, err
		}
	}

	latest := opts.LatestLedger
	if latest == 0 {
		latest = opts.highestChunk().LastLedger()
	}
	// Close time 0 means "unknown": the adapters fall back to a point read for
	// the tip's timestamp, which is correct here because no ingestion stamped
	// one.
	reg.SetLatestLedger(latest, 0)
	logger.WithField("oldest_ledger", first.FirstLedger()).
		WithField("latest_ledger", latest).
		Info("bench-serve: serving window")
	return reg, nil
}

// adoptColdChunks writes the frozen keys that make each chunk's on-disk cold
// artifacts reachable. Only kinds whose files are all present are marked, so a
// ledgers-only dataset serves ledgers and reports no events rather than
// erroring inside a read.
//
// The tx-hash .bin runs (KindTxHash) are deliberately never adopted: no read
// path opens them — by-hash lookups go through the .idx window indexes — and a
// finished dataset's .bins are demoted at index build time anyway.
func adoptColdChunks(cat *catalog.Catalog, logger *supportlog.Entry, first, last chunk.ID) error {
	for c := first; c <= last; c++ {
		kinds, err := coldKindsPresent(cat.Layout(), c)
		if err != nil {
			return err
		}
		if len(kinds) == 0 {
			return fmt.Errorf("chunk %s has no ledger pack at %s: is --cold-dir the dataset's pack root?",
				c, cat.Layout().LedgerPackPath(c))
		}
		if err := cat.MarkChunkFreezing(c, kinds...); err != nil {
			return fmt.Errorf("mark chunk %s freezing: %w", c, err)
		}
		if err := cat.FlipChunkFrozen(c, kinds...); err != nil {
			return fmt.Errorf("flip chunk %s frozen: %w", c, err)
		}
		logger.WithField("chunk", c.String()).
			WithField("kinds", kindNames(kinds)).
			WithField("ledgers", fmt.Sprintf("%d-%d", c.FirstLedger(), c.LastLedger())).
			Info("bench-serve: adopted cold chunk")
	}
	return nil
}

// coldKindsPresent returns the artifact kinds whose every file exists for chunk
// c. A ledger pack is mandatory — without it the chunk has no ledgers to serve
// and the caller rejects the range — so an empty result means "not a chunk".
func coldKindsPresent(layout geometry.Layout, c chunk.ID) ([]geometry.Kind, error) {
	ok, err := allExist(layout.ArtifactPaths(c, geometry.KindLedgers))
	if err != nil || !ok {
		return nil, err
	}
	kinds := []geometry.Kind{geometry.KindLedgers}
	ok, err = allExist(layout.ArtifactPaths(c, geometry.KindEvents))
	if err != nil {
		return nil, err
	}
	if ok {
		kinds = append(kinds, geometry.KindEvents)
	}
	return kinds, nil
}

// allExist reports whether every path is present. A stat error other than
// not-exist is returned: an unreadable artifact must not be silently treated as
// absent.
func allExist(paths []string) (bool, error) {
	for _, p := range paths {
		switch _, err := os.Stat(p); {
		case err == nil:
		case errors.Is(err, fs.ErrNotExist):
			return false, nil
		default:
			return false, fmt.Errorf("stat artifact %s: %w", p, err)
		}
	}
	return len(paths) > 0, nil
}

func kindNames(kinds []geometry.Kind) string {
	names := make([]string, len(kinds))
	for i, k := range kinds {
		names[i] = string(k)
	}
	return strings.Join(names, ",")
}

// adoptTxHashIndexes registers the on-disk .idx window indexes the adopted
// chunk range needs for by-hash lookups. The coverage a key must carry is the
// one encoded in the FILE NAME ({lo:08d}-{hi:08d}.idx), because that is the
// name the read path composes when it opens the index — so the names on disk
// are the authority, not the chunk range the caller asked for.
//
// This is the one place that lists a directory. Nothing else can: the index's
// coverage is chosen by whichever build produced it, so it is not derivable
// from the chunk range alone.
func adoptTxHashIndexes(cat *catalog.Catalog, logger *supportlog.Entry, first, last chunk.ID) error {
	idxLayout := cat.TxHashIndexLayout()
	firstIdx, lastIdx := idxLayout.TxHashIndexID(first), idxLayout.TxHashIndexID(last)
	adopted := 0
	for id := firstIdx; id <= lastIdx; id++ {
		cov, found, err := widestIndexCoverage(cat.Layout().TxHashIndexDir(id), id)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if err := adoptOneTxHashIndex(cat, cov); err != nil {
			return err
		}
		adopted++
		logger.WithField("index", id.String()).
			WithField("coverage", fmt.Sprintf("%s-%s", cov.Lo, cov.Hi)).
			Info("bench-serve: adopted cold tx-hash index")
	}
	if adopted == 0 {
		logger.Warn("bench-serve: no cold tx-hash index found; getTransaction cannot resolve cold hashes")
	}
	return nil
}

func adoptOneTxHashIndex(cat *catalog.Catalog, cov geometry.TxHashIndexCoverage) error {
	marked, err := cat.MarkTxHashIndexFreezing(cov.Index, cov.Lo, cov.Hi)
	if err != nil {
		return fmt.Errorf("mark tx-hash index %s freezing: %w", cov.Index, err)
	}
	if err := cat.CommitTxHashIndex(marked); err != nil {
		return fmt.Errorf("commit tx-hash index %s: %w", cov.Index, err)
	}
	return nil
}

// widestIndexCoverage picks the .idx in dir with the highest upper chunk. An
// index may hold several generations of .idx file, but at most ONE may be
// frozen at a time (the catalog asserts that invariant on every read), and the
// widest is the one a finished build left — so the others are earlier
// generations that a real daemon would have swept.
func widestIndexCoverage(dir string, id geometry.TxHashIndexID) (geometry.TxHashIndexCoverage, bool, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return geometry.TxHashIndexCoverage{}, false, nil
	}
	if err != nil {
		return geometry.TxHashIndexCoverage{}, false, fmt.Errorf("read tx-hash index dir %s: %w", dir, err)
	}
	var best geometry.TxHashIndexCoverage
	found := false
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		lo, hi, ok := parseIndexFileName(e.Name())
		if !ok {
			continue
		}
		if !found || hi > best.Hi {
			best = geometry.TxHashIndexCoverage{Index: id, Lo: lo, Hi: hi}
			found = true
		}
	}
	return best, found, nil
}

// parseIndexFileName decodes the {lo:08d}-{hi:08d}.idx leaf that
// Layout.TxHashIndexFilePath composes. Anything else in the dir is ignored.
func parseIndexFileName(name string) (lo, hi chunk.ID, ok bool) {
	base, found := strings.CutSuffix(name, ".idx")
	if !found {
		return 0, 0, false
	}
	loStr, hiStr, found := strings.Cut(base, "-")
	if !found {
		return 0, 0, false
	}
	loN, err := geometry.ParsePadded(loStr)
	if err != nil {
		return 0, 0, false
	}
	hiN, err := geometry.ParsePadded(hiStr)
	if err != nil || loN > hiN {
		return 0, 0, false
	}
	return chunk.ID(loN), chunk.ID(hiN), true
}

// adoptHotChunk publishes a pre-built hot DB as the hot tier. It flips the key
// ready and opens must-exist, never through the create bracket: BeginHotCreate
// wipes the dir, which would destroy the very DB being adopted.
func adoptHotChunk(
	cat *catalog.Catalog, reg *query.Registry, logger *supportlog.Entry, opts serveOptions,
) error {
	c, ok := opts.HotChunk.get()
	if !ok {
		return nil
	}
	dir := cat.Layout().HotChunkPath(c)
	if _, err := os.Stat(dir); err != nil {
		return fmt.Errorf("stat hot chunk dir %s: %w", dir, err)
	}
	if err := cat.FlipHotReady(c); err != nil {
		return fmt.Errorf("flip hot chunk %s ready: %w", c, err)
	}
	db, err := hotchunk.OpenReadyWrite(geometry.HotReady, dir, c, logger)
	if err != nil {
		return fmt.Errorf("open hot chunk %s at %s: %w", c, dir, err)
	}
	reg.PublishHandle(c, db)
	logger.WithField("chunk", c.String()).
		WithField("ledgers", fmt.Sprintf("%d-%d", c.FirstLedger(), c.LastLedger())).
		Info("bench-serve: adopted hot chunk")
	return nil
}

// markServingFrontier marks one chunk above the served range hot-ready WITHOUT
// a handle, which is what makes a cold-only dataset servable at all.
//
// Read-view acquisition derives its retention anchor from
// Snapshot.LastCompleteChunk, defined as "the highest ready hot chunk minus
// one" because in a running daemon the highest ready chunk is the live,
// still-ingesting one. An empty ready scan is ErrNoReadyHotChunk, and that
// error fails every read view — so a dataset with no hot tier serves nothing
// until some chunk is ready. This marker is that chunk: it stands in for the
// live chunk a daemon would have, putting the frontier exactly one above the
// data and leaving LastCompleteChunk equal to the highest adopted chunk.
//
// It is safe precisely because no handle is published for it: resolveTier needs
// a ready key AND a loaded handle to route hot, so this chunk resolves to no
// serving store. Nothing can read it, and nothing tries — it sits above the
// latest ledger, so the window gate rejects the range first.
func markServingFrontier(cat *catalog.Catalog, logger *supportlog.Entry, frontier chunk.ID) error {
	if err := cat.FlipHotReady(frontier); err != nil {
		return fmt.Errorf("mark serving frontier chunk %s: %w", frontier, err)
	}
	logger.WithField("chunk", frontier.String()).
		Info("bench-serve: marked serving frontier (no handle; stands in for the live chunk)")
	return nil
}
