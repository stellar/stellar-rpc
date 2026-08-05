package bench

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
)

// freezeOptions configures one freeze benchmark run.
type freezeOptions struct {
	// Source is the ledger source the populate phase reads from. Unused with
	// ReuseHot (the freeze itself never touches a ledger source — that is the
	// point of the measurement).
	Source sourceConfig

	// Chunk is the single chunk to freeze.
	Chunk chunk.ID

	// WorkRoot is the ONE layout root for both tiers: the hot DB lives at
	// geometry.NewLayout(WorkRoot).HotChunkPath(Chunk) and the cold artifacts
	// land under the same layout's cold trees — mirroring production, where
	// one Layout spans both and backfillSource finds the hot DB through the
	// catalog's own Layout.
	WorkRoot string

	// CatalogDir is the base dir the run-scoped scratch catalog is created
	// under. Empty means WorkRoot.
	CatalogDir string

	// ReuseHot skips the populate phase and adopts the hot DB a prior run
	// left in WorkRoot. Population costs a full paced-free hot ingest
	// (~minutes on a real chunk), so reuse is how freeze iterations stay
	// cheap — and it is also how a run gets a clean freeze-only RSS row and
	// CPU profile (an in-process populate contaminates both).
	ReuseHot bool

	// OutDir receives the CSV report.
	OutDir string
}

// validate checks the flags before runFreeze touches the filesystem.
func (o freezeOptions) validate() error {
	if o.WorkRoot == "" {
		return errors.New("--work-dir is required")
	}
	if o.Chunk > maxChunkID {
		return fmt.Errorf("--chunk=%d is past the last valid chunk ID %d", uint32(o.Chunk), uint32(maxChunkID))
	}
	if o.ReuseHot {
		return nil // the freeze reads no ledger source; source flags are unused
	}
	if err := o.Source.validate(); err != nil {
		return err
	}
	// Refuse re-packing a source pack tree in place, as runCold does: the
	// freeze materializes a ledger pack under WorkRoot's ledgers tree.
	if o.Source.Kind == sourcePack {
		outLedgers := geometry.NewLayout(o.WorkRoot).LedgersRoot()
		if samePath(o.Source.PackDir, outLedgers) {
			return fmt.Errorf("--work-dir's ledgers tree (%s) must differ from --pack-dir", outLedgers)
		}
	}
	return nil
}

// runFreeze benchmarks the freeze route: the daemon's hot→cold chunk freeze
// (backfill.RunBackfill resolving a COMPLETE hot DB as the source, through the
// one-write protocol). Setup first materializes that state — populate the hot
// chunk through the production bounded ingestion loop, or adopt a prior run's
// DB with ReuseHot — then the measured RunBackfill runs with NO bulk backend
// configured: backfillSource can then only resolve the hot tier, so success
// alone proves the freeze route was what got measured (the
// TestBackfillSource_HotComplete contract); any fall-through fails the run
// loudly instead of silently benchmarking a different source.
func runFreeze(ctx context.Context, logger *supportlog.Entry, opts freezeOptions) error {
	if err := opts.validate(); err != nil {
		return err
	}
	// Surface an unwritable --out before the expensive run, not after it.
	if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
		return fmt.Errorf("create --out dir %s: %w", opts.OutDir, err)
	}
	layout := geometry.NewLayout(opts.WorkRoot)
	// Create + fsync every root this run writes under — the daemon's own
	// root prep, for both tiers.
	if err := config.PrepareRoots(
		layout.HotRoot(), layout.LedgersRoot(), layout.EventsRoot(),
		layout.TxHashRawRoot(), layout.TxHashIndexRoot(),
	); err != nil {
		return fmt.Errorf("prepare --work-dir roots: %w", err)
	}
	catalogBase := opts.CatalogDir
	if catalogBase == "" {
		catalogBase = opts.WorkRoot
	}
	cat, releaseCat, err := openScratchCatalog(catalogBase, layout, logger)
	if err != nil {
		return err
	}
	defer releaseCat()

	if opts.ReuseHot {
		if err := adoptHotChunk(cat, opts.Chunk); err != nil {
			return err
		}
	} else if err := populateHotChunk(ctx, logger, cat, opts); err != nil {
		return err
	}

	sink := newCSVSink()
	start := time.Now()
	err = backfill.RunBackfill(ctx, backfill.ExecConfig{
		Catalog: cat,
		Logger:  logger,
		Metrics: sink,
		// Backend deliberately nil — see the function comment: the complete
		// hot DB must be the source, or the run fails.
		// The default encode workers on BOTH halves (the populate below and this
		// cold walk) — the format-affecting value must agree across them (see
		// hotchunk.Tuning).
		Process: backfill.ProcessConfig{Sink: sink, ZstdEncodeWorkers: ledger.DefaultZstdEncodeWorkers},
		Workers: 1,
		// Benchmarks measure one clean attempt; retries would fold failure +
		// backoff time into the samples.
		MaxRetries: 0,
	}, opts.Chunk, opts.Chunk)
	// VmHWM never decreases, so it can be read right here — before the error
	// check — and a failed run's partial CSV still gets the row.
	recordPeakRSS(logger, sink, readPeakRSS)
	if err != nil {
		writePartialCSVs(logger, sink, opts.OutDir)
		return fmt.Errorf("freeze chunk %s: %w", opts.Chunk, err)
	}
	freezeWall := time.Since(start)

	sink.logSummary(logger)
	logger.Infof(
		"freeze wall = %s (chunk %s; includes the range's txhash index build — driver.csv decomposes)",
		freezeWall.Round(time.Millisecond), opts.Chunk)
	if !opts.ReuseHot {
		logger.Info("peak_rss_bytes includes the in-process populate phase; " +
			"rerun with --reuse-hot for a freeze-only RSS row and profile")
	}
	written, err := sink.writeCSVs(opts.OutDir)
	if err != nil {
		return err
	}
	logger.Infof("wrote %d CSVs to %s", len(written), opts.OutDir)
	return nil
}

// populateHotChunk hot-ingests the chunk's full ledger range through the
// production bounded ingestion loop — create bracket, one atomic synced
// WriteBatch per ledger, boundary rotation — leaving a complete hot DB whose
// catalog key is "ready": exactly the state the daemon's lifecycle hands a
// freeze. This is setup, not measurement: it reports into a throwaway sink
// (only its last-committed gauge is read back, to verify completeness the way
// runHot does) and its wall-clock is logged separately. The boundary rotation
// also leaves the NEXT chunk's empty hot DB and "ready" key behind — inert
// scratch the single-chunk backfill range never plans.
func populateHotChunk(
	ctx context.Context, logger *supportlog.Entry, cat *catalog.Catalog, opts freezeOptions,
) error {
	backend, release, err := openSource(ctx, opts.Source)
	if err != nil {
		return err
	}
	// Released before the measured freeze runs (this function returns first),
	// so no source machinery is alive during measurement.
	defer release()

	first, last := opts.Chunk.FirstLedger(), opts.Chunk.LastLedger()
	setupSink := newCSVSink()
	start := time.Now()
	if err := rpcv2.RunBoundedIngestionLoop(ctx, rpcv2.BoundedIngestConfig{
		Stream:   boundedStream{inner: backend, first: first, last: last},
		Resume:   first,
		Catalog:  cat,
		Boundary: nopBoundary{},
		Logger:   logger,
		Metrics:  setupSink,
		Sink:     setupSink,
		Tuning:   hotchunk.DefaultTuning(),
	}); err != nil {
		return fmt.Errorf("populate hot chunk %s: %w", opts.Chunk, err)
	}
	// The loop cannot tell a complete bounded stream from one that ran dry.
	if got := setupSink.lastCommittedSeq(); got != last {
		return fmt.Errorf("populate ended at seq %d, expected through %d", got, last)
	}
	logger.Infof("populated hot chunk %s (%d ledgers) in %s — setup, excluded from the freeze measurement",
		opts.Chunk, last-first+1, time.Since(start).Round(time.Millisecond))
	return nil
}

// adoptHotChunk points the fresh scratch catalog at a hot DB a prior run left
// in the work dir (ReuseHot): verify the dir exists, flip its key "ready" —
// the durable state a production restart would find. It deliberately never
// opens or wipes the DB: completeness is judged by the freeze's own
// tryHotSource gate, and with no bulk backend configured an incomplete DB
// fails the run loudly rather than being silently re-derived.
func adoptHotChunk(cat *catalog.Catalog, c chunk.ID) error {
	dir := cat.Layout().HotChunkPath(c)
	if _, err := os.Stat(dir); err != nil {
		return fmt.Errorf("--reuse-hot: no hot DB at %s (run once without --reuse-hot to populate): %w", dir, err)
	}
	return cat.FlipHotReady(c)
}
