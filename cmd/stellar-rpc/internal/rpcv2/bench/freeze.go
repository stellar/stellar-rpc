package bench

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
)

// freezeOptions configures one freeze benchmark: the daemon's backfill
// freezing one complete hot chunk.
type freezeOptions struct {
	// HotRoot is a dataset root bench ingest hot left Chunk complete in. The
	// run reads Chunk's hot DB, and its builds use the hot root's scratch
	// directory.
	HotRoot string
	Chunk   chunk.ID

	// ColdRoot is the dataset root for this run's catalog and the cold
	// artifacts. It must be missing or empty.
	ColdRoot string

	// OutDir receives the CSV report.
	OutDir string
}

func (o freezeOptions) validate() error {
	if o.HotRoot == "" {
		return errors.New("--hot-dir is required")
	}
	if o.ColdRoot == "" {
		return errors.New("--cold-out-dir is required")
	}
	if o.Chunk > maxChunkID {
		return fmt.Errorf("--chunk=%d is past the last valid chunk ID %d", uint32(o.Chunk), uint32(maxChunkID))
	}
	if err := requireEmptyDir("cold-out-dir", o.ColdRoot); err != nil {
		return err
	}
	if o.OutDir != "" {
		return requireOutOutside("cold-out-dir", o.ColdRoot, o.OutDir)
	}
	return nil
}

// runFreeze freezes Chunk's hot DB under HotRoot into ColdRoot and writes the
// CSV report. Its catalog is a copy of the hot dataset's, so it has the hot
// chunk's "ready" key and the secret the hot keys are blinded with, and the
// backfill runs with no bulk backend, so the hot DB is the only source it can
// use: a run that succeeds measured the freeze from the hot DB. The hot DB
// opens read-only, so one populated chunk serves any number of runs.
func runFreeze(ctx context.Context, logger *supportlog.Entry, opts freezeOptions) error {
	if err := opts.validate(); err != nil {
		return err
	}
	hot, cold := geometry.NewLayout(opts.HotRoot), geometry.NewLayout(opts.ColdRoot)
	if _, err := os.Stat(hot.HotChunkPath(opts.Chunk)); err != nil {
		return fmt.Errorf("--hot-dir has no hot DB for chunk %s; populate it with bench ingest hot: %w", opts.Chunk, err)
	}
	if err := os.CopyFS(cold.CatalogPath(), os.DirFS(hot.CatalogPath())); err != nil {
		return fmt.Errorf("copy the --hot-dir catalog: %w", err)
	}
	layout := geometry.NewLayoutFromRoots(cold.CatalogPath(), hot.HotRoot(), cold.LedgersRoot(), cold.EventsRoot(),
		cold.EventsIndexRoot(), cold.TxHashRawRoot(), cold.TxHashIndexRoot())
	if err := config.PrepareRoots(layout.CatalogPath(), layout.LedgersRoot(), layout.EventsRoot(),
		layout.EventsIndexRoot(), layout.TxHashRawRoot(), layout.TxHashIndexRoot()); err != nil {
		return fmt.Errorf("prepare --cold-out-dir write roots: %w", err)
	}
	cat, err := openDatasetCatalog(layout, logger)
	if err != nil {
		return err
	}
	defer func() { _ = cat.Close() }()

	sink := newCSVSink()
	start := time.Now()
	err = backfill.RunBackfill(ctx, backfill.ExecConfig{
		Catalog: cat,
		Logger:  logger,
		Metrics: sink,
		Process: backfill.ProcessConfig{Sink: sink},
		Workers: 1,
		// One clean attempt: a retry would fold failure and backoff into the
		// samples.
		MaxRetries: 0,
	}, opts.Chunk, opts.Chunk)
	if err != nil {
		writePartialCSVs(logger, sink, opts.OutDir)
		return fmt.Errorf("freeze chunk %s: %w", opts.Chunk, err)
	}
	logger.Infof("freeze wall = %s (chunk %s, with its tx-hash window index)",
		time.Since(start).Round(time.Millisecond), opts.Chunk)
	sink.logSummary(logger)
	written, err := sink.writeCSVs(opts.OutDir)
	if err != nil {
		return err
	}
	logger.Infof("wrote %d CSVs to %s", len(written), opts.OutDir)
	return nil
}
