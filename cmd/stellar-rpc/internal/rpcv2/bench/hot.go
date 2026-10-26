package bench

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/ingest"
)

// hotOptions configures one hot-ingest benchmark run.
type hotOptions struct {
	// Source is the ledger source the loop reads from: a local pack tree or a
	// BSB datastore.
	Source sourceConfig

	// StartChunk and NumChunks give the chunk range to ingest,
	// [StartChunk, StartChunk+NumChunks). A range spanning more than one chunk
	// crosses a chunk boundary, exercising the loop's hot-DB rotation.
	StartChunk chunk.ID
	NumChunks  int

	// NumLedgers caps how many ledgers are ingested from the range's start
	// (0 = the whole range). fsync-per-ledger makes full runs slow, so a cap
	// gives a cheap smoke run without changing what is measured per ledger; a
	// cap below one chunk never reaches a boundary.
	NumLedgers uint32

	// HotRoot is the dataset root, laid out by geometry.NewLayout: the catalog
	// and the hot chunk databases. Each chunk's DB is created through the
	// production create bracket, so every run writes into empty DBs. It must be
	// missing or empty.
	HotRoot string

	// CloseInterval is the assumed time between ledger closes. When positive,
	// the run gives each ledger a due time interval apart and waits out the
	// idle gap after ingesting one, asking the steady-state question "if
	// ledgers closed every CloseInterval, could ingestion keep up?" — answered
	// by the pace_lag row. Zero (the default) ingests back-to-back and measures
	// pure catch-up throughput.
	CloseInterval time.Duration

	// OutDir receives the CSV report.
	OutDir string

	// Trace writes a per-ledger trace to OutDir/trace.csv.
	Trace bool

	// Txindex, when its BinDir is set, rebuilds a tx-hash window index back
	// to back beside the ingest loop and writes OutDir/txindex.csv.
	Txindex hotTxindexOptions
}

// validate checks the flags and chunk range before runHot touches the
// filesystem.
func (o hotOptions) validate() error {
	if err := o.Source.validate(); err != nil {
		return err
	}
	if o.HotRoot == "" {
		return errors.New("--hot-dir is required")
	}
	if err := requireEmptyDir("hot-dir", o.HotRoot); err != nil {
		return err
	}
	if o.OutDir != "" {
		if err := requireOutOutside("hot-dir", o.HotRoot, o.OutDir); err != nil {
			return err
		}
	}
	if o.NumChunks < 1 {
		return fmt.Errorf("--num-chunks must be >= 1, got %d", o.NumChunks)
	}
	if end := uint64(o.StartChunk) + uint64(o.NumChunks) - 1; end > uint64(maxChunkID) {
		return fmt.Errorf("--start-chunk=%d with --num-chunks=%d ends at chunk %d, past the last valid chunk ID %d",
			uint32(o.StartChunk), o.NumChunks, end, uint32(maxChunkID))
	}
	if o.CloseInterval < 0 {
		return fmt.Errorf("--close-interval must be >= 0, got %s", o.CloseInterval)
	}
	return o.Txindex.validate()
}

// runHot times the daemon's ingestion loop over the range's ledgers into new
// hot databases in the dataset under HotRoot, with no freeze, and writes the
// CSV report. With CloseInterval set it measures keep-up at that close cadence
// and records pace_lag.
func runHot(ctx context.Context, logger *supportlog.Entry, opts hotOptions) error {
	if err := opts.validate(); err != nil {
		return err
	}
	layout := geometry.NewLayout(opts.HotRoot)
	// Create + fsync the roots up front — the daemon's own root prep.
	if err := config.PrepareRoots(layout.CatalogPath(), layout.HotRoot()); err != nil {
		return fmt.Errorf("prepare --hot-dir roots: %w", err)
	}
	cat, err := openDatasetCatalog(layout, logger)
	if err != nil {
		return err
	}
	defer func() { _ = cat.Close() }()

	backend, release, err := openSource(ctx, opts.Source)
	if err != nil {
		return err
	}
	defer release()

	first := opts.StartChunk.FirstLedger()
	//nolint:gosec // validate() proved StartChunk+NumChunks-1 <= maxChunkID
	last := (opts.StartChunk + chunk.ID(uint32(opts.NumChunks-1))).LastLedger()
	// Overflow-safe cap: compare against the range's span rather than adding
	// a flag-supplied count to a ledger sequence.
	if span := last - first + 1; opts.NumLedgers > 0 && opts.NumLedgers < span {
		last = first + opts.NumLedgers - 1
	}

	sink := newCSVSink()
	stream, schedule := buildHotStream(backend, first, last, opts.CloseInterval)
	sink.schedule = schedule
	if opts.Trace {
		if sink.trace, err = newHotTrace(filepath.Join(opts.OutDir, traceFile)); err != nil {
			return err
		}
		// A failed run keeps the rows of the ledgers it committed.
		defer func() {
			if cerr := sink.trace.close(); cerr != nil {
				logger.Warnf("--trace: %v", cerr)
			}
		}()
	}

	wall, err := ingestRange(ctx, rpcv2.BoundedIngestConfig{
		Stream:   stream,
		Resume:   first,
		Catalog:  cat,
		Boundary: nopBoundary{},
		Logger:   logger,
		Metrics:  sink,
		Sink:     sink,
		// The daemon's loop always holds a gate.
		Gate: new(ingest.Gate),
	}, opts.Txindex, opts.OutDir)
	// The loop cannot tell a complete bounded stream from one that ran dry;
	// the sink's last-committed gauge (set once per ingested ledger) can.
	if err == nil && sink.lastCommittedSeq() != last {
		err = fmt.Errorf("stream ended at seq %d, expected through %d", sink.lastCommittedSeq(), last)
	}
	if err != nil {
		writePartialCSVs(logger, sink, opts.OutDir)
		return err
	}
	sink.observe(fileDriver, driverRunWall, wall, int(last-first+1))
	if err := finishIngest(cat, opts.StartChunk, logger); err != nil {
		return err
	}
	sink.logSummary(logger)
	written, err := sink.writeCSVs(opts.OutDir)
	if err != nil {
		return err
	}
	logger.Infof("wrote %d CSVs to %s", len(written), opts.OutDir)
	return nil
}

// ingestRange runs the bounded ingestion loop and returns its wall time. With
// txindex.BinDir set, the txindex builds run beside the loop and txindexFile
// is written under outDir: the build in flight when the loop returns is
// canceled and waited for, and a failed build stops the loop and is returned.
func ingestRange(
	ctx context.Context, cfg rpcv2.BoundedIngestConfig, txindex hotTxindexOptions, outDir string,
) (time.Duration, error) {
	g, gctx := errgroup.WithContext(ctx)
	buildCtx, stopBuilds := context.WithCancel(gctx)
	var rows [][]string
	if txindex.BinDir != "" {
		g.Go(func() error {
			var err error
			rows, err = runTxindexBuilds(buildCtx, txindex, cfg.Logger, cfg.Gate, cfg.Metrics)
			return err
		})
	}
	start := time.Now()
	err := rpcv2.RunBoundedIngestionLoop(gctx, cfg)
	wall := time.Since(start)
	stopBuilds()
	if berr := g.Wait(); berr != nil {
		err = berr
	}
	if txindex.BinDir != "" {
		err = errors.Join(err, writeCSVTable(filepath.Join(outDir, txindexFile), txindexHeader, rows))
	}
	return wall, err
}

// buildHotStream wraps backend for a bounded hot run. With closeInterval == 0
// it returns the plain boundedStream and a nil schedule — the back-to-back
// path. With closeInterval > 0 it composes a pacingStream over the
// boundedStream so each ledger yields no sooner than its due time, and returns
// the paceSchedule the sink measures pace_lag against.
func buildHotStream(
	backend ledgerbackend.LedgerStream, first, last uint32, closeInterval time.Duration,
) (ledgerbackend.LedgerStream, *paceSchedule) {
	bounded := boundedStream{inner: backend, first: first, last: last}
	if closeInterval <= 0 {
		return bounded, nil
	}
	schedule := newPaceSchedule(closeInterval, first)
	return pacingStream{inner: bounded, schedule: schedule, sleep: contextSleep}, schedule
}

// boundedStream pins the range a LedgerStream serves. The ingestion loop always
// requests an unbounded range, so the bench wraps its source to serve only
// [first, last]; the stream then ends after last, which is what stops the loop.
type boundedStream struct {
	inner       ledgerbackend.LedgerStream
	first, last uint32
}

// RawLedgers serves inner's ledgers clamped to [first, last], ignoring the
// range the loop asks for.
func (b boundedStream) RawLedgers(
	ctx context.Context, _ ledgerbackend.Range, opts ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return b.inner.RawLedgers(ctx, ledgerbackend.BoundedRange(b.first, b.last), opts...)
}

// nopBoundary discards the ingestion loop's boundary publications: a bounded
// bench run has no lifecycle to hand completed chunks to, so nothing is handed
// off to a freeze — keeping the hot measurement isolated from the cold path.
type nopBoundary struct{}

func (nopBoundary) Publish() {}
