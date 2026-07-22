package bench

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"os"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/spf13/cobra"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
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

	// HotRoot is the scratch root the hot RocksDBs are created under, at
	// geometry.NewLayout(HotRoot).HotChunkPath(chunk). Each chunk's DB is
	// opened through the production create bracket, which wipes any leftover
	// dir, so every run starts from an empty DB (hot timings are only
	// comparable from a fixed starting state).
	HotRoot string

	// CatalogDir is the base dir the run-scoped scratch catalog is created
	// under. Empty means HotRoot.
	CatalogDir string

	// CloseInterval is the assumed time between ledger closes. When positive,
	// the run gives each ledger a due time interval apart and waits out the
	// idle gap after ingesting one, asking the steady-state question "if
	// ledgers closed every CloseInterval, could ingestion keep up?" — answered
	// by the pace_lag row. Zero (the default) ingests back-to-back and measures
	// pure catch-up throughput.
	CloseInterval time.Duration

	// OutDir receives the CSV report.
	OutDir string
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
	return nil
}

// runHot benchmarks the hot path: the daemon's ingestion loop (via
// fullhistory.RunBoundedIngestionLoop) over the range's ledgers, into fresh hot
// DBs opened through a scratch catalog. A no-op boundary discards completed
// chunks so no cold-path freeze runs, isolating the hot measurement. The sink
// collects the loop's per-phase HotPhase timings; on success runHot records the
// whole-run wall-clock and writes the CSV report.
//
// With --close-interval (opts.CloseInterval) set, the source is paced to that
// close cadence: the run measures steady-state keep-up rather than catch-up
// throughput, recording per-ledger pace_lag.
func runHot(ctx context.Context, logger *supportlog.Entry, cmd *cobra.Command, opts hotOptions) error {
	startedAt := time.Now().UTC()
	if err := opts.validate(); err != nil {
		return err
	}
	// Surface an unwritable --out before the expensive run, not after it.
	if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
		return fmt.Errorf("create --out dir %s: %w", opts.OutDir, err)
	}
	layout := geometry.NewLayout(opts.HotRoot)
	// Create + fsync the hot root up front — the daemon's own root prep.
	if err := config.PrepareRoots(layout.HotRoot()); err != nil {
		return fmt.Errorf("prepare --hot-dir hot root: %w", err)
	}
	catalogBase := opts.CatalogDir
	if catalogBase == "" {
		catalogBase = opts.HotRoot
	}
	cat, releaseCat, err := openScratchCatalog(catalogBase, layout, logger)
	if err != nil {
		return err
	}
	defer releaseCat()

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

	start := time.Now()
	err = fullhistory.RunBoundedIngestionLoop(ctx, fullhistory.BoundedIngestConfig{
		Stream:   stream,
		Resume:   first,
		Catalog:  cat,
		Boundary: nopBoundary{},
		Logger:   logger,
		Metrics:  sink,
		Sink:     sink,
	})
	// VmHWM never decreases, so it can be read right here — before the
	// completion check — and a failed run's partial CSV still gets the row.
	recordPeakRSS(logger, sink, readPeakRSS)
	// The loop cannot tell a complete bounded stream from one that ran dry;
	// the sink's last-committed gauge (set once per ingested ledger) can.
	if err == nil && sink.lastCommittedSeq() != last {
		err = fmt.Errorf("stream ended at seq %d, expected through %d", sink.lastCommittedSeq(), last)
	}
	if err != nil {
		writePartialCSVs(logger, sink, opts.OutDir)
		return err
	}
	sink.observe(fileDriver, driverRunWall, time.Since(start), int(last-first+1))
	sink.logSummary(logger)
	written, err := sink.writeCSVs(opts.OutDir)
	if err != nil {
		return err
	}
	if err := writeInvocationJSON(opts.OutDir, cmd, captureFlags(cmd), startedAt, time.Now().UTC()); err != nil {
		return err
	}
	logger.Infof("wrote %d CSVs to %s", len(written), opts.OutDir)
	return nil
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

func (nopBoundary) Publish(chunk.ID) {}
