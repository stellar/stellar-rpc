package bench

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
)

// maxChunkID is the last chunk ID whose LastLedger fits in a uint32 ledger
// sequence (see chunk.ID.LastLedger).
const maxChunkID = chunk.ID(math.MaxUint32/chunk.LedgersPerChunk - 1)

// coldOptions configures one cold-ingest benchmark run.
type coldOptions struct {
	// Source is the ledger source the backfill reads from: a local pack tree
	// or a BSB datastore.
	Source sourceConfig

	// StartChunk and NumChunks give the chunk range to backfill,
	// [StartChunk, StartChunk+NumChunks). The run materializes that range's
	// full artifact set: ledger packs, txhash .bins, events segments, and the
	// cross-chunk txhash index.
	StartChunk chunk.ID
	NumChunks  int

	// Workers sizes the backfill worker pool, shared by chunk freezes and
	// index builds.
	Workers int

	// ColdRoot is the output root the artifacts land under, laid out by
	// geometry.NewLayout. It is scratch: a re-run over the same range
	// overwrites freely, but artifacts from other ranges are left in place and
	// can look valid to later tooling, so point each run at a fresh dir.
	ColdRoot string

	// CatalogDir is the base dir the run-scoped scratch catalog is created
	// under. Empty means ColdRoot.
	CatalogDir string

	// OutDir receives the CSV report.
	OutDir string
}

// validate checks the flags and chunk range before runCold touches the
// filesystem.
func (o coldOptions) validate() error {
	if err := o.Source.validate(); err != nil {
		return err
	}
	if o.NumChunks < 1 {
		return fmt.Errorf("--num-chunks must be >= 1, got %d", o.NumChunks)
	}
	if o.Workers < 1 {
		return fmt.Errorf("--workers must be >= 1, got %d", o.Workers)
	}
	// uint64 so StartChunk+NumChunks-1 cannot itself wrap before the compare.
	if end := uint64(o.StartChunk) + uint64(o.NumChunks) - 1; end > uint64(maxChunkID) {
		return fmt.Errorf("--start-chunk=%d with --num-chunks=%d ends at chunk %d, past the last valid chunk ID %d",
			uint32(o.StartChunk), o.NumChunks, end, uint32(maxChunkID))
	}
	if o.ColdRoot == "" {
		return errors.New("--cold-out-dir is required")
	}
	// Refuse re-packing a source pack tree in place: the backfill always
	// materializes ledger packs, the cold ledger writer overwrites its
	// destination, and destination == source would corrupt the pack mid-read.
	if o.Source.Kind == sourcePack {
		outLedgers := geometry.NewLayout(o.ColdRoot).LedgersRoot()
		if samePath(o.Source.PackDir, outLedgers) {
			return fmt.Errorf("--cold-out-dir's ledgers tree (%s) must differ from --pack-dir", outLedgers)
		}
	}
	return nil
}

// runCold benchmarks the cold path: one backfill.RunBackfill over the
// requested chunk range, against a fresh scratch catalog so the run is a clean
// backfill from empty. As the backfill runs, the sink collects its per-stage
// MetricSink timings and the scheduler's observability metrics. On success
// runCold writes the CSV report and logs a summary, including the effective
// chunk concurrency for multi-chunk runs.
func runCold(ctx context.Context, logger *supportlog.Entry, opts coldOptions) error {
	if err := opts.validate(); err != nil {
		return err
	}
	// Surface an unwritable --out before the run.
	if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
		return fmt.Errorf("create --out dir %s: %w", opts.OutDir, err)
	}
	// Create and fsync the write roots up front — the daemon's own root prep.
	layout := geometry.NewLayout(opts.ColdRoot)
	if err := config.PrepareRoots(
		layout.LedgersRoot(), layout.EventsRoot(), layout.TxHashRawRoot(), layout.TxHashIndexRoot(),
	); err != nil {
		return fmt.Errorf("prepare --cold-out-dir write roots: %w", err)
	}

	catalogBase := opts.CatalogDir
	if catalogBase == "" {
		catalogBase = opts.ColdRoot
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

	sink := newCSVSink()
	//nolint:gosec // validate() proved StartChunk+NumChunks-1 <= maxChunkID
	end := opts.StartChunk + chunk.ID(uint32(opts.NumChunks-1))

	start := time.Now()
	err = backfill.RunBackfill(ctx, backfill.ExecConfig{
		Catalog: cat,
		Logger:  logger,
		Metrics: sink,
		Process: backfill.ProcessConfig{Sink: sink, Backend: backend},
		Workers: opts.Workers,
		// Benchmarks measure one clean attempt; retries would fold failure +
		// backoff time into the samples.
		MaxRetries: 0,
	}, opts.StartChunk, end)
	// VmHWM never decreases, so it can be read right here — before the error
	// check — and a failed run's partial CSV still gets the row.
	recordPeakRSS(logger, sink, readPeakRSS)
	if err != nil {
		writePartialCSVs(logger, sink, opts.OutDir)
		return fmt.Errorf("backfill [%s,%s]: %w", opts.StartChunk, end, err)
	}
	totalWall := time.Since(start)

	sink.logSummary(logger)
	logColdWall(logger, sink, opts.NumChunks, totalWall)
	written, err := sink.writeCSVs(opts.OutDir)
	if err != nil {
		return err
	}
	logger.Infof("wrote %d CSVs to %s", len(written), opts.OutDir)
	return nil
}

// logColdWall logs the run's total wall-clock and, for multi-chunk runs, the
// effective chunk concurrency (sum of the engine's per-chunk totals over the
// total wall).
func logColdWall(logger *supportlog.Entry, sink *csvSink, numChunks int, totalWall time.Duration) {
	if numChunks > 1 && totalWall > 0 {
		sumChunkTotal := sink.sumDriver(driverChunkTotal)
		logger.Infof("total wall = %s (sum(chunk_total)/total = %.2fx effective concurrency)",
			totalWall.Round(time.Millisecond), float64(sumChunkTotal)/float64(totalWall))
		return
	}
	logger.Infof("total wall = %s", totalWall.Round(time.Millisecond))
}

// samePath reports whether a and b resolve to the same directory. Falls back
// to an abs-path compare when either does not exist yet.
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
