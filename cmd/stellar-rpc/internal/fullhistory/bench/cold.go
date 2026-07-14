// Package bench implements the full-history ingestion benchmarks behind the
// stellar-rpc bench-ingest subcommand (issue #771): drivers that feed a ledger
// source through the PRODUCTION ingest entry points — ingest.WriteColdChunk
// (cold) and ingest.HotService (hot) — and a csvSink that aggregates the
// MetricSink signals those paths already emit into per-stage percentile CSV
// reports. Nothing here re-instruments or re-implements ingestion; the engine
// under measurement is the one that ships.
package bench

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// maxChunkID is the last chunk ID whose LastLedger fits in a uint32 ledger
// sequence (see chunk.ID.LastLedger); past it the ledger-range arithmetic
// wraps, so a --chunk beyond it would silently benchmark a garbage range.
const maxChunkID = chunk.ID(math.MaxUint32/chunk.LedgersPerChunk - 1)

// coldOptions configures one cold ingest benchmark run.
type coldOptions struct {
	Source sourceConfig
	// Types selects which cold data types to materialize.
	Types ingest.Config
	// StartChunk..StartChunk+NumChunks-1 are ingested; ChunkWorkers chunks run
	// concurrently (clamped to NumChunks).
	StartChunk   chunk.ID
	NumChunks    int
	ChunkWorkers int
	// ArtifactRoot is the output root the cold artifacts land under, laid out
	// by geometry.NewLayout. It is scratch: no completion records are written,
	// so re-runs overwrite freely (the production artifact model).
	ArtifactRoot string
	// OutDir receives the CSV report.
	OutDir string
}

func (o coldOptions) validate() error {
	if !o.Types.Ledgers && !o.Types.Txhash && !o.Types.Events {
		return errors.New("--types must enable at least one of ledgers,txhash,events")
	}
	if o.NumChunks < 1 {
		return fmt.Errorf("--num-chunks must be >= 1, got %d", o.NumChunks)
	}
	if o.ChunkWorkers < 1 {
		return fmt.Errorf("--chunk-workers must be >= 1, got %d", o.ChunkWorkers)
	}
	// uint64 so StartChunk+NumChunks-1 cannot itself wrap before the compare.
	if end := uint64(o.StartChunk) + uint64(o.NumChunks) - 1; end > uint64(maxChunkID) {
		return fmt.Errorf("--chunk=%d with --num-chunks=%d ends at chunk %d, past the last valid chunk ID %d",
			uint32(o.StartChunk), o.NumChunks, end, uint32(maxChunkID))
	}
	if o.ArtifactRoot == "" {
		return errors.New("--cold-out-dir is required")
	}
	// Refuse re-packing a source pack tree in place: the cold ledger writer
	// overwrites its destination, and destination == source would corrupt the
	// pack mid-read.
	if o.Source.Kind == sourcePack && o.Types.Ledgers {
		outLedgers := geometry.NewLayout(o.ArtifactRoot).LedgersRoot()
		if samePath(o.Source.PackDir, outLedgers) {
			return fmt.Errorf("--cold-out-dir's ledgers tree (%s) must differ from --pack-dir", outLedgers)
		}
	}
	return nil
}

// runCold benchmarks the production cold ingest path: for each chunk in
// [StartChunk, StartChunk+NumChunks), it opens an independent ledger stream
// and calls ingest.WriteColdChunk against the CSV sink, with up to
// ChunkWorkers chunks in flight. On success it writes the CSV report and logs
// a per-row summary plus the run's effective chunk concurrency
// (sum(chunk_wall)/total_wall).
func runCold(ctx context.Context, logger *supportlog.Entry, opts coldOptions) error {
	if err := opts.validate(); err != nil {
		return err
	}
	if opts.ChunkWorkers > opts.NumChunks {
		logger.Infof("--chunk-workers=%d > --num-chunks=%d; clamping", opts.ChunkWorkers, opts.NumChunks)
		opts.ChunkWorkers = opts.NumChunks
	}
	// Surface an unwritable --out before the expensive run, not after it.
	if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
		return fmt.Errorf("create --out dir %s: %w", opts.OutDir, err)
	}
	// Cold artifacts land at production geometry under ArtifactRoot: create +
	// fsync every write root up front, the daemon's own root prep. There is no
	// cross-process root lock to take — single-process enforcement rides on
	// the catalog's RocksDB LOCK (which a catalog-less bench run never opens),
	// and cold trees are write-once behind the one-write protocol, so a
	// concurrent writer is redundant work, not corruption. All three roots are
	// prepared regardless of --types — ArtifactRoot is owned wholesale as
	// scratch.
	layout := geometry.NewLayout(opts.ArtifactRoot)
	if err := config.PrepareRoots(layout.LedgersRoot(), layout.EventsRoot(), layout.TxHashRawRoot()); err != nil {
		return fmt.Errorf("prepare --cold-out-dir write roots: %w", err)
	}

	streamFor, release, err := openSource(ctx, opts.Source)
	if err != nil {
		return err
	}
	defer release()

	sink := newCSVSink()

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(opts.ChunkWorkers)
	start := time.Now()
	for i := range opts.NumChunks {
		chunkID := opts.StartChunk + chunk.ID(uint32(i))
		g.Go(func() error {
			if err := runOneColdChunk(gctx, logger, streamFor, layout, sink, chunkID, opts.Types); err != nil {
				return fmt.Errorf("chunk %d: %w", uint32(chunkID), err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		writePartialCSVs(logger, sink, opts.OutDir)
		return err
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

// runOneColdChunk materializes one chunk's cold artifacts from its own stream
// and reports the driver-observed chunk wall (stream open + WriteColdChunk) to
// the sink.
func runOneColdChunk(
	ctx context.Context,
	logger *supportlog.Entry,
	streamFor streamFactory,
	layout geometry.Layout,
	sink *csvSink,
	chunkID chunk.ID,
	types ingest.Config,
) error {
	start := time.Now()
	stream, err := streamFor(chunkID)
	if err != nil {
		return err
	}
	raw := stream.RawLedgers(ctx, ledgerbackend.BoundedRange(chunkID.FirstLedger(), chunkID.LastLedger()))
	dirs := ingest.ColdDirs{
		LedgerPack: layout.LedgerPackPath(chunkID),
		TxhashBin:  layout.TxHashBinPath(chunkID),
		EventsDir:  layout.EventsBucketDir(chunkID),
	}
	if err := ingest.WriteColdChunk(ctx, logger, chunkID, raw, dirs, sink, types); err != nil {
		return err
	}
	sink.observeDriver(driverChunkWall, time.Since(start), 0)
	return nil
}

// logColdWall logs the run's total wall-clock and, for multi-chunk runs, the
// effective chunk concurrency (sum of per-chunk walls over the total wall).
func logColdWall(logger *supportlog.Entry, sink *csvSink, numChunks int, totalWall time.Duration) {
	if numChunks > 1 && totalWall > 0 {
		sumChunkWall := sink.sumDriver(driverChunkWall)
		logger.Infof("total wall = %s (sum(chunk_wall)/total = %.2fx effective concurrency)",
			totalWall.Round(time.Millisecond), float64(sumChunkWall)/float64(totalWall))
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
