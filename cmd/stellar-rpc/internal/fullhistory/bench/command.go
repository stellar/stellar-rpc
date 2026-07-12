package bench

import (
	"context"
	"fmt"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// NewCommand returns the `bench-ingest` command tree: `cold`
// benchmarks ingest.WriteColdChunk, `hot` benchmarks ingest.HotService — both
// against the production ingest package, reporting per-stage percentile CSVs.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bench-ingest",
		Short: "Benchmark full-history ingestion against the production ingest path",
	}
	cmd.AddCommand(newColdCommand(), newHotCommand())
	return cmd
}

// sourceFlags is the ledger-source flag set shared by both subcommands.
type sourceFlags struct {
	source        string
	packDir       string
	bucketPath    string
	bsbBufferSize uint32
	bsbNumWorkers uint32
	retryLimit    uint32
	retryWait     time.Duration
	datastoreType string
	region        string
}

func (f *sourceFlags) bind(cmd *cobra.Command) {
	fs := cmd.Flags()
	fs.StringVar(&f.source, "source", sourcePack, "ledger source: pack | bsb")
	fs.StringVar(&f.packDir, "pack-dir", "",
		"source ledgers tree root holding {bucket:05d}/{chunk:08d}.pack (required iff --source=pack)")
	fs.StringVar(&f.bucketPath, "bucket-path", "sdf-ledger-close-meta/v1/ledgers/pubnet",
		"datastore destination_bucket_path (used iff --source=bsb)")
	fs.Uint32Var(&f.bsbBufferSize, "bsb-buffer-size", 0,
		"BSB prefetch buffer depth PER chunk worker (0 = backfill default)")
	fs.Uint32Var(&f.bsbNumWorkers, "bsb-num-workers", 0,
		"BSB download workers PER chunk worker (0 = backfill default)")
	fs.Uint32Var(&f.retryLimit, "retry-limit", 0, "BSB retry attempts on transient failure (0 = backfill default)")
	fs.DurationVar(&f.retryWait, "retry-wait", 0, "BSB delay between retries (0 = backfill default)")
	fs.StringVar(&f.datastoreType, "datastore-type", "GCS", "BSB datastore type: GCS | S3 (used iff --source=bsb)")
	fs.StringVar(&f.region, "region", "", "bucket region for --datastore-type=S3, e.g. us-east-2")
}

func (f *sourceFlags) config() sourceConfig {
	return sourceConfig{
		Kind:          f.source,
		PackDir:       f.packDir,
		BucketPath:    f.bucketPath,
		BufferSize:    f.bsbBufferSize,
		NumWorkers:    f.bsbNumWorkers,
		RetryLimit:    f.retryLimit,
		RetryWait:     f.retryWait,
		DatastoreType: f.datastoreType,
		Region:        f.region,
	}
}

// parseTypes turns the --types flag value into an ingest.Config.
func parseTypes(arg string) (ingest.Config, error) {
	var cfg ingest.Config
	for t := range strings.SplitSeq(arg, ",") {
		switch strings.TrimSpace(t) {
		case "ledgers":
			cfg.Ledgers = true
		case "txhash":
			cfg.Txhash = true
		case "events":
			cfg.Events = true
		case "":
		default:
			return cfg, fmt.Errorf("--types: unknown data type %q (expected subset of ledgers,txhash,events)", t)
		}
	}
	return cfg, nil
}

// benchContext returns the run context (canceled on SIGINT/SIGTERM) and an
// Info-level logger (supportlog defaults to Warn, which would swallow the
// summary report).
func benchContext() (context.Context, context.CancelFunc, *supportlog.Entry) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)
	return ctx, stop, logger
}

// writePartialCSVs best-effort persists whatever the sink has collected when a
// run fails or is interrupted, so a long run's finished chunks survive. Write
// errors are logged, never returned — the run's own error must surface — and
// the report is logged as PARTIAL: its rows cover only work that completed.
func writePartialCSVs(logger *supportlog.Entry, sink *csvSink, outDir string) {
	written, err := sink.writeCSVs(outDir)
	if err != nil {
		logger.Warnf("writing partial CSVs: %v", err)
	}
	if len(written) > 0 {
		logger.Warnf("run incomplete: wrote %d PARTIAL CSVs to %s (rows cover only completed work)", len(written), outDir)
	}
}

func newColdCommand() *cobra.Command {
	var (
		src          sourceFlags
		typesArg     string
		chunkArg     uint32
		numChunks    int
		chunkWorkers int
		coldOutDir   string
		outDir       string
		prof         profileFlags
	)
	cmd := &cobra.Command{
		Use:   "cold",
		Short: "Benchmark cold ingestion (ingest.WriteColdChunk) chunk by chunk",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cmd.SilenceUsage = true
			types, err := parseTypes(typesArg)
			if err != nil {
				return err
			}
			ctx, stop, logger := benchContext()
			defer stop()
			return prof.around(logger, func() error {
				return runCold(ctx, logger, coldOptions{
					Source:       src.config(),
					Types:        types,
					StartChunk:   chunk.ID(chunkArg),
					NumChunks:    numChunks,
					ChunkWorkers: chunkWorkers,
					ArtifactRoot: coldOutDir,
					OutDir:       outDir,
				})
			})
		},
	}
	src.bind(cmd)
	prof.bind(cmd)
	fs := cmd.Flags()
	fs.StringVar(&typesArg, "types", "", "comma-separated subset of ledgers,txhash,events (required)")
	fs.Uint32Var(&chunkArg, "chunk", 0, "first chunk ID to ingest (required)")
	fs.IntVar(&numChunks, "num-chunks", 1, "how many consecutive chunks to ingest starting at --chunk")
	fs.IntVar(&chunkWorkers, "chunk-workers", 1, "how many chunks to run concurrently (clamped to --num-chunks)")
	fs.StringVar(&coldOutDir, "cold-out-dir", "",
		"output root for cold artifacts (required; scratch — re-runs overwrite)")
	fs.StringVar(&outDir, "out", "bench-out", "CSV output dir")
	markRequired(cmd, "types", "chunk", "cold-out-dir")
	return cmd
}

func newHotCommand() *cobra.Command {
	var (
		src        sourceFlags
		chunkArg   uint32
		numLedgers uint32
		hotDir     string
		outDir     string
		prof       profileFlags
	)
	cmd := &cobra.Command{
		Use:   "hot",
		Short: "Benchmark hot ingestion (ingest.HotService) over one chunk's ledgers",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cmd.SilenceUsage = true
			ctx, stop, logger := benchContext()
			defer stop()
			return prof.around(logger, func() error {
				return runHot(ctx, logger, hotOptions{
					Source:     src.config(),
					Chunk:      chunk.ID(chunkArg),
					NumLedgers: numLedgers,
					HotRoot:    hotDir,
					OutDir:     outDir,
				})
			})
		},
	}
	src.bind(cmd)
	prof.bind(cmd)
	fs := cmd.Flags()
	fs.Uint32Var(&chunkArg, "chunk", 0, "chunk ID to ingest (required)")
	fs.Uint32Var(&numLedgers, "num-ledgers", 0, "cap on ledgers ingested from the chunk (0 = whole chunk)")
	fs.StringVar(&hotDir, "hot-dir", "",
		"scratch root for the fresh hot RocksDB (required; the chunk's DB dir must not exist)")
	fs.StringVar(&outDir, "out", "bench-out", "CSV output dir")
	markRequired(cmd, "chunk", "hot-dir")
	return cmd
}

// markRequired marks flags required, panicking on a nonexistent name — a
// programming error caught by any test that builds the command.
func markRequired(cmd *cobra.Command, names ...string) {
	for _, n := range names {
		if err := cmd.MarkFlagRequired(n); err != nil {
			panic(err)
		}
	}
}
