package bench

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// NewCommand returns the `bench-ingest` command tree: `cold` benchmarks the
// daemon's backfill (backfill.RunBackfill), `hot` benchmarks the daemon's live
// ingestion loop.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bench-ingest",
		Short: "Benchmark full-history ingestion",
	}
	cmd.AddCommand(newColdCommand(), newHotCommand())
	return cmd
}

// sourceFlags is the ledger-source flag set shared by both subcommands.
type sourceFlags struct {
	source         string
	packDir        string
	bucketPath     string
	bsbBufferSize  uint32
	bsbBufferBytes int64
	bsbNumWorkers  uint32
	retryLimit     uint32
	retryWait      time.Duration
	datastoreType  string
	region         string
}

func (f *sourceFlags) bind(cmd *cobra.Command) {
	fs := cmd.Flags()
	fs.StringVar(&f.source, "source", sourcePack, "ledger source: pack | bsb")
	fs.StringVar(&f.packDir, "pack-dir", "",
		"source ledgers tree root holding {bucket:05d}/{chunk:08d}.pack (required iff --source=pack)")
	fs.StringVar(&f.bucketPath, "bucket-path", "sdf-ledger-close-meta/v1/ledgers/pubnet",
		"datastore destination_bucket_path, or the lake's local directory for "+
			"--datastore-type=Filesystem (used iff --source=bsb)")
	fs.Int64Var(&f.bsbBufferBytes, "bsb-buffer-bytes", 0,
		"prefetch budget in bytes for each chunk task "+
			"(sizes the download queue, not a hard memory ceiling); 0 = default 32 MiB")
	fs.Uint32Var(&f.bsbBufferSize, "bsb-buffer-size", 0,
		"BSB prefetch depth of one stream, in objects (0 = backfill default)")
	fs.Uint32Var(&f.bsbNumWorkers, "bsb-num-workers", 0,
		"concurrent object downloads inside each chunk task (0 = default 25)")
	fs.Uint32Var(&f.retryLimit, "retry-limit", backfill.DefaultBSBMaxRetries,
		"BSB retry attempts per object download (0 = no retries)")
	fs.DurationVar(&f.retryWait, "retry-wait", backfill.DefaultBSBRetryWait,
		"BSB delay between per-object retries")
	fs.StringVar(&f.datastoreType, "datastore-type", "GCS",
		"BSB datastore type: GCS | S3 | Filesystem (used iff --source=bsb)")
	fs.StringVar(&f.region, "region", "", "bucket region for --datastore-type=S3, e.g. us-east-2")
}

func (f *sourceFlags) config() sourceConfig {
	return sourceConfig{
		Kind:          f.source,
		PackDir:       f.packDir,
		BucketPath:    f.bucketPath,
		BufferSize:    f.bsbBufferSize,
		BufferBytes:   f.bsbBufferBytes,
		NumWorkers:    f.bsbNumWorkers,
		RetryLimit:    f.retryLimit,
		RetryWait:     f.retryWait,
		DatastoreType: f.datastoreType,
		Region:        f.region,
	}
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

// newBenchCommand creates a bench-ingest subcommand with shared flags,
// profiling, run metadata, and signal-driven cancellation. validate runs before
// the command touches --out, so a flag that validate rejects leaves nothing
// behind; errors found later in the run are recorded in run.json as failed.
func newBenchCommand(
	use, short string, src *sourceFlags, prof *profileFlags,
	validate func() error,
	run func(ctx context.Context, logger *supportlog.Entry, outDir string) error,
) *cobra.Command {
	var outDir string
	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			// Cobra checks required flags only after PreRunE; check them first
			// so a missing flag reports cobra's error, not a validate error.
			if err := cmd.ValidateRequiredFlags(); err != nil {
				return err
			}
			return validate()
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			cmd.SilenceUsage = true
			ctx, stop, logger := benchContext()
			defer stop()
			startedAt := time.Now().UTC()
			if err := requireEmptyOut(outDir); err != nil {
				return err
			}
			if err := os.MkdirAll(outDir, 0o755); err != nil {
				return fmt.Errorf("create --out dir %s: %w", outDir, err)
			}
			record := newRunRecord(cmd, captureFlags(cmd), startedAt)
			if err := writeRunRecord(outDir, record); err != nil {
				return err
			}
			runErr := prof.around(logger, func() error { return run(ctx, logger, outDir) })
			peakRSS, rssErr := readPeakRSS()
			if rssErr != nil {
				logger.Warnf("peak RSS unavailable: %v", rssErr)
			} else {
				logger.Infof("peak RSS: %d bytes", peakRSS)
			}
			record.finish(time.Now().UTC(), peakRSS, runErr)
			if err := writeRunRecord(outDir, record); err != nil {
				if runErr == nil {
					return err
				}
				logger.Warnf("writing %s: %v", runRecordFile, err)
			}
			return runErr
		},
	}
	cmd.Flags().StringVar(&outDir, "out", "bench-out",
		"output dir for the CSV report and run.json; must be missing or empty")
	src.bind(cmd)
	prof.bind(cmd)
	return cmd
}

// requireEmptyOut fails unless outDir is missing or empty, so a run never
// replaces the files of an earlier run.
func requireEmptyOut(outDir string) error {
	entries, err := os.ReadDir(outDir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read --out dir %s: %w", outDir, err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("--out dir %s is not empty (it holds %s); pass a missing or empty --out dir",
			outDir, entries[0].Name())
	}
	return nil
}

func newColdCommand() *cobra.Command {
	var (
		src        sourceFlags
		startChunk uint32
		numChunks  int
		workers    int
		coldOutDir string
		catalogDir string
		prof       profileFlags
	)
	opts := func() coldOptions {
		return coldOptions{
			Source:     src.config(),
			StartChunk: chunk.ID(startChunk),
			NumChunks:  numChunks,
			Workers:    workers,
			ColdRoot:   coldOutDir,
			CatalogDir: catalogDir,
		}
	}
	cmd := newBenchCommand("cold",
		"Benchmark cold ingestion: the daemon's backfill (chunk freezes + txhash index builds) over a chunk range",
		&src, &prof,
		func() error { return opts().validate() },
		func(ctx context.Context, logger *supportlog.Entry, outDir string) error {
			o := opts()
			o.OutDir = outDir
			return runCold(ctx, logger, o)
		})
	fs := cmd.Flags()
	fs.Uint32Var(&startChunk, "start-chunk", 0, "first chunk ID to backfill (required)")
	fs.IntVar(&numChunks, "num-chunks", 1, "how many consecutive chunks to backfill starting at --start-chunk")
	fs.IntVar(&workers, "workers", 1, "backfill worker-pool size, shared by chunk freezes and index builds")
	fs.StringVar(&coldOutDir, "cold-out-dir", "",
		"output root for cold artifacts (required; use a fresh dir — same-range "+
			"re-runs overwrite, but leftovers from other ranges are never swept)")
	fs.StringVar(&catalogDir, "catalog-dir", "",
		"base dir for the run's scratch catalog; default: --cold-out-dir")
	markRequired(cmd, "start-chunk", "cold-out-dir")
	return cmd
}

func newHotCommand() *cobra.Command {
	var (
		src           sourceFlags
		startChunk    uint32
		numChunks     int
		numLedgers    uint32
		hotDir        string
		catalogDir    string
		closeInterval time.Duration
		prof          profileFlags
	)
	opts := func() hotOptions {
		return hotOptions{
			Source:        src.config(),
			StartChunk:    chunk.ID(startChunk),
			NumChunks:     numChunks,
			NumLedgers:    numLedgers,
			HotRoot:       hotDir,
			CatalogDir:    catalogDir,
			CloseInterval: closeInterval,
		}
	}
	cmd := newBenchCommand("hot",
		"Benchmark hot ingestion: the daemon's live ingestion loop over a chunk range",
		&src, &prof,
		func() error { return opts().validate() },
		func(ctx context.Context, logger *supportlog.Entry, outDir string) error {
			o := opts()
			o.OutDir = outDir
			return runHot(ctx, logger, o)
		})
	fs := cmd.Flags()
	fs.Uint32Var(&startChunk, "start-chunk", 0, "first chunk ID to ingest (required)")
	fs.IntVar(&numChunks, "num-chunks", 1,
		"how many consecutive chunks to ingest starting at --start-chunk (>1 exercises the hot DB rotation)")
	fs.Uint32Var(&numLedgers, "num-ledgers", 0, "cap on ledgers ingested from the range's start (0 = whole range)")
	fs.StringVar(&hotDir, "hot-dir", "",
		"scratch root for the hot RocksDBs (required; leftover chunk DBs are wiped for a fixed starting state)")
	fs.StringVar(&catalogDir, "catalog-dir", "",
		"base dir for the run's scratch catalog; default: --hot-dir")
	fs.DurationVar(&closeInterval, "close-interval", 0,
		"assumed time between ledger closes; >0 paces ingestion to that steady-state cadence "+
			"and reports pace_lag (0 = ingest back-to-back, catch-up throughput)")
	markRequired(cmd, "start-chunk", "hot-dir")
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
