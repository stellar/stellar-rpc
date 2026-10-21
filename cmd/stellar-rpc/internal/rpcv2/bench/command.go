package bench

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// NewCommand returns the `bench` root command for benchmarking ingestion and reads.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Benchmark ingestion and reads",
	}
	cmd.AddCommand(newIngestCommand(), newQueryCommand())
	return cmd
}

// newIngestCommand returns the `bench ingest` command tree: `cold` benchmarks
// the daemon's backfill (backfill.RunBackfill), `hot` benchmarks the daemon's
// live ingestion loop.
func newIngestCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ingest",
		Short: "Benchmark ingestion",
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

// runEnv is what newBenchCommand hands a run. What the run puts in Settings
// and SetupTimes goes into run.json.
type runEnv struct {
	OutDir     string
	Settings   map[string]string
	SetupTimes map[string]time.Duration
}

// newBenchCommand creates a bench subcommand with shared flags, profiling, run
// metadata, and signal-driven cancellation.
func newBenchCommand(
	use, short string, prof *profileFlags,
	validate func(outDir string) error,
	run func(ctx context.Context, logger *supportlog.Entry, env runEnv) error,
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
			return validate(outDir)
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			cmd.SilenceUsage = true
			ctx, stop, logger := benchContext()
			defer stop()
			startedAt := time.Now().UTC()
			env := runEnv{
				OutDir:     outDir,
				Settings:   map[string]string{},
				SetupTimes: map[string]time.Duration{},
			}
			if err := requireEmptyDir("out", outDir); err != nil {
				return err
			}
			if err := os.MkdirAll(outDir, dirPerm); err != nil {
				return fmt.Errorf("create --out dir %s: %w", outDir, err)
			}
			record := newRunRecord(cmd, captureFlags(cmd), startedAt)
			if err := writeRunRecord(outDir, record); err != nil {
				return err
			}
			runErr := prof.around(logger, func() error { return run(ctx, logger, env) })
			peakRSS, rssErr := readPeakRSS()
			if rssErr != nil {
				logger.Warnf("peak RSS unavailable: %v", rssErr)
			} else {
				logger.Infof("peak RSS: %d bytes", peakRSS)
			}
			record.finish(time.Now().UTC(), env, peakRSS, runErr)
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
	prof.bind(cmd)
	return cmd
}

// requireEmptyDir fails unless dir, the value of --flag, is missing or empty,
// so a run never writes over the files of an earlier run.
func requireEmptyDir(flag, dir string) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read --%s %s: %w", flag, dir, err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("--%s %s is not empty (it holds %s); pass a missing or empty directory",
			flag, dir, entries[0].Name())
	}
	return nil
}

// requireOutOutside fails when out, the value of --out, is root, the value of
// --rootFlag, or a path inside it.
func requireOutOutside(rootFlag, root, out string) error {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("resolve --%s %s: %w", rootFlag, root, err)
	}
	absOut, err := filepath.Abs(out)
	if err != nil {
		return fmt.Errorf("resolve --out %s: %w", out, err)
	}
	rel, err := filepath.Rel(absRoot, absOut)
	if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("--out %s must be outside --%s %s", out, rootFlag, root)
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
		prof       profileFlags
	)
	opts := func() coldOptions {
		return coldOptions{
			Source:     src.config(),
			StartChunk: chunk.ID(startChunk),
			NumChunks:  numChunks,
			Workers:    workers,
			ColdRoot:   coldOutDir,
		}
	}
	cmd := newBenchCommand("cold",
		"Benchmark cold ingestion: the daemon's backfill (chunk freezes + txhash index builds) over a chunk range",
		&prof,
		func(outDir string) error {
			o := opts()
			o.OutDir = outDir
			return o.validate()
		},
		func(ctx context.Context, logger *supportlog.Entry, env runEnv) error {
			o := opts()
			o.OutDir = env.OutDir
			return runCold(ctx, logger, o)
		})
	src.bind(cmd)
	fs := cmd.Flags()
	fs.Uint32Var(&startChunk, "start-chunk", 0, "first chunk ID to backfill (required)")
	fs.IntVar(&numChunks, "num-chunks", 1, "how many consecutive chunks to backfill starting at --start-chunk")
	fs.IntVar(&workers, "workers", 1, "backfill worker-pool size, shared by chunk freezes and index builds")
	fs.StringVar(&coldOutDir, "cold-out-dir", "",
		"dataset root for the catalog and the cold artifacts (required); must be missing or empty")
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
			CloseInterval: closeInterval,
		}
	}
	cmd := newBenchCommand("hot",
		"Benchmark hot ingestion: the daemon's live ingestion loop over a chunk range",
		&prof,
		func(outDir string) error {
			o := opts()
			o.OutDir = outDir
			return o.validate()
		},
		func(ctx context.Context, logger *supportlog.Entry, env runEnv) error {
			o := opts()
			o.OutDir = env.OutDir
			return runHot(ctx, logger, o)
		})
	src.bind(cmd)
	fs := cmd.Flags()
	fs.Uint32Var(&startChunk, "start-chunk", 0, "first chunk ID to ingest (required)")
	fs.IntVar(&numChunks, "num-chunks", 1,
		"how many consecutive chunks to ingest starting at --start-chunk (>1 exercises the hot DB rotation)")
	fs.Uint32Var(&numLedgers, "num-ledgers", 0, "cap on ledgers ingested from the range's start (0 = whole range)")
	fs.StringVar(&hotDir, "hot-dir", "",
		"dataset root for the catalog and the hot chunk databases (required); must be missing or empty")
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
