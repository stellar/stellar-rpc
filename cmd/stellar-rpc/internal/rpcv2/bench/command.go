package bench

import (
	"context"
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
		"datastore destination_bucket_path, or the lake's local directory for "+
			"--datastore-type=Filesystem (used iff --source=bsb)")
	fs.Uint32Var(&f.bsbBufferSize, "bsb-buffer-size", 0,
		"BSB prefetch buffer depth PER worker (0 = backfill default)")
	fs.Uint32Var(&f.bsbNumWorkers, "bsb-num-workers", 0,
		"BSB download workers PER worker (0 = backfill default)")
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

// newBenchCommand builds one bench subcommand skeleton — no positional args,
// SIGINT-canceled context, Info-level logger, profiling around run, an
// invocation.json record written to --out after the run — with the profile and
// --out flags bound, plus the source flags when src is non-nil (bench-serve
// needs no ledger source).
func newBenchCommand(
	use, short string, src *sourceFlags, prof *profileFlags,
	run func(ctx context.Context, logger *supportlog.Entry, outDir string) error,
) *cobra.Command {
	var outDir string
	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cmd.SilenceUsage = true
			ctx, stop, logger := benchContext()
			defer stop()
			startedAt := time.Now().UTC()
			runErr := prof.around(logger, func() error { return run(ctx, logger, outDir) })
			// The --out dir is created by the run itself, so a run that
			// failed early (e.g. in validation) leaves nowhere to write the
			// record and this write fails too. In that case only warn about
			// the write: the error the user needs to see is the run's own.
			if err := writeInvocationJSON(
				outDir, cmd, captureFlags(cmd), startedAt, time.Now().UTC(), runErr,
			); err != nil {
				if runErr == nil {
					return err
				}
				logger.Warnf("writing invocation.json: %v", err)
			}
			return runErr
		},
	}
	cmd.Flags().StringVar(&outDir, "out", "bench-out", "output dir for the CSV report and invocation.json")
	if src != nil {
		src.bind(cmd)
	}
	prof.bind(cmd)
	return cmd
}

// NewServeCommand returns the `bench-serve` command: it adopts a prepared cold
// dataset (and optionally a pre-built hot DB) into a persistent catalog and
// serves the v2 read methods over HTTP, so a load generator can measure read
// latency against a fixed corpus. It runs no ingestion and needs no captive
// core.
func NewServeCommand() *cobra.Command {
	var (
		opts        serveOptions
		startChunk  uint32
		hotChunk    int64
		replayChunk int64
		src         sourceFlags
		prof        profileFlags
	)
	cmd := newBenchCommand("bench-serve",
		"Serve a prepared full-history dataset over JSON-RPC for read benchmarks",
		&src, &prof,
		func(ctx context.Context, logger *supportlog.Entry, outDir string) error {
			opts.StartChunk = chunk.ID(startChunk)
			opts.HotChunk = optionalChunkFrom(hotChunk)
			opts.ReplayChunk = optionalChunkFrom(replayChunk)
			opts.Source = src.config()
			opts.OutDir = outDir
			return runServe(ctx, logger, opts)
		})
	fs := cmd.Flags()
	fs.StringVar(&opts.ColdRoot, "cold-dir", "",
		"cold artifact root holding ledgers/, events/ and txhash/ (required; a dataset pack root "+
			"or bench-ingest cold's --cold-out-dir)")
	fs.StringVar(&opts.HotRoot, "hot-dir", "",
		"root holding the per-chunk hot RocksDBs ({chunk:08d}); omit to serve cold chunks only")
	fs.StringVar(&opts.CatalogDir, "catalog-dir", "",
		"dir for the adopted catalog (required); unlike the ingest benchmarks this catalog PERSISTS, "+
			"so a re-run over the same dataset reuses it")
	fs.Uint32Var(&startChunk, "start-chunk", 0, "first cold chunk ID to adopt (required)")
	fs.IntVar(&opts.NumChunks, "num-chunks", 1,
		"how many consecutive cold chunks to adopt starting at --start-chunk")
	fs.Int64Var(&hotChunk, "hot-chunk", -1,
		"chunk ID of a pre-built hot DB under --hot-dir to serve as the hot tier (-1 = none)")
	fs.Uint32Var(&opts.LatestLedger, "latest-ledger", 0,
		"newest ledger reads may serve (0 = the highest adopted chunk's last ledger)")
	fs.StringVar(&opts.Endpoint, "endpoint", "127.0.0.1:8000", "host:port the read server binds")
	fs.StringVar(&opts.NetworkPassphrase, "network-passphrase", "",
		"passphrase transaction hashes are computed against (required; must match the dataset's)")
	fs.Int64Var(&replayChunk, "replay-chunk", -1,
		"chunk to ingest live from --source while serving, measuring reads under ingest load "+
			"(-1 = static run); its hot DB is created FRESH, wiping any leftover dir. "+
			"Exclusive with --hot-chunk")
	fs.Uint32Var(&opts.ReplayLedgers, "replay-ledgers", 0,
		"cap on ledgers replayed from --replay-chunk's start (0 = the whole chunk)")
	fs.DurationVar(&opts.CloseInterval, "close-interval", 0,
		"pace the --replay-chunk ingest to this close cadence, as in bench-ingest hot "+
			"(0 = back-to-back). Size it so the replay outlasts the load run: "+
			"10,000 ledgers at 600ms is about 1h40m")
	markRequired(cmd, "cold-dir", "catalog-dir", "start-chunk", "network-passphrase")
	return cmd
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
	cmd := newBenchCommand("cold",
		"Benchmark cold ingestion: the daemon's backfill (chunk freezes + txhash index builds) over a chunk range",
		&src, &prof,
		func(ctx context.Context, logger *supportlog.Entry, outDir string) error {
			return runCold(ctx, logger, coldOptions{
				Source:     src.config(),
				StartChunk: chunk.ID(startChunk),
				NumChunks:  numChunks,
				Workers:    workers,
				ColdRoot:   coldOutDir,
				CatalogDir: catalogDir,
				OutDir:     outDir,
			})
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
	cmd := newBenchCommand("hot",
		"Benchmark hot ingestion: the daemon's live ingestion loop over a chunk range",
		&src, &prof,
		func(ctx context.Context, logger *supportlog.Entry, outDir string) error {
			return runHot(ctx, logger, hotOptions{
				Source:        src.config(),
				StartChunk:    chunk.ID(startChunk),
				NumChunks:     numChunks,
				NumLedgers:    numLedgers,
				HotRoot:       hotDir,
				CatalogDir:    catalogDir,
				CloseInterval: closeInterval,
				OutDir:        outDir,
			})
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
