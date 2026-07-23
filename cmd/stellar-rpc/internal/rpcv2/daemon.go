package rpcv2

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/pelletier/go-toml"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/support/storage"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
)

// RunDaemon is the full-history daemon's process entrypoint: load config, lock
// storage roots, open the catalog, validateConfig, build boundaries, then run
// the supervised run loop.
func RunDaemon(ctx context.Context, configPath string) error {
	return runDaemonWith(ctx, configPath, daemonOptions{})
}

// daemonOptions carries the daemon's injectable seams; production leaves every field zero.
type daemonOptions struct {
	// Backend is the bulk ledger source backfill freezes from and samples the tip from.
	// nil ⇒ runDaemonWith builds it: the bulk lake from [backfill.datastore], else —
	// with history archives configured — captive core replaying from the archives
	// (#833). Tests inject a fakeBackend.
	Backend backfill.Backend

	// Core starts captive core at the resume ledger and yields the live getter the
	// ingestion loop polls. nil ⇒ runDaemonWith builds a captiveCoreOpener from
	// [ingestion] (a complete production opener). Tests inject a fake getter.
	Core CoreOpener

	// ServeReads launches the RPC read server; it must return promptly, not block.
	// nil ⇒ the #772 no-op placeholder (reads still come from the v1 SQLite daemon).
	ServeReads func(ctx context.Context) error

	// RestartBackoff is the supervised loop's inter-restart sleep; zero ⇒ defaultRestartBackoff.
	RestartBackoff time.Duration

	// Logger overrides the daemon logger; nil ⇒ built from [logging].level/.format.
	Logger *supportlog.Entry

	// Metrics is the control-plane sink; nil ⇒ a *PrometheusMetrics on the daemon's registry.
	Metrics observability.Metrics

	// IngestSink is the per-type cold-path ingest sink; nil ⇒ a *ingest.PrometheusSink.
	IngestSink ingest.MetricSink

	// chunksPerTxhashIndex overrides the tx-hash index width (test-only). 0 ⇒ the
	// fixed geometry.ChunksPerTxhashIndex. Tests set it to 1 so a single chunk's
	// freeze is a terminal index (exercising the index rebuild + prune path cheaply).
	chunksPerTxhashIndex uint32
}

const defaultRestartBackoff = 5 * time.Second

// runDaemonWith is RunDaemon with explicit options — the seam tests drive.
//
//nolint:cyclop,funlen // linear startup sequence; each branch is one wiring step
func runDaemonWith(ctx context.Context, configPath string, opts daemonOptions) error {
	// --- Load + form-validate the config. ---
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return err
	}
	if cfg.Service.DefaultDataDir == "" {
		return errors.New("[service].default_data_dir is required")
	}

	logger := opts.Logger
	if logger == nil {
		logger, err = newLogger(cfg.Logging)
		if err != nil {
			return err
		}
	}

	// Readiness/health signal, fed by the ingestion loop per commit; both signals
	// derive from the last committed ledger. Created outside the supervised run
	// loop so it survives restarts (readiness stays latched across them).
	// TODO(#772): serve it from the read server (as HealthSignal).
	hs := &healthState{}

	paths := cfg.ResolvePaths()

	// --- Reject shared roots, then create + fsync any missing ones. Single-
	// process enforcement is the catalog's own RocksDB LOCK, taken next. ---
	if err := paths.ValidateRoots(); err != nil {
		return err
	}
	if err := config.PrepareRoots(paths.Roots()...); err != nil {
		return err
	}

	// --- Open the catalog (it owns its backing KV store; Close releases it). ---
	cat, err := openCatalog(paths, opts, logger)
	if err != nil {
		return err
	}
	defer func() { _ = cat.Close() }()

	// --- Resolve the captive-core opener up front: every daemon runs the live
	// ingestion loop, and the no-lake backfill source is built from its stream. A
	// bad [ingestion] config therefore surfaces before validateConfig's errors —
	// cosmetic, both are fatal startup errors. ---
	core, err := resolveCore(opts, cfg, logger)
	if err != nil {
		return err
	}

	// --- The history-archive pool: the no-lake backfill source's frontier. nil
	// when a bulk lake is configured (unused there) or no URLs are set. ---
	var pool rootHASGetter
	if cfg.Backfill.DataStore.Type == "" && len(cfg.Ingestion.HistoryArchiveURLs) > 0 {
		pool, err = newArchivePool(ctx, cfg.Ingestion.HistoryArchiveURLs, logger)
		if err != nil {
			return err
		}
	}

	// --- Resolve the backfill backend: injected (tests), the bulk lake
	// ([backfill.datastore]), or captive core replaying from the archives when no
	// lake is configured (#833). Its Tip is THE network frontier — every backfill
	// pass samples it and the freeze's coverage wait polls it — so validateConfig
	// (which needs the tip) runs after this. ---
	backend := opts.Backend
	if backend == nil {
		built, cleanup, berr := buildBackfillBackend(ctx, cfg, core, pool, logger)
		if berr != nil {
			return fmt.Errorf("build backfill backend: %w", berr)
		}
		if cleanup != nil {
			defer cleanup()
		}
		backend = built
	}
	if backend == nil {
		// In production newCaptiveCoreOpener already rejects missing archive URLs;
		// this config-shaped message covers an injected Core bypassing that gate.
		return errors.New("no bulk ledger source configured: set [backfill.datastore] " +
			"or [ingestion].history_archive_urls")
	}

	serveReads := opts.ServeReads
	if serveReads == nil {
		// TODO(#772): wire the full-history RPC read server; no-op until the cutover.
		serveReads = func(context.Context) error { return nil }
	}

	// --- validateConfig: pin/confirm the layout, resolve the earliest floor. ---
	earliest, err := validateConfig(ctx, cfg, cat, backend.Tip)
	if err != nil {
		return err
	}

	// Bind the retention policy once, on the far side of validation: earliest is
	// validateConfig's return — pinned and chunk-aligned.
	retention := geometry.NewRetention(deref(cfg.Retention.RetentionChunks), chunk.IDFromLedger(earliest))

	// Control-plane Metrics and the ingest sink share ONE registry, built after the
	// validateConfig gate (it registers Prometheus collectors).
	// TODO(#772): expose it on the read server's /metrics.
	registry := prometheus.NewRegistry()
	metrics, sink := buildSinks(opts, registry)

	// --- Assemble the StartConfig and run the supervised run loop. ---
	start := startConfig(
		cfg, cat, logger, backend, core, serveReads, metrics, sink, hs, retention)

	backoff := opts.RestartBackoff
	if backoff <= 0 {
		backoff = defaultRestartBackoff
	}
	return supervise(ctx, start, logger, backoff)
}

// openCatalog resolves the tx-hash index width (test override, else the fixed
// constant) and opens the catalog over it. The caller owns Close.
func openCatalog(paths config.Paths, opts daemonOptions, logger *supportlog.Entry) (*catalog.Catalog, error) {
	cpi := geometry.ChunksPerTxhashIndex
	if opts.chunksPerTxhashIndex != 0 {
		cpi = opts.chunksPerTxhashIndex
	}
	txLayout, err := geometry.NewTxHashIndexLayout(cpi)
	if err != nil {
		return nil, err
	}
	cat, err := catalog.Open(paths.Catalog, config.NewLayoutFromPaths(paths), txLayout, logger)
	if err != nil {
		return nil, fmt.Errorf("open catalog %q: %w", paths.Catalog, err)
	}
	return cat, nil
}

// resolveCore returns the injected CoreOpener (tests) or a production captive-core
// opener built from [ingestion].
func resolveCore(opts daemonOptions, cfg config.Config, logger *supportlog.Entry) (CoreOpener, error) {
	if opts.Core != nil {
		return opts.Core, nil
	}
	built, err := newCaptiveCoreOpener(cfg.Ingestion, cfg.Service.DefaultDataDir, logger)
	if err != nil {
		return nil, err
	}
	return built, nil
}

// startConfig assembles the StartConfig run consumes. run() builds the
// lifecycle.Config from Exec + Retention, so backfill and the lifecycle
// goroutine share ONE catalog, worker pool, and retention floor by construction.
func startConfig(
	cfg config.Config, cat *catalog.Catalog, logger *supportlog.Entry,
	backend backfill.Backend, core CoreOpener, serveReads func(context.Context) error,
	metrics observability.Metrics, sink ingest.MetricSink, hs *healthState, retention geometry.Retention,
) StartConfig {
	exec := backfill.ExecConfig{
		Catalog:    cat,
		Logger:     logger,
		Metrics:    observability.MetricsOrNop(metrics),
		Workers:    deref(cfg.Backfill.Workers),
		MaxRetries: deref(cfg.Backfill.MaxRetries),
		Process: backfill.ProcessConfig{
			Backend: backend,
			Sink:    sink,
		},
	}
	return StartConfig{
		Exec:       exec,
		Retention:  retention,
		Core:       core,
		ServeReads: serveReads,
		health:     hs,
	}
}

// buildSinks resolves the control-plane Metrics + per-type ingest sink, defaulting
// each to a Prometheus implementation on the shared registry when unset.
func buildSinks(opts daemonOptions, registry *prometheus.Registry) (observability.Metrics, ingest.MetricSink) {
	metrics := opts.Metrics
	if metrics == nil {
		metrics = observability.NewPrometheusMetrics(registry, host.PrometheusNamespace)
	}
	sink := opts.IngestSink
	if sink == nil {
		sink = ingest.NewPrometheusSink(registry, host.PrometheusNamespace)
	}
	return metrics, sink
}

// supervise is the daemon's clean-vs-restart decision point ("startup is the
// recovery path"): a ctx cancel is a clean shutdown, everything else is warned and
// retried after a backoff. run() never returns nil (a clean shutdown surfaces as a
// ctx-canceled error), so clean-vs-restart keys solely off ctx.Err(). There is
// deliberately no fatal-and-exit class — genuine loss presents as a crash-loop with
// a clear warn line. The never-auto-heal guarantee lives in the must-exist open
// (openHotDBForChunk), not here.
//
//nolint:unparam // error-shaped for the caller's contract; nil-on-shutdown is the design above
func supervise(
	ctx context.Context, start StartConfig, logger *supportlog.Entry, backoff time.Duration,
) error {
	for {
		err := run(ctx, start)
		if ctx.Err() != nil {
			return nil //nolint:nilerr // ctx canceled is a clean shutdown, not a run failure
		}
		logger.WithError(err).Warnf("daemon run failed; restarting in %s", backoff)
		if sleepCtx(ctx, backoff) != nil {
			return nil //nolint:nilerr // ctx canceled mid-backoff is a clean shutdown, not a failure
		}
	}
}

// sleepCtx blocks for d or until ctx is canceled, returning ctx.Err() if canceled
// first and nil otherwise. supervise's clean-vs-restart loop can't be
// a backoff.Retry, so it keeps a hand-rolled sleep — but shares this one helper
// rather than re-rolling the timer/select (and its easy-to-forget timer.Stop).
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// ---------------------------------------------------------------------------
// Production backfill backend construction.
// ---------------------------------------------------------------------------

// buildBackfillBackend opens the bulk ledger source backfill freezes from. With a
// [backfill.datastore] it is the SDK datastore (GCS/S3/Filesystem/...) wrapped as a
// backfill.Backend, plus a cleanup that releases the datastore handle at shutdown.
// With no datastore but history archives configured it is captive core instead
// (captiveSource): core replays each chunk's bounded range and the archives' root
// HAS is the frontier — so a below-now earliest_ledger floor is fillable, not just
// pinnable (#833). With neither it returns (nil, nil, nil) and runDaemonWith fails
// startup with the config-shaped message. core must be non-nil (runDaemonWith
// resolves the opener first).
func buildBackfillBackend(
	ctx context.Context, cfg config.Config, core CoreOpener, pool rootHASGetter, logger *supportlog.Entry,
) (backfill.Backend, func(), error) {
	if cfg.Backfill.DataStore.Type != "" {
		backend, cleanup, err := backfill.NewBSBBackendFromConfig(ctx, cfg.Backfill.DataStore, cfg.Backfill.BSB)
		if err != nil {
			return nil, nil, err
		}
		logger.WithField("datastore_type", cfg.Backfill.DataStore.Type).Info("wired BSB backfill backend")
		return backend, cleanup, nil
	}
	if pool == nil {
		return nil, nil, nil // no bulk source at all; runDaemonWith fails fast on this
	}
	stream, err := core.OpenCore(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("open captive-core backfill stream: %w", err)
	}
	logger.Info("wired captive-core backfill backend (no bulk lake configured)")
	return &captiveSource{LedgerStream: stream, archives: pool}, nil, nil
}

// captiveSource is the backfill.Backend for a no-lake deployment: captive core
// replays each chunk's bounded range, and the history archives' root HAS — the
// same archives core itself reads — is the frontier Tip. The SDK's captive stream
// builds a FRESH core per RawLedgers call (each in its own ephemeral working
// dir), so executePlan's parallel per-chunk pulls run independent cores rather
// than contending on one cursor.
//
// Each bounded replay catches its core up to the range start, so the cost is one
// catch-up per chunk: fine for a small retention window, infeasible for deep
// history — a deep-history deployment configures the bulk lake instead.
type captiveSource struct {
	ledgerbackend.LedgerStream // the captive-core stream (CoreOpener.OpenCore)

	archives rootHASGetter
}

var _ backfill.Backend = (*captiveSource)(nil)

// rootHASGetter is the slice of historyarchive.ArchiveInterface Tip needs: the
// network frontier is the CurrentLedger the root HAS publishes.
type rootHASGetter interface {
	GetRootHAS() (historyarchive.HistoryArchiveState, error)
}

// Tip reports the archives' current frontier — the root HAS CurrentLedger. The
// archives lag the network by up to one checkpoint (64 ledgers); backfill's
// max(tip, lastCommitted) target and the signed withinOneChunkOfTip already
// absorb that, so no lag adjustment here.
func (s *captiveSource) Tip(context.Context) (uint32, error) {
	has, err := s.archives.GetRootHAS()
	if err != nil {
		return 0, fmt.Errorf("history archive root HAS: %w", err)
	}
	return has.CurrentLedger, nil
}

// ---------------------------------------------------------------------------
// Production captive-core opener (the live ingestion source).
// ---------------------------------------------------------------------------

// captiveCoreOpener is the production CoreOpener. It holds a resolved
// CaptiveCoreConfig and hands back a captive-core LedgerStream that builds a FRESH
// core per run (each supervised restart reopens core anew) — the stream owns the
// process lifecycle, so there is no eager prepare or explicit closer here.
// Construction mirrors the RPC daemon's newCaptiveCore so the full-history daemon
// runs captive core and the ledgerbackend the same way (#772 can unify them at
// the cutover).
type captiveCoreOpener struct {
	config ledgerbackend.CaptiveCoreConfig
}

// newCaptiveCoreOpener resolves the captive-core config, treating the
// captive_core_config FILE as the single source of truth: NETWORK_PASSPHRASE is
// read back from it, and the stellar-core binary defaults to the one on PATH.
// Only the plain history-archive URLs (not derivable from the file's [HISTORY.*]
// get-commands) come from [ingestion].history_archive_urls. The toml params
// mirror the RPC daemon (strict, unified events, soroban diagnostic/meta
// enforcement) so the ingested meta is what the events + txhash stores need.
func newCaptiveCoreOpener(
	ing config.IngestionConfig, dataDir string, logger *supportlog.Entry,
) (*captiveCoreOpener, error) {
	if ing.CaptiveCoreConfig == "" {
		return nil, errors.New("[ingestion].captive_core_config is required for live ingestion")
	}
	if len(ing.HistoryArchiveURLs) == 0 {
		return nil, errors.New("[ingestion].history_archive_urls is required for live ingestion")
	}

	// NETWORK_PASSPHRASE lives in the captive-core file; read it back so the
	// operator configures it in one place. (go-toml v1 ignores the other fields.)
	data, err := os.ReadFile(ing.CaptiveCoreConfig)
	if err != nil {
		return nil, fmt.Errorf("read captive_core_config %q: %w", ing.CaptiveCoreConfig, err)
	}
	var peek struct {
		NetworkPassphrase string `toml:"NETWORK_PASSPHRASE"`
	}
	if perr := toml.Unmarshal(data, &peek); perr != nil {
		return nil, fmt.Errorf("parse captive_core_config %q: %w", ing.CaptiveCoreConfig, perr)
	}
	if peek.NetworkPassphrase == "" {
		return nil, fmt.Errorf("captive_core_config %q must define NETWORK_PASSPHRASE", ing.CaptiveCoreConfig)
	}

	// stellar-core binary: explicit path, else the one on PATH (RPC daemon default).
	binaryPath := ing.StellarCoreBinaryPath
	if binaryPath == "" {
		found, lerr := exec.LookPath("stellar-core")
		if lerr != nil {
			return nil, fmt.Errorf(
				"[ingestion].stellar_core_binary_path unset and stellar-core not found on PATH: %w", lerr)
		}
		binaryPath = found
	}

	storagePath := ing.CaptiveCoreStoragePath
	if storagePath == "" {
		storagePath = filepath.Join(dataDir, "captive-core")
	}

	// Build the toml from the bytes already read, not the path — re-reading via
	// NewCaptiveCoreTomlFromFile would parse the file twice and, worse, could
	// observe a different NETWORK_PASSPHRASE than the one peeked above if the file
	// changed between the two reads (surfacing as the SDK's confusing mismatch error).
	coreToml, err := ledgerbackend.NewCaptiveCoreTomlFromData(data, ledgerbackend.CaptiveCoreTomlParams{
		HistoryArchiveURLs:                 ing.HistoryArchiveURLs,
		NetworkPassphrase:                  peek.NetworkPassphrase,
		Strict:                             true,
		EnforceSorobanDiagnosticEvents:     true,
		EnforceSorobanTransactionMetaExtV1: true,
		EmitUnifiedEvents:                  true,
		CoreBinaryPath:                     binaryPath,
	})
	if err != nil {
		return nil, fmt.Errorf("invalid captive-core toml %q: %w", ing.CaptiveCoreConfig, err)
	}

	return &captiveCoreOpener{
		config: ledgerbackend.CaptiveCoreConfig{
			BinaryPath:         binaryPath,
			StoragePath:        storagePath,
			NetworkPassphrase:  peek.NetworkPassphrase,
			HistoryArchiveURLs: ing.HistoryArchiveURLs,
			Log:                logger.WithField("subservice", "stellar-core"),
			Toml:               coreToml,
			UserAgent:          "stellar-rpc-fullhistory",
		},
	}, nil
}

// OpenCore returns the live ingestion stream backed by captive stellar-core. A
// fresh core per run keeps supervised restarts clean.
func (c *captiveCoreOpener) OpenCore(ctx context.Context) (ledgerbackend.LedgerStream, error) {
	cfg := c.config
	cfg.Context = ctx
	return ledgerbackend.NewCaptiveCoreStream(cfg, c.config.Log), nil
}

// newArchivePool connects [ingestion].history_archive_urls — the same archives
// captive core reads; no new config. urls must be non-empty (the caller skips the
// pool entirely when none are configured).
func newArchivePool(ctx context.Context, urls []string, logger *supportlog.Entry) (rootHASGetter, error) {
	pool, err := historyarchive.NewArchivePool(urls, historyarchive.ArchiveOptions{
		ConnectOptions: storage.ConnectOptions{Context: ctx},
		Logger:         logger.WithField("subservice", "history-archive"),
	})
	if err != nil {
		return nil, fmt.Errorf("connect history archives: %w", err)
	}
	return pool, nil
}

// newLogger builds a daemon logger from the [logging] config.
func newLogger(cfg config.LoggingConfig) (*supportlog.Entry, error) {
	level, err := logrus.ParseLevel(cfg.Level)
	if err != nil {
		return nil, fmt.Errorf("invalid logging.level %q: %w", cfg.Level, err)
	}
	logger := supportlog.New()
	logger.SetLevel(level)
	if cfg.Format == config.LogFormatJSON {
		logger.UseJSONFormatter()
	}
	return logger, nil
}

// compile-time interface checks.
var _ CoreOpener = (*captiveCoreOpener)(nil)
