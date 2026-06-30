package fullhistory

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
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
	// nil ⇒ runDaemonWith builds it from [backfill.datastore] (which itself yields a
	// frontfill-only daemon when no datastore is configured). Tests inject a fakeBackend.
	Backend backfill.Backend

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
}

const defaultRestartBackoff = 5 * time.Second

// runDaemonWith is RunDaemon with explicit options — the seam tests drive.
func runDaemonWith(ctx context.Context, configPath string, opts daemonOptions) error {
	// --- Load + form-validate the config. ---
	cfg, err := LoadConfig(configPath)
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

	paths := cfg.ResolvePaths()

	// --- Lock every configured storage root for the daemon's whole life. ---
	locks, err := LockRoots(paths.RootsToLock()...)
	if err != nil {
		return err
	}
	defer locks.Release()

	// --- Open the catalog store and bind the catalog. ---
	store, err := metastore.New(paths.Catalog, logger)
	if err != nil {
		return fmt.Errorf("open catalog %q: %w", paths.Catalog, err)
	}
	defer func() { _ = store.Close() }()

	txLayout, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	if err != nil {
		return err
	}
	cat := catalog.NewCatalog(store, NewLayoutFromPaths(paths), txLayout)

	// --- Resolve the backfill backend: injected (tests) or built from
	// [backfill.datastore] (production; nil ⇒ frontfill-only). Its Tip drives both
	// backfill's network tip and the freeze's coverage frontier, so validateConfig
	// (which needs the tip) runs after this. ---
	backend := opts.Backend
	if backend == nil {
		built, cleanup, berr := buildBackfillBackend(ctx, cfg, logger)
		if berr != nil {
			return fmt.Errorf("build backfill backend: %w", berr)
		}
		if cleanup != nil {
			defer cleanup()
		}
		backend = built
	}
	networkTip := resolveNetworkTip(backend)

	serveReads := opts.ServeReads
	if serveReads == nil {
		// TODO(#772): wire the full-history RPC read server; no-op until the cutover.
		serveReads = func(context.Context) error { return nil }
	}

	tipBackoff, tipMaxAttempts := defaultTipBackoff, defaultTipMaxAttempts

	// --- validateConfig: pin/confirm the layout, resolve the earliest floor. ---
	if _, err := validateConfig(ctx, cfg, cat, networkTip, tipBackoff, tipMaxAttempts); err != nil {
		return err
	}

	// Control-plane Metrics and the ingest sink share ONE registry, built after the
	// validateConfig gate (it registers Prometheus collectors).
	// TODO(#772): expose it on the read server's /metrics.
	registry := prometheus.NewRegistry()
	metrics, sink := buildSinks(opts, registry)

	// --- Assemble the StartConfig and run the supervised run loop. ---
	start := startConfig(cfg, cat, logger, backend, networkTip, serveReads, metrics, sink, tipBackoff, tipMaxAttempts)

	backoff := opts.RestartBackoff
	if backoff <= 0 {
		backoff = defaultRestartBackoff
	}
	return supervise(ctx, start, logger, backoff)
}

// startConfig assembles the StartConfig run consumes.
func startConfig(
	cfg Config, cat *catalog.Catalog, logger *supportlog.Entry,
	backend backfill.Backend, networkTip NetworkTipBackend, serveReads func(context.Context) error,
	metrics observability.Metrics, sink ingest.MetricSink, tipBackoff time.Duration, tipMaxAttempts int,
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
		Exec:            exec,
		RetentionChunks: deref(cfg.Retention.RetentionChunks),
		NetworkTip:      networkTip,
		ServeReads:      serveReads,
		TipBackoff:      tipBackoff,
		TipMaxAttempts:  tipMaxAttempts,
	}
}

// buildSinks resolves the control-plane Metrics + per-type ingest sink, defaulting
// each to a Prometheus implementation on the shared registry when unset.
func buildSinks(opts daemonOptions, registry *prometheus.Registry) (observability.Metrics, ingest.MetricSink) {
	metrics := opts.Metrics
	if metrics == nil {
		metrics = observability.NewPrometheusMetrics(registry, interfaces.PrometheusNamespace)
	}
	sink := opts.IngestSink
	if sink == nil {
		sink = ingest.NewPrometheusSink(registry, interfaces.PrometheusNamespace)
	}
	return metrics, sink
}

// supervise restarts run on a restartable error after a backoff ("startup is the
// recovery path"); a clean shutdown or ctx cancel returns nil; ErrFirstStartNoTip
// is fatal and surfaces up.
func supervise(
	ctx context.Context, start StartConfig, logger *supportlog.Entry, backoff time.Duration,
) error {
	for {
		err := run(ctx, start)
		if err == nil {
			return nil // clean shutdown
		}
		if ctx.Err() != nil {
			return nil //nolint:nilerr // ctx canceled is a clean shutdown, not a run failure
		}
		// Unrecoverable: a fresh start cannot heal it, so don't spin restarting.
		if errors.Is(err, ErrFirstStartNoTip) {
			return err
		}
		logger.WithError(err).Warnf("daemon run failed; restarting in %s", backoff)
		if sleepCtx(ctx, backoff) != nil {
			return nil //nolint:nilerr // ctx canceled mid-backoff is a clean shutdown, not a failure
		}
	}
}

// sleepCtx blocks for d or until ctx is canceled, returning ctx.Err() if canceled
// first and nil otherwise. supervise's three-way clean/fatal/restart loop can't be
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

// buildBackfillBackend opens the bulk ledger source from [backfill.datastore]: any
// SDK datastore (GCS/S3/Filesystem/...) wrapped as a backfill.Backend, plus a cleanup
// that releases the datastore handle at shutdown. With no datastore configured it
// returns (nil, nil, nil) — a frontfill-only daemon (the network tip is then the
// not-configured placeholder and backfillSource errors only on a backend-only chunk).
func buildBackfillBackend(
	ctx context.Context, cfg Config, logger *supportlog.Entry,
) (backfill.Backend, func(), error) {
	if cfg.Backfill.DataStore.Type == "" {
		return nil, nil, nil // frontfill-only
	}
	backend, cleanup, err := backfill.NewBSBBackendFromConfig(ctx, cfg.Backfill.DataStore, cfg.Backfill.BSB)
	if err != nil {
		return nil, nil, err
	}
	logger.WithField("datastore_type", cfg.Backfill.DataStore.Type).Info("wired BSB backfill backend")
	return backend, cleanup, nil
}

// resolveNetworkTip adapts the backfill backend to backfill's tip sampler — its Tip
// frontier (so the tip and the freeze's coverage frontier are one source) — or the
// not-configured placeholder for a frontfill-only daemon (nil backend).
func resolveNetworkTip(backend backfill.Backend) NetworkTipBackend {
	if backend == nil {
		return notConfiguredTip{}
	}
	return backendTip{backend}
}

// notConfiguredTip is the NetworkTipBackend placeholder when no bulk backend is
// configured: every sample returns a clear not-configured error (until #772 wires
// the real lake tip).
type notConfiguredTip struct{}

func (notConfiguredTip) NetworkTip(context.Context) (uint32, error) {
	return 0, errors.New("no bulk backend configured ([backfill.datastore].type empty); " +
		"cannot sample the network tip (configure a backend, or this is a frontfill-only deployment)")
}

// backendTip adapts a backfill.Backend to NetworkTipBackend via its Tip frontier, so
// backfill's tip and the freeze's coverage frontier are sampled from one source.
type backendTip struct{ backend backfill.Backend }

func (t backendTip) NetworkTip(ctx context.Context) (uint32, error) { return t.backend.Tip(ctx) }

// newLogger builds a daemon logger from the [logging] config.
func newLogger(cfg LoggingConfig) (*supportlog.Entry, error) {
	level, err := logrus.ParseLevel(cfg.Level)
	if err != nil {
		return nil, fmt.Errorf("invalid logging.level %q: %w", cfg.Level, err)
	}
	logger := supportlog.New()
	logger.SetLevel(level)
	if cfg.Format == "json" {
		logger.UseJSONFormatter()
	}
	return logger, nil
}

// compile-time interface checks.
var (
	_ NetworkTipBackend = notConfiguredTip{}
	_ NetworkTipBackend = backendTip{}
)
