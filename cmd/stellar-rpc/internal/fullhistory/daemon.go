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
	// BuildBoundaries assembles the external boundaries; nil ⇒ buildProductionBoundaries.
	BuildBoundaries func(
		ctx context.Context, cfg Config, paths Paths, cat *catalog.Catalog, logger *supportlog.Entry,
	) (Boundaries, error)

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

// Boundaries bundles the external boundaries run and validateConfig inject.
type Boundaries struct {
	// NetworkTip samples the bulk backend's current network tip. Required.
	NetworkTip NetworkTipBackend

	// Backend is the bulk ledger source backfill freezes from; its own Tip drives the
	// coverage wait. Nil in a frontfill-only deployment (backfillSource then errors
	// if a chunk has no local copy).
	Backend backfill.Backend

	// ServeReads launches the RPC read server; it must return promptly, not block. Required.
	// TODO(#772): today a no-op (reads still come from the v1 SQLite daemon); the cutover wires handlers here.
	ServeReads func(ctx context.Context) error
}

func (b Boundaries) validate() error {
	if b.NetworkTip == nil {
		return errors.New("nil Boundaries.NetworkTip")
	}
	if b.ServeReads == nil {
		return errors.New("nil Boundaries.ServeReads")
	}
	return nil
}

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

	// --- Build the external boundaries (validateConfig needs NetworkTip, so this
	// must precede it). ---
	build := opts.BuildBoundaries
	if build == nil {
		build = buildProductionBoundaries
	}
	boundaries, err := build(ctx, cfg, paths, cat, logger)
	if err != nil {
		return fmt.Errorf("build boundaries: %w", err)
	}
	if err := boundaries.validate(); err != nil {
		return err
	}

	tipBackoff, tipMaxAttempts := defaultTipBackoff, defaultTipMaxAttempts

	// --- validateConfig: pin/confirm the layout, resolve the earliest floor. ---
	if _, err := validateConfig(ctx, cfg, cat, boundaries.NetworkTip, tipBackoff, tipMaxAttempts); err != nil {
		return err
	}

	// Control-plane Metrics and the ingest sink share ONE registry, built after the
	// validateConfig gate (it registers Prometheus collectors).
	// TODO(#772): expose it on the read server's /metrics.
	registry := prometheus.NewRegistry()
	metrics := opts.Metrics
	if metrics == nil {
		metrics = observability.NewPrometheusMetrics(registry, interfaces.PrometheusNamespace)
	}
	sink := opts.IngestSink
	if sink == nil {
		sink = ingest.NewPrometheusSink(registry, interfaces.PrometheusNamespace)
	}

	// --- Assemble the StartConfig and run the supervised run loop. ---
	start := startConfig(cfg, cat, logger, boundaries, metrics, sink, tipBackoff, tipMaxAttempts)

	backoff := opts.RestartBackoff
	if backoff <= 0 {
		backoff = defaultRestartBackoff
	}
	return supervise(ctx, start, logger, backoff)
}

// startConfig assembles the StartConfig run consumes.
func startConfig(
	cfg Config, cat *catalog.Catalog, logger *supportlog.Entry, b Boundaries, metrics observability.Metrics,
	sink ingest.MetricSink, tipBackoff time.Duration, tipMaxAttempts int,
) StartConfig {
	exec := backfill.ExecConfig{
		Catalog:    cat,
		Logger:     logger,
		Metrics:    observability.MetricsOrNop(metrics),
		Workers:    deref(cfg.Backfill.Workers),
		MaxRetries: deref(cfg.Backfill.MaxRetries),
		Process: backfill.ProcessConfig{
			Backend: b.Backend,
			Sink:    sink,
		},
	}
	return StartConfig{
		Exec:            exec,
		RetentionChunks: deref(cfg.Retention.RetentionChunks),
		NetworkTip:      b.NetworkTip,
		ServeReads:      b.ServeReads,
		TipBackoff:      tipBackoff,
		TipMaxAttempts:  tipMaxAttempts,
	}
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
			return nil // ctx cancelled: the error is the shutdown teardown
		}
		// Unrecoverable: a fresh start cannot heal it, so don't spin restarting.
		if errors.Is(err, ErrFirstStartNoTip) {
			return err
		}
		logger.WithError(err).Warnf("daemon run failed; restarting in %s", backoff)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

// ---------------------------------------------------------------------------
// Production boundary construction.
// ---------------------------------------------------------------------------

// buildProductionBoundaries assembles the real external boundaries from the config.
//
// TODO(#772): the bulk-backend tip/lake resolution doesn't exist on this branch yet.
// Until the cutover, a deployment needing catch-up against a real lake must wire
// NetworkTip/Backend via daemonOptions.BuildBoundaries.
func buildProductionBoundaries(
	_ context.Context, _ Config, _ Paths, _ *catalog.Catalog, _ *supportlog.Entry,
) (Boundaries, error) {
	b := Boundaries{
		// TODO(#772): wire the full-history RPC read server; no-op until the cutover.
		ServeReads: func(context.Context) error { return nil },
	}

	// Absent a configured backend this is frontfill-only: NetworkTip returns a
	// not-configured error and Backend stays nil.
	tip := &notConfiguredTip{}
	b.NetworkTip = tip
	return b, nil
}

// notConfiguredTip is the NetworkTipBackend placeholder when no bulk backend is
// configured: every sample returns a clear not-configured error (until #772 wires
// the real lake tip).
type notConfiguredTip struct{}

func (notConfiguredTip) NetworkTip(context.Context) (uint32, error) {
	return 0, errors.New("no bulk backend configured ([backfill.bsb].bucket_path empty); " +
		"cannot sample the network tip (configure a backend, or this is a frontfill-only deployment)")
}

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

// compile-time interface check.
var _ NetworkTipBackend = notConfiguredTip{}
