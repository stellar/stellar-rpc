package fullhistory

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
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
	return RunDaemonWith(ctx, configPath, DaemonOptions{})
}

// DaemonOptions carries the daemon's injectable seams; production leaves every field zero.
type DaemonOptions struct {
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

	// BackendWaiter bounds wait-for-coverage on a backend-only chunk; required iff Backend is set.
	BackendWaiter backfill.BackendWaiter

	// Backend is the bulk LedgerBackend as a ChunkSource; nil in a frontfill-only deployment.
	Backend ingest.ChunkSource

	// ServeReads launches the RPC read server; it must return promptly, not block. Required.
	//
	// TODO(#772): v1-cutover seam — today a no-op (reads still come from the v1 SQLite
	// daemon); the #772 cutover wires the full-history RPC handlers here.
	ServeReads func(ctx context.Context) error
}

func (b Boundaries) validate() error {
	if b.NetworkTip == nil {
		return errors.New("streaming: Boundaries.NetworkTip is nil")
	}
	if b.ServeReads == nil {
		return errors.New("streaming: Boundaries.ServeReads is nil")
	}
	if b.Backend != nil && b.BackendWaiter == nil {
		return errors.New("streaming: Boundaries.BackendWaiter is required when Backend is set")
	}
	return nil
}

// RunDaemonWith is RunDaemon with explicit options — the seam tests drive.
func RunDaemonWith(ctx context.Context, configPath string, opts DaemonOptions) error {
	// --- Load + form-validate the config. ---
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return err
	}
	if cfg.Service.DefaultDataDir == "" {
		return errors.New("streaming: [service].default_data_dir is required")
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
		return fmt.Errorf("streaming: open catalog %q: %w", paths.Catalog, err)
	}
	defer func() { _ = store.Close() }()

	// txhash-index layout is built from the fixed geometry.ChunksPerTxhashIndex constant.
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
		return fmt.Errorf("streaming: build boundaries: %w", err)
	}
	if err := boundaries.validate(); err != nil {
		return err
	}

	tipBackoff, tipMaxAttempts := defaultTipBackoff, defaultTipMaxAttempts

	// --- validateConfig: pin/confirm the layout, resolve the earliest floor. ---
	if _, err := validateConfig(ctx, cfg, cat, boundaries.NetworkTip, tipBackoff, tipMaxAttempts); err != nil {
		return err
	}

	// --- Control-plane Metrics and the ingest sink share ONE registry. Built
	// after the validateConfig gate (it registers Prometheus collectors).
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
		Workers:    derefInt(cfg.Backfill.Workers),
		MaxRetries: derefInt(cfg.Backfill.MaxRetries),
		Process: backfill.ProcessConfig{
			Backend:       b.Backend,
			BackendWaiter: b.BackendWaiter,
			Sink:          sink,
		},
	}
	return StartConfig{
		Exec:            exec,
		RetentionChunks: derefU32(cfg.Retention.RetentionChunks),
		NetworkTip:      b.NetworkTip,
		ServeReads:      b.ServeReads,
		TipBackoff:      tipBackoff,
		TipMaxAttempts:  tipMaxAttempts,
	}
}

// supervise runs run, restarting it on a restartable error
// after a backoff ("startup is the recovery path"); a clean shutdown or ctx
// cancel returns nil. The fatal sentinel ErrFirstStartNoTip surfaces up, not retried.
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
		logger.WithError(err).Warnf("streaming: daemon run failed; restarting in %s", backoff)
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
// TODO(#772): the bulk-backend tip boundary still depends on config/lake
// tip-resolution that doesn't exist on this branch. Until the cutover, a
// deployment needing catch-up against a real lake must wire
// NetworkTip/BackendWaiter/Backend via DaemonOptions.BuildBoundaries; this
// supplies a tip adapter that errors clearly when no backend is configured.
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

// notConfiguredTip is the NetworkTipBackend placeholder for a deployment with no
// bulk backend: every sample returns a clear not-configured error (until #772
// wires the real lake tip). Benign for the genesis-floor steady state; correctly
// blocks the cases that genuinely require a tip.
type notConfiguredTip struct{}

func (notConfiguredTip) NetworkTip(context.Context) (uint32, error) {
	return 0, errors.New("streaming: no bulk backend configured ([backfill.bsb].bucket_path empty); " +
		"cannot sample the network tip (configure a backend, or this is a frontfill-only deployment)")
}

// ---------------------------------------------------------------------------
// Bulk-backend tip/coverage adapter (reused verbatim by the #772 cutover).
// ---------------------------------------------------------------------------

// backendTip adapts a ledgerbackend.LedgerBackend to NetworkTipBackend + BackendWaiter.
type backendTip struct {
	backend   ledgerbackend.LedgerBackend
	pollEvery time.Duration
	deadline  time.Duration
}

// newBackendTip wraps a prepared LedgerBackend; zero pollEvery/deadline fall back to defaults.
func newBackendTip(backend ledgerbackend.LedgerBackend, pollEvery, deadline time.Duration) *backendTip {
	if pollEvery <= 0 {
		pollEvery = time.Second
	}
	if deadline <= 0 {
		deadline = 10 * time.Minute
	}
	return &backendTip{backend: backend, pollEvery: pollEvery, deadline: deadline}
}

func (t *backendTip) NetworkTip(ctx context.Context) (uint32, error) {
	return t.backend.GetLatestLedgerSequence(ctx)
}

// WaitForCoverage blocks until the tip covers chunkLastLedger, returning a
// wrapped ErrBackendCoverageTimeout past the deadline. A chunk with a local copy
// never reaches here.
func (t *backendTip) WaitForCoverage(ctx context.Context, chunkLastLedger uint32) error {
	deadline := time.Now().Add(t.deadline)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		tip, err := t.backend.GetLatestLedgerSequence(ctx)
		if err == nil && tip >= chunkLastLedger {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%w: tip never reached ledger %d within %s",
				backfill.ErrBackendCoverageTimeout, chunkLastLedger, t.deadline)
		}
		timer := time.NewTimer(t.pollEvery)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// newLogger builds a daemon logger from the [logging] config.
func newLogger(cfg LoggingConfig) (*supportlog.Entry, error) {
	level, err := logrus.ParseLevel(cfg.Level)
	if err != nil {
		return nil, fmt.Errorf("streaming: invalid logging.level %q: %w", cfg.Level, err)
	}
	logger := supportlog.New()
	logger.SetLevel(level)
	if cfg.Format == "json" {
		logger.UseJSONFormatter()
	}
	return logger, nil
}

// compile-time assertions: the production adapters satisfy the injected interfaces.
var (
	_ NetworkTipBackend      = (*backendTip)(nil)
	_ backfill.BackendWaiter = (*backendTip)(nil)
	_ NetworkTipBackend      = notConfiguredTip{}
)
