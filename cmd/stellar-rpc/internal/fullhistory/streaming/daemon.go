package streaming

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/geometry"
)

// RunDaemon is the full-history streaming daemon's process entrypoint — the
// design's "Daemon flow" from a cold start. It owns everything startStreaming
// cannot construct itself, in the order the design mandates:
//
//  1. LOAD + form-validate the TOML config (LoadConfig).
//  2. LOCK every configured storage root (one flock per root, design
//     "Single-process enforcement") — fail fast if a second daemon is using one.
//  3. OPEN the catalog store and bind the Catalog (the single durable-state view
//     both startup and the lifecycle goroutine read).
//  4. validateConfig — the stateful config gate: pin the two immutable layout
//     values on first start, confirm them unchanged on restart, and resolve the
//     earliest_ledger floor (consulting the bulk backend's tip for "now"/numeric
//     floors). It pins config:earliest_ledger BEFORE startStreaming reads it.
//  5. BUILD the production boundaries (captive core, the bulk ChunkSource +
//     its tip/coverage adapter, the read server) — injectable so a test drives
//     the whole flow with fakes.
//  6. RUN the supervised startStreaming loop: it returns nil only on a clean
//     shutdown (ctx cancelled); any other return is a restartable error the loop
//     retries on a backoff — the design's "startup is the recovery path".
//
// The locks are held for the daemon's whole life (released on return). A ctx
// cancel during the supervised loop returns nil; a cancel mid-build returns the
// build error.
func RunDaemon(ctx context.Context, configPath string) error {
	return RunDaemonWith(ctx, configPath, DaemonOptions{})
}

// DaemonOptions carries the daemon's injectable seams. Production leaves every
// field zero (RunDaemon); tests set them to drive the whole RunDaemon flow
// (config load, locking, validateConfig, the supervised loop) against fakes.
type DaemonOptions struct {
	// BuildBoundaries assembles the injected external boundaries from the loaded
	// config, the resolved paths, the bound catalog, and the logger. nil ⇒
	// buildProductionBoundaries (the real captive core + bulk datastore source).
	// A test passes fakes here to exercise RunDaemon end to end.
	BuildBoundaries func(
		ctx context.Context, cfg Config, paths Paths, cat *catalog.Catalog, logger *supportlog.Entry,
	) (Boundaries, error)

	// RestartBackoff is the supervised loop's inter-restart sleep after a
	// restartable startStreaming error. Zero ⇒ defaultRestartBackoff. A clean
	// shutdown (ctx cancelled) never sleeps.
	RestartBackoff time.Duration

	// Logger overrides the daemon logger. nil ⇒ a logger built from
	// [logging].level / [logging].format.
	Logger *supportlog.Entry

	// Metrics is the streaming control-plane sink (catch-up + lifecycle phases).
	// nil ⇒ a *PrometheusMetrics on the daemon's registry; tests pass a recorder.
	Metrics Metrics

	// IngestSink is the per-type INGEST metrics sink for the cold path (processChunk
	// → RunColdChunk → ColdService). nil ⇒ a *ingest.PrometheusSink on the daemon's
	// registry (#808 comment pt 3); tests pass a recorder.
	IngestSink ingest.MetricSink
}

const defaultRestartBackoff = 5 * time.Second

// Boundaries bundles the external boundaries startStreaming and validateConfig
// inject. buildProductionBoundaries fills them from a Config; startConfig threads
// them into the StartConfig. Gathered here so the production and test builders
// return one shape that RunDaemon wires one way.
type Boundaries struct {
	// NetworkTip samples the bulk backend's current network tip — consulted by
	// validateConfig (resolving "now"/numeric floors) and by catch-up. Required.
	NetworkTip NetworkTipBackend

	// BackendWaiter bounds backfillSource's wait-for-coverage on a backend-only
	// chunk. Required iff Backend is set (paired with it in ProcessConfig).
	BackendWaiter BackendWaiter

	// Backend is the bulk LedgerBackend as a ChunkSource (BSB by default), the
	// only source for a chunk with no local copy. May be nil in a frontfill-only
	// deployment that never backfills.
	Backend ingest.ChunkSource

	// Core starts captive core at the resume ledger and yields the live getter
	// the ingestion loop polls. Required.
	Core CoreOpener

	// ServeReads launches the RPC read server (it must return promptly, not block
	// until shutdown). Required.
	//
	// TODO(#772): this is the v1-cutover seam. Today buildProductionBoundaries
	// supplies a no-op ServeReads — the SQLite read path is still the v1 daemon's
	// (cmd/.../internal/daemon/daemon.go), and the full SQLite→full-history
	// cutover is issue #772. When #772 flips the read path, ServeReads wires the
	// full-history RPC handlers here; nothing else in this entrypoint changes.
	ServeReads func(ctx context.Context) error
}

func (b Boundaries) validate() error {
	if b.NetworkTip == nil {
		return errors.New("streaming: Boundaries.NetworkTip is nil")
	}
	if b.Core == nil {
		return errors.New("streaming: Boundaries.Core is nil")
	}
	if b.ServeReads == nil {
		return errors.New("streaming: Boundaries.ServeReads is nil")
	}
	if b.Backend != nil && b.BackendWaiter == nil {
		return errors.New("streaming: Boundaries.BackendWaiter is required when Backend is set")
	}
	return nil
}

// RunDaemonWith is RunDaemon with explicit options — the seam tests drive. The
// stages are documented on RunDaemon.
func RunDaemonWith(ctx context.Context, configPath string, opts DaemonOptions) error {
	// --- 1. Load + form-validate the config. ---
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

	// --- 2. Lock every configured storage root for the daemon's whole life. ---
	locks, err := LockRoots(paths.RootsToLock()...)
	if err != nil {
		return err
	}
	defer locks.Release()

	// --- 3. Open the catalog store and bind the catalog. ---
	store, err := metastore.New(paths.Catalog, logger)
	if err != nil {
		return fmt.Errorf("streaming: open catalog %q: %w", paths.Catalog, err)
	}
	defer func() { _ = store.Close() }()

	windows, err := geometry.NewTxHashIndexLayout(derefU32(cfg.Layout.ChunksPerTxhashIndex))
	if err != nil {
		return err
	}
	cat := catalog.NewCatalog(store, NewLayoutFromPaths(paths), windows)

	// --- 5a. Build the external boundaries (validateConfig needs NetworkTip). ---
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

	// --- 4. validateConfig: pin/confirm the layout, resolve the earliest floor. ---
	if _, err := validateConfig(ctx, cfg, cat, boundaries.NetworkTip, tipBackoff, tipMaxAttempts); err != nil {
		return err
	}

	// --- 5b. Wire the daemon metrics registry: the control-plane Metrics and the
	// per-type ingest sink share ONE registry (#808 comment pt 3), both defaulting
	// to real Prometheus collectors. TODO(#772): expose it on the read server's /metrics.
	registry := prometheus.NewRegistry()
	metrics := opts.Metrics
	if metrics == nil {
		metrics = NewPrometheusMetrics(registry, interfaces.PrometheusNamespace)
	}
	sink := opts.IngestSink
	if sink == nil {
		sink = ingest.NewPrometheusSink(registry, interfaces.PrometheusNamespace)
	}

	// --- 6. Assemble the StartConfig and run the supervised startStreaming loop. ---
	start := startConfig(cfg, cat, logger, boundaries, metrics, sink, tipBackoff, tipMaxAttempts)

	backoff := opts.RestartBackoff
	if backoff <= 0 {
		backoff = defaultRestartBackoff
	}
	return superviseStreaming(ctx, start, logger, backoff)
}

// startConfig threads the loaded Config, the bound catalog/logger, and the
// assembled boundaries into the StartConfig startStreaming consumes. The Exec
// and Lifecycle bundles share ONE catalog, worker pool, and retention floor (the
// design's "catch-up and the lifecycle goroutine share one set of
// postconditions"), so Lifecycle embeds the same ExecConfig.
func startConfig(
	cfg Config, cat *catalog.Catalog, logger *supportlog.Entry, b Boundaries, metrics Metrics,
	sink ingest.MetricSink, tipBackoff time.Duration, tipMaxAttempts int,
) StartConfig {
	exec := ExecConfig{
		Catalog:    cat,
		Logger:     logger,
		Metrics:    metricsOrNop(metrics),
		Workers:    derefInt(cfg.Backfill.Workers),
		MaxRetries: derefInt(cfg.Backfill.MaxRetries),
		Process: ProcessConfig{
			Backend:       b.Backend,
			BackendWaiter: b.BackendWaiter,
			Sink:          sink,
			HotProbe:      NewRocksHotProbe(cat.Layout().HotChunkPath, logger),
		},
	}
	life := LifecycleConfig{
		ExecConfig:      exec,
		RetentionChunks: derefU32(cfg.Retention.RetentionChunks),
	}
	return StartConfig{
		Exec:           exec,
		Lifecycle:      life,
		NetworkTip:     b.NetworkTip,
		Core:           b.Core,
		ServeReads:     b.ServeReads,
		TipBackoff:     tipBackoff,
		TipMaxAttempts: tipMaxAttempts,
	}
}

// superviseStreaming is the daemon's top-level loop: it runs startStreaming and
// restarts it on a restartable error after a backoff ("startup is the recovery
// path"). A clean shutdown or a ctx cancel during the backoff returns nil.
//
// It does NOT swallow the fatal sentinels (ErrHotVolumeLost, ErrFirstStartNoTip):
// those are returned UP so an operator/supervisor sees them. The retry here is
// for transient restartable failures (a backfill/ingest hiccup, a captive core
// crash) where a fresh start converges; the unrecoverable ones surface.
func superviseStreaming(
	ctx context.Context, start StartConfig, logger *supportlog.Entry, backoff time.Duration,
) error {
	for {
		err := startStreaming(ctx, start)
		if err == nil {
			return nil // clean shutdown
		}
		if ctx.Err() != nil {
			return nil // ctx cancelled: the error is the shutdown teardown
		}
		// Unrecoverable: surface up rather than spin restarting on a condition a
		// fresh start cannot heal.
		if errors.Is(err, ErrHotVolumeLost) || errors.Is(err, ErrFirstStartNoTip) {
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

// buildProductionBoundaries assembles the real external boundaries from the
// loaded config.
//
//   - Core: captive stellar-core via newCaptiveCoreOpener; OpenCore calls
//     PrepareRange(UnboundedRange(resume)) and hands back a LedgerGetter the
//     ingestion loop polls by sequence (the design's core.GetLedger(ctx, seq)),
//     plus a closer. The config plumbing is deferred (TODO below), so today the
//     constructor errors with a #772 pointer.
//   - Backend: the bulk datastore ChunkSource (NewDataStoreSource) when a bucket
//     path is configured; nil for a frontfill-only deployment.
//   - NetworkTip / BackendWaiter: an adapter over the bulk backend's tip.
//
// TODO(#772): both the captive-core config (binary path, passphrase, archives —
// see newCaptiveCoreOpener) and the bulk-backend TIP boundary (the datastore
// TYPE + schema; only [backfill.bsb].bucket_path is in Config today) are
// entangled with config that does not yet exist on this branch and with the lake
// tip-resolution the v1 path performs differently. Until #772 lands the cutover,
// a deployment must wire Core (and, for catch-up against a real lake, NetworkTip/
// BackendWaiter/Backend) through DaemonOptions.BuildBoundaries; the tip adapter
// here errors clearly when no bulk backend is configured, so a frontfill
// ("genesis" or "now" with no backfill) deployment runs unchanged.
func buildProductionBoundaries(
	_ context.Context, cfg Config, _ Paths, _ *catalog.Catalog, logger *supportlog.Entry,
) (Boundaries, error) {
	core, err := newCaptiveCoreOpener(cfg.Streaming.CaptiveCoreConfig, logger)
	if err != nil {
		return Boundaries{}, err
	}

	b := Boundaries{
		Core: core,
		// TODO(#772): wire the full-history RPC read server. The SQLite read path
		// is still the v1 daemon's; until the #772 cutover, serving is a no-op here
		// so the streaming daemon ingests + freezes without double-serving reads.
		ServeReads: func(context.Context) error { return nil },
	}

	// The bulk tip/coverage/source. Absent a configured backend this is a
	// frontfill-only deployment: NetworkTip degrades to an explicit
	// not-configured error (catch-up classifies it first-start-fatal vs degrade),
	// and Backend stays nil (backfillSource errors loudly only if a chunk actually
	// reaches the bulk branch).
	tip := &notConfiguredTip{}
	b.NetworkTip = tip
	return b, nil
}

// captiveCoreOpener is the production CoreOpener: it prepares captive core at the
// resume ledger and hands back a LedgerGetter the ingestion loop polls by
// sequence (the design's core.GetLedger(ctx, seq)) plus a closer.
type captiveCoreOpener struct {
	backend ledgerbackend.LedgerBackend
}

func newCaptiveCoreOpener(captiveCoreConfigPath string, logger *supportlog.Entry) (*captiveCoreOpener, error) {
	if captiveCoreConfigPath == "" {
		return nil, errors.New("streaming: [streaming].captive_core_config is required")
	}
	// TODO(#772): the captive-core CaptiveCoreConfig (binary path, network
	// passphrase, history-archive URLs, storage path) is assembled from the v1
	// daemon config today; threading those through the streaming Config is part
	// of the cutover. The factory below is the wiring point — once the fields are
	// in Config, build a ledgerbackend.CaptiveCoreConfig from
	// NewCaptiveCoreTomlFromFile(captiveCoreConfigPath, ...) and NewCaptive, then
	// PrepareRange(UnboundedRange(resume)) in OpenCore. The seam (a LedgerGetter
	// behind CoreOpener) is final; only the config plumbing is deferred.
	return nil, fmt.Errorf("streaming: production captive-core wiring is deferred to #772 "+
		"(config %q parsed; pass a CoreOpener via DaemonOptions.BuildBoundaries to run today)",
		captiveCoreConfigPath)
}

// OpenCore prepares the backend over the unbounded range from resumeLedger and
// returns a getter wrapping GetLedger plus the backend's Close.
func (c *captiveCoreOpener) OpenCore(
	ctx context.Context, resumeLedger uint32,
) (LedgerGetter, func() error, error) {
	if err := c.backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(resumeLedger)); err != nil {
		return nil, nil, fmt.Errorf("streaming: captive core prepare range from %d: %w", resumeLedger, err)
	}
	return backendGetter{backend: c.backend}, c.backend.Close, nil
}

// backendGetter adapts a ledgerbackend.LedgerBackend to LedgerGetter: GetLedger
// blocks until the ledger is available and returns its raw wire bytes.
type backendGetter struct {
	backend ledgerbackend.LedgerBackend
}

func (g backendGetter) GetLedger(ctx context.Context, seq uint32) (xdr.LedgerCloseMetaView, error) {
	lcm, err := g.backend.GetLedger(ctx, seq)
	if err != nil {
		return nil, err
	}
	raw, err := lcm.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("streaming: marshal ledger %d: %w", seq, err)
	}
	return xdr.LedgerCloseMetaView(raw), nil
}

// notConfiguredTip is the NetworkTipBackend for a deployment with no bulk
// backend configured: every sample returns a clear not-configured error — the
// honest placeholder until #772 wires the real lake tip.
//
// Benign for the genesis-floor steady state (validateConfig resolves genesis
// without a tip; with local progress catch-up degrades on a tip error). It DOES
// block the cases that genuinely require a tip — a first-start "now"/numeric
// floor and a catch-up extending storage downward — which is correct: those
// can't proceed against a backend that was never configured.
type notConfiguredTip struct{}

func (notConfiguredTip) NetworkTip(context.Context) (uint32, error) {
	return 0, errors.New("streaming: no bulk backend configured ([backfill.bsb].bucket_path empty); " +
		"cannot sample the network tip (configure a backend, or this is a frontfill-only deployment)")
}

// ---------------------------------------------------------------------------
// Bulk-backend tip/coverage adapter, split out so the #772 cutover can hand
// RunDaemon a prepared ledgerbackend.LedgerBackend and reuse it verbatim.
// ---------------------------------------------------------------------------

// backendTip adapts a ledgerbackend.LedgerBackend to NetworkTipBackend +
// BackendWaiter: NetworkTip reads the latest available ledger; WaitForCoverage
// polls until the tip covers a target or ctx/deadline elapses.
type backendTip struct {
	backend   ledgerbackend.LedgerBackend
	pollEvery time.Duration
	deadline  time.Duration
}

// newBackendTip wraps a prepared LedgerBackend. pollEvery is the coverage-poll
// interval; deadline bounds WaitForCoverage. Zero values fall back to sane
// defaults.
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

// WaitForCoverage blocks until the backend's tip covers chunkLastLedger, polling
// on pollEvery, returning ErrBackendCoverageTimeout (wrapped) past the deadline.
// A chunk with a local copy never reaches here, so this never gates a normal
// restart whose range is entirely local.
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
				ErrBackendCoverageTimeout, chunkLastLedger, t.deadline)
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

// newLogger builds a daemon logger from the [logging] config (level + format).
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

// compile-time assertions: the production adapters satisfy the injected
// interfaces startStreaming/processChunk consume.
var (
	_ CoreOpener        = (*captiveCoreOpener)(nil)
	_ LedgerGetter      = backendGetter{}
	_ NetworkTipBackend = (*backendTip)(nil)
	_ BackendWaiter     = (*backendTip)(nil)
	_ NetworkTipBackend = notConfiguredTip{}
)
