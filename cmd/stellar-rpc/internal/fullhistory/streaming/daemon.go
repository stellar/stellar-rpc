package streaming

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
)

// RunDaemon is the full-history streaming daemon's process entrypoint — the
// design's "Daemon flow" from a cold start. It owns everything startStreaming
// cannot construct itself, in the order the design mandates:
//
//  1. LOAD + form-validate the TOML config (LoadConfig).
//  2. LOCK every configured storage root (one flock per root, design
//     "Single-process enforcement") — fail fast if a second daemon is using one.
//  3. OPEN the meta store and bind the Catalog (the single durable-state view
//     both startup and the lifecycle goroutine read).
//  4. validateConfig — the stateful config gate: pin the two immutable layout
//     values on first start, confirm them unchanged on restart, and resolve the
//     earliest_ledger floor (consulting the bulk backend's tip for "now"/numeric
//     floors). It pins config:earliest_ledger BEFORE startStreaming reads it.
//  5. BUILD the production boundaries (captive core, the bulk ChunkSource +
//     its tip/coverage adapter, the read server) — injectable so a test drives
//     the whole flow with fakes.
//  6. RUN the supervised startStreaming loop: startStreaming returns nil only on
//     a clean shutdown (ctx cancelled); any other return is a restartable error
//     this loop surfaces and retries on a backoff, which is the design's
//     "startup is the recovery path" (a fresh start re-runs catch-up + the first
//     lifecycle tick, finishing crash debris and pruning downtime leftovers).
//
// The locks are held for the daemon's whole life (released on return). ctx
// cancellation propagates cleanly through every stage: a cancel during the
// supervised loop returns nil (clean shutdown), a cancel mid-build returns the
// build error.
func RunDaemon(ctx context.Context, configPath string) error {
	return RunDaemonWith(ctx, configPath, DaemonOptions{})
}

// DaemonOptions carries the daemon's injectable seams. Production leaves every
// field zero (RunDaemon), so the real captive core / bulk backend / RPC server
// are wired by buildProductionBoundaries. Tests set BuildBoundaries (and,
// optionally, RestartBackoff) to drive the whole RunDaemon flow — config load,
// locking, validateConfig, the supervised loop — against fakes, without standing
// up captive core or a real object store.
type DaemonOptions struct {
	// BuildBoundaries assembles the injected external boundaries from the loaded
	// config, the resolved paths, the bound catalog, and the logger. nil ⇒
	// buildProductionBoundaries (the real captive core + bulk datastore source).
	// A test passes fakes here to exercise RunDaemon end to end.
	BuildBoundaries func(
		ctx context.Context, cfg Config, paths Paths, cat *Catalog, logger *supportlog.Entry,
	) (Boundaries, error)

	// RestartBackoff is the supervised loop's inter-restart sleep after a
	// restartable startStreaming error. Zero ⇒ defaultRestartBackoff. A clean
	// shutdown (ctx cancelled) never sleeps.
	RestartBackoff time.Duration

	// Logger overrides the daemon logger. nil ⇒ a logger built from
	// [logging].level / [logging].format.
	Logger *supportlog.Entry

	// Metrics is the streaming control-plane observability sink threaded into
	// catch-up, the ingestion loop, and the lifecycle tick. nil ⇒ nopMetrics (the
	// daemon runs uninstrumented). Production wires a *PrometheusMetrics built from
	// the daemon's MetricsRegistry via NewPrometheusMetrics; tests pass a recorder
	// to assert the phase signals.
	Metrics Metrics
}

const defaultRestartBackoff = 5 * time.Second

// Boundaries bundles the four external boundaries startStreaming and
// validateConfig inject. buildProductionBoundaries fills them from a Config;
// startConfig threads them into the StartConfig startStreaming consumes. They
// are gathered here (rather than passed positionally) so the production builder
// and a test builder return the same shape and RunDaemon wires it one way.
type Boundaries struct {
	// NetworkTip samples the bulk backend's current network tip — consulted by
	// validateConfig (resolving "now"/numeric floors) and by catch-up. Required.
	NetworkTip NetworkTipBackend

	// BackendWaiter bounds catchupSource's wait-for-coverage on a backend-only
	// chunk. Required iff Backend is set (paired with it in ProcessConfig).
	BackendWaiter BackendWaiter

	// Backend is the bulk LedgerBackend as a ChunkSource (BSB by default), the
	// only source for a chunk with no local copy. May be nil in a frontfill-only
	// deployment that never backfills.
	Backend ingest.ChunkSource

	// Core starts captive core at the resume ledger and yields the live stream
	// the ingestion loop drains. Required.
	Core CoreStreamOpener

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
	locks, err := LockRoots(paths.LockRoots()...)
	if err != nil {
		return err
	}
	defer locks.Release()

	// --- 3. Open the meta store and bind the catalog. ---
	store, err := metastore.New(paths.MetaStore, logger)
	if err != nil {
		return fmt.Errorf("streaming: open meta store %q: %w", paths.MetaStore, err)
	}
	defer func() { _ = store.Close() }()

	windows, err := NewWindows(derefU32(cfg.CatchUp.ChunksPerTxhashIndex))
	if err != nil {
		return err
	}
	cat := NewCatalog(store, NewLayoutFromPaths(paths), windows)

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

	// --- 5b/6. Assemble the StartConfig and run the supervised startStreaming loop. ---
	start := startConfig(cfg, cat, logger, boundaries, opts.Metrics, tipBackoff, tipMaxAttempts)

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
	cfg Config, cat *Catalog, logger *supportlog.Entry, b Boundaries, metrics Metrics,
	tipBackoff time.Duration, tipMaxAttempts int,
) StartConfig {
	exec := ExecConfig{
		Catalog:    cat,
		Logger:     logger,
		Metrics:    metricsOrNop(metrics),
		Workers:    derefInt(cfg.CatchUp.Workers),
		MaxRetries: derefInt(cfg.CatchUp.MaxRetries),
		Process: ProcessConfig{
			HotProbe:      NewRocksHotProbe(cat.Layout().HotChunkPath, logger),
			Backend:       b.Backend,
			BackendWaiter: b.BackendWaiter,
		},
	}
	life := LifecycleConfig{
		ExecConfig:      exec,
		RetentionChunks: derefU32(cfg.Streaming.RetentionChunks),
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

// superviseStreaming is the daemon's top-level loop: it runs startStreaming and,
// per the design ("startup is the recovery path"), restarts it on a restartable
// error after a backoff. A clean shutdown (startStreaming returns nil, which it
// only does on ctx cancellation) returns nil. A cancelled ctx during the backoff
// also returns nil — no restart after a shutdown request.
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
// loaded config:
//
//   - Core: captive stellar-core via NewCaptiveCoreStream, wrapped so
//     OpenLedgerStream hands the live stream to the ingestion loop (the stream
//     owns the core process lifecycle — started on the first RawLedgers pull,
//     torn down when iteration ends — so this builder constructs it without
//     sequencing PrepareRange/Close itself).
//   - Backend: the bulk datastore ChunkSource (NewDataStoreSource) when a bucket
//     path is configured; nil for a frontfill-only deployment.
//   - NetworkTip / BackendWaiter: an adapter over the bulk backend's tip.
//
// TODO(#772): the bulk-backend TIP boundary is the one piece still entangled
// with config that does not yet exist on this branch (the datastore TYPE +
// schema — only [catch_up.bsb].bucket_path is in Config today) and with the lake
// tip-resolution the v1 path performs differently. Until #772 lands the cutover,
// a deployment that needs catch-up against a real lake must wire NetworkTip/
// BackendWaiter/Backend through DaemonOptions.BuildBoundaries; buildProduction-
// Boundaries supplies the captive-core Core (fully wired) and a tip adapter that
// errors clearly when no bulk backend is configured, so a frontfill ("genesis"
// or "now" with no backfill) deployment runs unchanged.
func buildProductionBoundaries(
	ctx context.Context, cfg Config, _ Paths, _ *Catalog, logger *supportlog.Entry,
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
	// and Backend stays nil (catchupSource errors loudly only if a chunk actually
	// reaches the bulk branch).
	tip := &notConfiguredTip{}
	b.NetworkTip = tip
	return b, nil
}

// captiveCoreOpener is the production CoreStreamOpener: it builds a captive-core
// LedgerStream once (the stream is stateless until its first RawLedgers pull,
// which the ingestion loop makes), and hands the SAME stream back on each
// OpenLedgerStream. The resumeLedger argument is informational here — the
// ingestion loop drives the stream with UnboundedRange(resume) itself, and the
// captive-core stream sets up core from that range on the first pull.
type captiveCoreOpener struct {
	stream ledgerbackend.LedgerStream
}

func newCaptiveCoreOpener(captiveCoreConfigPath string, logger *supportlog.Entry) (*captiveCoreOpener, error) {
	if captiveCoreConfigPath == "" {
		return nil, errors.New("streaming: [streaming].captive_core_config is required")
	}
	// TODO(#772): the captive-core CaptiveCoreConfig (binary path, network
	// passphrase, history-archive URLs, storage path) is assembled from the v1
	// daemon config today; threading those through the streaming Config is part
	// of the cutover. The stream factory below is the wiring point — once the
	// fields are in Config, build a ledgerbackend.CaptiveCoreConfig from
	// NewCaptiveCoreTomlFromFile(captiveCoreConfigPath, ...) and pass it to
	// NewCaptiveCoreStream. The seam (a LedgerStream behind CoreStreamOpener) is
	// final; only the config plumbing is deferred.
	return nil, fmt.Errorf("streaming: production captive-core wiring is deferred to #772 "+
		"(config %q parsed; pass a CoreStreamOpener via DaemonOptions.BuildBoundaries to run today)",
		captiveCoreConfigPath)
}

func (c *captiveCoreOpener) OpenLedgerStream(
	_ context.Context, _ uint32,
) (ledgerbackend.LedgerStream, error) {
	return c.stream, nil
}

// notConfiguredTip is the NetworkTipBackend for a deployment with no bulk
// backend configured: every sample returns a clear not-configured error. It is
// the honest placeholder until the #772 cutover wires the real lake tip.
//
// It is benign for the genesis-floor steady state: validateConfig resolves a
// genesis floor without a tip, and once there is local progress catch-up
// degrades on a tip error rather than fatals. It DOES block the cases that
// genuinely require a tip — a first-start "now"/numeric floor (validateConfig
// must resolve it) and a catch-up that needs to extend storage downward — which
// is correct: those cannot proceed against a backend that was never configured.
// A deployment needing either must wire a real NetworkTip via
// DaemonOptions.BuildBoundaries (or wait for #772).
type notConfiguredTip struct{}

func (notConfiguredTip) NetworkTip(context.Context) (uint32, error) {
	return 0, errors.New("streaming: no bulk backend configured ([catch_up.bsb].bucket_path empty); " +
		"cannot sample the network tip (configure a backend, or this is a frontfill-only deployment)")
}

// ---------------------------------------------------------------------------
// Bulk-backend tip/coverage adapter. Production wires these over a real
// ledgerbackend.LedgerBackend (a BufferedStorageBackend); they are split out so
// the #772 cutover can hand RunDaemon a prepared backend and reuse them verbatim.
// ---------------------------------------------------------------------------

// backendTip adapts a ledgerbackend.LedgerBackend to NetworkTipBackend +
// BackendWaiter. NetworkTip reads the backend's latest available ledger;
// WaitForCoverage polls it until the tip covers a target ledger or ctx/deadline
// elapses.
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
	_ CoreStreamOpener  = (*captiveCoreOpener)(nil)
	_ NetworkTipBackend = (*backendTip)(nil)
	_ BackendWaiter     = (*backendTip)(nil)
	_ NetworkTipBackend = notConfiguredTip{}
)
