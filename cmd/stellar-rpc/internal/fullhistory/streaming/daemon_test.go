package streaming

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
)

// openMetaAt opens a metastore.Store at path for read-back assertions.
func openMetaAt(t *testing.T, path string) (*metastore.Store, error) {
	t.Helper()
	return metastore.New(path, silentLogger())
}

// writeTempConfig writes a minimal-but-valid streaming-daemon TOML rooted at a
// temp data dir and returns the config path plus the data dir. A genesis
// earliest_ledger needs no tip, so the daemon validates and starts without a
// reachable backend — the wiring the entrypoint test exercises.
//
//nolint:nonamedreturns // named outputs label the (config path, data dir) pair
func writeTempConfig(t *testing.T, extra string) (configPath, dataDir string) {
	t.Helper()
	dataDir = t.TempDir()
	configPath = filepath.Join(t.TempDir(), "daemon.toml")
	body := fmt.Sprintf(`
[service]
default_data_dir = %q

[streaming]
earliest_ledger = "genesis"
captive_core_config = "/dev/null"

[logging]
level = "debug"
format = "text"
%s
`, dataDir, extra)
	require.NoError(t, os.WriteFile(configPath, []byte(body), 0o644))
	return configPath, dataDir
}

// fakeBoundaries returns a BuildBoundaries func that hands RunDaemon a set of
// faked external boundaries (a young-network tip ⇒ no backfill, a fake core
// stream that blocks until ctx cancel, a recording ServeReads). It also records
// the resolved config/paths the daemon passed the builder, so a test asserts the
// daemon threaded LoadConfig+ResolvePaths through correctly.
type capturedBuild struct {
	called   atomic.Int32
	gotCfg   Config
	gotPaths Paths
	served   atomic.Int32
	core     *fakeCore
}

func (c *capturedBuild) build(
	_ context.Context, cfg Config, paths Paths, _ *Catalog, _ *supportlog.Entry,
) (Boundaries, error) {
	c.called.Add(1)
	c.gotCfg = cfg
	c.gotPaths = paths
	return Boundaries{
		// A young-network tip (inside chunk 0) ⇒ backfill is a no-op, so the
		// daemon needs no real backend to reach serve+ingest.
		NetworkTip: &fakeTipBackend{tips: []uint32{chunk.FirstLedgerSeq + 10}},
		Core:       c.core,
		ServeReads: func(context.Context) error { c.served.Add(1); return nil },
	}, nil
}

// ---------------------------------------------------------------------------
// RunDaemonWith — the full entrypoint flow against faked boundaries.
// ---------------------------------------------------------------------------

// The happy path: load TOML → lock → open meta store → validateConfig (pins the
// genesis floor) → build boundaries → startStreaming → clean shutdown on ctx
// cancel. Asserts the daemon pinned the layout, served reads, started core at
// genesis, and threaded the resolved config/paths into the boundary builder.
func TestRunDaemon_LoadValidateWireStartCleanShutdown(t *testing.T) {
	configPath, dataDir := writeTempConfig(t, "")

	capture := &capturedBuild{core: &fakeCore{getter: &fakeLedgerGetter{frames: map[uint32][]byte{}, blockOnCtx: true}}}
	opts := DaemonOptions{BuildBoundaries: capture.build, Logger: silentLogger()}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- RunDaemonWith(ctx, configPath, opts) }()

	// Wait until reads are served (the daemon is parked on the blocking stream).
	require.Eventually(t, func() bool { return capture.served.Load() == 1 }, 3*time.Second, 5*time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err, "ctx cancel is a clean shutdown")
	case <-time.After(3 * time.Second):
		t.Fatal("RunDaemonWith did not return after ctx cancel")
	}

	assert.Equal(t, int32(1), capture.called.Load(), "boundary builder invoked once")
	assert.Equal(t, int32(1), capture.served.Load(), "reads served once")
	assert.Equal(t, int32(1), capture.core.openedCount.Load(), "captive core started once")
	assert.Equal(t, uint32(chunk.FirstLedgerSeq), capture.core.resumeSeen.Load(),
		"resume ledger is genesis on a fresh start")

	// The daemon threaded the loaded config + resolved paths into the builder.
	assert.Equal(t, dataDir, capture.gotCfg.Service.DefaultDataDir)
	assert.Equal(t, filepath.Join(dataDir, "hot"), capture.gotPaths.HotStorage)
	assert.Equal(t, filepath.Join(dataDir, "catalog", "rocksdb"), capture.gotPaths.Catalog)

	// validateConfig pinned the immutable layout (cpi + earliest) before start.
	store, err := openMetaAt(t, capture.gotPaths.Catalog)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	windows, err := NewWindows(testCPI)
	require.NoError(t, err)
	cat := NewCatalog(store, NewLayout(dataDir), windows)
	earliest, pinned, err := cat.EarliestLedger()
	require.NoError(t, err)
	require.True(t, pinned, "validateConfig must pin earliest_ledger before startStreaming")
	assert.Equal(t, uint32(chunk.FirstLedgerSeq), earliest)
	cpi, cpiPinned, err := cat.ChunksPerTxhashIndex()
	require.NoError(t, err)
	require.True(t, cpiPinned)
	assert.Equal(t, DefaultChunksPerTxhashIndex, cpi)
}

// Storage-path overrides must be HONORED by the data path, not just locked. The
// daemon resolves [catalog]/[immutable_storage.*]/[streaming.hot_storage]
// overrides into Paths, flocks them, and binds the Catalog via
// NewLayoutFromPaths(paths) — so the Layout the data path reads/writes must
// place every artifact and the hot DB under the OVERRIDE, never under DataDir.
// Before the fix the Layout derived all paths from DataDir alone: the lock and
// the data location diverged silently. This test pins both halves: (1) the
// bound Layout's paths all live under the overrides, and (2) actually opening a
// hot DB through the data path (openHotTierForChunk) lands the dir under the hot
// override with NOTHING under {DataDir}/hot.
func TestRunDaemon_StoragePathOverridesHonored(t *testing.T) {
	dataDir := t.TempDir()
	overrideRoot := t.TempDir() // a distinct mount, e.g. /mnt/nvme
	hotOverride := filepath.Join(overrideRoot, "hot")
	coldOverride := filepath.Join(overrideRoot, "cold")
	txhashIndexOverride := filepath.Join(overrideRoot, "txidx") // the one cold artifact with its own override
	catalogOverride := filepath.Join(overrideRoot, "meta")

	cfg := Config{
		Service: ServiceConfig{DefaultDataDir: dataDir},
		Catalog: CatalogConfig{Path: catalogOverride},
		ImmutableStorage: ImmutableStorageConfig{
			Path:            coldOverride,
			TxhashIndexPath: txhashIndexOverride,
		},
		Streaming: StreamingConfig{HotStorage: StoragePathConfig{Path: hotOverride}},
	}.WithDefaults()

	paths := cfg.ResolvePaths()
	layout := NewLayoutFromPaths(paths) // exactly the daemon's binding

	// (1) Every path the Layout composes lives under the override, NOT DataDir.
	const cid = chunk.ID(5350)
	assert.Equal(t, catalogOverride, layout.CatalogPath())
	assert.Equal(t, hotOverride, layout.HotRoot())
	assert.Equal(t, filepath.Join(hotOverride, cid.String()), layout.HotChunkPath(cid))
	ledgersRoot := filepath.Join(coldOverride, "ledgers") // ledgers is a fixed subdir of the cold root
	assert.Equal(t, filepath.Join(ledgersRoot, cid.BucketID(), cid.String()+".pack"),
		layout.LedgerPackPath(cid))
	assert.Equal(t, ledgersRoot, layout.LedgersRoot())
	// events and txhash-raw are fixed subdirs of the cold root; only the
	// txhash index honors its own override.
	eventsRoot := filepath.Join(coldOverride, "events")
	txhashRawRoot := filepath.Join(coldOverride, "txhash", "raw")
	assert.Equal(t, eventsRoot, layout.EventsRoot())
	assert.Equal(t, txhashRawRoot, layout.TxHashRawRoot())
	assert.Equal(t, filepath.Join(txhashRawRoot, cid.BucketID(), cid.String()+".bin"),
		layout.TxHashBinPath(cid))
	assert.Equal(t, txhashIndexOverride, layout.TxHashIndexRoot())
	for _, p := range layout.EventsPaths(cid) {
		assert.True(t, filepathHasPrefix(p, eventsRoot), "events path %q under cold override", p)
	}
	// Nothing resolves under {DataDir}/hot or {DataDir}/ledgers.
	assert.NotEqual(t, filepath.Join(dataDir, "hot", cid.String()), layout.HotChunkPath(cid))

	// (2) The data path actually creates the hot DB under the override. Bind a
	// real catalog on this Layout and open a hot tier through the same call the
	// ingestion loop uses.
	store, err := metastore.New(paths.Catalog, silentLogger())
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	windows, err := NewWindows(testCPI)
	require.NoError(t, err)
	cat := NewCatalog(store, layout, windows)

	db, err := openHotTierForChunk(cat, cid, silentLogger())
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// The hot DB dir exists under the override...
	hotDir := filepath.Join(hotOverride, cid.String())
	info, err := os.Stat(hotDir)
	require.NoError(t, err, "hot DB must be created under the hot_storage override")
	assert.True(t, info.IsDir())
	// ...and NOTHING was written under {DataDir}/hot (the old, buggy location).
	_, err = os.Stat(filepath.Join(dataDir, "hot"))
	assert.True(t, os.IsNotExist(err), "no hot data may land under DataDir when an override is set")
}

// filepathHasPrefix reports whether path lives under prefix (prefix is an
// ancestor dir of path). It compares cleaned components, not raw string
// prefixes, so /a/bc is not treated as under /a/b.
func filepathHasPrefix(path, prefix string) bool {
	rel, err := filepath.Rel(prefix, path)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// A second daemon on the same data dir fails fast on the storage-root flock — the
// single-process invariant the entrypoint must enforce before opening any store.
func TestRunDaemon_LockContentionFailsFast(t *testing.T) {
	configPath, dataDir := writeTempConfig(t, "")

	// Hold the hot-root lock as a "first daemon" for the test's duration.
	paths := Paths{HotStorage: filepath.Join(dataDir, "hot")}
	locks, err := LockRoots(paths.HotStorage)
	require.NoError(t, err)
	defer locks.Release()

	capture := &capturedBuild{core: &fakeCore{}}
	err = RunDaemonWith(context.Background(), configPath,
		DaemonOptions{BuildBoundaries: capture.build, Logger: silentLogger()})
	require.ErrorIs(t, err, ErrRootLocked)
	assert.Zero(t, capture.called.Load(), "boundary build never reached when a root is locked")
}

// A first start with a missing tip and a "now" floor is fatal at validateConfig:
// "now" cannot resolve without a reachable backend, and the daemon must surface
// it rather than start serving an empty history.
func TestRunDaemon_NowFloorRequiresTip(t *testing.T) {
	configPath, _ := writeTempConfigNow(t)

	capture := &capturedBuild{core: &fakeCore{}}
	// The builder returns an unreachable tip, so "now" cannot resolve.
	build := func(_ context.Context, cfg Config, paths Paths, c *Catalog, l *supportlog.Entry) (Boundaries, error) {
		b, _ := capture.build(context.Background(), cfg, paths, c, l) //nolint:contextcheck // fresh ctx is intentional (test)
		b.NetworkTip = &fakeTipBackend{err: errors.New("unreachable"), errFirst: 99}
		return b, nil
	}
	err := RunDaemonWith(context.Background(), configPath,
		DaemonOptions{BuildBoundaries: build, Logger: silentLogger(), RestartBackoff: time.Millisecond})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "now")
}

//nolint:nonamedreturns // named outputs label the (config path, data dir) pair
func writeTempConfigNow(t *testing.T) (configPath, dataDir string) {
	t.Helper()
	dataDir = t.TempDir()
	configPath = filepath.Join(t.TempDir(), "daemon.toml")
	body := fmt.Sprintf(`
[service]
default_data_dir = %q
[streaming]
earliest_ledger = "now"
captive_core_config = "/dev/null"
`, dataDir)
	require.NoError(t, os.WriteFile(configPath, []byte(body), 0o644))
	return configPath, dataDir
}

// A boundary-build failure surfaces (the daemon cannot start without its
// external boundaries) and never reaches startStreaming.
func TestRunDaemon_BuildBoundariesError(t *testing.T) {
	configPath, _ := writeTempConfig(t, "")
	wantErr := errors.New("captive core binary missing")
	build := func(context.Context, Config, Paths, *Catalog, *supportlog.Entry) (Boundaries, error) {
		return Boundaries{}, wantErr
	}
	err := RunDaemonWith(context.Background(), configPath,
		DaemonOptions{BuildBoundaries: build, Logger: silentLogger()})
	require.ErrorIs(t, err, wantErr)
}

// A missing default_data_dir is rejected before any store opens.
func TestRunDaemon_RequiresDataDir(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "daemon.toml")
	require.NoError(t, os.WriteFile(configPath, []byte(`
[streaming]
earliest_ledger = "genesis"
captive_core_config = "/dev/null"
`), 0o644))
	err := RunDaemonWith(context.Background(), configPath, DaemonOptions{Logger: silentLogger()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "default_data_dir")
}

// A nonexistent config path errors at load.
func TestRunDaemon_MissingConfigFile(t *testing.T) {
	err := RunDaemonWith(context.Background(), "/no/such/config.toml", DaemonOptions{Logger: silentLogger()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read config")
}

// ---------------------------------------------------------------------------
// superviseStreaming — the top-level restart loop.
// ---------------------------------------------------------------------------

// A restartable error retries on a backoff, then a clean ctx cancel during the
// backoff returns nil (no restart after a shutdown request).
func TestSuperviseStreaming_RetriesThenCleanShutdown(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)

	var attempts atomic.Int32
	core := &fakeCore{openErr: errors.New("transient core open failure")}
	tip := &fakeTipBackend{tips: []uint32{chunk.FirstLedgerSeq + 10}} // young: no backfill
	start := startTestConfig(t, cat, tip, core, nil)
	// Count startStreaming attempts by observing core opens (one per attempt past
	// backfill); openErr makes each attempt a restartable failure.
	start.ServeReads = func(context.Context) error { return nil }

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- superviseStreaming(ctx, start, silentLogger(), 5*time.Millisecond) }()

	// Let a few restarts happen, then cancel.
	require.Eventually(t, func() bool {
		attempts.Store(core.openedCount.Load())
		return attempts.Load() >= 2
	}, 3*time.Second, 5*time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err, "ctx cancel during backoff returns nil")
	case <-time.After(3 * time.Second):
		t.Fatal("superviseStreaming did not return after cancel")
	}
	assert.GreaterOrEqual(t, core.openedCount.Load(), int32(2), "restarted on the transient failure")
}

// The fatal sentinels are surfaced UP, not retried (a fresh start cannot heal
// them).
func TestSuperviseStreaming_FatalSentinelSurfaces(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	// Unreachable tip + no local progress ⇒ ErrFirstStartNoTip, a fatal that must
	// surface rather than spin.
	tip := &fakeTipBackend{err: errors.New("unreachable"), errFirst: 99}
	start := startTestConfig(t, cat, tip, &fakeCore{}, nil)

	err := superviseStreaming(context.Background(), start, silentLogger(), time.Hour)
	require.ErrorIs(t, err, ErrFirstStartNoTip, "fatal sentinel surfaces immediately, no retry")
}

// ---------------------------------------------------------------------------
// backendTip — the production tip/coverage adapter over a LedgerBackend.
// ---------------------------------------------------------------------------

// fakeLedgerBackend is a minimal ledgerbackend.LedgerBackend whose latest ledger
// is programmable; only GetLatestLedgerSequence is exercised by backendTip.
type fakeLedgerBackend struct {
	latest atomic.Uint32
	err    error
}

func (b *fakeLedgerBackend) GetLatestLedgerSequence(context.Context) (uint32, error) {
	if b.err != nil {
		return 0, b.err
	}
	return b.latest.Load(), nil
}

func (b *fakeLedgerBackend) GetLedger(context.Context, uint32) (xdr.LedgerCloseMeta, error) {
	return xdr.LedgerCloseMeta{}, errors.New("not implemented")
}
func (b *fakeLedgerBackend) PrepareRange(context.Context, ledgerbackend.Range) error { return nil }
func (b *fakeLedgerBackend) IsPrepared(context.Context, ledgerbackend.Range) (bool, error) {
	return true, nil
}
func (b *fakeLedgerBackend) Close() error { return nil }

func TestBackendTip_NetworkTip(t *testing.T) {
	be := &fakeLedgerBackend{}
	be.latest.Store(123_456)
	adapter := newBackendTip(be, time.Millisecond, time.Second)
	tip, err := adapter.NetworkTip(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint32(123_456), tip)
}

func TestBackendTip_WaitForCoverageReady(t *testing.T) {
	be := &fakeLedgerBackend{}
	be.latest.Store(500)
	adapter := newBackendTip(be, time.Millisecond, time.Second)
	require.NoError(t, adapter.WaitForCoverage(context.Background(), 400), "tip already covers target")
}

func TestBackendTip_WaitForCoverageAdvances(t *testing.T) {
	be := &fakeLedgerBackend{}
	be.latest.Store(100)
	adapter := newBackendTip(be, time.Millisecond, 2*time.Second)
	// Advance the tip past the target after a few polls.
	go func() {
		time.Sleep(20 * time.Millisecond)
		be.latest.Store(1000)
	}()
	require.NoError(t, adapter.WaitForCoverage(context.Background(), 900))
}

func TestBackendTip_WaitForCoverageTimeout(t *testing.T) {
	be := &fakeLedgerBackend{}
	be.latest.Store(10) // never reaches the target
	adapter := newBackendTip(be, time.Millisecond, 20*time.Millisecond)
	err := adapter.WaitForCoverage(context.Background(), 1_000_000)
	require.ErrorIs(t, err, ErrBackendCoverageTimeout)
}

func TestBackendTip_WaitForCoverageCtxCancel(t *testing.T) {
	be := &fakeLedgerBackend{}
	be.latest.Store(10)
	adapter := newBackendTip(be, 10*time.Millisecond, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := adapter.WaitForCoverage(ctx, 1_000_000)
	require.ErrorIs(t, err, context.Canceled)
}

// ---------------------------------------------------------------------------
// notConfiguredTip — frontfill-only deployment behavior.
// ---------------------------------------------------------------------------

func TestNotConfiguredTip_ErrorsClearly(t *testing.T) {
	_, err := notConfiguredTip{}.NetworkTip(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no bulk backend configured")
}

// ---------------------------------------------------------------------------
// buildProductionBoundaries — captive-core wiring is deferred to #772.
// ---------------------------------------------------------------------------

func TestBuildProductionBoundaries_CaptiveCoreDeferred(t *testing.T) {
	cfg := Config{}.WithDefaults()
	cfg.Streaming.CaptiveCoreConfig = "/some/core.toml"
	_, err := buildProductionBoundaries(context.Background(), cfg, Paths{}, nil, silentLogger())
	require.Error(t, err, "captive-core production wiring is deferred to #772")
	assert.Contains(t, err.Error(), "#772")
}

func TestBuildProductionBoundaries_RequiresCaptiveCoreConfig(t *testing.T) {
	cfg := Config{}.WithDefaults() // no captive_core_config
	_, err := buildProductionBoundaries(context.Background(), cfg, Paths{}, nil, silentLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "captive_core_config")
}

func TestNewLogger(t *testing.T) {
	l, err := newLogger(LoggingConfig{Level: "warn", Format: "json"})
	require.NoError(t, err)
	require.NotNil(t, l)

	_, err = newLogger(LoggingConfig{Level: "bogus", Format: "text"})
	require.Error(t, err)
}
