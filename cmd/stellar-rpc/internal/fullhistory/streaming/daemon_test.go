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
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
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

// capturedBuild returns a BuildBoundaries func that hands RunDaemon a set of
// faked external boundaries (a young-network tip ⇒ no backfill, a recording
// ServeReads). It also records the resolved config/paths the daemon passed the
// builder, so a test asserts the daemon threaded LoadConfig+ResolvePaths through
// correctly.
type capturedBuild struct {
	called   atomic.Int32
	gotCfg   Config
	gotPaths Paths
	served   atomic.Int32
}

func (c *capturedBuild) build(
	_ context.Context, cfg Config, paths Paths, _ *Catalog, _ *supportlog.Entry,
) (Boundaries, error) {
	c.called.Add(1)
	c.gotCfg = cfg
	c.gotPaths = paths
	return Boundaries{
		// A young-network tip (inside chunk 0) ⇒ backfill is a no-op, so the
		// daemon needs no real backend to reach the serve step.
		NetworkTip: &fakeTipBackend{tips: []uint32{chunk.FirstLedgerSeq + 10}},
		ServeReads: func(context.Context) error { c.served.Add(1); return nil },
	}, nil
}

// ---------------------------------------------------------------------------
// RunDaemonWith — the full entrypoint flow against faked boundaries.
// ---------------------------------------------------------------------------

// The happy path: load TOML → lock → open meta store → validateConfig (pins the
// genesis floor) → build boundaries → startStreaming (catch-up no-op + serve) →
// clean return. Asserts the daemon pinned the layout, served reads, and threaded
// the resolved config/paths into the boundary builder. The cold-only daemon has
// no live ingestion loop, so startStreaming returns once ServeReads returns.
func TestRunDaemon_LoadValidateWireStartCleanShutdown(t *testing.T) {
	configPath, dataDir := writeTempConfig(t, "")

	capture := &capturedBuild{}
	opts := DaemonOptions{BuildBoundaries: capture.build, Logger: silentLogger()}

	errCh := make(chan error, 1)
	go func() { errCh <- RunDaemonWith(context.Background(), configPath, opts) }()

	select {
	case err := <-errCh:
		require.NoError(t, err, "cold catch-up + serve returns cleanly")
	case <-time.After(3 * time.Second):
		t.Fatal("RunDaemonWith did not return")
	}

	assert.Equal(t, int32(1), capture.called.Load(), "boundary builder invoked once")
	assert.Equal(t, int32(1), capture.served.Load(), "reads served once")

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
	assert.Equal(t, uint32(DefaultChunksPerTxhashIndex), cpi)
}

// someTxBackend serves a chunk whose ledgers are zero-tx EXCEPT a sparse few that
// carry one transaction each. A wholly zero-tx chunk cannot build a txhash index
// ("zero keys"), so the index path needs at least some keys; sparseness keeps the
// 10k-ledger pass nearly as cheap as the all-zero-tx fixture.
func someTxBackend(t *testing.T) *countingChunkSource {
	t.Helper()
	src := xdr.MustMuxedAddress(keypair.MustRandom().Address())
	gen := func(t *testing.T, seq uint32) []byte {
		if seq%2500 != 0 { // the vast majority: cheap zero-tx ledgers
			return zeroTxLCMBytes(t, seq)
		}
		return oneTxLCMBytes(t, seq, src) // a handful carry one unique tx
	}
	return &countingChunkSource{
		make: func(chunk.ID) (ledgerbackend.LedgerStream, error) {
			return &fullChunkStream{t: t, gen: gen}, nil
		},
	}
}

// oneTxLCMBytes is zeroTxLCMBytes plus a single transaction (a fixed source
// account, a per-seq sequence number for a unique hash) so ExtractTxHashes yields
// exactly one txhash key for seq.
func oneTxLCMBytes(t *testing.T, seq uint32, src xdr.MuxedAccount) []byte {
	t.Helper()
	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				SourceAccount: src,
				SeqNum:        xdr.SequenceNumber(seq), // unique per ledger ⇒ unique hash
				Ext:           xdr.TransactionExt{V: 1, SorobanData: &xdr.SorobanTransactionData{}},
			},
		},
	}
	hash, err := network.HashTransactionInEnvelope(envelope, network.PublicNetworkPassphrase)
	require.NoError(t, err)
	comp := []xdr.TxSetComponent{{
		Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
		TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
			Txs: []xdr.TransactionEnvelope{envelope},
		},
	}}
	lcm := xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(0)},
					LedgerSeq: xdr.Uint32(seq),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: []xdr.TransactionPhase{{V: 0, V0Components: &comp}}},
			},
			TxProcessing: []xdr.TransactionResultMetaV1{{
				// A non-nil versioned meta: a zero-value TransactionMeta (V=0) nil-derefs
				// in EncodeTo. Empty V4 ⇒ no events, which is fine for this fixture.
				TxApplyProcessing: xdr.TransactionMeta{V: 4, V4: &xdr.TransactionMetaV4{}},
				Result: xdr.TransactionResultPair{
					TransactionHash: hash,
					Result: xdr.TransactionResult{
						FeeCharged: 100,
						Result: xdr.TransactionResultResult{
							Code:    xdr.TransactionResultCodeTxSuccess,
							Results: &[]xdr.OperationResult{},
						},
					},
				},
			}},
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}

// The #815 end-to-end acceptance: one TOML boots the daemon and it catches up to
// the tip for ALL THREE cold data types + the window index — proven THROUGH the
// real entrypoint (LoadConfig → validateConfig → catchUp → executePlan →
// processChunk → buildTxhashIndex → buildThenSweep), not by calling the
// primitives in isolation. The happy-path test above sits the tip inside chunk 0
// so its catch-up is a deliberate no-op; here a tip at chunk 0's last ledger
// backfills the COMPLETE chunk 0, and chunks_per_txhash_index=1 makes window 0 a
// single-chunk window so its index build is terminal — letting us assert the
// whole txhash lifecycle (.bin → merged .idx → .bin swept). workers=1 also drives
// the index-waits-on-its-chunk path (the deadlock-prone case) through the daemon.
//
// A frozen chunk is a full LedgersPerChunk (10k) pass, but the ledgers are cheap
// (zero-tx bodies + a sparse few one-tx ones), so the whole catch-up runs in well
// under a second — fast enough for -short. The merge DEPTH (multi-.bin windows,
// rolling/terminal demotion) is unit-tested in txindex_test; this test's job is
// the daemon-level COMPOSITION.
func TestRunDaemon_CatchUpMaterializesAllColdTypesAndIndex(t *testing.T) {
	configPath, dataDir := writeTempConfig(t, "[backfill]\nchunks_per_txhash_index = 1\nworkers = 1\n")

	build := func(_ context.Context, _ Config, _ Paths, _ *Catalog, _ *supportlog.Entry) (Boundaries, error) {
		return Boundaries{
			// Tip at chunk 0's last ledger ⇒ chunk 0 is complete, so the catch-up
			// freezes it and (cpi=1) its single-chunk window index is terminal.
			NetworkTip:    &fakeTipBackend{tips: []uint32{chunk.ID(0).LastLedger()}},
			Backend:       someTxBackend(t), // mostly zero-tx, a sparse few carry a tx
			BackendWaiter: &fakeWaiter{},    // coverage is always satisfied
			ServeReads:    func(context.Context) error { return nil },
		}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunDaemonWith(ctx, configPath,
			DaemonOptions{BuildBoundaries: build, Logger: silentLogger()})
	}()
	select {
	case err := <-errCh:
		require.NoError(t, err, "daemon catches up to tip then exits cleanly (no-op ServeReads)")
	case <-time.After(60 * time.Second):
		cancel()
		t.Fatal("RunDaemonWith did not finish catch-up within 60s (regressed into a hang/restart loop?)")
	}

	// Read the catalog back after the daemon released its locks + closed its store.
	store, err := openMetaAt(t, filepath.Join(dataDir, "catalog", "rocksdb"))
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	windows, err := NewWindows(1)
	require.NoError(t, err)
	layout := NewLayout(dataDir)
	cat := NewCatalog(store, layout, windows)

	// (1) Chunk 0's ledger + events artifacts are frozen, with files on disk.
	ls, err := cat.State(0, KindLedgers)
	require.NoError(t, err)
	assert.Equal(t, StateFrozen, ls, "chunk 0 ledgers frozen")
	es, err := cat.State(0, KindEvents)
	require.NoError(t, err)
	assert.Equal(t, StateFrozen, es, "chunk 0 events frozen")
	assert.FileExists(t, layout.LedgerPackPath(0))
	for _, p := range layout.EventsPaths(0) {
		assert.FileExists(t, p)
	}

	// (2) The window's txhash index built terminally: one frozen coverage [0,0]
	// with its .idx on disk.
	cov, ok, err := cat.FrozenCoverage(0)
	require.NoError(t, err)
	require.True(t, ok, "window 0 has a frozen txhash index coverage")
	assert.Equal(t, chunk.ID(0), cov.Lo)
	assert.Equal(t, chunk.ID(0), cov.Hi)
	assert.FileExists(t, layout.IndexFilePath(cov))

	// (3) The terminal build demoted + swept the per-chunk .bin run: the txhash
	// key is gone and the raw file unlinked (the index is now the durable form).
	ts, err := cat.State(0, KindTxHash)
	require.NoError(t, err)
	assert.Equal(t, State(""), ts, "chunk 0 txhash key demoted by the terminal index build")
	assert.NoFileExists(t, layout.TxHashBinPath(0))
}

// Storage-path overrides must be HONORED by the data path, not just locked. The
// daemon resolves [catalog]/[immutable_storage.*]/[streaming.hot_storage]
// overrides into Paths, flocks them, and binds the Catalog via
// NewLayoutFromPaths(paths) — so the Layout the data path reads/writes must
// place every artifact under the OVERRIDE, never under DataDir. Before the fix
// the Layout derived all paths from DataDir alone: the lock and the data
// location diverged silently. This test pins that the bound Layout's paths all
// live under the overrides.
func TestRunDaemon_StoragePathOverridesHonored(t *testing.T) {
	dataDir := t.TempDir()
	overrideRoot := t.TempDir() // a distinct mount, e.g. /mnt/nvme
	hotOverride := filepath.Join(overrideRoot, "hot")
	ledgersOverride := filepath.Join(overrideRoot, "ledgers")
	eventsOverride := filepath.Join(overrideRoot, "events")
	txhashRawOverride := filepath.Join(overrideRoot, "txraw")
	txhashIndexOverride := filepath.Join(overrideRoot, "txidx")
	catalogOverride := filepath.Join(overrideRoot, "meta")

	cfg := Config{
		Service: ServiceConfig{DefaultDataDir: dataDir},
		Catalog: CatalogConfig{Path: catalogOverride},
		ImmutableStorage: ImmutableStorageConfig{
			Ledgers:     StoragePathConfig{Path: ledgersOverride},
			Events:      StoragePathConfig{Path: eventsOverride},
			TxhashRaw:   StoragePathConfig{Path: txhashRawOverride},
			TxhashIndex: StoragePathConfig{Path: txhashIndexOverride},
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
	assert.Equal(t, filepath.Join(ledgersOverride, cid.BucketID(), cid.String()+".pack"),
		layout.LedgerPackPath(cid))
	assert.Equal(t, ledgersOverride, layout.LedgersRoot())
	assert.Equal(t, eventsOverride, layout.EventsRoot())
	assert.Equal(t, txhashRawOverride, layout.TxHashRawRoot())
	assert.Equal(t, filepath.Join(txhashRawOverride, cid.BucketID(), cid.String()+".bin"),
		layout.TxHashBinPath(cid))
	assert.Equal(t, txhashIndexOverride, layout.TxHashIndexRoot())
	for _, p := range layout.EventsPaths(cid) {
		assert.True(t, filepathHasPrefix(p, eventsOverride), "events path %q under override", p)
	}
	// Nothing resolves under {DataDir}/hot or {DataDir}/ledgers.
	assert.NotEqual(t, filepath.Join(dataDir, "hot", cid.String()), layout.HotChunkPath(cid))
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

	capture := &capturedBuild{}
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

	capture := &capturedBuild{}
	// The builder returns an unreachable tip, so "now" cannot resolve.
	build := func(_ context.Context, cfg Config, paths Paths, c *Catalog, l *supportlog.Entry) (Boundaries, error) {
		b, _ := capture.build(context.Background(), cfg, paths, c, l)
		b.NetworkTip = &fakeTipBackend{err: errors.New("unreachable"), errFirst: 99}
		return b, nil
	}
	err := RunDaemonWith(context.Background(), configPath,
		DaemonOptions{BuildBoundaries: build, Logger: silentLogger(), RestartBackoff: time.Millisecond})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "now")
}

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
	tip := &fakeTipBackend{tips: []uint32{chunk.FirstLedgerSeq + 10}} // young: no backfill
	start := startTestConfig(t, cat, tip, nil)
	// A ServeReads that always errors makes each startStreaming attempt a
	// restartable failure; counting its calls counts the restarts.
	start.ServeReads = func(context.Context) error {
		attempts.Add(1)
		return errors.New("transient serve failure")
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- superviseStreaming(ctx, start, silentLogger(), 5*time.Millisecond) }()

	// Let a few restarts happen, then cancel.
	require.Eventually(t, func() bool {
		return attempts.Load() >= 2
	}, 3*time.Second, 5*time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err, "ctx cancel during backoff returns nil")
	case <-time.After(3 * time.Second):
		t.Fatal("superviseStreaming did not return after cancel")
	}
	assert.GreaterOrEqual(t, attempts.Load(), int32(2), "restarted on the transient failure")
}

// The fatal sentinels are surfaced UP, not retried (a fresh start cannot heal
// them).
func TestSuperviseStreaming_FatalSentinelSurfaces(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	// Unreachable tip + no local progress ⇒ ErrFirstStartNoTip, a fatal that must
	// surface rather than spin.
	tip := &fakeTipBackend{err: errors.New("unreachable"), errFirst: 99}
	start := startTestConfig(t, cat, tip, nil)

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
// buildProductionBoundaries — the cold-only frontfill boundaries.
// ---------------------------------------------------------------------------

// The cold-only daemon has no captive core: buildProductionBoundaries returns a
// frontfill Boundaries (a no-op ServeReads and a not-configured tip) with no
// error, and that set validates.
func TestBuildProductionBoundaries_FrontfillNoBackend(t *testing.T) {
	cfg := Config{}.WithDefaults()
	b, err := buildProductionBoundaries(context.Background(), cfg, Paths{}, nil, silentLogger())
	require.NoError(t, err)
	require.NotNil(t, b.ServeReads, "a no-op ServeReads is wired")
	require.NoError(t, b.validate(), "the frontfill boundaries validate")

	// The tip is the not-configured placeholder: sampling it errors clearly.
	_, tipErr := b.NetworkTip.NetworkTip(context.Background())
	require.Error(t, tipErr)
	assert.Contains(t, tipErr.Error(), "no bulk backend configured")
}

func TestNewLogger(t *testing.T) {
	l, err := newLogger(LoggingConfig{Level: "warn", Format: "json"})
	require.NoError(t, err)
	require.NotNil(t, l)

	_, err = newLogger(LoggingConfig{Level: "bogus", Format: "text"})
	require.Error(t, err)
}
