package rpcv2

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
)

// openCatalogAt opens a catalog over the daemon's on-disk KV under dataDir for
// read-back assertions (closed via t.Cleanup).
func openCatalogAt(t *testing.T, dataDir string) *catalog.Catalog {
	t.Helper()
	txLayout, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	require.NoError(t, err)
	cat, err := catalog.Open(
		filepath.Join(dataDir, "catalog", "rocksdb"), geometry.NewLayout(dataDir), txLayout, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })
	return cat
}

// writeTempConfig writes a minimal-valid daemon TOML (genesis floor ⇒ no tip
// needed) and returns the config path and data dir.
func writeTempConfig(t *testing.T, extra string) (string, string) {
	t.Helper()
	dataDir := t.TempDir()
	configPath := filepath.Join(t.TempDir(), "daemon.toml")
	body := fmt.Sprintf(`
[storage]
default_data_dir = %q

[retention]
earliest_ledger = "genesis"

[ingestion]
captive_core_config = "/dev/null"

[logging]
level = "debug"
format = "text"
%s
`, dataDir, extra)
	require.NoError(t, os.WriteFile(configPath, []byte(body), 0o644))
	return configPath, dataDir
}

// ---------------------------------------------------------------------------
// runDaemonWith — the full entrypoint flow against an injected backend.
// ---------------------------------------------------------------------------

// Happy path pins earliest_ledger, serves reads once, then ingests. The injected
// backend's young-network tip (inside chunk 0) ⇒ no-op backfill; the injected core
// blocks until ctx cancel (the daemon's steady state), and a ctx cancel is a clean
// shutdown. No LedgerStream needed.
func TestRunDaemon_LoadValidateWireStartCleanShutdown(t *testing.T) {
	configPath, dataDir := writeTempConfig(t, "")

	var served atomic.Int32
	opts := daemonOptions{
		Backend:    &fakeBackend{tip: chunk.FirstLedgerSeq + 10},
		Core:       &fakeCore{}, // default getter blocks until ctx cancel
		ServeReads: countingServeReads(&served),
		Logger:     silentLogger(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- runDaemonWith(ctx, configPath, opts) }()

	// ServeReads is called after backfill, just before the (blocking) ingestion loop.
	require.Eventually(t, func() bool { return served.Load() == 1 }, 3*time.Second, 5*time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err, "a ctx-canceled ingestion loop is a clean shutdown")
	case <-time.After(3 * time.Second):
		t.Fatal("runDaemonWith did not return after ctx cancel")
	}

	assert.Equal(t, int32(1), served.Load(), "reads served once")

	// validateConfig pinned earliest_ledger before start (cpi is a constant now,
	// not a pinned value).
	cat := openCatalogAt(t, dataDir)
	earliest, pinned, err := cat.EarliestLedger()
	require.NoError(t, err)
	require.True(t, pinned, "validateConfig must pin earliest_ledger before run")
	assert.Equal(t, uint32(chunk.FirstLedgerSeq), earliest)
}

// someTxGen generates mostly zero-tx ledgers with a sparse few carrying one tx:
// an all-zero-tx chunk can't build a txhash index ("zero keys"), so it needs some.
func someTxGen(t *testing.T) func(*testing.T, uint32) []byte {
	t.Helper()
	src := xdr.MustMuxedAddress(keypair.MustRandom().Address())
	return func(t *testing.T, seq uint32) []byte {
		if seq%2500 != 0 {
			return rpcv2test.ZeroTxLCMBytes(t, seq)
		}
		raw, _ := oneTxLCMBytes(t, seq, src)
		return raw
	}
}

// someTxBackend is a fake bulk backend over someTxGen's ledgers.
func someTxBackend(t *testing.T) *fakeBackend {
	t.Helper()
	return &fakeBackend{
		LedgerStream: &fullChunkStream{t: t, gen: someTxGen(t)},
		// Tip covers chunk 0 ⇒ the freeze's coverage wait passes at once.
		tip: chunk.ID(0).LastLedger(),
	}
}

// oneTxLCMBytes is rpcv2test.ZeroTxLCMBytes plus one tx (per-seq SeqNum ⇒ unique hash) so
// ExtractLedgerTxParts yields exactly one hash for seq. Returns the wire bytes and the
// real, network-hashed transaction hash (the hash the daemon commits for seq), so
// callers can assert a getTransaction-style hash→seq lookup.
func oneTxLCMBytes(t *testing.T, seq uint32, src xdr.MuxedAccount) ([]byte, [32]byte) {
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
				// Non-nil versioned meta (zero-value V=0 nil-derefs in EncodeTo);
				// empty V4 ⇒ no events.
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
	return raw, hash
}

// #815 acceptance: one TOML boots the daemon and it backfills the complete chunk
// 0 for all three cold types + the tx-hash index THROUGH the real entrypoint (not
// primitives in isolation). workers=1 also drives the deadlock-prone
// index-waits-on-its-chunk path. cpi=1000 ⇒ window 0 spans [0,999], so one complete
// chunk yields a non-terminal rolling coverage [0,0] that keeps its .bin inputs.
func TestRunDaemon_BackfillMaterializesAllColdTypesAndIndex(t *testing.T) {
	configPath, dataDir := writeTempConfig(t, "[backfill]\nworkers = 1\n")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// ServeReads runs after backfill completes, just before the blocking ingestion
	// loop — so it is the "backfill done" signal. The injected core then blocks until
	// the ctx cancel below, and a ctx-canceled ingestion loop is a clean shutdown.
	servedCh := make(chan struct{}, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- runDaemonWith(ctx, configPath, daemonOptions{
			// Backend's tip is chunk 0's last ledger ⇒ chunk 0 complete, backfill freezes it.
			// The network tip is derived from this same backend's Tip.
			Backend:    someTxBackend(t),
			Core:       &fakeCore{}, // default getter blocks until ctx cancel
			ServeReads: signalingServeReads(servedCh),
			Logger:     silentLogger(),
		})
	}()
	select {
	case <-servedCh: // backfill complete; the daemon is now parked in ingestion
	case err := <-errCh:
		t.Fatalf("daemon returned before backfill completed: %v", err)
	case <-time.After(60 * time.Second):
		cancel()
		t.Fatal("runDaemonWith did not finish backfill within 60s (regressed into a hang/restart loop?)")
	}
	cancel() // request a clean shutdown of the parked ingestion loop
	select {
	case err := <-errCh:
		require.NoError(t, err, "a ctx-canceled ingestion loop is a clean shutdown")
	case <-time.After(10 * time.Second):
		t.Fatal("runDaemonWith did not return after ctx cancel")
	}

	// Read the catalog back after the daemon released locks + closed its store.
	cat := openCatalogAt(t, dataDir)
	layout := cat.Layout()

	// (1) Chunk 0's ledger + events artifacts are frozen, with files on disk.
	ls, err := cat.State(0, geometry.KindLedgers)
	require.NoError(t, err)
	assert.Equal(t, geometry.StateFrozen, ls, "chunk 0 ledgers frozen")
	es, err := cat.State(0, geometry.KindEvents)
	require.NoError(t, err)
	assert.Equal(t, geometry.StateFrozen, es, "chunk 0 events frozen")
	assert.FileExists(t, layout.LedgerPackPath(0))
	for _, p := range layout.EventsPaths(0) {
		assert.FileExists(t, p)
	}

	// (2) One frozen rolling coverage [0,0] with its .idx on disk.
	cov, ok, err := cat.FrozenTxHashIndex(0)
	require.NoError(t, err)
	require.True(t, ok, "window 0 has a frozen txhash index coverage")
	assert.Equal(t, chunk.ID(0), cov.Lo)
	assert.Equal(t, chunk.ID(0), cov.Hi)
	assert.FileExists(t, layout.TxHashIndexFilePath(cov))

	// (3) Non-terminal build keeps its inputs: chunk 0's txhash key stays frozen,
	// .bin retained for a future merge (terminal demote+sweep is in txindex_test).
	ts, err := cat.State(0, geometry.KindTxHash)
	require.NoError(t, err)
	assert.Equal(t, geometry.StateFrozen, ts, "chunk 0 txhash key retained (non-terminal rolling index)")
	assert.FileExists(t, layout.TxHashBinPath(0))
}

// Storage-path overrides are honored by the data path: every artifact the bound
// Layout (NewLayoutFromPaths) composes lives under the override, never DataDir.
func TestRunDaemon_StoragePathOverridesHonored(t *testing.T) {
	dataDir := t.TempDir()
	overrideRoot := t.TempDir() // a distinct mount, e.g. /mnt/nvme
	hotOverride := filepath.Join(overrideRoot, "hot")
	ledgersOverride := filepath.Join(overrideRoot, "ledgers")
	eventsOverride := filepath.Join(overrideRoot, "events")
	txhashRawOverride := filepath.Join(overrideRoot, "txraw")
	txhashIndexOverride := filepath.Join(overrideRoot, "txidx")
	catalogOverride := filepath.Join(overrideRoot, "meta")

	cfg := config.Config{
		Storage: config.StorageConfig{
			DefaultDataDir: dataDir,
			Catalog:        catalogOverride,
			Ledgers:        ledgersOverride,
			Events:         eventsOverride,
			TxhashRaw:      txhashRawOverride,
			TxhashIndex:    txhashIndexOverride,
			Hot:            hotOverride,
		},
	}.WithDefaults()

	paths := cfg.ResolvePaths()
	layout := config.NewLayoutFromPaths(paths) // exactly the daemon's binding

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

// filepathHasPrefix reports whether path lives under prefix, comparing cleaned
// components (so /a/bc is not under /a/b).
func filepathHasPrefix(path, prefix string) bool {
	rel, err := filepath.Rel(prefix, path)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// A second daemon on the same data dir fails fast at catalog.Open on RocksDB's
// own exclusive LOCK — the single-process invariant, enforced before ingestion
// or serving starts.
func TestRunDaemon_LockContentionFailsFast(t *testing.T) {
	configPath, dataDir := writeTempConfig(t, "")

	// Hold the catalog open as a "first daemon" for the test's duration.
	_ = openCatalogAt(t, dataDir)

	var served atomic.Int32
	err := runDaemonWith(context.Background(), configPath, daemonOptions{
		Backend:    &fakeBackend{tip: chunk.FirstLedgerSeq + 10},
		ServeReads: countingServeReads(&served),
		Logger:     silentLogger(),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LOCK", "the failure is RocksDB's catalog lock")
	assert.Zero(t, served.Load(), "run never reached when the catalog is held")
}

// First start with a "now" floor and no usable tip is fatal at validateConfig:
// "now" cannot resolve, so the daemon surfaces it.
func TestRunDaemon_NowFloorRequiresTip(t *testing.T) {
	configPath, _ := writeTempConfigNow(t)

	err := runDaemonWith(context.Background(), configPath, daemonOptions{
		// A never-ready bulk source (sub-genesis tip ⇒ fast permanent failure) means
		// the network tip can't resolve "now". Core injected: the opener now resolves
		// up front and the stub captive_core_config would otherwise error first.
		Backend: &fakeBackend{tip: 0},
		Core:    &fakeCore{},
		Logger:  silentLogger(),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "now")
}

func writeTempConfigNow(t *testing.T) (string, string) {
	t.Helper()
	dataDir := t.TempDir()
	configPath := filepath.Join(t.TempDir(), "daemon.toml")
	body := fmt.Sprintf(`
[storage]
default_data_dir = %q
[retention]
earliest_ledger = "now"
[ingestion]
captive_core_config = "/dev/null"
`, dataDir)
	require.NoError(t, os.WriteFile(configPath, []byte(body), 0o644))
	return configPath, dataDir
}

// A missing default_data_dir is rejected before any store opens.
func TestRunDaemon_RequiresDataDir(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "daemon.toml")
	require.NoError(t, os.WriteFile(configPath, []byte(`
[retention]
earliest_ledger = "genesis"
[ingestion]
captive_core_config = "/dev/null"
`), 0o644))
	err := runDaemonWith(context.Background(), configPath, daemonOptions{Logger: silentLogger()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "default_data_dir")
}

// A nonexistent config path errors at load.
func TestRunDaemon_MissingConfigFile(t *testing.T) {
	err := runDaemonWith(context.Background(), "/no/such/config.toml", daemonOptions{Logger: silentLogger()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read config")
}

// ---------------------------------------------------------------------------
// runBody — the one-shot daemon body and its clean-vs-crash classification.
// ---------------------------------------------------------------------------

// A ctx cancel during the run classifies as a clean shutdown (nil), not an
// error — main exits 0 on SIGTERM.
func TestRunBody_CleanShutdownOnCancel(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	tip := &fakeTipBackend{tips: []uint32{chunk.FirstLedgerSeq + 10}} // young: no backfill
	start := startTestConfig(t, cat, tip, &fakeCore{}, nil)
	served := make(chan struct{}, 1)
	start.ServeReads = signalingServeReads(served)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- runBody(ctx, start, silentLogger()) }()
	<-served
	waitClean(t, cancel, errCh)
}

// Crash-only: a failing run with a live ctx surfaces its error to the caller —
// the process exits and the orchestrator owns the restart; there is no
// in-process retry.
func TestRunBody_FailureSurfacesOnce(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	var binds atomic.Int32
	tip := &fakeTipBackend{tips: []uint32{chunk.FirstLedgerSeq + 10}}
	start := startTestConfig(t, cat, tip, &fakeCore{}, nil)
	start.ServeReads = func(context.Context, *query.Registry, net.Listener) error {
		binds.Add(1)
		return errors.New("serve failure")
	}

	err := runBody(context.Background(), start, silentLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "serve failure")
	assert.Equal(t, int32(1), binds.Load(), "one attempt, no in-process retry")
}

// ---------------------------------------------------------------------------
// Captive-core resolution: HTTP ports on the live core only.
// ---------------------------------------------------------------------------

// writeCaptiveCoreFile writes the minimum captive-core config the SDK's toml
// builder accepts: a network passphrase and a quorum set. extra is appended as
// top-level keys.
func writeCaptiveCoreFile(t *testing.T, extra string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "captive-core.toml")
	body := fmt.Sprintf(`
NETWORK_PASSPHRASE = %q
%s

[[HOME_DOMAINS]]
HOME_DOMAIN = "testnet.stellar.org"
QUALITY = "HIGH"

[[VALIDATORS]]
NAME = "sdf_testnet_1"
HOME_DOMAIN = "testnet.stellar.org"
PUBLIC_KEY = "GDKXE2OZMJIPOSLNA6N6F2BVCI3O777I2OOC4BV7VOYUEHYX7RTRYA7Y"
ADDRESS = "core-testnet1.stellar.org"
`, network.TestNetworkPassphrase, extra)
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
	return path
}

// coreFileIngestion is a defaulted [ingestion] section pointed at corePath. The
// binary path does not exist, keeping these tests off whatever stellar-core is
// on PATH; nothing execs it (the SDK's version probe ignores a failed lookup).
func coreFileIngestion(t *testing.T, corePath string) config.IngestionConfig {
	t.Helper()
	return config.Config{Ingestion: config.IngestionConfig{
		CaptiveCoreConfig:     corePath,
		HistoryArchiveURLs:    []string{"https://archive.example"},
		StellarCoreBinaryPath: filepath.Join(t.TempDir(), "stellar-core"),
	}}.WithDefaults().Ingestion
}

func TestNewCaptiveCoreOpeners_PortsOnLiveCoreOnly(t *testing.T) {
	uintPtr := func(v uint) *uint { return &v }
	ing := coreFileIngestion(t, writeCaptiveCoreFile(t, ""))
	ing.CoreHTTPPort = uintPtr(21626)
	ing.CoreHTTPQueryPort = uintPtr(21628)
	ing.CoreHTTPQueryThreadPoolSize = uintPtr(7)
	ing.CoreHTTPQuerySnapshotLedgers = uintPtr(9)

	core, err := newCaptiveCoreOpeners(ing, t.TempDir(), silentLogger())
	require.NoError(t, err)

	live, ok := core.live.(*captiveCoreOpener)
	require.True(t, ok, "the live opener is the production captive-core opener")
	backfill, ok := core.backfill.(*captiveCoreOpener)
	require.True(t, ok)

	// Live: both servers, on the configured ports.
	assert.Equal(t, uint(21626), live.config.Toml.HTTPPort)
	require.NotNil(t, live.config.Toml.HTTPQueryPort)
	assert.Equal(t, uint(21628), *live.config.Toml.HTTPQueryPort)
	require.NotNil(t, live.config.Toml.QueryThreadPoolSize)
	assert.Equal(t, uint(7), *live.config.Toml.QueryThreadPoolSize)
	require.NotNil(t, live.config.Toml.QuerySnapshotLedgers)
	assert.Equal(t, uint(9), *live.config.Toml.QuerySnapshotLedgers)

	// Backfill: no query server, and no admin server either.
	assert.Nil(t, backfill.config.Toml.HTTPQueryPort,
		"a backfill core must not bind the query port")
	assert.Nil(t, backfill.config.Toml.QueryThreadPoolSize)
	catchup, err := backfill.config.Toml.CatchupToml()
	require.NoError(t, err)
	assert.Zero(t, catchup.HTTPPort, "bounded replay runs core with no admin server")

	// Both openers describe the same core.
	assert.Equal(t, network.TestNetworkPassphrase, core.networkPassphrase)
	assert.Equal(t, ing.StellarCoreBinaryPath, core.binaryPath)
	assert.Equal(t, live.config.NetworkPassphrase, backfill.config.NetworkPassphrase)
	assert.Equal(t, live.config.StoragePath, backfill.config.StoragePath)
}

func TestNewCaptiveCoreOpeners_BackfillPortlessEvenWhenCoreFileDeclaresPorts(t *testing.T) {
	extra := "HTTP_PORT = 11626\nHTTP_QUERY_PORT = 11628\nQUERY_THREAD_POOL_SIZE = 2\n"
	ing := coreFileIngestion(t, writeCaptiveCoreFile(t, extra))
	poolSize := uint(2)
	ing.CoreHTTPQueryThreadPoolSize = &poolSize

	core, err := newCaptiveCoreOpeners(ing, t.TempDir(), silentLogger())
	require.NoError(t, err)

	backfill, ok := core.backfill.(*captiveCoreOpener)
	require.True(t, ok)
	assert.Nil(t, backfill.config.Toml.HTTPQueryPort)
	assert.Nil(t, backfill.config.Toml.QueryThreadPoolSize)
	assert.Nil(t, backfill.config.Toml.QuerySnapshotLedgers)
	assert.Zero(t, backfill.config.Toml.HTTPPort)

	// The generated file is what core reads, so assert on that too.
	raw, err := backfill.config.Toml.Marshal()
	require.NoError(t, err)
	assert.NotContains(t, string(raw), "HTTP_QUERY_PORT")
	catchup, err := backfill.config.Toml.CatchupToml()
	require.NoError(t, err)
	rawCatchup, err := catchup.Marshal()
	require.NoError(t, err)
	assert.NotContains(t, string(rawCatchup), "HTTP_QUERY_PORT")

	// The live core still gets both servers.
	live, ok := core.live.(*captiveCoreOpener)
	require.True(t, ok)
	require.NotNil(t, live.config.Toml.HTTPQueryPort)
	assert.Equal(t, uint(11628), *live.config.Toml.HTTPQueryPort)
}

func TestNewCaptiveCoreOpeners_OverridesCoreFileHTTPSettings(t *testing.T) {
	tests := []struct {
		name    string
		extra   string
		fileKey string
		read    func(*ledgerbackend.CaptiveCoreToml) uint
		want    uint
	}{
		{
			"admin port", "HTTP_PORT = 11625\n", keyCoreHTTPPort,
			func(c *ledgerbackend.CaptiveCoreToml) uint { return c.HTTPPort },
			config.DefaultCoreHTTPPort,
		},
		{
			"query port", "HTTP_QUERY_PORT = 11111\n", keyCoreHTTPQueryPort,
			func(c *ledgerbackend.CaptiveCoreToml) uint { return *c.HTTPQueryPort },
			config.DefaultCoreHTTPQueryPort,
		},
		{
			"snapshot ledgers", "QUERY_SNAPSHOT_LEDGERS = 77\n", keyCoreQuerySnapshotLedgers,
			func(c *ledgerbackend.CaptiveCoreToml) uint { return *c.QuerySnapshotLedgers },
			config.DefaultCoreHTTPQuerySnapshotLedgers,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, logs := capturingLogger()
			ing := coreFileIngestion(t, writeCaptiveCoreFile(t, tc.extra))

			core, err := newCaptiveCoreOpeners(ing, t.TempDir(), logger)
			require.NoError(t, err)

			live, ok := core.live.(*captiveCoreOpener)
			require.True(t, ok)
			assert.Equal(t, tc.want, tc.read(live.config.Toml))
			assert.Contains(t, logs.String(), tc.fileKey)
		})
	}
}

func TestNewCaptiveCoreOpeners_OverridesCoreFileThreadPoolSize(t *testing.T) {
	logger, logs := capturingLogger()
	ing := coreFileIngestion(t, writeCaptiveCoreFile(t, "QUERY_THREAD_POOL_SIZE = 999\n"))

	core, err := newCaptiveCoreOpeners(ing, t.TempDir(), logger)
	require.NoError(t, err)

	live, ok := core.live.(*captiveCoreOpener)
	require.True(t, ok)
	require.NotNil(t, live.config.Toml.QueryThreadPoolSize)
	assert.Equal(t, config.NumCPU(), *live.config.Toml.QueryThreadPoolSize)
	assert.Contains(t, logs.String(), keyCoreQueryThreadPoolSize)
}

func TestNewCaptiveCoreOpeners_SilentWhenCoreFileAgrees(t *testing.T) {
	logger, logs := capturingLogger()
	extra := fmt.Sprintf("HTTP_PORT = %d\n", config.DefaultCoreHTTPPort)
	ing := coreFileIngestion(t, writeCaptiveCoreFile(t, extra))

	_, err := newCaptiveCoreOpeners(ing, t.TempDir(), logger)
	require.NoError(t, err)
	assert.NotContains(t, logs.String(), keyCoreHTTPPort)
}

func TestResolveCore_InjectedOpenerServesBothRoles(t *testing.T) {
	injected := &fakeCore{}
	core, err := resolveCore(daemonOptions{Core: injected}, config.Config{}, silentLogger())
	require.NoError(t, err)

	assert.Same(t, injected, core.live)
	assert.Same(t, injected, core.backfill)
	assert.Empty(t, core.networkPassphrase, "an injected opener carries no passphrase")
	assert.Empty(t, core.binaryPath, "and no binary path, so no core version is reported")
}

// ---------------------------------------------------------------------------
// buildBackfillBackend — the no-lake (no datastore) paths.
// ---------------------------------------------------------------------------

// With neither [backfill.datastore] nor an archive pool, buildBackfillBackend
// returns no backend (runDaemonWith then fails startup).
func TestBuildBackfillBackend_NoSourcesNoBackend(t *testing.T) {
	cfg := config.Config{}.WithDefaults()
	backend, cleanup, err := buildBackfillBackend(context.Background(), cfg, &fakeCore{}, nil, "", silentLogger())
	require.NoError(t, err)
	require.Nil(t, backend, "no datastore and no archives ⇒ no bulk source")
	require.Nil(t, cleanup, "nothing to release when no backend was opened")
}

// A daemon with no bulk source at all (no datastore, no archives; the injected
// Core bypasses newCaptiveCoreOpeners' own archive-URL requirement) fails fast at
// startup with the config-shaped message — not at the first backfill pass.
func TestRunDaemon_NoBulkSourceFailsFast(t *testing.T) {
	configPath, _ := writeTempConfig(t, "")
	err := runDaemonWith(context.Background(), configPath, daemonOptions{
		Core:   &fakeCore{},
		Logger: silentLogger(),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no bulk ledger source configured")
}

// With no lake but archives configured, the backfill backend is captive core
// (#833): the stream is the core opener's, and Tip is the archives' root HAS —
// so a below-now earliest_ledger floor is fillable, not just pinnable.
func TestBuildBackfillBackend_NoLakeBuildsCaptiveSource(t *testing.T) {
	cfg := config.Config{}.WithDefaults()
	core := &fakeCore{}
	backend, cleanup, err := buildBackfillBackend(
		context.Background(), cfg, core, fakeRootHAS{current: 70_000}, "", silentLogger())
	require.NoError(t, err)
	require.Nil(t, cleanup, "no datastore handle to release")
	require.IsType(t, &captiveSource{}, backend)
	assert.Equal(t, int32(1), core.openedCount.Load(), "the backend stream is the core opener's")

	tip, err := backend.Tip(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint32(70_000), tip, "the frontier is the archives' root HAS CurrentLedger")
}

// fakeRootHAS is a rootHASGetter over a fixed CurrentLedger (or a fixed error),
// the narrow archive seam captiveSource.Tip reads.
type fakeRootHAS struct {
	current uint32
	err     error
}

func (f fakeRootHAS) GetRootHAS() (historyarchive.HistoryArchiveState, error) {
	if f.err != nil {
		return historyarchive.HistoryArchiveState{}, f.err
	}
	return historyarchive.HistoryArchiveState{CurrentLedger: f.current}, nil
}

// A GetRootHAS failure surfaces as a wrapped tip error (retryable upstream).
func TestCaptiveSourceTip_RootHASErrorSurfaces(t *testing.T) {
	src := &captiveSource{archives: fakeRootHAS{err: errors.New("archive 503")}}
	_, err := src.Tip(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "root HAS")
	assert.Contains(t, err.Error(), "archive 503")
}

// ---------------------------------------------------------------------------
// The no-lake backfill path, end to end through the real entrypoint.
// ---------------------------------------------------------------------------

// streamCore is a CoreOpener over a fixed LedgerStream (the captive-core seam
// for the no-lake backfill test: production hands the same opener's stream to
// both the captive backfill source and the live loop).
type streamCore struct{ stream ledgerbackend.LedgerStream }

func (c *streamCore) OpenCore(context.Context) (ledgerbackend.LedgerStream, error) {
	return c.stream, nil
}

// coreReplayStream is the fake captive-core stream for the no-lake path: a
// BOUNDED pull (the captive backfill source replaying a chunk) serves gen's
// ledgers for exactly [From, To], and an UNBOUNDED pull (the live loop's steady
// state) blocks until ctx cancel — the fake analog of core's catchup-vs-runFrom
// modes.
type coreReplayStream struct {
	t   *testing.T
	gen func(*testing.T, uint32) []byte
}

var _ ledgerbackend.LedgerStream = (*coreReplayStream)(nil)

func (s *coreReplayStream) RawLedgers(
	ctx context.Context, r ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		if !r.Bounded() {
			<-ctx.Done()
			yield(nil, ctx.Err())
			return
		}
		for seq := r.From(); seq <= r.To(); seq++ {
			if !yield(s.gen(s.t, seq), nil) {
				return
			}
		}
	}
}

// writeFileArchive lays out a minimal file:// history archive — just the root
// HAS naming the frontier — and returns its URL.
func writeFileArchive(t *testing.T, currentLedger uint32) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, ".well-known"), 0o755))
	has := fmt.Sprintf(`{"version": 1, "server": "test", "currentLedger": %d, "currentBuckets": []}`,
		currentLedger)
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, ".well-known", "stellar-history.json"), []byte(has), 0o644))
	return "file://" + dir
}

// #833 acceptance: a no-lake daemon (no [backfill.datastore]) with history
// archives BACKFILLS — a below-now floor is fillable through captive core, not
// just pinnable. Boots the real entrypoint with a genesis floor, a file://
// archive whose root HAS covers chunk 0 (the REAL pool construction and frontier
// read), and a core stream serving the bounded replay; chunk 0's cold artifacts
// freeze exactly as on the lake path.
func TestRunDaemon_NoLakeBackfillsThroughCaptiveCore(t *testing.T) {
	archiveURL := writeFileArchive(t, chunk.ID(0).LastLedger())

	dataDir := t.TempDir()
	configPath := filepath.Join(t.TempDir(), "daemon.toml")
	body := fmt.Sprintf(`
[storage]
default_data_dir = %q

[retention]
earliest_ledger = "genesis"

[backfill]
workers = 1

[ingestion]
captive_core_config = "/dev/null"
history_archive_urls = [%q]
`, dataDir, archiveURL)
	require.NoError(t, os.WriteFile(configPath, []byte(body), 0o644))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	servedCh := make(chan struct{}, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- runDaemonWith(ctx, configPath, daemonOptions{
			// NO Backend injected: the daemon wires the captive source itself from
			// the injected core opener + the file:// archive pool.
			Core:       &streamCore{stream: &coreReplayStream{t: t, gen: someTxGen(t)}},
			ServeReads: signalingServeReads(servedCh),
			Logger:     silentLogger(),
		})
	}()
	select {
	case <-servedCh: // backfill complete; the daemon is now parked in ingestion
	case err := <-errCh:
		t.Fatalf("daemon returned before backfill completed: %v", err)
	case <-time.After(60 * time.Second):
		cancel()
		t.Fatal("no-lake backfill did not finish within 60s")
	}
	cancel()
	select {
	case err := <-errCh:
		require.NoError(t, err, "a ctx-canceled ingestion loop is a clean shutdown")
	case <-time.After(10 * time.Second):
		t.Fatal("runDaemonWith did not return after ctx cancel")
	}

	// Chunk 0 froze from the captive replay: ledgers + events + the index coverage.
	cat := openCatalogAt(t, dataDir)
	for _, kind := range []geometry.Kind{geometry.KindLedgers, geometry.KindEvents} {
		state, err := cat.State(0, kind)
		require.NoError(t, err)
		assert.Equal(t, geometry.StateFrozen, state, "chunk 0 %s frozen from the captive replay", kind)
	}
	assert.FileExists(t, cat.Layout().LedgerPackPath(0))
	cov, ok, err := cat.FrozenTxHashIndex(0)
	require.NoError(t, err)
	require.True(t, ok, "window 0 has a frozen txhash index coverage")
	assert.Equal(t, chunk.ID(0), cov.Lo)
	assert.Equal(t, chunk.ID(0), cov.Hi)
}

func TestNewLogger(t *testing.T) {
	l, err := newLogger(config.LoggingConfig{Level: "warn", Format: "json"})
	require.NoError(t, err)
	require.NotNil(t, l)

	_, err = newLogger(config.LoggingConfig{Level: "bogus", Format: "text"})
	require.Error(t, err)
}
