package fullhistory

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

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
)

// openMetaAt opens a metastore.Store at path for read-back assertions.
func openMetaAt(t *testing.T, path string) (*metastore.Store, error) {
	t.Helper()
	return metastore.New(path, silentLogger())
}

// writeTempConfig writes a minimal-valid daemon TOML (genesis floor ⇒ no tip
// needed) and returns the config path and data dir.
func writeTempConfig(t *testing.T, extra string) (string, string) {
	t.Helper()
	dataDir := t.TempDir()
	configPath := filepath.Join(t.TempDir(), "daemon.toml")
	body := fmt.Sprintf(`
[service]
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

// Happy path pins earliest_ledger and serves reads once. The injected backend's
// young-network tip (inside chunk 0) ⇒ no-op backfill, no LedgerStream needed.
func TestRunDaemon_LoadValidateWireStartCleanShutdown(t *testing.T) {
	configPath, dataDir := writeTempConfig(t, "")

	var served atomic.Int32
	opts := daemonOptions{
		Backend:    &fakeBackend{tip: chunk.FirstLedgerSeq + 10},
		ServeReads: func(context.Context) error { served.Add(1); return nil },
		Logger:     silentLogger(),
	}

	errCh := make(chan error, 1)
	go func() { errCh <- runDaemonWith(context.Background(), configPath, opts) }()

	select {
	case err := <-errCh:
		require.NoError(t, err, "cold catch-up + serve returns cleanly")
	case <-time.After(3 * time.Second):
		t.Fatal("runDaemonWith did not return")
	}

	assert.Equal(t, int32(1), served.Load(), "reads served once")

	// validateConfig pinned earliest_ledger before start (cpi is a constant now,
	// not a pinned value).
	store, err := openMetaAt(t, filepath.Join(dataDir, "catalog", "rocksdb"))
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	txLayout, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	require.NoError(t, err)
	cat := catalog.NewCatalog(store, geometry.NewLayout(dataDir), txLayout)
	earliest, pinned, err := cat.EarliestLedger()
	require.NoError(t, err)
	require.True(t, pinned, "validateConfig must pin earliest_ledger before run")
	assert.Equal(t, uint32(chunk.FirstLedgerSeq), earliest)
}

// someTxBackend serves mostly zero-tx ledgers with a sparse few carrying one tx:
// an all-zero-tx chunk can't build a txhash index ("zero keys"), so it needs some.
func someTxBackend(t *testing.T) *fakeBackend {
	t.Helper()
	src := xdr.MustMuxedAddress(keypair.MustRandom().Address())
	gen := func(t *testing.T, seq uint32) []byte {
		if seq%2500 != 0 {
			return zeroTxLCMBytes(t, seq)
		}
		return oneTxLCMBytes(t, seq, src)
	}
	return &fakeBackend{
		LedgerStream: &fullChunkStream{t: t, gen: gen},
		// Tip covers chunk 0 ⇒ the freeze's coverage wait passes at once.
		tip: chunk.ID(0).LastLedger(),
	}
}

// oneTxLCMBytes is zeroTxLCMBytes plus one tx (per-seq SeqNum ⇒ unique hash) so
// ExtractTxHashes yields exactly one key for seq.
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
	return raw
}

// #815 acceptance: one TOML boots the daemon and it catches up the complete chunk
// 0 for all three cold types + the tx-hash index THROUGH the real entrypoint (not
// primitives in isolation). workers=1 also drives the deadlock-prone
// index-waits-on-its-chunk path. cpi=1000 ⇒ window 0 spans [0,999], so one complete
// chunk yields a non-terminal rolling coverage [0,0] that keeps its .bin inputs.
func TestRunDaemon_CatchUpMaterializesAllColdTypesAndIndex(t *testing.T) {
	configPath, dataDir := writeTempConfig(t, "[backfill]\nworkers = 1\n")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- runDaemonWith(ctx, configPath, daemonOptions{
			// Backend's tip is chunk 0's last ledger ⇒ chunk 0 complete, catch-up freezes it.
			// The network tip is derived from this same backend's Tip.
			Backend:    someTxBackend(t),
			ServeReads: func(context.Context) error { return nil },
			Logger:     silentLogger(),
		})
	}()
	select {
	case err := <-errCh:
		require.NoError(t, err, "daemon catches up to tip then exits cleanly (no-op ServeReads)")
	case <-time.After(60 * time.Second):
		cancel()
		t.Fatal("runDaemonWith did not finish catch-up within 60s (regressed into a hang/restart loop?)")
	}

	// Read the catalog back after the daemon released locks + closed its store.
	store, err := openMetaAt(t, filepath.Join(dataDir, "catalog", "rocksdb"))
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	txLayout, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	require.NoError(t, err)
	layout := geometry.NewLayout(dataDir)
	cat := catalog.NewCatalog(store, layout, txLayout)

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

	cfg := Config{
		Service: ServiceConfig{DefaultDataDir: dataDir},
		Storage: StorageConfig{
			Catalog:     catalogOverride,
			Ledgers:     ledgersOverride,
			Events:      eventsOverride,
			TxhashRaw:   txhashRawOverride,
			TxhashIndex: txhashIndexOverride,
			Hot:         hotOverride,
		},
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

// filepathHasPrefix reports whether path lives under prefix, comparing cleaned
// components (so /a/bc is not under /a/b).
func filepathHasPrefix(path, prefix string) bool {
	rel, err := filepath.Rel(prefix, path)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// A second daemon on the same data dir fails fast on the storage-root flock
// (single-process invariant, enforced before any store opens).
func TestRunDaemon_LockContentionFailsFast(t *testing.T) {
	configPath, dataDir := writeTempConfig(t, "")

	// Hold the hot-root lock as a "first daemon" for the test's duration.
	paths := Paths{HotStorage: filepath.Join(dataDir, "hot")}
	locks, err := LockRoots(paths.HotStorage)
	require.NoError(t, err)
	defer locks.Release()

	var served atomic.Int32
	err = runDaemonWith(context.Background(), configPath, daemonOptions{
		Backend:    &fakeBackend{tip: chunk.FirstLedgerSeq + 10},
		ServeReads: func(context.Context) error { served.Add(1); return nil },
		Logger:     silentLogger(),
	})
	require.ErrorIs(t, err, ErrRootLocked)
	assert.Zero(t, served.Load(), "run never reached when a root is locked")
}

// First start with a "now" floor and an unreachable tip is fatal at
// validateConfig: "now" cannot resolve, so the daemon surfaces it.
func TestRunDaemon_NowFloorRequiresTip(t *testing.T) {
	configPath, _ := writeTempConfigNow(t)

	err := runDaemonWith(context.Background(), configPath, daemonOptions{
		// An unreachable bulk source (Tip errors) ⇒ the derived network tip can't
		// resolve "now".
		Backend:        &fakeBackend{tipErr: errors.New("unreachable")},
		Logger:         silentLogger(),
		RestartBackoff: time.Millisecond,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "now")
}

func writeTempConfigNow(t *testing.T) (string, string) {
	t.Helper()
	dataDir := t.TempDir()
	configPath := filepath.Join(t.TempDir(), "daemon.toml")
	body := fmt.Sprintf(`
[service]
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
// supervise — the top-level restart loop.
// ---------------------------------------------------------------------------

// A restartable error retries on a backoff, then a clean ctx cancel during the
// backoff returns nil (no restart after a shutdown request).
func TestSupervise_RetriesThenCleanShutdown(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)

	var attempts atomic.Int32
	tip := &fakeTipBackend{tips: []uint32{chunk.FirstLedgerSeq + 10}} // young: no backfill
	start := startTestConfig(t, cat, tip, nil)
	// An always-erroring ServeReads makes each attempt a restartable failure.
	start.ServeReads = func(context.Context) error {
		attempts.Add(1)
		return errors.New("transient serve failure")
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- supervise(ctx, start, silentLogger(), 5*time.Millisecond) }()

	// Let a few restarts happen, then cancel.
	require.Eventually(t, func() bool {
		return attempts.Load() >= 2
	}, 3*time.Second, 5*time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err, "ctx cancel during backoff returns nil")
	case <-time.After(3 * time.Second):
		t.Fatal("supervise did not return after cancel")
	}
	assert.GreaterOrEqual(t, attempts.Load(), int32(2), "restarted on the transient failure")
}

// Fatal sentinels surface up, not retried (a fresh start cannot heal them).
func TestSupervise_FatalSentinelSurfaces(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	// Unreachable tip + no local progress ⇒ fatal ErrFirstStartNoTip.
	tip := &fakeTipBackend{err: errors.New("unreachable"), errFirst: 99}
	start := startTestConfig(t, cat, tip, nil)

	err := supervise(context.Background(), start, silentLogger(), time.Hour)
	require.ErrorIs(t, err, ErrFirstStartNoTip, "fatal sentinel surfaces immediately, no retry")
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
// buildBackfillBackend — the frontfill (no datastore) path.
// ---------------------------------------------------------------------------

// With no [backfill.datastore], buildBackfillBackend returns no backend (frontfill),
// and resolveNetworkTip then yields the not-configured placeholder.
func TestBuildBackfillBackend_FrontfillNoBackend(t *testing.T) {
	cfg := Config{}.WithDefaults()
	backend, cleanup, err := buildBackfillBackend(context.Background(), cfg, silentLogger())
	require.NoError(t, err)
	require.Nil(t, backend, "no datastore ⇒ frontfill-only, no backend")
	require.Nil(t, cleanup, "nothing to release when no backend was opened")

	// The derived tip is the not-configured placeholder: sampling it errors clearly.
	_, tipErr := resolveNetworkTip(backend).NetworkTip(context.Background())
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
