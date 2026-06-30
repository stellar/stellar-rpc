package fullhistory

// =============================================================================
// In-process end-to-end integration of the full-history daemon.
//
// WHAT IS REAL HERE
//   Everything inside the process is the real production code path:
//     - runDaemonWith (the true daemon entrypoint): TOML load + form-validate,
//       per-root flock, meta-store open + Catalog bind, the stateful
//       validateConfig gate (pins the floor), and the supervised run loop.
//     - run → backfillToTip → openHotTierForChunk → runIngestionLoop (the real
//       atomic per-ledger WriteBatch across all CFs of the real per-chunk
//       hotchunk RocksDB), the real boundary handoff, the real doorbell.
//     - lifecycle.RunLoop / runLifecycleTick: the real resolve + executePlan
//       freeze (cold artifacts derived FROM the live hot DB), the real txhash
//       index fold (a real streamhash .idx on disk), the real discard + prune.
//     - The real txhash stores on both sides of a getTransaction-style hash→seq
//       lookup: the cold ColdReader over the frozen .idx and the live hot CF.
//
// WHAT IS FAKED (the two EXTERNAL boundaries the daemon injects on purpose)
//     - The ledger SOURCE. Production drives ingestion from captive
//       stellar-core and backfill from a bulk object-store backend. Here both
//       cross their injected interfaces (CoreOpener / backfill.Backend) and are
//       fed synthetic-but-well-formed LedgerCloseMeta. No captive core, no
//       object store, no network.
//     - ServeReads is a no-op recorder (the read cutover is #772). The read PATH
//       exercised is the txhash index lookup getTransaction will sit on.
//
// cpi=1 (the chunksPerTxhashIndex test seam) makes every one-chunk window
// terminal the instant its chunk freezes, so the freeze→fold→discard→prune
// sequence completes on a boundary tick without ingesting 1000 chunks.
// =============================================================================

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/lifecycle"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// e2eCore is the CoreOpener handing back a fresh e2eGetter per daemon run (a
// restart opens core anew). frames is the seq→raw backlog every getter serves;
// the atomics aggregate observations across opens for the restart assertions.
type e2eCore struct {
	frames     map[uint32][]byte
	resumeSeen atomic.Uint32
	fromSeen   atomic.Uint32
	delivered  atomic.Uint32
	opens      atomic.Int32
}

func (c *e2eCore) OpenCore(_ context.Context, resume uint32) (LedgerGetter, func() error, error) {
	c.opens.Add(1)
	c.resumeSeen.Store(resume)
	return &e2eGetter{core: c}, func() error { return nil }, nil
}

// e2eGetter is the FAKE captive-core ledger getter: a resumable LedgerGetter the
// ingestion loop polls by sequence. It returns the frame for the requested seq
// when its core has one, and once the poll runs past the synthetic backlog it
// blocks until ctx is cancelled (a live tip stream ends only on shutdown). It
// records (into its core) the FIRST seq it was asked for, so the restart step can
// assert the daemon re-derived the watermark and resumed with no gap.
type e2eGetter struct {
	core    *e2eCore
	sawFrom atomic.Bool
}

var _ LedgerGetter = (*e2eGetter)(nil)

func (s *e2eGetter) GetLedger(ctx context.Context, seq uint32) (xdr.LedgerCloseMetaView, error) {
	if s.sawFrom.CompareAndSwap(false, true) {
		s.core.fromSeen.Store(seq)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if raw, ok := s.core.frames[seq]; ok {
		s.core.delivered.Store(seq)
		return xdr.LedgerCloseMetaView(raw), nil
	}
	// Past the synthetic backlog: a live tip blocks until shutdown so the loop
	// does not see an error that would look like a core crash.
	<-ctx.Done()
	return nil, ctx.Err()
}

// e2eMetrics is a concurrency-safe observability.Metrics that counts the chunk
// boundaries and freezes the daemon emits (the rest discarded via NopMetrics).
type e2eMetrics struct {
	observability.NopMetrics
	mu         sync.Mutex
	boundaries int
	freezes    int
}

func (m *e2eMetrics) ChunkBoundary(uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.boundaries++
}

func (m *e2eMetrics) Freeze(time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.freezes++
}

func (m *e2eMetrics) boundaryCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.boundaries
}

func (m *e2eMetrics) snapshotFreezeCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.freezes
}

// e2eConfigPath writes a daemon TOML for an in-process E2E: genesis floor (no
// tip needed to validate/start) and the given retention width. captive_core_config
// is a stub path the test's injected CoreOpener replaces, never opening a real core.
// The one-chunk index window is set via the chunksPerTxhashIndex test seam, not config.
func e2eConfigPath(t *testing.T, dataDir string, retentionChunks uint32) string {
	t.Helper()
	cfgPath := filepath.Join(t.TempDir(), "daemon.toml")
	body := fmt.Sprintf(`
[service]
default_data_dir = %q

[retention]
earliest_ledger = "genesis"
retention_chunks = %d

[ingestion]
captive_core_config = "/dev/null"

[logging]
level = "error"
format = "text"
`, dataDir, retentionChunks)
	require.NoError(t, os.WriteFile(cfgPath, []byte(body), 0o644))
	return cfgPath
}

// runDaemonInBackground starts runDaemonWith on a cancellable ctx and returns a
// cancel func, a channel carrying its (clean-shutdown) return, and a channel
// delivering the daemon's OWN bound *catalog.Catalog (captured via the onCatalog
// seam). The metastore is opened RocksDB-primary (exclusive LOCK), so a test
// cannot open a second handle while the daemon runs — instead it reads durable
// state through the daemon's own catalog (safe for concurrent reads). A young-
// network tip (inside chunk 0) means backfill is a no-op and first-start ingests
// directly from genesis via the fake core.
func runDaemonInBackground(
	t *testing.T, cfgPath string, core *e2eCore, served *atomic.Int32, metrics observability.Metrics,
) (context.CancelFunc, <-chan error, <-chan *catalog.Catalog) {
	t.Helper()
	ctx, cancelFn := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	catChan := make(chan *catalog.Catalog, 1)
	opts := daemonOptions{
		Backend:              &fakeBackend{tip: chunk.FirstLedgerSeq + 5}, // young: no backfill
		Core:                 core,
		ServeReads:           func(context.Context) error { served.Add(1); return nil },
		Logger:               silentLogger(),
		Metrics:              metrics,
		RestartBackoff:       10 * time.Millisecond,
		chunksPerTxhashIndex: 1,
		onCatalog: func(cat *catalog.Catalog) {
			select {
			case catChan <- cat:
			default:
			}
		},
	}
	go func() { errCh <- runDaemonWith(ctx, cfgPath, opts) }()
	return cancelFn, errCh, catChan
}

// awaitCatalog waits for the daemon to hand back its bound catalog.
func awaitCatalog(t *testing.T, catCh <-chan *catalog.Catalog) *catalog.Catalog {
	t.Helper()
	select {
	case cat := <-catCh:
		return cat
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not bind a catalog")
		return nil
	}
}

// waitClean cancels the daemon and requires a clean (nil) shutdown.
func waitClean(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err, "ctx cancel is a clean daemon shutdown")
	case <-time.After(60 * time.Second):
		// Post-cancel shutdown joins one in-flight lifecycle unit; a mid-flight
		// freeze's Finalize fsync + index build is unpreemptible and slow under
		// -race + contention — the same reason the boundary-cross budget is 600s.
		t.Fatal("daemon did not shut down cleanly after ctx cancel")
	}
}

// hotKeyExists reports whether chunk c's hot:chunk key is present (any non-empty state).
func hotKeyExists(cat *catalog.Catalog, c chunk.ID) (bool, error) {
	st, err := cat.HotState(c)
	if err != nil {
		return false, err
	}
	return st != geometry.HotState(""), nil
}

// hashAt builds a deterministic 32-byte hash from n (for the never-committed miss).
func hashAt(n uint64) [32]byte {
	var h [32]byte
	for i := 0; i < 8; i++ {
		h[i] = byte(n >> (8 * i))
	}
	return h
}

// TestE2E_DaemonLifecycle_FirstStartIngestFreezeLookupRestartPrune drives the
// whole daemon lifecycle in one process against the real stores and the fake
// ledger source:
//
//	first start (genesis, young-network tip ⇒ direct ingest) →
//	ingest a FULL chunk + cross into the next (real boundary handoff) →
//	lifecycle tick freezes chunk 0 + folds its terminal txhash index + discards
//	  its hot tier →
//	getTransaction-style hash→seq lookup resolves from the cold .idx (chunk 0)
//	  AND from the live hot CF (chunk 2) →
//	clean shutdown →
//	RESTART: re-derive the watermark, resume at exactly watermark+1 (no gap) →
//	drive retention far enough to prune chunk 0, confirm a pruned read is not-found.
//
// Correctness is asserted at every step.
func TestE2E_DaemonLifecycle_FirstStartIngestFreezeLookupRestartPrune(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e ingests a full 10k-ledger chunk; skipped in -short")
	}

	dataDir := t.TempDir()

	const c0 = chunk.ID(0)
	const c1 = chunk.ID(1)
	const c2 = chunk.ID(2)

	// Cross TWO chunk boundaries so chunks 0 AND 1 both freeze, leaving chunk 2 as
	// the live (un-frozen) chunk. That layout lets a later retention_chunks=1 run
	// prune chunk 0 (wholly below the floor) while chunk 1 survives.
	c0First := c0.FirstLedger()
	c1First := c1.FirstLedger()
	c2First := c2.FirstLedger()

	// One shared source account; the per-seq SeqNum makes each tx hash unique.
	src := xdr.MustMuxedAddress(keypair.MustRandom().Address())
	coldRaw, coldHash := oneTxLCMBytes(t, c0First, src) // → frozen cold .idx (chunk 0)
	hotRaw, hotHash := oneTxLCMBytes(t, c2First, src)   // → live hot CF (chunk 2)
	// Chunk 1's first ledger also carries a tx so its txhash .bin is non-empty —
	// streamhash refuses to build a cold index over zero keys (ErrEmptyBuildSet).
	c1Raw, _ := oneTxLCMBytes(t, c1First, src)

	frames := make(map[uint32][]byte, 2*int(chunk.LedgersPerChunk)+2)
	appendLedger := func(seq uint32) {
		switch seq {
		case c0First:
			frames[seq] = coldRaw
		case c1First:
			frames[seq] = c1Raw
		case c2First:
			frames[seq] = hotRaw
		default:
			frames[seq] = zeroTxLCMBytes(t, seq)
		}
	}
	// Chunks 0 and 1 in full (both freeze), then chunk 2's first two ledgers.
	for seq := c0First; seq <= c1.LastLedger(); seq++ {
		appendLedger(seq)
	}
	appendLedger(c2First)
	appendLedger(c2First + 1)

	core := &e2eCore{frames: frames}
	var served atomic.Int32
	metrics := &e2eMetrics{}

	// =====================================================================
	// STEP 1 — first start: config → lock → validate (pin genesis) → start →
	// direct ingest across the chunk-0 AND chunk-1 boundaries, the lifecycle
	// freezing, folding, and discarding each just-closed chunk off the doorbell.
	// =====================================================================
	cfgPath := e2eConfigPath(t, dataDir, 0) // retention 0 (full history) for now
	cancel, done, catCh := runDaemonInBackground(t, cfgPath, core, &served, metrics)

	// Inspect durable state through the daemon's OWN bound catalog (metastore is
	// RocksDB-primary, so a second handle would fail the LOCK).
	cat := awaitCatalog(t, catCh)

	// Wait until ingestion crosses BOTH boundaries and commits into chunk 2.
	// Delivering c2First proves both boundary handoffs fired (chunks 0 and 1
	// closed, chunk 2 opened) and seeds the live hot-CF lookup. 600s absorbs the
	// worst-case contended -race path (per-ledger synced WriteBatches racing the
	// freezes that re-read 10k ledgers each).
	require.Eventually(t, func() bool {
		return core.delivered.Load() >= c2First
	}, 600*time.Second, 200*time.Millisecond, "ingestion must cross both boundaries into chunk 2")

	// The boundary doorbells have rung. Per chunk, the durable completion signal is:
	// the window has a FROZEN txhash coverage (the .idx) AND the chunk's hot key is
	// gone (discarded).
	w0 := cat.TxHashIndexLayout().TxHashIndexID(c0)
	w1 := cat.TxHashIndexLayout().TxHashIndexID(c1)
	require.Eventually(t, func() bool {
		for w, c := range map[geometry.TxHashIndexID]chunk.ID{w0: c0, w1: c1} {
			_, hasCov, err := cat.FrozenTxHashIndex(w)
			if err != nil || !hasCov {
				return false
			}
			has, err := hotKeyExists(cat, c)
			if err != nil || has {
				return false
			}
		}
		return true
	}, 60*time.Second, 50*time.Millisecond, "the boundary ticks must freeze+fold+discard chunks 0 and 1")

	require.GreaterOrEqual(t, served.Load(), int32(1), "reads were served")
	require.Equal(t, uint32(c0First), core.resumeSeen.Load(),
		"first start resumes captive core at genesis (watermark+1)")

	// --- Correctness: chunks 0 and 1 per-chunk cold artifacts (ledgers + events) froze. ---
	for _, c := range []chunk.ID{c0, c1} {
		for _, kind := range []geometry.Kind{geometry.KindLedgers, geometry.KindEvents} {
			st, err := cat.State(c, kind)
			require.NoError(t, err)
			assert.Equal(t, geometry.StateFrozen, st, "chunk %s %s is frozen", c, kind)
		}
	}
	// The window's txhash index is a frozen, terminal coverage (the .idx the cold
	// getTransaction read resolves against).
	frozenCov, ok, err := cat.FrozenTxHashIndex(w0)
	require.NoError(t, err)
	require.True(t, ok, "chunk 0's window has a frozen txhash coverage")
	require.True(t, cat.TxHashIndexLayout().IsTerminalCoverage(frozenCov), "a one-chunk (cpi=1) window is terminal")

	// =====================================================================
	// STEP 2 — getTransaction-style hash→seq lookup, cold tier.
	// =====================================================================

	// Cold .idx — the exact reader getTransaction will sit on for frozen history.
	coldReader, err := txhash.OpenColdReader(cat.Layout().TxHashIndexFilePath(frozenCov))
	require.NoError(t, err)
	gotSeq, err := coldReader.Get(coldHash)
	require.NoError(t, err, "the chunk-0 tx hash must resolve from the frozen cold index")
	assert.Equal(t, c0First, gotSeq, "cold lookup returns the ledger the tx was committed in")
	// A hash that was never committed misses (not-found, not a wrong answer).
	_, missErr := coldReader.Get(hashAt(0xE2EDEADBEEF))
	require.ErrorIs(t, missErr, stores.ErrNotFound, "an uncommitted hash misses the cold index")
	require.NoError(t, coldReader.Close())

	// Observability: the daemon emitted the boundary + freeze phase signals.
	assert.GreaterOrEqual(t, metrics.boundaryCount(), 1, "at least one chunk boundary was signaled")
	assert.GreaterOrEqual(t, metrics.snapshotFreezeCount(), 1, "at least one freeze stage ran")

	// =====================================================================
	// STEP 3 — clean shutdown. The supervised loop returns nil on ctx cancel.
	// =====================================================================
	waitClean(t, cancel, done)

	// Bind a fresh inspection catalog on the (now lock-free) data dir for the
	// post-shutdown reads. It MUST be closed before the restart reopens the metastore.
	postCat, closePost := e2eReadCatalog(t, dataDir)

	// The durable watermark, re-derived from post-shutdown state (the basis for the
	// restart's resume-with-no-gap assertion).
	wmBeforeRestart := mustDeriveWatermark(t, postCat)
	require.GreaterOrEqual(t, wmBeforeRestart, c2First, "watermark advanced into chunk 2")

	// Live hot CF — now the daemon has stopped, chunk 2 (still the un-frozen live
	// chunk) is reopenable. Resolve the chunk-2 tx hash through the txhash CF — the
	// read path getTransaction uses for live history before a chunk freezes.
	hotState, err := postCat.HotState(c2)
	require.NoError(t, err)
	require.Equal(t, geometry.HotReady, hotState, "chunk 2 is the un-frozen live chunk")
	c2lfs, err := postCat.State(c2, geometry.KindLedgers)
	require.NoError(t, err)
	require.Equal(t, geometry.State(""), c2lfs, "the live chunk has no cold artifacts yet")

	// Retry the open: RocksDB's process-level LOCK can linger momentarily after the
	// writer closed (the same transient a production reader retries through).
	var liveDB *hotchunk.DB
	require.Eventually(t, func() bool {
		db, oerr := hotchunk.Open(cat.Layout().HotChunkPath(c2), c2, silentLogger())
		if oerr != nil {
			return false
		}
		liveDB = db
		return true
	}, 10*time.Second, 50*time.Millisecond, "chunk 2's hot DB must be reopenable after shutdown")
	hotSeq, err := liveDB.Txhash().Get(hotHash)
	require.NoError(t, err, "the chunk-2 tx hash must resolve from the live hot CF")
	assert.Equal(t, c2First, hotSeq, "hot lookup returns the live tx's ledger")
	require.NoError(t, liveDB.Close()) // release before the restart reopens it as the live writer

	// =====================================================================
	// STEP 4 — RESTART. A fresh runDaemonWith re-opens everything, re-derives the
	// watermark from durable state, and resumes captive core at watermark+1 with no gap.
	// =====================================================================
	closePost() // release the inspection metastore handle before the daemon reopens it
	core.opens.Store(0)
	core.resumeSeen.Store(0)
	core.fromSeen.Store(0)
	cancel2, done2, _ := runDaemonInBackground(t, cfgPath, core, &served, &e2eMetrics{})

	require.Eventually(t, func() bool { return core.opens.Load() >= 1 }, 30*time.Second, 20*time.Millisecond,
		"the restarted daemon re-opened captive core")
	require.Eventually(t, func() bool { return core.fromSeen.Load() != 0 }, 30*time.Second, 20*time.Millisecond,
		"the restarted ingestion loop requested a resume range")

	wantResume := wmBeforeRestart + 1
	assert.Equal(t, wantResume, core.resumeSeen.Load(),
		"restart resumes captive core at the re-derived watermark+1 (no gap, no re-fetch of the bottom)")
	assert.Equal(t, wantResume, core.fromSeen.Load(),
		"the ingestion loop streamed from watermark+1 — the durable frontier, re-derived not stored")

	waitClean(t, cancel2, done2)

	// =====================================================================
	// STEP 5 — retention prune. Re-run with retention_chunks = 1: the floor anchors
	// at chunk 1, so chunk 0 (frozen + folded) falls WHOLLY below it and the prune
	// scan sweeps its files + keys, while chunk 1 (the floor chunk) survives. A read
	// of a pruned chunk-0 hash is then not-found (no coverage to resolve it).
	// =====================================================================
	prunedCfg := e2eConfigPath(t, dataDir, 1) // retain ~1 chunk
	prunedIdxPath := cat.Layout().TxHashIndexFilePath(frozenCov)
	require.FileExists(t, prunedIdxPath, "chunk 0's cold index exists before the prune")

	cancel3, done3, catCh3 := runDaemonInBackground(t, prunedCfg, core, &served, &e2eMetrics{})
	pruneCat := awaitCatalog(t, catCh3) // the pruning daemon's own catalog

	// The prune scan runs on the first lifecycle tick (the at-start doorbell ring).
	// Poll for chunk 0's per-chunk artifact keys (ledgers + events) to vanish.
	require.Eventually(t, func() bool {
		ledgers, err := pruneCat.State(c0, geometry.KindLedgers)
		if err != nil {
			return false
		}
		ev, err := pruneCat.State(c0, geometry.KindEvents)
		if err != nil {
			return false
		}
		return ledgers == geometry.State("") && ev == geometry.State("")
	}, 60*time.Second, 50*time.Millisecond, "retention must prune chunk 0's artifact keys")

	// Chunk 1 (the floor chunk) is WITHIN retention and survives the prune.
	c1lfs, err := pruneCat.State(c1, geometry.KindLedgers)
	require.NoError(t, err)
	assert.Equal(t, geometry.StateFrozen, c1lfs, "chunk 1 is at the retention floor and survives")

	// The on-disk cold index file is gone too (prune unlinks the files, not just keys).
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(prunedIdxPath)
		return os.IsNotExist(statErr)
	}, 10*time.Second, 50*time.Millisecond, "the pruned cold index file is unlinked")

	// "pruned read is not-found": after prune the window has no frozen coverage
	// (ok=false) — the read layer's "no coverage ⇒ not-found" gate.
	_, covOK, err := pruneCat.FrozenTxHashIndex(w0)
	require.NoError(t, err)
	assert.False(t, covOK, "chunk 0's window coverage is pruned ⇒ a chunk-0 hash read is not-found")

	waitClean(t, cancel3, done3)
}

// e2eReadCatalog binds a Catalog over a SEPARATE metastore handle on the daemon's
// data dir, with the same one-chunk window the daemon's test seam uses, for
// read-only inspection BETWEEN daemon runs (the metastore is RocksDB-primary, so
// this MUST be closed via the returned close func before the next daemon run).
func e2eReadCatalog(t *testing.T, dataDir string) (*catalog.Catalog, func()) {
	t.Helper()
	paths := Config{Service: ServiceConfig{DefaultDataDir: dataDir}}.WithDefaults().ResolvePaths()
	store, err := openMetaAt(t, paths.Catalog)
	require.NoError(t, err)
	windows, err := geometry.NewTxHashIndexLayout(1) // matches chunksPerTxhashIndex = 1
	require.NoError(t, err)
	return catalog.NewCatalog(store, NewLayoutFromPaths(paths), windows), func() { _ = store.Close() }
}

// mustDeriveWatermark derives the durable watermark through the production probe.
func mustDeriveWatermark(t *testing.T, cat *catalog.Catalog) uint32 {
	t.Helper()
	wm, err := lifecycle.LastCommittedLedger(cat, NewRocksHotProbe(cat.Layout().HotChunkPath, silentLogger()))
	require.NoError(t, err)
	return wm
}
