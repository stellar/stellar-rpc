package streaming

// =============================================================================
// Issue 19 — in-process end-to-end integration of the streaming daemon.
//
// WHAT IS REAL HERE
//   Everything inside the process is the real production code path:
//     - RunDaemonWith (the true daemon entrypoint): TOML load + form-validate,
//       per-root flock, meta-store open + Catalog bind, the stateful
//       validateConfig gate (pins the immutable layout + resolves the floor),
//       and the supervised startStreaming loop.
//     - startStreaming → catchUp → openHotTierForChunk → runIngestionLoop (the
//       real atomic per-ledger WriteBatch across all CFs of the real per-chunk
//       hotchunk RocksDB), the real boundary handoff, the real doorbell.
//     - lifecycleLoop / runLifecycleTick: the real resolve + executePlan freeze
//       (cold artifacts derived FROM the live hot DB via processChunk's hot
//       branch), the real txhash index fold (a real streamhash .idx on disk),
//       the real discard + prune scans.
//     - The real txhash stores on both sides of a getTransaction-style hash→seq
//       lookup: the cold ColdReader over the frozen .idx and the live HotStore
//       CF.
//     - Catalog.Audit (INV-1..4) over the real durable keys + files.
//
// WHAT IS FAKED (and why that is the right boundary)
//   Only the two EXTERNAL boundaries the daemon injects on purpose:
//     - The ledger SOURCE. Production drives ingestion from captive
//       stellar-core (a child process) and backfill from a bulk object-store
//       backend. Here both cross their injected interfaces (CoreStreamOpener /
//       NetworkTipBackend) and are fed SYNTHETIC-BUT-WELL-FORMED LedgerCloseMeta
//       built by the same fixtures the merged store tests use (zero-tx LCM for
//       bulk, plus a one-tx LCM where a real, network-hashed transaction hash is
//       needed so the txhash index has a real key to resolve). No captive core,
//       no docker-stellar-core, no object store, no network.
//     - ServeReads is a no-op recorder (the SQLite→full-history read cutover is
//       #772; see daemon.go). The read PATH we actually exercise is the txhash
//       index lookup the getTransaction handler will sit on top of.
//
// FOLLOW-UP (out of scope here; requires infra not available in this sandbox)
//   A full captive-core + docker-stellar-core E2E belongs in the existing
//   integrationtest harness (cmd/stellar-rpc/internal/integrationtest): it
//   stands up a real core + a real history archive and ingests real network
//   ledgers. That validates the ledger SOURCE adapters (captiveCoreOpener,
//   backendTip/DataStoreSource) this test fakes, and is gated on the #772 read
//   cutover for an end-user getTransaction round-trip over RPC. This in-process
//   test deliberately stops at the daemon's injected boundaries so it runs with
//   no external services.
// =============================================================================

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// e2ePassphrase is the network passphrase the synthetic tx hashes are computed
// against. Any stable value works; the index only needs deterministic hashes
// the test can then look up.
const e2ePassphrase = network.PublicNetworkPassphrase

// oneTxLCMReturningHash builds a well-formed V2 LedgerCloseMeta carrying exactly
// ONE transaction for seq and returns BOTH the wire bytes and the real,
// network-hashed transaction hash. A non-zero-tx ledger is required somewhere in
// a chunk so its txhash .bin is non-empty (streamhash refuses a zero-key cold
// index, txhash.ErrEmptyBuildSet); returning the hash lets the E2E assert the
// getTransaction-style hash→seq lookup against a hash the daemon really
// committed. It mirrors lifecycle_test's oneTxLCMBytes, exposing the hash.
func oneTxLCMReturningHash(t *testing.T, seq uint32) ([]byte, [32]byte) {
	t.Helper()
	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
				Ext:           xdr.TransactionExt{V: 1, SorobanData: &xdr.SorobanTransactionData{}},
			},
		},
	}
	hash, err := network.HashTransactionInEnvelope(envelope, e2ePassphrase)
	require.NoError(t, err)

	comp := []xdr.TxSetComponent{{
		Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
		TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
			Txs: []xdr.TransactionEnvelope{envelope},
		},
	}}
	opResults := []xdr.OperationResult{}
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
				TxApplyProcessing: xdr.TransactionMeta{
					V:  4,
					V4: &xdr.TransactionMetaV4{Operations: []xdr.OperationMetaV2{}},
				},
				Result: xdr.TransactionResultPair{
					TransactionHash: hash,
					Result: xdr.TransactionResult{
						FeeCharged: 100,
						Result:     xdr.TransactionResultResult{Code: xdr.TransactionResultCodeTxSuccess, Results: &opResults},
					},
				},
			}},
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw, hash
}

// e2eGetter is the FAKE captive-core ledger getter: a resumable LedgerGetter the
// ingestion loop polls by sequence (the design's core.GetLedger(ctx, seq)). It
// returns the frame for the requested seq when it has one, and once the poll
// runs past the synthetic backlog it blocks until ctx is canceled (a live tip
// stream ends only on shutdown). It records the FIRST seq it was asked for so
// the restart step can assert the daemon re-derived the watermark and resumed
// with no gap. The ctx-cancelled GetLedger return is the clean-shutdown path the
// daemon top level classifies as clean.
type e2eGetter struct {
	frames    map[uint32][]byte
	maxSeq    uint32
	fromSeen  *atomic.Uint32 // first GetLedger seq (for the restart assertion)
	delivered *atomic.Uint32 // highest seq actually yielded (test sync)
	sawFrom   atomic.Bool
}

type e2eFrame struct {
	seq uint32
	raw []byte
}

var _ LedgerGetter = (*e2eGetter)(nil)

func (s *e2eGetter) GetLedger(ctx context.Context, seq uint32) (xdr.LedgerCloseMetaView, error) {
	if s.sawFrom.CompareAndSwap(false, true) {
		s.fromSeen.Store(seq)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if raw, ok := s.frames[seq]; ok {
		s.delivered.Store(seq)
		return xdr.LedgerCloseMetaView(raw), nil
	}
	// Past the synthetic backlog: a live tip blocks until shutdown so the loop
	// does not see an error that would look like a core crash.
	<-ctx.Done()
	return nil, ctx.Err()
}

// e2eCore is the CoreOpener handing back a fresh e2eGetter per daemon run (a
// restart opens core anew). It records the resume ledger every open was driven
// from.
type e2eCore struct {
	frames     []e2eFrame
	resumeSeen atomic.Uint32
	fromSeen   atomic.Uint32
	delivered  atomic.Uint32
	opens      atomic.Int32
}

func (c *e2eCore) OpenCore(_ context.Context, resume uint32) (LedgerGetter, func() error, error) {
	c.opens.Add(1)
	c.resumeSeen.Store(resume)
	byseq := make(map[uint32][]byte, len(c.frames))
	var maxSeq uint32
	for _, f := range c.frames {
		byseq[f.seq] = f.raw
		if f.seq > maxSeq {
			maxSeq = f.seq
		}
	}
	getter := &e2eGetter{frames: byseq, maxSeq: maxSeq, fromSeen: &c.fromSeen, delivered: &c.delivered}
	return getter, func() error { return nil }, nil
}

// e2eConfigPath writes a daemon TOML for an in-process E2E: genesis floor (no
// tip needed to validate/start), a one-chunk index window (chunks_per_txhash_-
// index = 1, so every window is terminal the instant its chunk freezes — the
// freeze→fold→discard sequence completes on the boundary tick), and the given
// retention width. captive_core_config is a stub path the test's BuildBoundaries
// replaces with a fake stream, never opening a real core.
func e2eConfigPath(t *testing.T, dataDir string, retentionChunks uint32) string {
	t.Helper()
	cfgPath := filepath.Join(t.TempDir(), "daemon.toml")
	body := fmt.Sprintf(`
[service]
default_data_dir = %q

[streaming]
earliest_ledger = "genesis"
captive_core_config = "/dev/null"
retention_chunks = %d

[backfill]
chunks_per_txhash_index = 1

[logging]
level = "error"
format = "text"
`, dataDir, retentionChunks)
	require.NoError(t, os.WriteFile(cfgPath, []byte(body), 0o644))
	return cfgPath
}

// runDaemonInBackground starts RunDaemonWith on a cancellable ctx and returns a
// cancel func, a channel carrying its (clean-shutdown) return, and a channel
// delivering the daemon's OWN bound *Catalog (captured from the BuildBoundaries
// callback). The metastore is opened RocksDB-primary (exclusive LOCK), so a test
// CANNOT open a second handle on the same path while the daemon runs — instead
// it reads durable state through the daemon's own catalog, which is safe for
// concurrent reads. ServeReads records the serve count; a young-network tip
// (inside chunk 0) means backfill is a no-op and first-start ingests directly
// from genesis via the fake core.
//
//nolint:nonamedreturns // named outputs label the (cancel, done, catalog) handles
func runDaemonInBackground(
	t *testing.T, cfgPath string, core *e2eCore, served *atomic.Int32, metrics Metrics,
) (cancel context.CancelFunc, done <-chan error, catCh <-chan *Catalog) {
	t.Helper()
	ctx, cancelFn := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	catChan := make(chan *Catalog, 1)
	build := func(_ context.Context, _ Config, _ Paths, cat *Catalog, _ *supportlog.Entry) (Boundaries, error) {
		select {
		case catChan <- cat: // hand the daemon's bound catalog to the test
		default:
		}
		return Boundaries{
			NetworkTip: &fakeTipBackend{tips: []uint32{chunk.FirstLedgerSeq + 5}},
			Core:       core,
			ServeReads: func(context.Context) error { served.Add(1); return nil },
		}, nil
	}
	opts := DaemonOptions{
		BuildBoundaries: build,
		Logger:          silentLogger(),
		Metrics:         metrics,
		RestartBackoff:  10 * time.Millisecond,
	}
	go func() { errCh <- RunDaemonWith(ctx, cfgPath, opts) }()
	return cancelFn, errCh, catChan
}

// awaitCatalog waits for the daemon to hand back its bound catalog.
func awaitCatalog(t *testing.T, catCh <-chan *Catalog) *Catalog {
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

// ============================================================================
// The end-to-end walk.
// ============================================================================

// TestE2E_DaemonLifecycle_FirstStartIngestFreezeLookupRestartPrune drives the
// whole daemon lifecycle in one process against the real stores and the fake
// ledger source:
//
//	first start (genesis, young-network tip ⇒ direct ingest) →
//	ingest a FULL chunk + cross into the next (real boundary handoff) →
//	lifecycle tick freezes chunk 0 + folds its terminal txhash index + discards
//	  its hot tier →
//	getTransaction-style hash→seq lookup resolves from the cold .idx (chunk 0)
//	  AND from the live hot CF (chunk 1) →
//	clean shutdown →
//	RESTART: re-derive the watermark, resume at exactly watermark+1 (no gap) →
//	drive retention far enough to prune chunk 0, and confirm a pruned read is
//	  not-found →
//	finish with Catalog.Audit → Clean.
//
// Correctness is asserted at every step.
//
//nolint:funlen // full lifecycle E2E with assertions at every step
func TestE2E_DaemonLifecycle_FirstStartIngestFreezeLookupRestartPrune(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e ingests a full 10k-ledger chunk; skipped in -short")
	}

	dataDir := t.TempDir()

	const c0 = chunk.ID(0)
	const c1 = chunk.ID(1)
	const c2 = chunk.ID(2)

	// --- Synthetic ledgers. We cross TWO chunk boundaries so chunks 0 AND 1 both
	// freeze (completeThrough reaches chunk 1's last ledger), leaving chunk 2 as
	// the live (un-frozen) chunk. That layout lets a later retention_chunks=1 run
	// prune chunk 0 (wholly below the floor) while chunk 1 survives.
	//
	// Each chunk is ingested in FULL and contiguously from its first ledger (the
	// events CF's strict-contiguity precondition), so the freeze derives every
	// cold artifact. One real, network-hashed tx is planted where a resolvable
	// hash is needed — chunk 0's first ledger (→ frozen cold .idx) and chunk 2's
	// first ledger (→ the live hot CF). Every other ledger is zero-tx for speed.
	c0First := c0.FirstLedger()
	c1First := c1.FirstLedger()
	c2First := c2.FirstLedger()

	coldRaw, coldHash := oneTxLCMReturningHash(t, c0First) // → frozen cold .idx (chunk 0)
	hotRaw, hotHash := oneTxLCMReturningHash(t, c2First)   // → live hot CF (chunk 2)
	// Chunk 1's first ledger also carries a tx so its txhash .bin is non-empty —
	// streamhash refuses to build a cold index over zero keys (ErrEmptyBuildSet),
	// which would otherwise abort the lifecycle tick when chunk 1 freezes.
	c1Raw, _ := oneTxLCMReturningHash(t, c1First)

	frames := make([]e2eFrame, 0, 2*int(chunk.LedgersPerChunk)+2)
	appendLedger := func(seq uint32) {
		var raw []byte
		switch seq {
		case c0First:
			raw = coldRaw
		case c1First:
			raw = c1Raw
		case c2First:
			raw = hotRaw
		default:
			raw = zeroTxLCMBytes(t, seq)
		}
		frames = append(frames, e2eFrame{seq: seq, raw: raw})
	}
	// Chunks 0 and 1 in full (both freeze), then chunk 2's first two ledgers (the
	// live chunk; boundary 1→2 fired, chunk 2 opened, its first ledger committed).
	for seq := c0First; seq <= c1.LastLedger(); seq++ {
		appendLedger(seq)
	}
	appendLedger(c2First)
	appendLedger(c2First + 1)

	core := &e2eCore{frames: frames}
	var served atomic.Int32
	metrics := newRecordingMetrics()

	// =====================================================================
	// STEP 1 — first start: config → lock → validate (pin genesis) → start →
	// direct ingest across the chunk-0 AND chunk-1 boundaries, with the lifecycle
	// freezing, folding, and discarding each just-closed chunk off the doorbell.
	// =====================================================================
	cfgPath := e2eConfigPath(t, dataDir, 0) // retention 0 (full history) for now
	cancel, done, catCh := runDaemonInBackground(t, cfgPath, core, &served, metrics)

	// Inspect durable state through the daemon's OWN bound catalog (the metastore
	// is opened RocksDB-primary, so a second handle would fail the LOCK). The
	// catalog is safe for concurrent reads alongside the daemon's writes.
	cat := awaitCatalog(t, catCh)

	// First wait until ingestion crosses BOTH boundaries and commits into chunk 2
	// (the new live chunk). Delivering c2First proves both boundary handoffs fired
	// (chunks 0 and 1 closed, chunk 2 opened) and seeds the live hot-CF lookup.
	// (NOTE: we must NOT gate on "chunk 0's hot key absent" first — the daemon
	// hands the test its catalog from BuildBoundaries, BEFORE startStreaming opens
	// the resume chunk's hot DB, so that key is transiently absent at start.)
	// Budget note: crossing both boundaries is ~20k per-ledger SYNCED WriteBatches
	// (the design's one-atomic-synced-batch-per-ledger durability boundary) racing
	// the lifecycle freezes that re-read 10k ledgers each. fsync throughput is
	// highly variable under contention: in isolation this reaches chunk 2 in ~110s
	// (no -race) but ~175s under -race, and the CI gate runs the whole tree under
	// `-race` (so this E2E is NOT -short-skipped there) alongside this package's
	// six t.Parallel() full-chunk ticks, all competing for the same disk. 180s was
	// too tight (flaky timeouts at 161/167s/killed). 600s absorbs the worst-case
	// contended -race path while staying far under the 25m package envelope.
	require.Eventually(t, func() bool {
		return core.delivered.Load() >= c2First
	}, 600*time.Second, 200*time.Millisecond, "ingestion must cross both boundaries into chunk 2")

	// The boundary doorbells have rung. A lifecycle tick freezes each just-closed
	// chunk's cold artifacts (from its closed hot DB), folds its terminal (cpi=1)
	// txhash index, then discards its hot tier. The durable completion signal per
	// chunk: the window has a FROZEN txhash coverage (the .idx) AND the chunk's hot
	// key is gone (discarded). (NOTE: the per-chunk chunk:{c}:txhash key is the
	// .bin input the one-write index fold CONSUMES — after the fold it is
	// demoted+swept, reading "" not "frozen"; the durable txhash artifact is the
	// window's frozen coverage, not the per-chunk key.)
	w0 := cat.windows.WindowID(c0)
	w1 := cat.windows.WindowID(c1)
	require.Eventually(t, func() bool {
		for w, c := range map[WindowID]chunk.ID{w0: c0, w1: c1} {
			_, hasCov, err := cat.FrozenCoverage(w)
			if err != nil || !hasCov {
				return false
			}
			has, err := cat.Has(hotChunkKey(c))
			if err != nil || has {
				return false
			}
		}
		return true
	}, 60*time.Second, 50*time.Millisecond, "the boundary ticks must freeze+fold+discard chunks 0 and 1")

	require.GreaterOrEqual(t, served.Load(), int32(1), "reads were served")
	require.Equal(t, c0First, core.resumeSeen.Load(),
		"first start resumes captive core at genesis (watermark+1)")

	// --- Correctness: chunks 0 and 1 per-chunk cold artifacts (ledgers + events) froze. ---
	for _, c := range []chunk.ID{c0, c1} {
		for _, kind := range []Kind{KindLedgers, KindEvents} {
			st, err := cat.State(c, kind)
			require.NoError(t, err)
			assert.Equal(t, StateFrozen, st, "chunk %s %s is frozen", c, kind)
		}
	}
	// The window's txhash index is a frozen, terminal coverage (the .idx the cold
	// getTransaction read resolves against).
	frozenCov, ok, err := cat.FrozenCoverage(w0)
	require.NoError(t, err)
	require.True(t, ok, "chunk 0's window has a frozen txhash coverage")
	require.True(t, cat.windows.IsTerminalCoverage(frozenCov), "a one-chunk (cpi=1) window is terminal")

	// =====================================================================
	// STEP 2 — getTransaction-style hash→seq lookup, both tiers.
	//   (a) cold: resolve chunk 0's tx via the frozen .idx on disk.
	//   (b) hot: resolve chunk 2's tx via the live hot DB's txhash CF.
	// =====================================================================

	// (a) Cold .idx — the exact reader getTransaction will sit on for frozen
	// history. It resolves the committed hash to its real ledger seq.
	coldReader, err := txhash.OpenColdReader(cat.layout.IndexFilePath(frozenCov))
	require.NoError(t, err)
	gotSeq, err := coldReader.Get(coldHash)
	require.NoError(t, err, "the chunk-0 tx hash must resolve from the frozen cold index")
	assert.Equal(t, c0First, gotSeq, "cold lookup returns the ledger the tx was committed in")
	// A hash that was never committed misses (not-found, not a wrong answer).
	_, missErr := coldReader.Get(hashAt(0xE2EDEADBEEF))
	require.ErrorIs(t, missErr, stores.ErrNotFound, "an uncommitted hash misses the cold index")
	require.NoError(t, coldReader.Close())

	// (b) is performed AFTER the clean shutdown below — opening chunk 2's hot DB
	// read-only would conflict with the live ingestion writer's exclusive RocksDB
	// LOCK while the daemon runs; once the daemon stops cleanly the live chunk's
	// hot DB is on disk and reopenable. The hot tier is the UN-frozen live chunk's
	// sole copy, so this still exercises the hot read path.

	// Observability: the daemon emitted the boundary + freeze phase signals (the
	// control-plane health gauges).
	assert.GreaterOrEqual(t, len(metrics.snapshotBoundaries()), 1, "at least one chunk boundary was signaled")
	assert.GreaterOrEqual(t, metrics.snapshotFreezeCount(), 1, "at least one freeze stage ran")

	// =====================================================================
	// STEP 3 — clean shutdown. The supervised loop returns nil on ctx cancel.
	// =====================================================================
	// (Watermark derivation opens the live hot DB read-only, so it MUST run after
	// the daemon — the live writer — releases the exclusive RocksDB LOCK; do it
	// after waitClean below.)
	waitClean(t, cancel, done)

	// The daemon's catalog rode its now-closed metastore handle; bind a fresh
	// inspection catalog on the (now lock-free) data dir for the post-shutdown
	// reads. It MUST be closed before the restart reopens the metastore.
	postCat, closePost := e2eReadCatalog(t, dataDir)

	// The durable watermark, re-derived from the post-shutdown state (the basis
	// for the restart's resume-with-no-gap assertion).
	wmBeforeRestart := mustDeriveWatermark(t, postCat)
	require.GreaterOrEqual(t, wmBeforeRestart, c2First, "watermark advanced into chunk 2")

	// (b) Live hot CF — now the daemon has stopped, chunk 2 (still the un-frozen
	// live chunk: its hot key is "ready", no cold artifacts) is reopenable. Open
	// its real hot DB and resolve the chunk-2 tx hash through the txhash CF — the
	// read path getTransaction uses for live history before a chunk freezes.
	hotState, err := postCat.HotState(c2)
	require.NoError(t, err)
	require.Equal(t, HotReady, hotState, "chunk 2 is the un-frozen live chunk")
	c2lfs, err := postCat.State(c2, KindLedgers)
	require.NoError(t, err)
	require.Equal(t, State(""), c2lfs, "the live chunk has no cold artifacts yet")

	// Retry the open: RocksDB's process-level LOCK can linger momentarily after the
	// writer closed (the same transient a production reader retries through).
	var liveDB *hotchunk.DB
	require.Eventually(t, func() bool {
		db, oerr := hotchunk.Open(cat.layout.HotChunkPath(c2), c2, silentLogger())
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
	// STEP 4 — RESTART. A fresh RunDaemonWith re-opens everything, re-derives the
	// watermark from durable state, and resumes captive core at watermark+1 with
	// no gap. (The shared e2eCore records the new resume + the stream's From.)
	// =====================================================================
	closePost() // release the inspection metastore handle before the daemon reopens it
	core.opens.Store(0)
	core.resumeSeen.Store(0)
	core.fromSeen.Store(0)
	cancel2, done2, _ := runDaemonInBackground(t, cfgPath, core, &served, newRecordingMetrics())

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
	// STEP 5 — retention prune. Re-run the daemon with retention_chunks = 1: the
	// effective floor anchors at chunk 1 (lastCompleteChunkAt(through=chunk 1) -
	// 1 + 1), so chunk 0 (frozen + folded) falls WHOLLY below the floor and the
	// prune scan sweeps its files + keys, while chunk 1 (the floor chunk) survives.
	// A read of a pruned chunk-0 hash is then not-found (no coverage to resolve it).
	// =====================================================================
	prunedCfg := e2eConfigPath(t, dataDir, 1) // retain ~1 chunk
	// Capture chunk 0's frozen .idx path BEFORE the prune so we can confirm the
	// file itself is gone afterward. (cat's layout is path-only and stays valid
	// even though its metastore handle closed at the Step-3 shutdown.)
	prunedIdxPath := cat.layout.IndexFilePath(frozenCov)
	require.FileExists(t, prunedIdxPath, "chunk 0's cold index exists before the prune")

	cancel3, done3, catCh3 := runDaemonInBackground(t, prunedCfg, core, &served, newRecordingMetrics())
	pruneCat := awaitCatalog(t, catCh3) // the pruning daemon's own catalog

	// The prune scan runs on the first lifecycle tick (the at-start doorbell ring,
	// which is startup convergence). Poll for chunk 0's per-chunk artifact keys
	// (ledgers + events — the frozen cold artifacts) to vanish.
	require.Eventually(t, func() bool {
		ledgers, err := pruneCat.State(c0, KindLedgers)
		if err != nil {
			return false
		}
		ev, err := pruneCat.State(c0, KindEvents)
		if err != nil {
			return false
		}
		return ledgers == State("") && ev == State("")
	}, 60*time.Second, 50*time.Millisecond, "retention must prune chunk 0's artifact keys")

	// Chunk 1 (the floor chunk) is WITHIN retention and survives the prune.
	c1lfs, err := pruneCat.State(c1, KindLedgers)
	require.NoError(t, err)
	assert.Equal(t, StateFrozen, c1lfs, "chunk 1 is at the retention floor and survives")

	// The on-disk cold index file is gone too (prune unlinks the files, not just
	// the keys) — a pruned read therefore cannot even open the reader.
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(prunedIdxPath)
		return os.IsNotExist(statErr)
	}, 10*time.Second, 50*time.Millisecond, "the pruned cold index file is unlinked")

	// getTransaction-style "pruned read is not-found": the frozen coverage key is
	// gone, so the read path has no index to resolve the (formerly resolvable)
	// chunk-0 hash against — the production reader returns not-found. After prune
	// the window has no frozen coverage (ok=false): the read layer's "no coverage
	// ⇒ not-found" gate.
	_, covOK, err := pruneCat.FrozenCoverage(w0)
	require.NoError(t, err)
	assert.False(t, covOK, "chunk 0's window coverage is pruned ⇒ a chunk-0 hash read is not-found")

	waitClean(t, cancel3, done3)

	// =====================================================================
	// STEP 6 — Catalog.Audit (INV-1..4) → Clean. The store must be at a single
	// canonical state with no orphans/dangling/duplicates and nothing below the
	// retention floor. RetentionChunks matches the daemon's last config so INV-4
	// checks against the EXACT floor it enforced.
	// =====================================================================
	auditCat, closeAudit := e2eReadCatalog(t, dataDir)
	defer closeAudit()
	report, err := auditCat.Audit(AuditOptions{RetentionChunks: 1})
	require.NoError(t, err, "audit completes (error only for I/O)")
	require.True(t, report.Clean(),
		"after the full lifecycle the store satisfies INV-1..4; violations:\n%s", violationsString(report))
}

// ============================================================================
// helpers
// ============================================================================

// e2eReadCatalog binds a Catalog over a SEPARATE metastore handle on the
// daemon's data dir, with the same one-chunk window the daemon config pins, for
// read-only inspection BETWEEN daemon runs (the metastore is RocksDB-primary /
// exclusive-LOCK, so this MUST be closed via the returned close func before the
// next daemon run reopens it).
func e2eReadCatalog(t *testing.T, dataDir string) (*Catalog, func()) {
	t.Helper()
	paths := Config{Service: ServiceConfig{DefaultDataDir: dataDir}}.WithDefaults().ResolvePaths()
	store, err := openMetaAt(t, paths.Catalog)
	require.NoError(t, err)
	windows, err := NewWindows(1) // matches chunks_per_txhash_index = 1
	require.NoError(t, err)
	return NewCatalog(store, NewLayoutFromPaths(paths), windows), func() { _ = store.Close() }
}

// mustDeriveWatermark derives the durable watermark through the production probe.
func mustDeriveWatermark(t *testing.T, cat *Catalog) uint32 {
	t.Helper()
	wm, err := deriveWatermark(cat, NewRocksHotProbe(cat.layout.HotChunkPath, silentLogger()))
	require.NoError(t, err)
	return wm
}

// The E2E reuses observability_test.go's recordingMetrics (a full Metrics sink)
// and its snapshotBoundaries; snapshotFreezeCount (added there) reports the
// number of freeze-stage signals.
