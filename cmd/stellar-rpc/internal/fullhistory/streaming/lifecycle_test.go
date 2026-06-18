package streaming

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// lifecyclePassphrase is the network passphrase the one-tx fixture hashes
// against (any stable value works; the index only needs deterministic hashes).
const lifecyclePassphrase = network.PublicNetworkPassphrase

// oneTxLCMBytes builds the wire bytes of a V2 LedgerCloseMeta carrying ONE
// transaction for seq, so a chunk ingested with at least one such ledger yields
// a NON-empty txhash .bin — streamhash refuses to build a cold index over zero
// keys (txhash.ErrEmptyBuildSet), so a fully zero-tx chunk cannot exercise the
// real index fold. Mirrors ingest_test's buildLCMReturningHashes, trimmed to one
// tx.
func oneTxLCMBytes(t *testing.T, seq uint32) []byte {
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
	hash, err := network.HashTransactionInEnvelope(envelope, lifecyclePassphrase)
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
	return raw
}

// ---------------------------------------------------------------------------
// Arithmetic: lastCompleteChunkAt, effectiveRetentionFloor.
// ---------------------------------------------------------------------------

func TestLastCompleteChunkAt(t *testing.T) {
	tests := []struct {
		name   string
		ledger uint32
		want   int64
	}{
		{"below first chunk's last ledger => sentinel -1", chunk.ID(0).LastLedger() - 1, -1},
		{"genesis sentinel (FirstLedgerSeq-1) => -1", chunk.FirstLedgerSeq - 1, -1},
		{"ledger 0 does not underflow => -1", 0, -1},
		{"chunk 0's last ledger => 0", chunk.ID(0).LastLedger(), 0},
		{"chunk 0's last ledger + 1 (into chunk 1) => still 0", chunk.ID(0).LastLedger() + 1, 0},
		{"chunk 5's last ledger => 5", chunk.ID(5).LastLedger(), 5},
		{"the doc's example 10_001 => 0", 10_001, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, lastCompleteChunkAt(tc.ledger))
		})
	}
}

func TestEffectiveRetentionFloor(t *testing.T) {
	genesis := uint32(chunk.FirstLedgerSeq)
	tests := []struct {
		name            string
		upperBound      uint32
		retentionChunks uint32
		earliest        uint32
		want            uint32
	}{
		{
			name:            "no sliding (retention 0): earliest floor wins",
			upperBound:      chunk.ID(100).LastLedger(),
			retentionChunks: 0,
			earliest:        chunk.ID(10).FirstLedger(),
			want:            chunk.ID(10).FirstLedger(),
		},
		{
			name:            "no sliding, no earliest pin: genesis",
			upperBound:      chunk.ID(100).LastLedger(),
			retentionChunks: 0,
			earliest:        0,
			want:            genesis,
		},
		{
			name:            "sliding floor leads when above earliest",
			upperBound:      chunk.ID(100).LastLedger(), // last complete chunk = 100
			retentionChunks: 10,                         // floor chunk = 100-10+1 = 91
			earliest:        0,
			want:            chunk.ID(91).FirstLedger(),
		},
		{
			name:            "earliest floor leads when above the sliding floor",
			upperBound:      chunk.ID(100).LastLedger(),
			retentionChunks: 10,                         // sliding floor chunk = 91
			earliest:        chunk.ID(95).FirstLedger(), // higher
			want:            chunk.ID(95).FirstLedger(),
		},
		{
			name:            "retention wider than history clamps to chunk 0, never wraps",
			upperBound:      chunk.ID(3).LastLedger(),
			retentionChunks: 1000, // sliding chunk = 3-1000+1 < 0 => clamp to chunk 0
			earliest:        0,
			want:            chunk.ID(0).FirstLedger(),
		},
		{
			name:            "young store (upperBound below first chunk) clamps to chunk 0",
			upperBound:      chunk.FirstLedgerSeq + 5, // no complete chunk yet
			retentionChunks: 5,
			earliest:        0,
			want:            chunk.ID(0).FirstLedger(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, effectiveRetentionFloor(tc.upperBound, tc.retentionChunks, tc.earliest))
		})
	}
}

// ---------------------------------------------------------------------------
// lowestMaterializedChunk.
// ---------------------------------------------------------------------------

func TestLowestMaterializedChunk(t *testing.T) {
	t.Run("empty catalog => ok=false", func(t *testing.T) {
		cat, _ := testCatalog(t)
		_, ok, err := lowestMaterializedChunk(cat)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("min over chunk artifact keys and hot keys", func(t *testing.T) {
		cat, _ := testCatalog(t)
		freezeKinds(t, cat, 7, KindLedgers)        // chunk artifact key at 7
		require.NoError(t, cat.PutHotTransient(4)) // hot key at 4 (lower)
		freezeKinds(t, cat, 9, KindEvents)
		low, ok, err := lowestMaterializedChunk(cat)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, chunk.ID(4), low)
	})
}

// ---------------------------------------------------------------------------
// End-to-end tick harness: real catalog + real hotchunk DBs.
// ---------------------------------------------------------------------------

// ingestFullHotChunk creates a "ready" hot DB for chunk c and ingests every
// ledger in the chunk (all CFs, contiguous from FirstLedger), then closes the
// write handle — the post-boundary state the lifecycle freezes from. The hot
// key is left "ready" and the dir is on disk, as the boundary handoff leaves it.
func ingestFullHotChunk(t *testing.T, cat *Catalog, c chunk.ID) {
	t.Helper()
	db := openLiveHotDB(t, cat, c)
	for seq := c.FirstLedger(); seq <= c.LastLedger(); seq++ {
		// The first ledger carries one tx so the chunk's txhash .bin is non-empty
		// (streamhash refuses a zero-key index); the rest stay zero-tx for speed.
		var raw []byte
		if seq == c.FirstLedger() {
			raw = oneTxLCMBytes(t, seq)
		} else {
			raw = zeroTxLCMBytes(t, seq)
		}
		_, err := db.IngestLedger(seq, xdr.LedgerCloseMetaView(raw), allHotTypes)
		require.NoError(t, err)
	}
	require.NoError(t, db.Close()) // release the write handle (boundary handoff)
}

// lifecycleTestConfig wires a LifecycleConfig over the real production primitives
// (a real RocksHotProbe over the catalog's hot layout) plus a fatal recorder so a
// tick abort is observable instead of killing the test process.
func lifecycleTestConfig(t *testing.T, cat *Catalog, retentionChunks uint32) (LifecycleConfig, *fatalRecorder) {
	t.Helper()
	rec := &fatalRecorder{}
	cfg := LifecycleConfig{
		ExecConfig: ExecConfig{
			Catalog: cat,
			Logger:  silentLogger(),
			Workers: 2,
			Process: ProcessConfig{
				HotProbe: NewRocksHotProbe(cat.layout.HotChunkPath, silentLogger()),
			},
		},
		RetentionChunks: retentionChunks,
		Fatalf:          rec.fatalf,
	}
	return cfg, rec
}

// fatalRecorder captures Fatalf calls so a test can assert a tick did (or did
// NOT) abort the daemon.
type fatalRecorder struct {
	count atomic.Int32
	last  atomic.Value // string
}

func (r *fatalRecorder) fatalf(format string, args ...any) {
	r.count.Add(1)
	r.last.Store(fmt.Sprintf(format, args...))
}

func (r *fatalRecorder) fired() bool { return r.count.Load() > 0 }

// TestRunLifecycleTick_BoundaryFreezesFoldsDiscards is the "one boundary, end to
// end" walk: chunk 0 just closed (its full hot DB is on disk, ready), chunk 1 is
// the new live chunk. One tick must:
//   - freeze chunk 0's cold artifacts FROM its hot DB (via processChunk's hot
//     branch),
//   - fold chunk 0 into its window's index (terminal coverage, cpi=1),
//   - discard chunk 0's hot DB (cold artifacts now fully serve it),
//   - leave the live chunk 1 untouched.
//
// Then re-running the tick is a no-op (quiescence).
func TestRunLifecycleTick_BoundaryFreezesFoldsDiscards(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 1) // window w == chunk w; a one-chunk window finalizes immediately
	cfg, rec := lifecycleTestConfig(t, cat, 0)

	// Chunk 0: just-closed, full hot DB on disk. Chunk 1: the new live chunk.
	ingestFullHotChunk(t, cat, 0)
	live := openLiveHotDB(t, cat, 1) // the live chunk's hot DB (held open by "ingestion")
	t.Cleanup(func() { _ = live.Close() })

	runTickForCatalog(context.Background(), t, cfg, cat)
	require.False(t, rec.fired(), "a healthy tick never aborts: %v", rec.last.Load())

	// Chunk 0's cold artifacts are all frozen.
	for _, kind := range []Kind{KindLedgers, KindEvents} {
		state, err := cat.State(0, kind)
		require.NoError(t, err)
		assert.Equal(t, StateFrozen, state, "chunk 0 %s frozen", kind)
	}
	// The window's index is terminal and covers chunk 0.
	covered, err := indexCovers(0, cat)
	require.NoError(t, err)
	assert.True(t, covered, "the window index folded chunk 0 in")
	fk, ok, err := cat.FrozenCoverage(cat.windows.WindowID(0))
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, cat.windows.IsTerminalCoverage(fk), "a one-chunk window is terminal")

	// Chunk 0's hot DB is discarded (cold artifacts fully serve it).
	has, err := cat.Has(hotChunkKey(0))
	require.NoError(t, err)
	assert.False(t, has, "chunk 0's hot key is gone")

	// The live chunk 1 is untouched: its hot key still "ready", no cold artifacts.
	hotState, err := cat.HotState(1)
	require.NoError(t, err)
	assert.Equal(t, HotReady, hotState, "the live chunk's hot key is untouched")
	lfs1, err := cat.State(1, KindLedgers)
	require.NoError(t, err)
	assert.Equal(t, State(""), lfs1, "the live chunk is not frozen")

	// Quiescence: re-running the tick produces no work.
	through, err := deriveCompleteThrough(cat)
	require.NoError(t, err)
	assertQuiescent(t, cfg, cat, through)
}

// TestRunLifecycleTick_DiscardGatedOnIndexCoverage: a complete chunk whose cold
// ledgers+events are frozen but whose window index does NOT yet cover it keeps its
// hot DB (it still serves tx lookups). Only once a terminal coverage exists does
// the discard fire. cpi=2 so a single chunk does NOT finalize the window.
func TestRunLifecycleTick_DiscardGatedOnIndexCoverage(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 2) // window 0 = chunks [0,1]
	cfg, _ := lifecycleTestConfig(t, cat, 0)

	// Pre-freeze chunk 0's ledgers+events+txhash directly (no hot dependence), and
	// leave it with a "ready" hot DB on disk. The window is NOT finalized (cpi=2,
	// only chunk 0 present), so no terminal coverage exists.
	freezeKinds(t, cat, 0, KindLedgers, KindEvents, KindTxHash)
	makeReadyHotDirNoData(t, cat, 0)
	// A live chunk 1 above it so chunk 0 is below the partition boundary.
	require.NoError(t, cat.PutHotTransient(1))

	through := chunk.ID(0).LastLedger() // chunk 0 complete via cold
	// txhash is frozen, ledgers/events frozen, but the window has no FROZEN coverage
	// yet => indexCovers(0) is false => NOT discarded (still needed for lookups via
	// its .bin/hot DB until the index folds it in).
	ops, err := eligibleDiscardOps(cfg, cat, through)
	require.NoError(t, err)
	require.Empty(t, ops, "no index coverage yet: the hot DB stays")

	// Now finalize the window's index so it covers chunk 0 (terminal needs chunk
	// 1's .bin too; build a non-terminal-but-covering frozen coverage [0,0]).
	freezeCoverage(t, cat, 0, 0, 0)
	covered, err := indexCovers(0, cat)
	require.NoError(t, err)
	require.True(t, covered)

	ops, err = eligibleDiscardOps(cfg, cat, through)
	require.NoError(t, err)
	require.Len(t, ops, 1, "covered + nothing pending => discard eligible")
	require.NoError(t, ops[0]())

	has, err := cat.Has(hotChunkKey(0))
	require.NoError(t, err)
	assert.False(t, has, "the now-covered chunk's hot DB is discarded")
}

// TestRunLifecycleTick_PastFloorPrune: a chunk wholly below the effective
// retention floor has its artifact files and hot DB swept, regardless of state.
func TestRunLifecycleTick_PastFloorPrune(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 1)
	cfg, rec := lifecycleTestConfig(t, cat, 2) // retain ~2 chunks

	// completeThrough will be chunk 5's last ledger (positional: live chunk 6).
	// floor = lastCompleteChunkAt(through)-retention+1 = 5-2+1 = chunk 4's first
	// ledger. So chunks 0..3 are wholly past the floor and must be swept.
	for c := chunk.ID(0); c <= 5; c++ {
		freezeKinds(t, cat, c, KindLedgers, KindEvents, KindTxHash)
		writeArtifact(t, cat.layout.LedgerPackPath(c))
		freezeCoverage(t, cat, cat.windows.WindowID(c), c, c) // each one-chunk window terminal
	}
	// A past-floor hot DB too (chunk 1).
	makeReadyHotDirNoData(t, cat, 1)
	live := openLiveHotDB(t, cat, 6) // live chunk
	t.Cleanup(func() { _ = live.Close() })

	through, err := deriveCompleteThrough(cat)
	require.NoError(t, err)
	require.Equal(t, chunk.ID(5).LastLedger(), through)
	floor := effectiveRetentionFloor(through, cfg.RetentionChunks, 0)
	require.Equal(t, chunk.ID(4).FirstLedger(), floor, "floor anchors 2 chunks back")

	runTickForCatalog(context.Background(), t, cfg, cat)
	require.False(t, rec.fired(), "prune tick never aborts: %v", rec.last.Load())

	// Chunks 0..3 (wholly below the floor) are gone: keys and files.
	for c := chunk.ID(0); c <= 3; c++ {
		ledgers, serr := cat.State(c, KindLedgers)
		require.NoError(t, serr)
		assert.Equal(t, State(""), ledgers, "chunk %s ledgers key swept", c)
		assert.NoFileExists(t, cat.layout.LedgerPackPath(c), "chunk %s pack swept", c)
		has, herr := cat.Has(hotChunkKey(c))
		require.NoError(t, herr)
		assert.False(t, has, "chunk %s hot key swept", c)
	}
	// Chunk 4 (the floor chunk) and 5 are within retention and survive.
	for c := chunk.ID(4); c <= 5; c++ {
		ledgers, serr := cat.State(c, KindLedgers)
		require.NoError(t, serr)
		assert.Equal(t, StateFrozen, ledgers, "chunk %s in retention survives", c)
	}

	assertQuiescent(t, cfg, cat, through)
}

// TestRunLifecycleTick_PrunesTransientIndexDebris: a "freezing" index key (a
// crashed build attempt) is swept regardless of window, even within retention.
func TestRunLifecycleTick_PrunesTransientIndexDebris(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 2)
	cfg, rec := lifecycleTestConfig(t, cat, 0)

	// A crashed build left a "freezing" coverage key (no commit).
	_, err := cat.MarkIndexFreezing(0, 0, 0)
	require.NoError(t, err)

	through, err := deriveCompleteThrough(cat)
	require.NoError(t, err)
	ops, err := eligiblePruneOps(cfg, cat, through)
	require.NoError(t, err)
	require.Len(t, ops, 1, "the freezing debris is swept")
	require.NoError(t, ops[0]())
	require.False(t, rec.fired())

	covs, err := cat.AllIndexKeys()
	require.NoError(t, err)
	require.Empty(t, covs, "the freezing index key is gone")
}

// ---------------------------------------------------------------------------
// CLEAN SHUTDOWN: a ctx cancelled mid-tick returns WITHOUT fatal.
// ---------------------------------------------------------------------------

// TestRunLifecycleTick_CleanShutdownNoFatal: when executePlan returns because
// ctx was cancelled, the tick must NOT call Fatalf — cancellation is a shutdown,
// never an op failure. The plan stage's work is real (a backend-only chunk that
// the cancelled ctx aborts), so executePlan genuinely returns an error here.
func TestRunLifecycleTick_CleanShutdownNoFatal(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 1)
	rec := &fatalRecorder{}

	// A READY live chunk 1 so chunk 0 sits BELOW the partition and counts as
	// complete (positional term => through = chunk 0's last ledger), making the
	// plan range [0,0] non-empty. Chunk 0 has no frozen artifacts, so resolve
	// schedules a ChunkBuild whose seamed execution we cancel mid-flight.
	readyHot(t, cat, 1)                        // live chunk (ready + dir)
	require.NoError(t, cat.PutHotTransient(0)) // chunk 0 in storage, below live

	// Block the chunk build long enough to cancel, then make it observe the cancel.
	started := make(chan struct{})
	cfg := LifecycleConfig{
		ExecConfig: ExecConfig{
			Catalog: cat,
			Logger:  silentLogger(),
			Workers: 1,
			runChunk: func(ctx context.Context, _ ChunkBuild, _ ExecConfig) error {
				close(started)
				<-ctx.Done() // wait for the cancel, then return the ctx error
				return ctx.Err()
			},
		},
		RetentionChunks: 0,
		Fatalf:          rec.fatalf,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runLifecycleTick(ctx, cfg, cat, 0) // lastChunk 0: plan range [0,0], the build we cancel
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the chunk build never started")
	}
	cancel() // shutdown mid-tick

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the tick did not return after ctx cancellation")
	}
	require.False(t, rec.fired(), "a cancelled ctx is a clean shutdown, NOT an op failure — no Fatalf")
}

// TestRunLifecycleTick_GenuineFailureAborts: when a plan op fails for a real
// reason (NOT ctx cancellation), the tick aborts via Fatalf per the error policy.
func TestRunLifecycleTick_GenuineFailureAborts(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 1)
	rec := &fatalRecorder{}

	readyHot(t, cat, 1)                        // ready live chunk => through = chunk 0 last ledger
	require.NoError(t, cat.PutHotTransient(0)) // chunk 0 below live, no frozen artifacts

	cfg := LifecycleConfig{
		ExecConfig: ExecConfig{
			Catalog: cat,
			Logger:  silentLogger(),
			Workers: 1,
			runChunk: func(context.Context, ChunkBuild, ExecConfig) error {
				return assertErr // a genuine, non-cancellation failure
			},
		},
		Fatalf: rec.fatalf,
	}
	runLifecycleTick(context.Background(), cfg, cat, 0) // lastChunk 0: plan range [0,0], the failing build
	require.True(t, rec.fired(), "a genuine op failure aborts the daemon")
}

// ---------------------------------------------------------------------------
// lifecycleLoop: selects on BOTH ctx.Done and the notification channel; drains
// to the most-recent queued chunk id.
// ---------------------------------------------------------------------------

// TestLifecycleLoop_RunsTickPerNotifyThenStopsOnCtx: a notification (a completed
// chunk id) runs a tick; a ctx cancellation returns the loop. The loop never
// blocks forever and never fatals on shutdown.
func TestLifecycleLoop_RunsTickPerNotifyThenStopsOnCtx(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 1)
	cfg, rec := lifecycleTestConfig(t, cat, 0)

	// Make the tick observable WITHOUT a slow full ingest: chunk 0 is already
	// fully frozen and folded into its (terminal, cpi=1) window, with a leftover
	// "ready" hot DB on disk. The plan stage is a no-op; the discard scan retires
	// chunk 0's hot DB. A live chunk 1 keeps chunk 0 below the partition.
	freezeKinds(t, cat, 0, KindLedgers, KindEvents, KindTxHash)
	freezeCoverage(t, cat, cat.windows.WindowID(0), 0, 0) // terminal coverage of chunk 0
	makeReadyHotDirNoData(t, cat, 0)
	live := openLiveHotDB(t, cat, 1)
	t.Cleanup(func() { _ = live.Close() })

	ch := make(chan chunk.ID, lifecycleQueueDepth)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		lifecycleLoop(ctx, cfg, cat, ch)
		close(done)
	}()

	ch <- chunk.ID(0) // ingestion hands over the just-completed chunk 0
	require.Eventually(t, func() bool {
		has, err := cat.Has(hotChunkKey(0))
		return err == nil && !has
	}, 10*time.Second, 20*time.Millisecond, "the notification ran a tick that discarded chunk 0")
	require.False(t, rec.fired())

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not return on ctx cancellation")
	}
}

// TestLifecycleLoop_DrainsToMostRecent: several chunk ids queued behind one
// notification are coalesced into ONE tick over the most-recent. With chunks 0
// and 1 both frozen+covered and a live chunk 2, sending 0 then 1 runs a single
// tick up to chunk 1 that discards both.
func TestLifecycleLoop_DrainsToMostRecent(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 1)
	cfg, rec := lifecycleTestConfig(t, cat, 0)

	for c := chunk.ID(0); c <= 1; c++ {
		freezeKinds(t, cat, c, KindLedgers, KindEvents, KindTxHash)
		freezeCoverage(t, cat, cat.windows.WindowID(c), c, c)
		makeReadyHotDirNoData(t, cat, c)
	}
	live := openLiveHotDB(t, cat, 2)
	t.Cleanup(func() { _ = live.Close() })

	ch := make(chan chunk.ID, lifecycleQueueDepth)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		lifecycleLoop(ctx, cfg, cat, ch)
		close(done)
	}()

	ch <- chunk.ID(0)
	ch <- chunk.ID(1) // drained-to: one tick over [floor, 1] discards both
	require.Eventually(t, func() bool {
		h0, e0 := cat.Has(hotChunkKey(0))
		h1, e1 := cat.Has(hotChunkKey(1))
		return e0 == nil && e1 == nil && !h0 && !h1
	}, 10*time.Second, 20*time.Millisecond, "one drained tick discarded both completed chunks")
	require.False(t, rec.fired())

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not return on ctx cancellation")
	}
}

// TestLifecycleLoop_ReturnsImmediatelyOnAlreadyCancelledCtx: an already-cancelled
// ctx makes the loop return without running any tick (never blocks on the
// channel forever).
func TestLifecycleLoop_ReturnsImmediatelyOnAlreadyCancelledCtx(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 1)
	cfg, _ := lifecycleTestConfig(t, cat, 0)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch := make(chan chunk.ID) // unbuffered, never sent to
	done := make(chan struct{})
	go func() {
		lifecycleLoop(ctx, cfg, cat, ch)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop blocked instead of observing the cancelled ctx")
	}
}

// ---------------------------------------------------------------------------
// helpers.
// ---------------------------------------------------------------------------

// runTickForCatalog runs one lifecycle tick the way ingestion would drive it:
// it derives the highest complete chunk from the catalog (the chunk id ingestion
// hands over at a boundary) and passes it as lastChunk. A negative result (young
// network, no complete chunk) is passed as chunk 0 — the resolve range guard
// then makes the plan empty, matching the design's young-network no-op.
func runTickForCatalog(ctx context.Context, t *testing.T, cfg LifecycleConfig, cat *Catalog) {
	t.Helper()
	through, err := deriveCompleteThrough(cat)
	require.NoError(t, err)
	last, ok := lastCompleteChunkAtID(through)
	if !ok {
		last = 0
	}
	runLifecycleTick(ctx, cfg, cat, last)
}

// assertErr is a fixed non-cancellation error for the genuine-failure path.
var assertErr = errStr("streaming: synthetic op failure")

type errStr string

func (e errStr) Error() string { return string(e) }

// makeReadyHotDirNoData opens and closes a real (empty) hot DB for c so its dir
// exists on disk and its key is "ready" — the state a discard scan inspects
// without needing a full ingest.
func makeReadyHotDirNoData(t *testing.T, cat *Catalog, c chunk.ID) {
	t.Helper()
	db, err := openHotTierForChunk(cat, c, silentLogger())
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

// assertQuiescent re-runs the tick's three derivations against the SAME through
// snapshot and asserts none schedule work — the quiescence postcondition.
func assertQuiescent(t *testing.T, cfg LifecycleConfig, cat *Catalog, through uint32) {
	t.Helper()
	earliest, _, err := cat.EarliestLedger()
	require.NoError(t, err)
	floor := effectiveRetentionFloor(through, cfg.RetentionChunks, earliest)
	start := chunkIDOfLedger(floor)
	low, hasLow, err := lowestMaterializedChunk(cat)
	require.NoError(t, err)
	if hasLow && int64(low) > start {
		start = int64(low)
	}
	if rangeEnd, ok := lastCompleteChunkAtID(through); ok && start >= 0 {
		plan, perr := resolve(cfg.ExecConfig, chunk.ID(start), rangeEnd)
		require.NoError(t, perr)
		assert.True(t, plan.Empty(), "re-resolve schedules no work at quiescence: %+v", plan)
	}
	dops, err := eligibleDiscardOps(cfg, cat, through)
	require.NoError(t, err)
	assert.Empty(t, dops, "re-scan finds no discard work at quiescence")
	pops, err := eligiblePruneOps(cfg, cat, through)
	require.NoError(t, err)
	assert.Empty(t, pops, "re-scan finds no prune work at quiescence")
}
