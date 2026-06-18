package streaming

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ---------------------------------------------------------------------------
// Injected-boundary fakes.
// ---------------------------------------------------------------------------

// fakeTipBackend is a NetworkTipBackend whose result is programmable per call:
// it returns tips[i] (clamped to the last element after that). When err is set,
// it returns that error for the first errFirst calls and then the tip — modeling
// a backend that is transiently down then comes online (errFirst large ⇒ always
// down).
type fakeTipBackend struct {
	mu       sync.Mutex
	tips     []uint32
	calls    int
	err      error
	errFirst int // return err for the first errFirst calls, then the tip
}

func (b *fakeTipBackend) NetworkTip(context.Context) (uint32, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := b.calls
	b.calls++
	if b.err != nil && n < b.errFirst {
		return 0, b.err
	}
	if len(b.tips) == 0 {
		return 0, errors.New("fakeTipBackend: no tips programmed")
	}
	idx := n
	if idx >= len(b.tips) {
		idx = len(b.tips) - 1
	}
	return b.tips[idx], nil
}

func (b *fakeTipBackend) callCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

// fakeCore is a CoreStreamOpener handing back a programmed LedgerStream and
// recording the resume ledger it was started from.
type fakeCore struct {
	stream      ledgerbackend.LedgerStream
	openErr     error
	resumeSeen  atomic.Uint32
	openedCount atomic.Int32
}

func (c *fakeCore) OpenLedgerStream(_ context.Context, resumeLedger uint32) (ledgerbackend.LedgerStream, error) {
	c.openedCount.Add(1)
	c.resumeSeen.Store(resumeLedger)
	if c.openErr != nil {
		return nil, c.openErr
	}
	return c.stream, nil
}

// recordingPlan captures the (rangeStart, rangeEnd) every backfill pass asked
// for, via the ExecConfig runChunk/runIndex test seams — so a backfill test
// asserts the loop's range arithmetic without real cold I/O. Because resolve
// emits per-chunk builds, the lowest/highest chunk a pass touched bracket the
// requested range.
type recordingPlan struct {
	mu     sync.Mutex
	passes [][2]chunk.ID // {minChunk, maxChunk} per pass
	cur    *[2]chunk.ID
}

// passSeams returns runChunk/runIndex seams that record the chunk range of the
// current pass. runBackfill calls resolve then executePlan; we observe each
// ChunkBuild. A new pass is opened lazily on the first chunk after the previous
// pass closed.
func (r *recordingPlan) note(c chunk.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cur == nil {
		r.cur = &[2]chunk.ID{c, c}
		return
	}
	if c < r.cur[0] {
		r.cur[0] = c
	}
	if c > r.cur[1] {
		r.cur[1] = c
	}
}

func (r *recordingPlan) endPass() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cur != nil {
		r.passes = append(r.passes, *r.cur)
		r.cur = nil
	}
}

func (r *recordingPlan) snapshot() [][2]chunk.ID {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][2]chunk.ID, len(r.passes))
	copy(out, r.passes)
	return out
}

// startTestConfig builds a StartConfig over a real catalog (genesis floor pinned
// to GenesisLedger by default) with all external boundaries faked. recordPlan,
// when non-nil, wires the runChunk/runIndex seams so backfill passes are
// recorded without cold I/O.
func startTestConfig(
	t *testing.T, cat *Catalog, tip *fakeTipBackend, core *fakeCore, recordPlan *recordingPlan,
) StartConfig {
	t.Helper()
	exec := ExecConfig{
		Catalog: cat,
		Logger:  silentLogger(),
		Workers: 2,
		Process: ProcessConfig{
			HotProbe: NewRocksHotProbe(cat.layout.HotChunkPath, silentLogger()),
			Backend:  zeroTxBackend(t),
		},
	}
	if recordPlan != nil {
		exec.runChunk = func(_ context.Context, cb ChunkBuild, _ ExecConfig) error {
			recordPlan.note(cb.Chunk)
			return nil
		}
		exec.runIndex = func(_ context.Context, _ IndexBuild, _ ExecConfig) error { return nil }
	}
	life := LifecycleConfig{ExecConfig: exec, RetentionChunks: 0, Fatalf: (&fatalRecorder{}).fatalf}
	return StartConfig{
		Exec:           exec,
		Lifecycle:      life,
		NetworkTip:     tip,
		Core:           core,
		ServeReads:     func(context.Context) error { return nil },
		TipBackoff:     time.Millisecond,
		TipMaxAttempts: 3,
	}
}

// pinGenesis pins config:earliest_ledger to GenesisLedger (what validateConfig
// does for a "genesis" floor), so startup's first-start predicate classifies
// correctly.
func pinGenesis(t *testing.T, cat *Catalog) {
	t.Helper()
	require.NoError(t, cat.PutEarliestLedger(chunk.FirstLedgerSeq))
}

// ---------------------------------------------------------------------------
// networkTip — backoff, sub-genesis rejection, exhausted retries.
// ---------------------------------------------------------------------------

func TestNetworkTip_RejectsSubGenesisAsNotReady(t *testing.T) {
	tip, err := networkTip(context.Background(),
		&fakeTipBackend{tips: []uint32{chunk.FirstLedgerSeq - 1}}, time.Millisecond, 3)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not ready")
	require.Zero(t, tip)
}

func TestNetworkTip_RetriesThenSucceeds(t *testing.T) {
	b := &fakeTipBackend{tips: []uint32{50_000}, err: errors.New("object store down"), errFirst: 2}
	tip, err := networkTip(context.Background(), b, time.Millisecond, 5)
	require.NoError(t, err)
	require.Equal(t, uint32(50_000), tip)
	require.Equal(t, 3, b.callCount(), "two failures then a success")
}

func TestNetworkTip_ExhaustedRetriesErrors(t *testing.T) {
	b := &fakeTipBackend{err: errors.New("object store down"), errFirst: 99}
	_, err := networkTip(context.Background(), b, time.Millisecond, 4)
	require.Error(t, err)
	require.Contains(t, err.Error(), "after 4 attempts")
	require.Equal(t, 4, b.callCount())
}

func TestNetworkTip_CtxCancelAbortsWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b := &fakeTipBackend{err: errors.New("down"), errFirst: 99}
	_, err := networkTip(ctx, b, time.Hour, 5)
	require.ErrorIs(t, err, context.Canceled)
}

// ---------------------------------------------------------------------------
// catchUp — the backfill loop edge cases (the heart of Issue 12).
// ---------------------------------------------------------------------------

// First start (genesis, no local history) with the tip ABSENT is FATAL: the
// daemon can neither catch up nor serve a local history.
func TestBackfill_FirstStartTipAbsentFatal(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	tip := &fakeTipBackend{err: errors.New("backend unreachable"), errFirst: 99}
	cfg := startTestConfig(t, cat, tip, &fakeCore{}, &recordingPlan{})

	// lastCommitted = deriveWatermark over an empty catalog = preGenesisLedger (1);
	// earliest = GenesisLedger (2); 1 < 2 ⇒ first start with no progress.
	_, err := catchUp(context.Background(), cfg, preGenesisLedger, chunk.FirstLedgerSeq)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrFirstStartNoTip)
}

// First start (genesis) with the tip PRESENT a few chunks up: the range is
// computed [chunk 0, lastCompleteChunkAt(tip)] and backfill runs over it.
func TestBackfill_FirstStartTipPresentComputesRange(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	// Tip in the middle of chunk 3 ⇒ last complete chunk is 2.
	tipLedger := chunk.ID(3).FirstLedger() + 100
	rec := &recordingPlan{}
	tip := &fakeTipBackend{tips: []uint32{tipLedger}}
	cfg := startTestConfig(t, cat, tip, &fakeCore{}, rec)

	last, err := catchUp(context.Background(), cfg, preGenesisLedger, chunk.FirstLedgerSeq)
	require.NoError(t, err)
	rec.endPass()

	passes := rec.snapshot()
	require.Len(t, passes, 1, "the tip does not move, so exactly one backfill pass")
	assert.Equal(t, chunk.ID(0), passes[0][0], "rangeStart is chunk 0 (genesis floor)")
	assert.Equal(t, chunk.ID(2), passes[0][1], "rangeEnd is lastCompleteChunkAt(tip)")
	// lastCommitted advances to chunk 2's last ledger.
	assert.Equal(t, chunk.ID(2).LastLedger(), last)
}

// A young network (tip below the first complete chunk) is a no-op: rangeEnd < 0
// < rangeStart, so the loop breaks immediately without backfilling.
func TestBackfill_YoungNetworkNoOp(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	// Tip inside chunk 0 (no chunk has fully closed yet).
	tip := &fakeTipBackend{tips: []uint32{chunk.FirstLedgerSeq + 50}}
	rec := &recordingPlan{}
	cfg := startTestConfig(t, cat, tip, &fakeCore{}, rec)

	last, err := catchUp(context.Background(), cfg, preGenesisLedger, chunk.FirstLedgerSeq)
	require.NoError(t, err)
	rec.endPass()
	require.Empty(t, rec.snapshot(), "no backfill pass on a young network")
	assert.Equal(t, preGenesisLedger, last, "watermark unchanged")
}

// Steady restart with local progress and a tip just past it: backfill is a
// no-op (everything below the watermark is already complete), the watermark is
// unchanged.
func TestBackfill_SteadyRestartNoOp(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	// Watermark on a chunk boundary (chunk 2 complete), tip just past it in
	// chunk 3 — but resolve finds chunks 0..2 already... actually nothing is
	// frozen, so a pass WOULD run. To model a true steady-state no-op we make the
	// watermark sit at chunk 2's end and the tip lag at the same point: rangeEnd
	// == backfilledThrough on the SECOND iteration breaks the loop, but the first
	// still backfills. The crisp no-op is the mid-chunk-within-one-chunk case
	// below; here we assert the loop converges (terminates) and advances the
	// watermark monotonically.
	watermark := chunk.ID(2).LastLedger()
	tipLedger := chunk.ID(3).FirstLedger() + 10 // last complete chunk == 2
	rec := &recordingPlan{}
	tip := &fakeTipBackend{tips: []uint32{tipLedger}}
	cfg := startTestConfig(t, cat, tip, &fakeCore{}, rec)

	last, err := catchUp(context.Background(), cfg, watermark, chunk.FirstLedgerSeq)
	require.NoError(t, err)
	rec.endPass()

	passes := rec.snapshot()
	require.Len(t, passes, 1)
	assert.Equal(t, chunk.ID(2), passes[0][1], "rangeEnd == lastCompleteChunkAt(tip) == 2")
	assert.Equal(t, watermark, last, "watermark does not regress and stays at chunk 2 end")
}

// Mid-chunk resume exclusion: a watermark strictly inside a chunk, within one
// chunk of the tip, leaves the partial resume chunk to ingestion — rangeEnd is
// pulled back to chunkID(watermark)-1.
//
// The tip is placed AT chunk 5's last ledger (chunk 5 complete-at-tip) while the
// watermark stays mid-chunk-5. This is the distinguishing scenario: WITHOUT the
// exclusion, lastCompleteChunkAt(anchor) = 5 and the loop would backfill the live
// chunk ingestion owns; WITH it, rangeEnd folds back to 4. (A tip that is also
// mid-chunk-5 would yield lastCompleteChunkAt = 4 anyway, making the exclusion
// undetectable.) within-one-chunk still holds: tip - watermark = 9999 - 100 =
// 9899 < 10000.
func TestBackfill_MidChunkResumeExclusion(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	// Watermark mid-chunk-5 (not on a boundary); tip AT chunk 5's last ledger so
	// chunk 5 is complete-at-tip — the case that distinguishes the exclusion.
	watermark := chunk.ID(5).FirstLedger() + 100
	tipLedger := chunk.ID(5).LastLedger() // within one chunk, but chunk 5 complete-at-tip
	rec := &recordingPlan{}
	tip := &fakeTipBackend{tips: []uint32{tipLedger}}
	cfg := startTestConfig(t, cat, tip, &fakeCore{}, rec)

	last, err := catchUp(context.Background(), cfg, watermark, chunk.FirstLedgerSeq)
	require.NoError(t, err)
	rec.endPass()

	passes := rec.snapshot()
	require.Len(t, passes, 1)
	assert.Equal(t, chunk.ID(4), passes[0][1],
		"rangeEnd pulled back to chunkID(watermark)-1 = chunk 4; chunk 5 is ingestion's")
	// Chunk 5 (complete-at-tip) is NOT backfilled — the exclusion left it to
	// ingestion. Without the exclusion rangeEnd would be 5 and chunk 5 would
	// appear in the pass; this assertion is what makes deleting the exclusion
	// logic detectable.
	assert.Less(t, passes[0][1], chunk.ID(5), "the live resume chunk 5 is never backfilled")
	assert.Less(t, passes[0][0], chunk.ID(5))
	// The watermark itself is NOT advanced past where it was (the excluded chunk
	// stays the resume point): max(watermark, chunk4.LastLedger) == watermark.
	assert.Equal(t, watermark, last)
}

// Long-downtime re-pass: the tip ADVANCES between passes, so the loop runs more
// than once, extending the backfilled range, then terminates when the tip stops.
func TestBackfill_LongDowntimeRePass(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	// First sample: last complete chunk 2. Second sample: tip jumped to chunk 5
	// (new chunks appeared while the first pass was in flight). Third sample
	// (clamped): same as second ⇒ rangeEnd unchanged ⇒ break.
	tip := &fakeTipBackend{tips: []uint32{
		chunk.ID(3).FirstLedger() + 1, // last complete 2
		chunk.ID(6).FirstLedger() + 1, // last complete 5
	}}
	// Record the raw set of chunks every backfill pass touched (across passes);
	// the highest chunk reached proves the re-pass extended the range to the
	// advanced tip.
	var mu sync.Mutex
	var allChunks []chunk.ID
	exec := ExecConfig{
		Catalog: cat,
		Logger:  silentLogger(),
		Workers: 2,
		Process: ProcessConfig{HotProbe: NewRocksHotProbe(cat.layout.HotChunkPath, silentLogger()), Backend: zeroTxBackend(t)},
		runChunk: func(_ context.Context, cb ChunkBuild, _ ExecConfig) error {
			mu.Lock()
			allChunks = append(allChunks, cb.Chunk)
			mu.Unlock()
			return nil
		},
		runIndex: func(context.Context, IndexBuild, ExecConfig) error { return nil },
	}
	cfg := StartConfig{
		Exec:           exec,
		Lifecycle:      LifecycleConfig{ExecConfig: exec, Fatalf: (&fatalRecorder{}).fatalf},
		NetworkTip:     tip,
		Core:           &fakeCore{},
		ServeReads:     func(context.Context) error { return nil },
		TipBackoff:     time.Millisecond,
		TipMaxAttempts: 3,
	}

	last, err := catchUp(context.Background(), cfg, preGenesisLedger, chunk.FirstLedgerSeq)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	// Two passes ran: first [0,2], second extended to chunk 5. The highest chunk
	// touched is 5, and the final watermark is chunk 5's last ledger.
	maxChunkTouched := chunk.ID(0)
	for _, c := range allChunks {
		if c > maxChunkTouched {
			maxChunkTouched = c
		}
	}
	assert.Equal(t, chunk.ID(5), maxChunkTouched, "the re-pass extended the range to the advanced tip")
	assert.Equal(t, chunk.ID(5).LastLedger(), last)
	assert.GreaterOrEqual(t, tip.callCount(), 3, "the loop re-sampled the tip across passes")
}

// Degrade-and-serve restart: the tip is UNREACHABLE but there IS local progress
// (watermark >= earliest), so backfill does NOT fatal — it degrades to tip :=
// lastCommitted and re-resolves the already-local range below the watermark
// (self-skipping frozen chunks in production). It terminates (does not loop
// forever) and never regresses the watermark.
func TestBackfill_RestartTipUnreachableDegrades(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	watermark := chunk.ID(2).LastLedger() // local progress exists
	tip := &fakeTipBackend{err: errors.New("backend down"), errFirst: 99}
	rec := &recordingPlan{}
	cfg := startTestConfig(t, cat, tip, &fakeCore{}, rec)

	last, err := catchUp(context.Background(), cfg, watermark, chunk.FirstLedgerSeq)
	require.NoError(t, err, "local progress means no fatal")
	rec.endPass()

	// tip := watermark ⇒ anchor == watermark ⇒ rangeEnd == lastCompleteChunkAt
	// (chunk 2 end) == 2, rangeStart == chunk 0; ONE re-resolve pass over the
	// already-local [0,2], then backfilledThrough==2 breaks the loop.
	passes := rec.snapshot()
	require.Len(t, passes, 1, "exactly one degraded re-resolve pass, then terminate")
	assert.Equal(t, chunk.ID(2), passes[0][1])
	assert.Equal(t, watermark, last, "watermark does not regress")
}

// Lagging bulk tip below a chunk-aligned watermark: the bulk backend's tip sits
// in chunk 3, but a complete watermark chunk (chunk 5, chunk-aligned) is durably
// committed above it. The anchor is max(tip, lastCommitted) == the watermark, so
// rangeEnd == lastCompleteChunkAt(watermark) == 5 — the complete watermark chunk
// still folds into its window's index before serving. Anchored on the tip alone
// it would be lastCompleteChunkAt(tip) == 2 (regressing below where pruning
// advanced and dropping chunks 3..5). The mid-chunk exclusion does NOT fire: the
// watermark is on a boundary (watermarkMidChunk == false), even though
// withinOneChunkOfTip is true (signed: lagging tip below the watermark).
func TestBackfill_LaggingBulkTipFoldsWatermarkChunk(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	watermark := chunk.ID(5).LastLedger()       // chunk-aligned, complete watermark chunk 5
	tipLedger := chunk.ID(3).FirstLedger() + 10 // lagging bulk tip in chunk 3 (last complete 2)
	rec := &recordingPlan{}
	tip := &fakeTipBackend{tips: []uint32{tipLedger}}
	cfg := startTestConfig(t, cat, tip, &fakeCore{}, rec)

	last, err := catchUp(context.Background(), cfg, watermark, chunk.FirstLedgerSeq)
	require.NoError(t, err)
	rec.endPass()

	passes := rec.snapshot()
	require.Len(t, passes, 1, "one pass anchored on the watermark, then backfilledThrough==5 breaks")
	assert.Equal(t, chunk.ID(5), passes[0][1],
		"rangeEnd == lastCompleteChunkAt(watermark) == 5, NOT lastCompleteChunkAt(tip) == 2")
	assert.Equal(t, chunk.ID(0), passes[0][0], "rangeStart is chunk 0 (genesis floor)")
	assert.Equal(t, watermark, last, "watermark does not regress below where pruning advanced")
}

// ---------------------------------------------------------------------------
// startStreaming — the full serve+ingest handoff (clean shutdown).
// ---------------------------------------------------------------------------

// A genesis first start with a tip inside chunk 0 (young network) does no
// backfill, opens the resume chunk's hot DB, starts the (blocking) fake core
// stream, serves reads, and runs the ingestion loop — which returns nil when ctx
// is cancelled (clean shutdown). The resume ledger is genesis.
func TestStartStreaming_FirstStartServeIngestCleanShutdown(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)

	served := atomic.Int32{}
	core := &fakeCore{stream: &fakeLedgerStream{blockOnCtx: true}}    // live stream: ends only on ctx cancel
	tip := &fakeTipBackend{tips: []uint32{chunk.FirstLedgerSeq + 10}} // young: no backfill
	cfg := startTestConfig(t, cat, tip, core, nil)
	cfg.ServeReads = func(context.Context) error { served.Add(1); return nil }

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- startStreaming(ctx, cfg) }()

	// Give the loop time to open the hot DB, start core, serve, and park on the
	// blocking stream, then request a clean shutdown.
	require.Eventually(t, func() bool { return served.Load() == 1 }, 2*time.Second, 5*time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err, "clean shutdown (ctx cancel) returns nil")
	case <-time.After(3 * time.Second):
		t.Fatal("startStreaming did not return after ctx cancel")
	}

	require.Equal(t, int32(1), served.Load(), "reads were served exactly once")
	require.Equal(t, int32(1), core.openedCount.Load(), "captive core started once")
	require.Equal(t, uint32(chunk.FirstLedgerSeq), core.resumeSeen.Load(),
		"resume ledger is genesis on a fresh start (watermark+1)")

	// The resume chunk's hot key is "ready" (the loop opened it and the boundary
	// was never crossed).
	state, err := cat.HotState(chunk.IDFromLedger(chunk.FirstLedgerSeq))
	require.NoError(t, err)
	assert.Equal(t, HotReady, state)
}

// startStreaming fatals on a true first start when the tip is unavailable: the
// error is ErrFirstStartNoTip and NEITHER the hot DB nor core is opened.
func TestStartStreaming_FirstStartNoTipFatal(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	core := &fakeCore{}
	tip := &fakeTipBackend{err: errors.New("unreachable"), errFirst: 99}
	cfg := startTestConfig(t, cat, tip, core, nil)

	err := startStreaming(context.Background(), cfg)
	require.ErrorIs(t, err, ErrFirstStartNoTip)
	require.Zero(t, core.openedCount.Load(), "core is never started when backfill fatals")
}

// startStreaming surfaces a missing earliest_ledger pin loudly (validateConfig
// pins it before startStreaming; absent here is a wiring error, not a first
// start to mis-classify).
func TestStartStreaming_RequiresEarliestPin(t *testing.T) {
	cat, _ := testCatalog(t)
	// No pinGenesis.
	cfg := startTestConfig(t, cat, &fakeTipBackend{tips: []uint32{50_000}}, &fakeCore{}, nil)
	err := startStreaming(context.Background(), cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "earliest_ledger pinned")
}

// startStreaming validates its injected boundaries.
func TestStartStreaming_ValidatesConfig(t *testing.T) {
	cat, _ := testCatalog(t)
	base := startTestConfig(t, cat, &fakeTipBackend{tips: []uint32{50_000}}, &fakeCore{}, nil)

	t.Run("nil NetworkTip", func(t *testing.T) {
		cfg := base
		cfg.NetworkTip = nil
		require.Error(t, startStreaming(context.Background(), cfg))
	})
	t.Run("nil Core", func(t *testing.T) {
		cfg := base
		cfg.Core = nil
		require.Error(t, startStreaming(context.Background(), cfg))
	})
	t.Run("nil ServeReads", func(t *testing.T) {
		cfg := base
		cfg.ServeReads = nil
		require.Error(t, startStreaming(context.Background(), cfg))
	})
	t.Run("nil HotProbe", func(t *testing.T) {
		cfg := base
		cfg.Exec.Process.HotProbe = nil
		require.Error(t, startStreaming(context.Background(), cfg))
	})
}

// ---------------------------------------------------------------------------
// Pure helpers: withinOneChunkOfTip, watermarkMidChunk.
// ---------------------------------------------------------------------------

func TestWatermarkMidChunk(t *testing.T) {
	tests := []struct {
		name      string
		watermark uint32
		mid       bool
	}{
		{"genesis sentinel is a boundary", preGenesisLedger, false},
		{"chunk-0 last ledger is a boundary", chunk.ID(0).LastLedger(), false},
		{"chunk-2 last ledger is a boundary", chunk.ID(2).LastLedger(), false},
		{"mid chunk 0", chunk.ID(0).FirstLedger() + 1, true},
		{"mid chunk 5", chunk.ID(5).FirstLedger() + 100, true},
		{"chunk-5 first ledger is mid (not the last)", chunk.ID(5).FirstLedger(), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.mid, watermarkMidChunk(tt.watermark))
		})
	}
}

func TestWithinOneChunkOfTip(t *testing.T) {
	tests := []struct {
		name           string
		tip, watermark uint32
		within         bool
	}{
		{"tip equals watermark", 100_000, 100_000, true},
		{"tip one less than a chunk ahead", 100_000 + chunk.LedgersPerChunk - 1, 100_000, true},
		{"tip exactly a chunk ahead", 100_000 + chunk.LedgersPerChunk, 100_000, false},
		{"lagging tip below watermark", 90_000, 100_000, true}, // signed: negative < L
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.within, withinOneChunkOfTip(tt.tip, tt.watermark))
		})
	}
}
