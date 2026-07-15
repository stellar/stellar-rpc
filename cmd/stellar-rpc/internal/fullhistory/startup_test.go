package fullhistory

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

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/fhtest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/registry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// ---------------------------------------------------------------------------
// Injected-boundary fakes.
// ---------------------------------------------------------------------------

// fakeTipBackend returns tips[i] per call (clamped to the last); if err is set it
// returns err for the first errFirst calls, then the tip (errFirst large ⇒ always down).
// Its Tip method makes it a backfill.Backend — the embedded LedgerStream stays nil
// since these tests never stream (the runBackfill seam records passes instead).
type fakeTipBackend struct {
	ledgerbackend.LedgerStream

	mu       sync.Mutex
	tips     []uint32
	calls    int
	err      error
	errFirst int // return err for the first errFirst calls, then the tip
}

var _ backfill.Backend = (*fakeTipBackend)(nil)

func (b *fakeTipBackend) Tip(context.Context) (uint32, error) {
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

// recordingPlan captures the [lo,hi] each backfill pass asked for via the
// runBackfill seam, so tests assert range arithmetic without cold I/O.
type recordingPlan struct {
	mu     sync.Mutex
	passes [][2]chunk.ID // {lo, hi} per pass
}

func (r *recordingPlan) record(lo, hi chunk.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.passes = append(r.passes, [2]chunk.ID{lo, hi})
}

func (r *recordingPlan) snapshot() [][2]chunk.ID {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][2]chunk.ID, len(r.passes))
	copy(out, r.passes)
	return out
}

// startTestConfig builds a StartConfig over a real catalog with faked boundaries.
// core may be nil for backfillToTip tests (which call backfillToTip directly and
// never reach validate or the ingestion path); run() tests pass a fakeCore. A
// non-nil recordPlan wires the runBackfill seam to record passes without cold I/O.
func startTestConfig(
	t *testing.T, cat *catalog.Catalog, tip *fakeTipBackend, core *fakeCore, recordPlan *recordingPlan,
) StartConfig {
	t.Helper()
	exec := backfill.ExecConfig{
		Catalog: cat,
		Logger:  silentLogger(),
		Workers: 2,
		Process: backfill.ProcessConfig{Backend: tip}, // the tip source every pass samples
	}
	cfg := StartConfig{
		Exec:       exec,
		Retention:  fhtest.RetentionFor(t, cat, 0),
		Core:       core,
		ServeReads: func(context.Context, *registry.Registry) error { return nil },
	}
	if recordPlan != nil {
		cfg.runBackfill = func(_ context.Context, _ backfill.ExecConfig, lo, hi chunk.ID) error {
			recordPlan.record(lo, hi)
			return nil
		}
	}
	return cfg
}

// fakeCore is a CoreOpener handing back a programmed LedgerStream. The loop opens
// the stream at its resume ledger via RawLedgers(UnboundedRange(resume)), so the
// resume the loop started from is the stream's recorded firstSeen (resumeSeen()).
type fakeCore struct {
	stream      *fakeCoreStream // programmed; nil → default block-on-ctx stream
	openErr     error
	openedCount atomic.Int32
}

func (c *fakeCore) OpenCore(context.Context) (ledgerbackend.LedgerStream, error) {
	c.openedCount.Add(1)
	if c.openErr != nil {
		return nil, c.openErr
	}
	if c.stream == nil {
		// Default: a live stream that blocks until ctx is canceled (the daemon's
		// steady state). Tests that need a finite stream set c.stream.
		c.stream = &fakeCoreStream{frames: map[uint32][]byte{}, blockOnCtx: true}
	}
	return c.stream, nil
}

// resumeSeen returns the resume ledger the loop opened the stream at (the range's
// From()), 0 before the loop has pulled.
func (c *fakeCore) resumeSeen() uint32 {
	if c.stream == nil {
		return 0
	}
	return c.stream.firstSeen.Load()
}

// pinGenesis pins earliest_ledger to genesis (as validateConfig does for a
// "genesis" floor) so the first-start predicate classifies correctly.
func pinGenesis(t *testing.T, cat *catalog.Catalog) {
	t.Helper()
	require.NoError(t, cat.PinEarliestLedger(chunk.FirstLedgerSeq))
}

// ---------------------------------------------------------------------------
// sampleTipWithRetry — backoff, sub-genesis rejection, exhausted retries, ctx cancel.
// ---------------------------------------------------------------------------

func TestSampleTip_RejectsSubGenesisAsNotReady(t *testing.T) {
	b := &fakeTipBackend{tips: []uint32{chunk.FirstLedgerSeq - 1}}
	tip, err := sampleTipWithRetry(context.Background(), b.Tip, time.Millisecond, 3)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not ready")
	require.Zero(t, tip)
	require.Equal(t, 1, b.callCount(), "sub-genesis is permanent — no retries burned")
}

func TestSampleTip_RetriesThenSucceeds(t *testing.T) {
	b := &fakeTipBackend{tips: []uint32{50_000}, err: errors.New("object store down"), errFirst: 2}
	tip, err := sampleTipWithRetry(context.Background(), b.Tip, time.Millisecond, 5)
	require.NoError(t, err)
	require.Equal(t, uint32(50_000), tip)
	require.Equal(t, 3, b.callCount(), "two failures then a success")
}

func TestSampleTip_ExhaustedRetriesErrors(t *testing.T) {
	b := &fakeTipBackend{err: errors.New("object store down"), errFirst: 99}
	_, err := sampleTipWithRetry(context.Background(), b.Tip, time.Millisecond, 4)
	require.Error(t, err)
	require.Contains(t, err.Error(), "after 4 attempts")
	require.Equal(t, 4, b.callCount())
}

func TestSampleTip_CtxCancelAbortsWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b := &fakeTipBackend{err: errors.New("down"), errFirst: 99}
	_, err := sampleTipWithRetry(ctx, b.Tip, time.Hour, 5)
	require.ErrorIs(t, err, context.Canceled)
}

// ---------------------------------------------------------------------------
// backfillToTip — backfill loop edge cases.
// ---------------------------------------------------------------------------

// First start (genesis, no local history) with no usable tip errors out
// (restartable — no sentinel; the supervisor retries). Sub-genesis reads as a
// permanent "not ready", so the sample fails fast instead of burning the
// production retry backoff backfillToTip now applies.
func TestBackfill_FirstStartTipAbsentErrors(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	tip := &fakeTipBackend{tips: []uint32{0}}
	cfg := startTestConfig(t, cat, tip, nil, &recordingPlan{})

	// Empty catalog ⇒ lastCommitted=1 < earliest=2 ⇒ first start with no progress.
	_, err := backfillToTip(context.Background(), cfg, preGenesisLedger)
	require.Error(t, err)
}

// First start (genesis) with the tip present computes range [chunk 0,
// lastCompleteChunkAt(tip)] and backfills it.
func TestBackfill_FirstStartTipPresentComputesRange(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	// Tip mid-chunk-3 ⇒ last complete chunk is 2.
	tipLedger := chunk.ID(3).FirstLedger() + 100
	rec := &recordingPlan{}
	tip := &fakeTipBackend{tips: []uint32{tipLedger}}
	cfg := startTestConfig(t, cat, tip, nil, rec)

	last, err := backfillToTip(context.Background(), cfg, preGenesisLedger)
	require.NoError(t, err)
	passes := rec.snapshot()
	require.Len(t, passes, 1, "the tip does not move, so exactly one backfill pass")
	assert.Equal(t, chunk.ID(0), passes[0][0], "rangeStart is chunk 0 (genesis floor)")
	assert.Equal(t, chunk.ID(2), passes[0][1], "rangeEnd is lastCompleteChunkAt(tip)")
	// lastCommitted advances to chunk 2's last ledger.
	assert.Equal(t, chunk.ID(2).LastLedger(), last)
}

// A young network (tip below the first complete chunk) is a no-op.
func TestBackfill_YoungNetworkNoOp(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	// Tip inside chunk 0 (no chunk has fully closed yet).
	tip := &fakeTipBackend{tips: []uint32{chunk.FirstLedgerSeq + 50}}
	rec := &recordingPlan{}
	cfg := startTestConfig(t, cat, tip, nil, rec)

	last, err := backfillToTip(context.Background(), cfg, preGenesisLedger)
	require.NoError(t, err)
	require.Empty(t, rec.snapshot(), "no backfill pass on a young network")
	assert.Equal(t, preGenesisLedger, last, "lastCommitted unchanged")
}

// Steady restart with a chunk-aligned lastCommitted and a tip one chunk past it: the
// loop converges in one pass and advances the lastCommitted monotonically.
func TestBackfill_SteadyRestartNoOp(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	lastCommitted := chunk.ID(2).LastLedger()
	tipLedger := chunk.ID(3).FirstLedger() + 10 // last complete chunk == 2
	rec := &recordingPlan{}
	tip := &fakeTipBackend{tips: []uint32{tipLedger}}
	cfg := startTestConfig(t, cat, tip, nil, rec)

	last, err := backfillToTip(context.Background(), cfg, lastCommitted)
	require.NoError(t, err)
	passes := rec.snapshot()
	require.Len(t, passes, 1)
	assert.Equal(t, chunk.ID(2), passes[0][1], "rangeEnd == lastCompleteChunkAt(tip) == 2")
	assert.Equal(t, lastCommitted, last, "lastCommitted does not regress and stays at chunk 2 end")
}

// Mid-chunk resume exclusion: a lastCommitted inside chunk 5 leaves the partial
// resume chunk to ingestion — rangeEnd folds back to chunkID(lastCommitted)-1=4. Tip
// is AT chunk 5's last ledger (complete-at-tip) so the exclusion is detectable:
// without it lastCompleteChunkAt(max(tip,lastCommitted))=5 and the live chunk would be backfilled.
func TestBackfill_MidChunkResumeExclusion(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	lastCommitted := chunk.ID(5).FirstLedger() + 100
	tipLedger := chunk.ID(5).LastLedger() // within one chunk, chunk 5 complete-at-tip
	rec := &recordingPlan{}
	tip := &fakeTipBackend{tips: []uint32{tipLedger}}
	cfg := startTestConfig(t, cat, tip, nil, rec)

	last, err := backfillToTip(context.Background(), cfg, lastCommitted)
	require.NoError(t, err)
	passes := rec.snapshot()
	require.Len(t, passes, 1)
	assert.Equal(t, chunk.ID(4), passes[0][1],
		"rangeEnd pulled back to chunkID(lastCommitted)-1 = chunk 4; chunk 5 is ingestion's")
	// Chunk 5 not backfilled — this is what makes deleting the exclusion detectable.
	assert.Less(t, passes[0][1], chunk.ID(5), "the live resume chunk 5 is never backfilled")
	assert.Less(t, passes[0][0], chunk.ID(5))
	// The excluded chunk stays the resume point ⇒ lastCommitted unchanged.
	assert.Equal(t, lastCommitted, last)
}

// TestBackfill_FloorAnchorsOnTargetNotTip is the finding's scenario: a mid-chunk
// last-committed within one chunk of a tip one chunk ahead, with finite retention.
// The floor must anchor on the post-exclusion target (the last complete chunk at
// last-committed = chunk 99), not on the tip's chunk 100 — otherwise the pass
// builds [96,99] and the seed tick immediately rebuilds [95,99], a transient
// coverage gap at chunk 95.
func TestBackfill_FloorAnchorsOnTargetNotTip(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	lastCommitted := chunk.ID(100).FirstLedger() + 100 // mid chunk 100
	tipLedger := chunk.ID(101).FirstLedger() + 50      // chunk 101, within one chunk of last-committed
	rec := &recordingPlan{}
	tip := &fakeTipBackend{tips: []uint32{tipLedger}}
	cfg := startTestConfig(t, cat, tip, nil, rec)
	cfg.Retention = fhtest.RetentionFor(t, cat, 5) // retain 5 chunks

	last, err := backfillToTip(context.Background(), cfg, lastCommitted)
	require.NoError(t, err)
	passes := rec.snapshot()
	require.Len(t, passes, 1)
	assert.Equal(t, chunk.ID(95), passes[0][0],
		"floor anchors on the post-exclusion target (chunk 99 ⇒ 95), NOT the tip (chunk 100 ⇒ 96)")
	assert.Equal(t, chunk.ID(99), passes[0][1], "rangeEnd is the last complete chunk at last-committed")
	assert.Equal(t, lastCommitted, last, "the mid-chunk resume point stays last-committed")
}

// Long-downtime re-pass: the tip advances between passes, so the loop re-passes
// to extend the range, then terminates when the tip stops.
func TestBackfill_LongDowntimeRePass(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	// Tip jumps chunk 2 → chunk 5 between samples; third (clamped) sample == second
	// ⇒ rangeEnd unchanged ⇒ break.
	tip := &fakeTipBackend{tips: []uint32{
		chunk.ID(3).FirstLedger() + 1, // last complete 2
		chunk.ID(6).FirstLedger() + 1, // last complete 5
	}}
	rec := &recordingPlan{}
	cfg := startTestConfig(t, cat, tip, nil, rec)

	last, err := backfillToTip(context.Background(), cfg, preGenesisLedger)
	require.NoError(t, err)

	var maxHi chunk.ID
	for _, p := range rec.snapshot() {
		maxHi = max(maxHi, p[1])
	}
	assert.Equal(t, chunk.ID(5), maxHi, "the re-pass extended the range to the advanced tip")
	assert.Equal(t, chunk.ID(5).LastLedger(), last)
	assert.GreaterOrEqual(t, tip.callCount(), 3, "the loop re-sampled the tip across passes")
}

// Restart with no usable tip errors out even when local progress exists: the
// synthetic tip:=lastCommitted degraded mode is gone. No backfill pass runs; the
// supervisor restarts and re-samples. Sub-genesis reads as a permanent "not
// ready" — one poll, no retry sleeps — and resolves to the same "tip
// unavailable" outcome as an erroring tip (whose retry exhaustion
// TestSampleTip_ExhaustedRetriesErrors covers at a fast interval).
func TestBackfill_RestartTipUnavailableErrors(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	lastCommitted := chunk.ID(2).LastLedger() // local progress exists
	tip := &fakeTipBackend{tips: []uint32{0}}
	rec := &recordingPlan{}
	cfg := startTestConfig(t, cat, tip, nil, rec)

	_, err := backfillToTip(context.Background(), cfg, lastCommitted)
	require.Error(t, err, "an unavailable tip errors out — no degraded mode")
	require.Empty(t, rec.snapshot(), "no backfill pass runs when the tip is unavailable")
}

// Lagging bulk tip below a chunk-aligned lastCommitted: max(tip,
// lastCommitted)==lastCommitted, so rangeEnd==lastCompleteChunkAt(lastCommitted)==5, not
// ==2 (which would regress below where pruning advanced). Mid-chunk exclusion
// does NOT fire — the lastCommitted is on a boundary.
func TestBackfill_LaggingBulkTipCoversLastCommittedChunk(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	lastCommitted := chunk.ID(5).LastLedger()   // chunk-aligned, complete lastCommitted chunk 5
	tipLedger := chunk.ID(3).FirstLedger() + 10 // lagging bulk tip in chunk 3 (last complete 2)
	rec := &recordingPlan{}
	tip := &fakeTipBackend{tips: []uint32{tipLedger}}
	cfg := startTestConfig(t, cat, tip, nil, rec)

	last, err := backfillToTip(context.Background(), cfg, lastCommitted)
	require.NoError(t, err)
	passes := rec.snapshot()
	require.Len(t, passes, 1, "one pass over the lastCommitted's chunk, then backfilledThrough==5 breaks")
	assert.Equal(t, chunk.ID(5), passes[0][1],
		"rangeEnd == lastCompleteChunkAt(lastCommitted) == 5, NOT lastCompleteChunkAt(tip) == 2")
	assert.Equal(t, chunk.ID(0), passes[0][0], "rangeStart is chunk 0 (genesis floor)")
	assert.Equal(t, lastCommitted, last, "lastCommitted does not regress below where pruning advanced")
}

// ---------------------------------------------------------------------------
// run — the backfill + serve + ingest flow.
// ---------------------------------------------------------------------------

// A young-network first start does no backfill, opens the resume hot DB, starts
// the (blocking) fake core, serves reads, and runs the ingestion loop — which
// surfaces the ctx-canceled stream error on a clean shutdown (the daemon top
// level classifies it as clean). The resume ledger is genesis (last committed ledger + 1).
func TestRun_FirstStartServeIngestCleanShutdown(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)

	served := atomic.Int32{}
	core := &fakeCore{stream: &fakeCoreStream{frames: map[uint32][]byte{}, blockOnCtx: true}}
	tip := &fakeTipBackend{tips: []uint32{chunk.FirstLedgerSeq + 10}} // young: no backfill
	cfg := startTestConfig(t, cat, tip, core, nil)
	cfg.ServeReads = func(context.Context, *registry.Registry) error { served.Add(1); return nil }

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- run(ctx, cfg) }()

	// Wait until the loop has opened the hot DB, started core, served, and parked on
	// the blocking stream, then request a clean shutdown.
	require.Eventually(t, func() bool { return served.Load() == 1 }, 2*time.Second, 5*time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled, "clean shutdown surfaces the ctx-canceled error")
	case <-time.After(3 * time.Second):
		t.Fatal("run did not return after ctx cancel")
	}

	require.Equal(t, int32(1), served.Load(), "reads were served exactly once")
	require.Equal(t, int32(1), core.openedCount.Load(), "captive core started once")
	require.Equal(t, uint32(chunk.FirstLedgerSeq), core.resumeSeen(),
		"resume ledger is genesis on a fresh start (last committed ledger + 1)")

	// The resume chunk's hot key is "ready" (opened, boundary never crossed).
	state, err := cat.HotState(chunk.IDFromLedger(chunk.FirstLedgerSeq))
	require.NoError(t, err)
	assert.Equal(t, geometry.HotReady, state)
}

// TestRun_IngestionCleanEndSurfacesErrorNotHang: if the ingestion stream ends
// gracefully (exhausts with no error and no shutdown), run() must surface a non-nil
// error and RETURN — g.Wait must never hang on a silent nil. The loop converts the
// graceful end to an error, and run()'s errgroup guard additionally degrades a nil
// ingestion return to an error, so a graceful end can never read as a clean shutdown.
func TestRun_IngestionCleanEndSurfacesErrorNotHang(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)

	first := uint32(chunk.FirstLedgerSeq) // fresh start resumes at genesis == chunk 0's first ledger
	stream := streamForSeqs(t, first, first+2)
	stream.endClean = true // exhaust cleanly (no error, no ctx cancel)
	core := &fakeCore{stream: stream}
	tip := &fakeTipBackend{tips: []uint32{chunk.FirstLedgerSeq + 10}} // young: no backfill
	cfg := startTestConfig(t, cat, tip, core, nil)

	errCh := make(chan error, 1)
	go func() { errCh <- run(context.Background(), cfg) }()

	select {
	case err := <-errCh:
		require.Error(t, err, "a graceful stream end surfaces as an error, not a nil clean shutdown")
		require.NotErrorIs(t, err, context.Canceled, "a graceful end is restartable, not a clean shutdown")
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return on a graceful stream end — g.Wait hung on a silent nil")
	}
}

// A ServeReads error is surfaced wrapped as a restartable failure (NOT clean).
// run() opens the resume hot DB and starts core BEFORE serving; a serve error
// after those returns via run()'s defer, which closes the DB (the loop never took
// ownership), so a restart can reopen it — asserted by the reopen below.
func TestRun_ServeReadsErrorSurfaces(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	core := &fakeCore{stream: &fakeCoreStream{frames: map[uint32][]byte{}, blockOnCtx: true}}
	tip := &fakeTipBackend{tips: []uint32{chunk.FirstLedgerSeq + 10}}
	cfg := startTestConfig(t, cat, tip, core, nil)
	cfg.ServeReads = func(context.Context, *registry.Registry) error { return errors.New("rpc bind failed") }

	err := run(context.Background(), cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "serve reads")
	require.NotErrorIs(t, err, context.Canceled, "a ServeReads error is restartable, not a clean shutdown")
	require.Equal(t, int32(1), core.openedCount.Load(), "core was started before serving")

	// run() opened the resume hot DB before serving and closed it on the error path
	// (the loop never took ownership): reopening it succeeds (LOCK released).
	db, err := openHotDBForChunk(cat, chunk.IDFromLedger(chunk.FirstLedgerSeq), silentLogger())
	require.NoError(t, err, "the resume hot DB is reopenable — run released its LOCK")
	require.NoError(t, db.Close())
}

// The resume hot DB and core are opened BEFORE reads are served (the design's
// fail-fast order): by the time ServeReads runs, the resume chunk's hot key is
// already "ready" and core has started — so a broken hot tier / core fails startup
// instead of serving behind a crash-looping loop. Asserted from inside ServeReads,
// which then errors to avoid entering the blocking loop.
func TestRun_OpensHotDBAndCoreBeforeServe(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	resumeChunk := chunk.IDFromLedger(chunk.FirstLedgerSeq) // fresh start ⇒ resume at genesis
	core := &fakeCore{stream: &fakeCoreStream{frames: map[uint32][]byte{}, blockOnCtx: true}}
	tip := &fakeTipBackend{tips: []uint32{chunk.FirstLedgerSeq + 10}} // young ⇒ no backfill
	cfg := startTestConfig(t, cat, tip, core, nil)

	var stateAtServe geometry.HotState
	var coreAtServe int32
	cfg.ServeReads = func(context.Context, *registry.Registry) error {
		st, herr := cat.HotState(resumeChunk)
		require.NoError(t, herr)
		stateAtServe = st
		coreAtServe = core.openedCount.Load()
		return errors.New("stop before the blocking loop")
	}

	err := run(context.Background(), cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "serve reads")
	assert.Equal(t, geometry.HotReady, stateAtServe, "resume hot DB is open+ready before serve")
	assert.Equal(t, int32(1), coreAtServe, "core is opened before serve")
}

// run errors on a first start with an unavailable tip (restartable, no sentinel);
// reads are never served and ingestion never starts. Sub-genesis ⇒ one
// permanent-failure poll, no retry sleeps.
func TestRun_FirstStartNoTipErrors(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	served := atomic.Int32{}
	core := &fakeCore{}
	tip := &fakeTipBackend{tips: []uint32{0}}
	cfg := startTestConfig(t, cat, tip, core, nil)
	cfg.ServeReads = func(context.Context, *registry.Registry) error { served.Add(1); return nil }

	err := run(context.Background(), cfg)
	require.Error(t, err)
	require.Zero(t, served.Load(), "reads are never served when backfill errors")
	require.Zero(t, core.openedCount.Load(), "core never starts when backfill errors")
}

// run surfaces a missing earliest_ledger pin loudly (a wiring error, not a first
// start to mis-classify).
func TestRun_RequiresEarliestPin(t *testing.T) {
	cat, _ := testCatalog(t)
	// No pinGenesis.
	cfg := startTestConfig(t, cat, &fakeTipBackend{tips: []uint32{50_000}}, &fakeCore{}, nil)
	err := run(context.Background(), cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "earliest_ledger pinned")
}

// run validates its injected boundaries.
func TestRun_ValidatesConfig(t *testing.T) {
	cat, _ := testCatalog(t)
	base := startTestConfig(t, cat, &fakeTipBackend{tips: []uint32{50_000}}, &fakeCore{}, nil)

	t.Run("nil Backend", func(t *testing.T) {
		cfg := base
		cfg.Exec.Process.Backend = nil // the backfill tip + freeze source
		require.Error(t, run(context.Background(), cfg))
	})
	t.Run("nil Core", func(t *testing.T) {
		cfg := base
		cfg.Core = nil
		require.Error(t, run(context.Background(), cfg))
	})
	t.Run("nil ServeReads", func(t *testing.T) {
		cfg := base
		cfg.ServeReads = nil
		require.Error(t, run(context.Background(), cfg))
	})
}

// ---------------------------------------------------------------------------
// Pure helpers: withinOneChunkOfTip, lastCommittedMidChunk.
// ---------------------------------------------------------------------------

func TestLastCommittedMidChunk(t *testing.T) {
	tests := []struct {
		name          string
		lastCommitted uint32
		mid           bool
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
			assert.Equal(t, tt.mid, lastCommittedMidChunk(tt.lastCommitted))
		})
	}
}

func TestWithinOneChunkOfTip(t *testing.T) {
	tests := []struct {
		name               string
		tip, lastCommitted uint32
		within             bool
	}{
		{"tip equals lastCommitted", 100_000, 100_000, true},
		{"tip one less than a chunk ahead", 100_000 + chunk.LedgersPerChunk - 1, 100_000, true},
		{"tip exactly a chunk ahead", 100_000 + chunk.LedgersPerChunk, 100_000, false},
		{"lagging tip below lastCommitted", 90_000, 100_000, true}, // signed: negative < L
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.within, withinOneChunkOfTip(tt.tip, tt.lastCommitted))
		})
	}
}

// retentionFloorLedger emits the floor chunk's first ledger, or the earliest
// chunk's on a young store (no complete chunk yet).
func TestRetentionFloorLedger(t *testing.T) {
	ret := geometry.NewRetention(5, chunk.ID(3)) // retain 5 chunks, earliest chunk 3
	// A complete chunk 100 ⇒ sliding floor 96 (above the earliest).
	assert.Equal(t, chunk.ID(96).FirstLedger(), retentionFloorLedger(ret, chunk.ID(100).LastLedger()))
	// Young store, nothing complete ⇒ the earliest chunk, not a sliding floor.
	assert.Equal(t, chunk.ID(3).FirstLedger(), retentionFloorLedger(ret, preGenesisLedger))
}

// ---------------------------------------------------------------------------
// backfill observability.
// ---------------------------------------------------------------------------

// A multi-chunk backfill reports a BackfillPass and refreshes the last-committed
// (+ retention-floor) gauge.
func TestBackfill_ReportsPassAndProgress(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)

	rp := &recordingPlan{}
	tipLedger := chunk.ID(3).LastLedger() + 5
	tip := &fakeTipBackend{tips: []uint32{tipLedger}}
	start := startTestConfig(t, cat, tip, nil, rp)
	metrics := newRecordingMetrics()
	start.Exec.Metrics = metrics

	got, err := backfillToTip(context.Background(), start, preGenesisLedger)
	require.NoError(t, err)

	assert.Positive(t, metrics.backfillPasses, "at least one backfill pass reported")
	assert.Positive(t, metrics.gaugesSet["last_committed"], "last-committed gauge refreshed during backfill")
	assert.Equal(t, chunk.ID(3).LastLedger(), got, "lastCommitted advanced to the backfilled range end")
}
