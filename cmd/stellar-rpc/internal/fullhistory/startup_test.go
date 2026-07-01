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

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/lifecycle"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ---------------------------------------------------------------------------
// Injected-boundary fakes.
// ---------------------------------------------------------------------------

// fakeTipBackend returns tips[i] per call (clamped to the last); if err is set it
// returns err for the first errFirst calls, then the tip (errFirst large ⇒ always down).
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
		Process: backfill.ProcessConfig{
			HotProbe: NewRocksHotProbe(cat.Layout().HotChunkPath, silentLogger()),
		},
	}
	cfg := StartConfig{
		Exec: exec,
		Lifecycle: lifecycle.Config{
			ExecConfig:      exec,
			RetentionChunks: 0,
			// A tick op failure should fail the test loudly, not kill the process; the
			// loop goroutine is joined before run() returns, so t.Errorf is safe here.
			Fatalf: func(format string, args ...any) { t.Errorf("unexpected lifecycle fatal: "+format, args...) },
		},
		NetworkTip:     tip,
		Core:           core,
		ServeReads:     func(context.Context) error { return nil },
		TipBackoff:     time.Millisecond,
		TipMaxAttempts: 3,
	}
	if recordPlan != nil {
		cfg.runBackfill = func(_ context.Context, _ backfill.ExecConfig, lo, hi chunk.ID) error {
			recordPlan.record(lo, hi)
			return nil
		}
	}
	return cfg
}

// fakeCore is a CoreOpener handing back a programmed LedgerGetter and recording
// the resume ledger it was started from.
type fakeCore struct {
	getter      LedgerGetter
	openErr     error
	resumeSeen  atomic.Uint32
	openedCount atomic.Int32
}

func (c *fakeCore) OpenCore(_ context.Context, resumeLedger uint32) (LedgerGetter, func() error, error) {
	c.openedCount.Add(1)
	c.resumeSeen.Store(resumeLedger)
	if c.openErr != nil {
		return nil, nil, c.openErr
	}
	getter := c.getter
	if getter == nil {
		// Default: a live getter that blocks until ctx is canceled (the daemon's
		// steady state). Tests that need a finite poll set c.getter.
		getter = &fakeLedgerGetter{frames: map[uint32][]byte{}, blockOnCtx: true}
	}
	return getter, func() error { return nil }, nil
}

// pinGenesis pins earliest_ledger to genesis (as validateConfig does for a
// "genesis" floor) so the first-start predicate classifies correctly.
func pinGenesis(t *testing.T, cat *catalog.Catalog) {
	t.Helper()
	require.NoError(t, cat.PinEarliestLedger(chunk.FirstLedgerSeq))
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
// backfillToTip — backfill loop edge cases.
// ---------------------------------------------------------------------------

// First start (genesis, no local history) with the tip absent is fatal.
func TestBackfill_FirstStartTipAbsentFatal(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	tip := &fakeTipBackend{err: errors.New("backend unreachable"), errFirst: 99}
	cfg := startTestConfig(t, cat, tip, nil, &recordingPlan{})

	// Empty catalog ⇒ lastCommitted=1 < earliest=2 ⇒ first start with no progress.
	_, err := backfillToTip(context.Background(), cfg, preGenesisLedger, chunk.FirstLedgerSeq)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrFirstStartNoTip)
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

	last, err := backfillToTip(context.Background(), cfg, preGenesisLedger, chunk.FirstLedgerSeq)
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

	last, err := backfillToTip(context.Background(), cfg, preGenesisLedger, chunk.FirstLedgerSeq)
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

	last, err := backfillToTip(context.Background(), cfg, lastCommitted, chunk.FirstLedgerSeq)
	require.NoError(t, err)
	passes := rec.snapshot()
	require.Len(t, passes, 1)
	assert.Equal(t, chunk.ID(2), passes[0][1], "rangeEnd == lastCompleteChunkAt(tip) == 2")
	assert.Equal(t, lastCommitted, last, "lastCommitted does not regress and stays at chunk 2 end")
}

// Mid-chunk resume exclusion: a lastCommitted inside chunk 5 leaves the partial
// resume chunk to ingestion — rangeEnd folds back to chunkID(lastCommitted)-1=4. Tip
// is AT chunk 5's last ledger (complete-at-tip) so the exclusion is detectable:
// without it lastCompleteChunkAt(anchor)=5 and the live chunk would be backfilled.
func TestBackfill_MidChunkResumeExclusion(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	lastCommitted := chunk.ID(5).FirstLedger() + 100
	tipLedger := chunk.ID(5).LastLedger() // within one chunk, chunk 5 complete-at-tip
	rec := &recordingPlan{}
	tip := &fakeTipBackend{tips: []uint32{tipLedger}}
	cfg := startTestConfig(t, cat, tip, nil, rec)

	last, err := backfillToTip(context.Background(), cfg, lastCommitted, chunk.FirstLedgerSeq)
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

	last, err := backfillToTip(context.Background(), cfg, preGenesisLedger, chunk.FirstLedgerSeq)
	require.NoError(t, err)

	var maxHi chunk.ID
	for _, p := range rec.snapshot() {
		maxHi = max(maxHi, p[1])
	}
	assert.Equal(t, chunk.ID(5), maxHi, "the re-pass extended the range to the advanced tip")
	assert.Equal(t, chunk.ID(5).LastLedger(), last)
	assert.GreaterOrEqual(t, tip.callCount(), 3, "the loop re-sampled the tip across passes")
}

// Degrade-and-serve restart: tip unreachable but local progress exists, so
// backfill degrades to tip:=lastCommitted, re-resolves [0,2] once, terminates,
// and never regresses the lastCommitted.
func TestBackfill_RestartTipUnreachableDegrades(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	lastCommitted := chunk.ID(2).LastLedger() // local progress exists
	tip := &fakeTipBackend{err: errors.New("backend down"), errFirst: 99}
	rec := &recordingPlan{}
	cfg := startTestConfig(t, cat, tip, nil, rec)

	last, err := backfillToTip(context.Background(), cfg, lastCommitted, chunk.FirstLedgerSeq)
	require.NoError(t, err, "local progress means no fatal")
	passes := rec.snapshot()
	require.Len(t, passes, 1, "exactly one degraded re-resolve pass, then terminate")
	assert.Equal(t, chunk.ID(2), passes[0][1])
	assert.Equal(t, lastCommitted, last, "lastCommitted does not regress")
}

// Lagging bulk tip below a chunk-aligned lastCommitted: the anchor is max(tip,
// lastCommitted)==lastCommitted, so rangeEnd==lastCompleteChunkAt(lastCommitted)==5, not
// ==2 (which would regress below where pruning advanced). Mid-chunk exclusion
// does NOT fire — the lastCommitted is on a boundary.
func TestBackfill_LaggingBulkTipFoldsLastCommittedChunk(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	lastCommitted := chunk.ID(5).LastLedger()   // chunk-aligned, complete lastCommitted chunk 5
	tipLedger := chunk.ID(3).FirstLedger() + 10 // lagging bulk tip in chunk 3 (last complete 2)
	rec := &recordingPlan{}
	tip := &fakeTipBackend{tips: []uint32{tipLedger}}
	cfg := startTestConfig(t, cat, tip, nil, rec)

	last, err := backfillToTip(context.Background(), cfg, lastCommitted, chunk.FirstLedgerSeq)
	require.NoError(t, err)
	passes := rec.snapshot()
	require.Len(t, passes, 1, "one pass anchored on the lastCommitted, then backfilledThrough==5 breaks")
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
// surfaces the ctx-canceled GetLedger error on a clean shutdown (the daemon top
// level classifies it as clean). The resume ledger is genesis (watermark+1).
func TestRun_FirstStartServeIngestCleanShutdown(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)

	served := atomic.Int32{}
	core := &fakeCore{getter: &fakeLedgerGetter{frames: map[uint32][]byte{}, blockOnCtx: true}}
	tip := &fakeTipBackend{tips: []uint32{chunk.FirstLedgerSeq + 10}} // young: no backfill
	cfg := startTestConfig(t, cat, tip, core, nil)
	cfg.ServeReads = func(context.Context) error { served.Add(1); return nil }

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- run(ctx, cfg) }()

	// Wait until the loop has opened the hot DB, started core, served, and parked on
	// the blocking getter, then request a clean shutdown.
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
	require.Equal(t, uint32(chunk.FirstLedgerSeq), core.resumeSeen.Load(),
		"resume ledger is genesis on a fresh start (watermark+1)")

	// The resume chunk's hot key is "ready" (opened, boundary never crossed).
	state, err := cat.HotState(chunk.IDFromLedger(chunk.FirstLedgerSeq))
	require.NoError(t, err)
	assert.Equal(t, geometry.HotReady, state)
}

// A ServeReads error is surfaced wrapped as a restartable failure (NOT clean) and
// the already-opened resume hot DB is closed on the way out, so a restart can
// reopen it (the rocksdb LOCK is released). ServeReads runs after the hot DB opens
// and core starts but before the blocking ingestion loop, so run returns here.
func TestRun_ServeReadsErrorSurfaces(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	core := &fakeCore{getter: &fakeLedgerGetter{frames: map[uint32][]byte{}, blockOnCtx: true}}
	tip := &fakeTipBackend{tips: []uint32{chunk.FirstLedgerSeq + 10}}
	cfg := startTestConfig(t, cat, tip, core, nil)
	cfg.ServeReads = func(context.Context) error { return errors.New("rpc bind failed") }

	err := run(context.Background(), cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "serve reads")
	require.NotErrorIs(t, err, context.Canceled, "a ServeReads error is restartable, not a clean shutdown")
	require.Equal(t, int32(1), core.openedCount.Load(), "core was started before serving")

	// The resume hot DB was closed on the error path (LOCK released): reopening it succeeds.
	db, err := openHotDBForChunk(cat, chunk.IDFromLedger(chunk.FirstLedgerSeq), silentLogger())
	require.NoError(t, err, "the resume hot DB is reopenable — run released its LOCK")
	require.NoError(t, db.Close())
}

// run fatals with ErrFirstStartNoTip on a first start with an unavailable tip;
// reads are never served and ingestion never starts.
func TestRun_FirstStartNoTipFatal(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)
	served := atomic.Int32{}
	core := &fakeCore{}
	tip := &fakeTipBackend{err: errors.New("unreachable"), errFirst: 99}
	cfg := startTestConfig(t, cat, tip, core, nil)
	cfg.ServeReads = func(context.Context) error { served.Add(1); return nil }

	err := run(context.Background(), cfg)
	require.ErrorIs(t, err, ErrFirstStartNoTip)
	require.Zero(t, served.Load(), "reads are never served when backfill fatals")
	require.Zero(t, core.openedCount.Load(), "core never starts when backfill fatals")
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

	t.Run("nil NetworkTip", func(t *testing.T) {
		cfg := base
		cfg.NetworkTip = nil
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

	got, err := backfillToTip(context.Background(), start, preGenesisLedger, chunk.FirstLedgerSeq)
	require.NoError(t, err)

	assert.Positive(t, metrics.backfillPasses, "at least one backfill pass reported")
	assert.Positive(t, metrics.gaugesSet["last_committed"], "last-committed gauge refreshed during backfill")
	assert.Equal(t, chunk.ID(3).LastLedger(), got, "lastCommitted advanced to the backfilled range end")
}
