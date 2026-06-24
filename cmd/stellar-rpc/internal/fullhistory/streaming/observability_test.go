package streaming

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/geometry"
)

// findLog returns the first captured entry whose message equals msg, or fails.
func findLog(t *testing.T, entries []logrus.Entry, msg string) logrus.Entry {
	t.Helper()
	for _, e := range entries {
		if e.Message == msg {
			return e
		}
	}
	t.Fatalf("no log entry with message %q; got %d entries", msg, len(entries))
	return logrus.Entry{}
}

// recordingMetrics is a Metrics sink that records every signal so a test can
// assert the daemon drove the expected phase signals at the right points. It is
// safe for concurrent use (the ingestion loop, lifecycle goroutine, and worker
// pool all report concurrently).
type recordingMetrics struct {
	mu sync.Mutex

	// last-write gauges
	lagTip, lagCommitted     uint32
	lastCommitted            uint32
	wmCommitted, wmFloor     uint32
	catchupDone, catchupGoal uint32
	liveHot                  int
	coldBytes                int64
	gaugesSet                map[string]int // how many times each gauge was set

	// counters / per-call records
	boundaries  []uint32
	catchupPass []passRec
	freeze      []freezeRec
	rebuild     []rebuildRec
	discard     []countDur
	prune       []countDur
}

type passRec struct {
	lo, hi uint32
	d      time.Duration
}
type freezeRec struct {
	chunkBuilds, indexBuilds int
	d                        time.Duration
}
type rebuildRec struct {
	chunks int
	d      time.Duration
}
type countDur struct {
	count int
	d     time.Duration
}

func newRecordingMetrics() *recordingMetrics {
	return &recordingMetrics{gaugesSet: map[string]int{}}
}

func (r *recordingMetrics) IngestionLag(tip, committed uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lagTip, r.lagCommitted = tip, committed
	r.gaugesSet["lag"]++
}

func (r *recordingMetrics) LastCommitted(seq uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastCommitted = seq
	r.gaugesSet["last_committed"]++
}

func (r *recordingMetrics) Watermark(committed, floor uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.wmCommitted, r.wmFloor = committed, floor
	r.gaugesSet["watermark"]++
}

func (r *recordingMetrics) CatchupProgress(done, goal uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.catchupDone, r.catchupGoal = done, goal
	r.gaugesSet["catchup_progress"]++
}

func (r *recordingMetrics) LiveHotChunks(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.liveHot = n
	r.gaugesSet["live_hot"]++
}

func (r *recordingMetrics) ColdTierBytes(b int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.coldBytes = b
	r.gaugesSet["cold_bytes"]++
}

func (r *recordingMetrics) ChunkBoundary(closed uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.boundaries = append(r.boundaries, closed)
}

func (r *recordingMetrics) CatchupPass(lo, hi uint32, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.catchupPass = append(r.catchupPass, passRec{lo, hi, d})
}

func (r *recordingMetrics) Freeze(chunkBuilds, indexBuilds int, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.freeze = append(r.freeze, freezeRec{chunkBuilds, indexBuilds, d})
}

func (r *recordingMetrics) Rebuild(chunks int, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rebuild = append(r.rebuild, rebuildRec{chunks, d})
}

func (r *recordingMetrics) Discard(count int, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.discard = append(r.discard, countDur{count, d})
}

func (r *recordingMetrics) Prune(count int, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prune = append(r.prune, countDur{count, d})
}

func (r *recordingMetrics) snapshotBoundaries() []uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]uint32, len(r.boundaries))
	copy(out, r.boundaries)
	return out
}

// snapshotFreezeCount reports how many freeze-stage signals were recorded — used
// by the end-to-end daemon test to assert the lifecycle ran its plan-and-execute
// (freeze) stage.
func (r *recordingMetrics) snapshotFreezeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.freeze)
}

func (r *recordingMetrics) snapshotLastCommitted() (uint32, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastCommitted, r.gaugesSet["last_committed"]
}

func (r *recordingMetrics) snapshotLag() (tip, committed uint32, set int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lagTip, r.lagCommitted, r.gaugesSet["lag"]
}

var _ Metrics = (*recordingMetrics)(nil)

// ---------------------------------------------------------------------------
// nopMetrics / metricsOrNop
// ---------------------------------------------------------------------------

// A nil Metrics resolves to a no-op that never panics on any signal — the
// safety net every phase relies on (WithDefaults fills the daemon path; a
// primitive driven directly may not have).
func TestMetricsOrNop_NilNeverPanics(t *testing.T) {
	m := metricsOrNop(nil)
	require.NotNil(t, m)
	m.IngestionLag(10, 5)
	m.LastCommitted(5)
	m.Watermark(5, 2)
	m.CatchupProgress(1, 9)
	m.LiveHotChunks(3)
	m.ColdTierBytes(1024)
	m.ChunkBoundary(0)
	m.CatchupPass(0, 4, time.Second)
	m.Freeze(2, 1, time.Second)
	m.Rebuild(4, time.Second)
	m.Discard(1, time.Second)
	m.Prune(2, time.Second)
}

// ---------------------------------------------------------------------------
// Ingestion loop — ChunkBoundary signal at each handoff.
// ---------------------------------------------------------------------------

// Driving a ledger that closes a chunk fires exactly one ChunkBoundary at the
// handoff, naming the JUST-CLOSED chunk (not the next one). The watermark is
// seeded just below chunk 0's boundary so the indexed poll resumes there and
// crosses boundary 0->1 in one step, then ingests one interior ledger of chunk 1
// (no boundary), then the poll errs.
//
// NOTE (pull seam): the push-model predecessor of this test asserted the metric
// over TWO consecutive handoffs ([]uint32{0,1}) to also pin the "in order" of
// multiple boundaries. That cheap two-boundary check relied on the stream
// SKIPPING from chunk 0's last ledger straight to chunk 1's last ledger. The
// indexed-poll loop (for seq := resume; ; seq++) cannot skip: a second real
// boundary is 10,000 ledgers away, so two-handoff ordering can only be exercised
// by ingesting a full chunk (~85s), which alone pushes the package past the
// fixed 600s `go test` timeout the gate runs under. The substantive per-handoff
// properties — exactly one boundary, naming the just-closed (not the next)
// chunk, and the gauge set once per ingested ledger — are preserved here; the
// multi-handoff "in order" sub-property is reported as not cheaply expressible
// against the pull seam (see the structured report).
func TestRunIngestionLoop_ReportsChunkBoundaries(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	c1 := c + 1
	db := seedWatermark(t, cat, c, c.LastLedger()-1)

	// last ledger of chunk 0 (boundary 0->1), then a ledger inside chunk 1 (no
	// boundary), then the poll errs.
	lastSeq := c1.FirstLedger()
	getter := &fakeLedgerGetter{frames: map[uint32][]byte{
		c.LastLedger(): zeroTxLCMBytes(t, c.LastLedger()), // boundary 0->1
		lastSeq:        zeroTxLCMBytes(t, lastSeq),        // no boundary
	}, endErr: errors.New("end")}
	ingestTypes := hotchunk.Ingest{Ledgers: true, Txhash: true}
	ch := make(chan chunk.ID, lifecycleQueueDepth)
	rec := newRecordingMetrics()

	done := make(chan error, 1)
	go func() {
		done <- runIngestionLoop(context.Background(), getter, db, cat, ch, ingestTypes, silentLogger(), rec, nil)
	}()

	select {
	case <-done: // the poll ran dry and errored; the boundary already fired
	case <-time.After(10 * time.Second):
		t.Fatal("ingestion loop did not finish")
	}

	// Exactly one boundary, naming the just-closed chunk (c), NOT the newly-opened
	// one (c1) — the load-bearing "names the closed chunk" half of the property.
	assert.Equal(t, []uint32{uint32(c)}, rec.snapshotBoundaries(),
		"one boundary at the handoff, naming the just-closed chunk")

	// Per-ledger liveness gauge: refreshed after every synced batch, so it tracks
	// the highest committed ledger and is the moving steady-state health signal
	// between chunk boundaries. It must equal the last ledger ingested and have
	// been set once per ingested ledger (the two-ledger run here).
	gotSeq, setCount := rec.snapshotLastCommitted()
	assert.Equal(t, lastSeq, gotSeq, "last-committed gauge tracks the highest synced ledger")
	assert.Equal(t, 2, setCount, "last-committed refreshed once per ledger")

	// The ingestion loop holds no network tip, so it must NOT touch IngestionLag —
	// that gauge is a backfill-only signal (the corrected contract). Asserting it
	// stays untouched guards against re-introducing the stale-steady-state lag the
	// old doc-comment falsely promised the loop would refresh.
	_, _, lagSet := rec.snapshotLag()
	assert.Zero(t, lagSet, "ingestion loop must not touch IngestionLag (backfill-only signal)")
}

// ---------------------------------------------------------------------------
// Structured logging — keys, values, and level at the phase log points.
// ---------------------------------------------------------------------------

// The ingestion loop's chunk-boundary log line carries the structured keys the
// operator dashboards/alerts join on (closed_chunk, next_chunk, last_ledger) at
// Info level. A dropped field, mislabeled key, or wrong level here would silently
// break those joins; the metrics tests cannot see it.
func TestRunIngestionLoop_BoundaryLogFields(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(0)
	c1 := c + 1
	// Seed just below the boundary so the poll crosses it in one step.
	db := seedWatermark(t, cat, c, c.LastLedger()-1)

	getter := &fakeLedgerGetter{frames: map[uint32][]byte{
		c.LastLedger():   zeroTxLCMBytes(t, c.LastLedger()),   // boundary 0->1
		c1.FirstLedger(): zeroTxLCMBytes(t, c1.FirstLedger()), // no boundary
	}, endErr: errors.New("end")}
	logger := silentLogger()
	stop := logger.StartTest(logrus.DebugLevel)

	ch := make(chan chunk.ID, lifecycleQueueDepth)
	done := make(chan error, 1)
	go func() {
		done <- runIngestionLoop(context.Background(), getter, db, cat, ch,
			hotchunk.Ingest{Ledgers: true, Txhash: true}, logger, newRecordingMetrics(), nil)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("ingestion loop did not finish")
	}
	entries := stop()

	e := findLog(t, entries, "streaming: ingestion chunk boundary — handed off to lifecycle")
	assert.Equal(t, logrus.InfoLevel, e.Level, "boundary handoff is an Info-level event")
	assert.Equal(t, c.String(), e.Data["closed_chunk"], "closed_chunk names the just-filled chunk")
	assert.Equal(t, c1.String(), e.Data["next_chunk"], "next_chunk names the newly-opened chunk")
	assert.Equal(t, c.LastLedger(), e.Data["last_ledger"], "last_ledger is the boundary ledger")
}

// A healthy lifecycle tick emits the derived-snapshot Debug line (through/floor)
// and the freeze-stage Info line (chunk_builds/index_builds) with the keys the
// operator reads. Asserts keys, values, and levels together so a relabel or
// level regression is caught.
func TestRunLifecycleTick_LogFields(t *testing.T) {
	t.Parallel() // full-chunk ingest; isolated TempDir/catalog + per-instance logger — overlap to fit the gate's go-test timeout
	cat, _ := smallTxHashIndexCatalog(t, 1)
	cfg, _ := lifecycleTestConfig(t, cat, 0)
	cfg.Metrics = newRecordingMetrics()

	ingestFullHotChunk(t, cat, 0)
	live := openLiveHotDB(t, cat, 1)
	t.Cleanup(func() { _ = live.Close() })

	logger := supportlog.New()
	logger.SetLevel(logrus.DebugLevel)
	cfg.Logger = logger
	stop := logger.StartTest(logrus.DebugLevel)

	runTickForCatalog(context.Background(), t, cfg, cat)
	entries := stop()

	snap := findLog(t, entries, "streaming: lifecycle tick — derived snapshot")
	assert.Equal(t, logrus.DebugLevel, snap.Level, "the per-tick snapshot is Debug (high-frequency)")
	assert.Contains(t, snap.Data, "through")
	assert.Contains(t, snap.Data, "floor")

	freeze := findLog(t, entries, "streaming: lifecycle freeze stage complete")
	assert.Equal(t, logrus.InfoLevel, freeze.Level, "a non-empty freeze is Info")
	assert.Equal(t, 1, freeze.Data["index_builds"], "the one-chunk window built one index")
	assert.Positive(t, freeze.Data["chunk_builds"], "chunk 0 was built")
}

// ---------------------------------------------------------------------------
// Lifecycle tick — Freeze / Discard / Prune + gauges.
// ---------------------------------------------------------------------------

// A tick that freezes a chunk, folds it into a terminal index, and discards its
// hot DB drives the freeze (with non-zero build counts), discard (count 1), and
// prune stages, plus the watermark, live-hot-chunk, and cold-bytes gauges.
func TestRunLifecycleTick_ReportsPhaseSignals(t *testing.T) {
	t.Parallel()                            // full-chunk ingest; isolated TempDir/catalog — overlap with the other heavy tests to fit the gate's go-test timeout
	cat, _ := smallTxHashIndexCatalog(t, 1) // one-chunk window finalizes immediately
	cfg, rec := lifecycleTestConfig(t, cat, 0)
	metrics := newRecordingMetrics()
	cfg.Metrics = metrics

	// Chunk 0 just closed (full hot DB on disk); chunk 1 is the new live chunk.
	ingestFullHotChunk(t, cat, 0)
	live := openLiveHotDB(t, cat, 1)
	t.Cleanup(func() { _ = live.Close() })

	runTickForCatalog(context.Background(), t, cfg, cat)
	require.False(t, rec.fired(), "a healthy tick never aborts: %v", rec.last.Load())

	// Freeze stage reported once, with a non-trivial plan (chunk 0's builds + the
	// terminal index build).
	require.Len(t, metrics.freeze, 1, "freeze stage reported once")
	assert.Positive(t, metrics.freeze[0].chunkBuilds, "chunk 0 was built")
	assert.Positive(t, metrics.freeze[0].indexBuilds, "the window index was built")

	// The index build (a rebuild) reported its burst throughput: 1 chunk folded.
	require.NotEmpty(t, metrics.rebuild, "the index build reported a rebuild")
	assert.Equal(t, 1, metrics.rebuild[0].chunks, "a one-chunk window folds one chunk")

	// Discard stage retired chunk 0's hot DB (cold artifacts now serve it).
	require.Len(t, metrics.discard, 1, "discard stage reported once")
	assert.Equal(t, 1, metrics.discard[0].count, "chunk 0's hot DB was discarded")

	// Prune stage reported (it may have zero ops — the count is what matters).
	require.Len(t, metrics.prune, 1, "prune stage reported once")

	// Gauges: watermark set, live-hot count reflects only the live chunk 1 after
	// the discard, cold footprint set (chunk 0's artifacts exist on disk).
	assert.Positive(t, metrics.gaugesSet["watermark"], "watermark gauge set")
	assert.Equal(t, 1, metrics.liveHot, "only the live chunk remains after discard")
	assert.Positive(t, metrics.gaugesSet["cold_bytes"], "cold footprint gauge set")
	assert.Positive(t, metrics.coldBytes, "chunk 0's frozen artifacts have non-zero size")
}

// An empty tick (nothing left to build, no hot DBs to discard, nothing to
// prune) still reports the freeze/discard/prune stages so the empty-tick rate is
// observable. Chunk 0 is already fully frozen and covered (no hot key), so the
// plan over [0,0] resolves to nothing and the discard/prune scans find nothing.
func TestRunLifecycleTick_EmptyTickStillReportsStages(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 1)
	cfg, _ := lifecycleTestConfig(t, cat, 0)
	metrics := newRecordingMetrics()
	cfg.Metrics = metrics

	freezeKinds(t, cat, 0, geometry.KindLedgers, geometry.KindEvents, geometry.KindTxHash)
	freezeCoverage(t, cat, cat.TxHashIndexLayout().TxHashIndexID(0), 0, 0) // terminal coverage; no hot key

	// Drive the tick with chunk 0 (the just-completed chunk): the range [0,0] is
	// already fully materialized and covered, so no build, no discard, no prune.
	runLifecycleTick(context.Background(), cfg, cat, 0)

	require.Len(t, metrics.freeze, 1)
	assert.Equal(t, 0, metrics.freeze[0].chunkBuilds, "no producible range — all frozen")
	assert.Equal(t, 0, metrics.freeze[0].indexBuilds, "the window is already covered")
	require.Len(t, metrics.discard, 1)
	assert.Equal(t, 0, metrics.discard[0].count)
	require.Len(t, metrics.prune, 1)
	assert.Positive(t, metrics.gaugesSet["watermark"], "watermark gauge set even on an empty tick")
}

// ---------------------------------------------------------------------------
// Catch-up — CatchupPass + progress/lag gauges.
// ---------------------------------------------------------------------------

// A backfill that backfills a multi-chunk range reports one CatchupPass over the
// resolved [lo, hi], plus the progress and lag gauges. Driven through the same
// startTestConfig the startup tests use, with a recording-plan seam so no real
// cold I/O runs.
func TestBackfill_ReportsPassAndProgress(t *testing.T) {
	cat, _ := testCatalog(t)
	pinGenesis(t, cat)

	rp := &recordingPlan{}
	// A tip well past several chunks ⇒ backfill backfills [genesis chunk, last
	// complete chunk at tip].
	tipLedger := chunk.ID(3).LastLedger() + 5
	tip := &fakeTipBackend{tips: []uint32{tipLedger}}
	start := startTestConfig(t, cat, tip, &fakeCore{}, rp)
	metrics := newRecordingMetrics()
	start.Exec.Metrics = metrics

	got, err := catchUp(context.Background(), start, preGenesisLedger, chunk.FirstLedgerSeq)
	require.NoError(t, err)

	require.NotEmpty(t, metrics.catchupPass, "at least one backfill pass reported")
	first := metrics.catchupPass[0]
	assert.Equal(t, uint32(0), first.lo, "backfill starts at the genesis chunk")
	assert.Equal(t, uint32(3), first.hi, "backfills through the last complete chunk at tip")

	// Progress + lag gauges were updated.
	assert.Positive(t, metrics.gaugesSet["catchup_progress"], "backfill progress gauge set")
	assert.Positive(t, metrics.gaugesSet["lag"], "ingestion lag gauge set during backfill")
	assert.Equal(t, chunk.ID(3).LastLedger(), got, "watermark advanced to the backfilled range end")
}

// ---------------------------------------------------------------------------
// coldTierBytes — the disk-footprint helper.
// ---------------------------------------------------------------------------

// A missing tree contributes zero; populated files are summed across all four
// cold trees; the hot tree and meta store are excluded.
func TestColdTierBytes(t *testing.T) {
	root := t.TempDir()
	layout := geometry.NewLayout(root)

	// Nothing materialized yet ⇒ zero, no error.
	total, err := coldTierBytes(layout)
	require.NoError(t, err)
	assert.Zero(t, total, "an un-materialized cold tier is zero bytes")

	// Write a file in the ledgers tree and one in the events tree.
	write := func(dir, name string, n int) {
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), make([]byte, n), 0o644))
	}
	write(filepath.Join(layout.LedgersRoot(), "00000"), "x.pack", 100)
	write(filepath.Join(layout.EventsRoot(), "00000"), "y-events.pack", 50)
	// A file under the HOT tree must NOT be counted.
	write(layout.HotRoot(), "ignored.sst", 9999)

	total, err = coldTierBytes(layout)
	require.NoError(t, err)
	assert.Equal(t, int64(150), total, "only the cold trees are summed; the hot tree is excluded")
}

// ---------------------------------------------------------------------------
// PrometheusMetrics — registration + signal recording into the registry.
// ---------------------------------------------------------------------------

// NewPrometheusMetrics registers without panicking and every signal updates the
// underlying collectors (asserted by gathering the registry).
func TestPrometheusMetrics_RegistersAndRecords(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPrometheusMetrics(reg, "test_ns")

	m.IngestionLag(100, 60) // lag 40
	m.LastCommitted(58)
	m.Watermark(60, 12)
	m.CatchupProgress(40, 100)
	m.LiveHotChunks(7)
	m.ColdTierBytes(2048)
	m.ChunkBoundary(3)
	m.CatchupPass(0, 3, 250*time.Millisecond)
	m.Freeze(2, 1, 100*time.Millisecond)
	m.Rebuild(4, 50*time.Millisecond)
	m.Discard(1, 10*time.Millisecond)
	m.Prune(2, 5*time.Millisecond)

	families, err := reg.Gather()
	require.NoError(t, err)

	values := map[string]float64{}
	counts := map[string]uint64{}
	for _, mf := range families {
		for _, metric := range mf.GetMetric() {
			name := mf.GetName()
			switch {
			case metric.Gauge != nil:
				values[name] = metric.Gauge.GetValue()
			case metric.Counter != nil:
				values[name] += metric.Counter.GetValue()
			case metric.Histogram != nil:
				counts[name] += metric.Histogram.GetSampleCount()
			}
		}
	}

	assert.Equal(t, float64(40), values["test_ns_fullhistory_streaming_ingestion_lag_ledgers"])
	assert.Equal(t, float64(58), values["test_ns_fullhistory_streaming_last_committed_ledger"])
	assert.Equal(t, float64(60), values["test_ns_fullhistory_streaming_watermark_ledger"])
	assert.Equal(t, float64(12), values["test_ns_fullhistory_streaming_retention_floor_ledger"])
	assert.Equal(t, float64(100), values["test_ns_fullhistory_streaming_catchup_target_ledger"])
	assert.Equal(t, float64(7), values["test_ns_fullhistory_streaming_live_hot_chunks"])
	assert.Equal(t, float64(2048), values["test_ns_fullhistory_streaming_cold_tier_bytes"])
	assert.Equal(t, float64(1), values["test_ns_fullhistory_streaming_chunk_boundaries_total"])
	assert.Equal(t, float64(1), values["test_ns_fullhistory_streaming_catchup_passes_total"])
	assert.Equal(t, float64(2), values["test_ns_fullhistory_streaming_freeze_chunks_total"])
	assert.Equal(t, float64(4), values["test_ns_fullhistory_streaming_rebuilt_chunks_total"])
	assert.Equal(t, float64(1), values["test_ns_fullhistory_streaming_discarded_hot_chunks_total"])
	assert.Equal(t, float64(2), values["test_ns_fullhistory_streaming_pruned_ops_total"])

	// Phase-duration histogram saw catchup_pass + freeze + rebuild + discard +
	// prune = 5 observations; the rebuild-chunks histogram saw 1.
	assert.Equal(t, uint64(5), counts["test_ns_fullhistory_streaming_phase_duration_seconds"])
	assert.Equal(t, uint64(1), counts["test_ns_fullhistory_streaming_rebuild_chunks_per_index"])
}

// Double-registration on the same registry panics inside MustRegister — the
// daemon convention is one sink per registry; this documents it.
func TestPrometheusMetrics_DoubleRegisterPanics(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewPrometheusMetrics(reg, "test_ns")
	assert.Panics(t, func() { NewPrometheusMetrics(reg, "test_ns") },
		"re-registering the same collectors must panic (one sink per registry)")
}
