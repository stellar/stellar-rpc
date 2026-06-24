package streaming

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

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
	coldBytes                int64
	gaugesSet                map[string]int // how many times each gauge was set

	// counters / per-call records
	boundaries  []uint32
	catchupPass []passRec
	freeze      []freezeRec
	rebuild     []rebuildRec
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

func (r *recordingMetrics) Prune(count int, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prune = append(r.prune, countDur{count, d})
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
	m.ColdTierBytes(1024)
	m.ChunkBoundary(0)
	m.CatchupPass(0, 4, time.Second)
	m.Freeze(2, 1, time.Second)
	m.Rebuild(4, time.Second)
	m.Prune(2, time.Second)
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
	start := startTestConfig(t, cat, tip, rp)
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
	layout := NewLayout(root)

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
	m.ColdTierBytes(2048)
	m.ChunkBoundary(3)
	m.CatchupPass(0, 3, 250*time.Millisecond)
	m.Freeze(2, 1, 100*time.Millisecond)
	m.Rebuild(4, 50*time.Millisecond)
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
	assert.Equal(t, float64(2048), values["test_ns_fullhistory_streaming_cold_tier_bytes"])
	assert.Equal(t, float64(1), values["test_ns_fullhistory_streaming_chunk_boundaries_total"])
	assert.Equal(t, float64(1), values["test_ns_fullhistory_streaming_catchup_passes_total"])
	assert.Equal(t, float64(2), values["test_ns_fullhistory_streaming_freeze_chunks_total"])
	assert.Equal(t, float64(4), values["test_ns_fullhistory_streaming_rebuilt_chunks_total"])
	assert.Equal(t, float64(2), values["test_ns_fullhistory_streaming_pruned_ops_total"])

	// Phase-duration histogram saw catchup_pass + freeze + rebuild + prune = 4 observations;
	// the rebuild-chunks histogram saw 1.
	assert.Equal(t, uint64(4), counts["test_ns_fullhistory_streaming_phase_duration_seconds"])
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
