package observability

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
)

// ---------------------------------------------------------------------------
// NopMetrics / MetricsOrNop
// ---------------------------------------------------------------------------

// A nil Metrics resolves to a no-op that never panics on any signal.
func TestMetricsOrNop_NilNeverPanics(t *testing.T) {
	m := MetricsOrNop(nil)
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
// coldTierBytes — the disk-footprint helper.
// ---------------------------------------------------------------------------

// A missing tree contributes zero; populated files are summed across all four
// cold trees; the hot tree and meta store are excluded.
func TestColdTierBytes(t *testing.T) {
	root := t.TempDir()
	layout := geometry.NewLayout(root)

	// Nothing materialized yet ⇒ zero, no error.
	total, err := MeasureColdTierBytes(layout)
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

	total, err = MeasureColdTierBytes(layout)
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

// Double-registration on the same registry panics (one sink per registry).
func TestPrometheusMetrics_DoubleRegisterPanics(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewPrometheusMetrics(reg, "test_ns")
	assert.Panics(t, func() { NewPrometheusMetrics(reg, "test_ns") },
		"re-registering the same collectors must panic (one sink per registry)")
}
