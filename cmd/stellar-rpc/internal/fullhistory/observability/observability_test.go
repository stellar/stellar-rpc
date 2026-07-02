package observability

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// NopMetrics / MetricsOrNop
// ---------------------------------------------------------------------------

// A nil Metrics resolves to a no-op that never panics on any signal.
func TestMetricsOrNop_NilNeverPanics(t *testing.T) {
	m := MetricsOrNop(nil)
	require.NotNil(t, m)
	m.LastCommitted(5, 2)
	m.BackfillPass(time.Second)
	m.Freeze(time.Second)
	m.Rebuild(time.Second)
	m.Prune(2, time.Second)
}

// ---------------------------------------------------------------------------
// PrometheusMetrics — registration + signal recording into the registry.
// ---------------------------------------------------------------------------

// NewPrometheusMetrics registers without panicking and every signal updates the
// underlying collectors (asserted by gathering the registry).
func TestPrometheusMetrics_RegistersAndRecords(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPrometheusMetrics(reg, "test_ns")

	m.LastCommitted(58, 12)
	m.BackfillPass(250 * time.Millisecond)
	m.Freeze(100 * time.Millisecond)
	m.Rebuild(50 * time.Millisecond)
	m.Prune(2, 5*time.Millisecond)

	families, err := reg.Gather()
	require.NoError(t, err)

	values := map[string]float64{}
	counts := map[string]uint64{}
	for _, mf := range families {
		for _, metric := range mf.GetMetric() {
			name := mf.GetName()
			switch {
			case metric.GetGauge() != nil:
				values[name] = metric.GetGauge().GetValue()
			case metric.GetCounter() != nil:
				values[name] += metric.GetCounter().GetValue()
			case metric.GetHistogram() != nil:
				counts[name] += metric.GetHistogram().GetSampleCount()
			}
		}
	}

	assert.InDelta(t, float64(58), values["test_ns_fullhistory_streaming_last_committed_ledger"], 0)
	assert.InDelta(t, float64(12), values["test_ns_fullhistory_streaming_retention_floor_ledger"], 0)
	assert.InDelta(t, float64(2), values["test_ns_fullhistory_streaming_pruned_artifacts_total"], 0)

	// Phase-duration histogram saw backfill_pass + freeze + rebuild + prune = 4 observations.
	assert.Equal(t, uint64(4), counts["test_ns_fullhistory_streaming_phase_duration_seconds"])
}

// Double-registration on the same registry panics (one sink per registry).
func TestPrometheusMetrics_DoubleRegisterPanics(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewPrometheusMetrics(reg, "test_ns")
	assert.Panics(t, func() { NewPrometheusMetrics(reg, "test_ns") },
		"re-registering the same collectors must panic (one sink per registry)")
}
