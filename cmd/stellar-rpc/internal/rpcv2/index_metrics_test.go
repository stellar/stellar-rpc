package rpcv2

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

// gatherIndexMetrics returns each events_inmemory_index sample as "name{state}" -> value.
func gatherIndexMetrics(t *testing.T, registry *prometheus.Registry) map[string]float64 {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	got := map[string]float64{}
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			require.Len(t, metric.GetLabel(), 1)
			key := fmt.Sprintf("%s{%s}", family.GetName(), metric.GetLabel()[0].GetValue())
			got[key] = metric.GetGauge().GetValue()
		}
	}
	return got
}

// hotChunkWithEvents seeds the hot DB of chunk c with one ledger whose events
// all have the same contract and a different topic each.
func hotChunkWithEvents(t *testing.T, c chunk.ID, events int) *hotchunk.DB {
	t.Helper()
	var contractID xdr.ContractId
	contractID[0] = 0xab
	evs := make([]xdr.ContractEvent, events)
	for i := range evs {
		evs[i] = rpcv2test.SymbolContractEvent(contractID, "data", fmt.Sprintf("topic%d", i))
	}

	cat, _ := testCatalog(t)
	var seeded *hotchunk.DB
	rpcv2test.SeedHotChunkLCMs(t, cat, c, func(db *hotchunk.DB) { seeded = db },
		rpcv2test.EventsLCMBytes(t, c.FirstLedger(), evs...))
	return seeded
}

func TestIndexMetrics_NoSourceExportsZeros(t *testing.T) {
	registry := prometheus.NewRegistry()
	newIndexMetrics(registry, "ns")

	assert.Equal(t, map[string]float64{
		"ns_events_inmemory_index_terms{ingesting}":       0,
		"ns_events_inmemory_index_terms{completed}":       0,
		"ns_events_inmemory_index_dense_terms{ingesting}": 0,
		"ns_events_inmemory_index_dense_terms{completed}": 0,
	}, gatherIndexMetrics(t, registry))
}

// The newest chunk is live. The older chunks are summed into completed.
func TestIndexMetrics_SplitsLiveFromCompleted(t *testing.T) {
	// 100 events with one contract: the contract term is dense.
	oldest := hotChunkWithEvents(t, 0, 100)
	older := hotChunkWithEvents(t, 1, 100)
	live := hotChunkWithEvents(t, 2, 3)

	oldestStats := oldest.Events().IndexStats()
	olderStats := older.Events().IndexStats()
	liveStats := live.Events().IndexStats()
	require.Positive(t, oldestStats.DenseTerms)
	require.Positive(t, liveStats.Terms)
	require.Zero(t, liveStats.DenseTerms)
	require.NotEqual(t, liveStats.Terms, oldestStats.Terms)

	registry := prometheus.NewRegistry()
	metrics := newIndexMetrics(registry, "ns")
	metrics.setSource(func() []*hotchunk.DB { return []*hotchunk.DB{oldest, older, live} })

	got := gatherIndexMetrics(t, registry)
	assert.InDelta(t, liveStats.Terms, got["ns_events_inmemory_index_terms{ingesting}"], 0)
	assert.InDelta(t, oldestStats.Terms+olderStats.Terms, got["ns_events_inmemory_index_terms{completed}"], 0)
	assert.InDelta(t, 0, got["ns_events_inmemory_index_dense_terms{ingesting}"], 0)
	completedDense := oldestStats.DenseTerms + olderStats.DenseTerms
	assert.InDelta(t, completedDense, got["ns_events_inmemory_index_dense_terms{completed}"], 0)
}

func TestIndexMetrics_NilIgnoresSource(t *testing.T) {
	var metrics *indexMetrics
	assert.NotPanics(t, func() {
		metrics.setSource(func() []*hotchunk.DB { return nil })
	})
}

// The registry publishes only read-write chunks today. A read-only chunk must
// not make the scrape panic.
func TestIndexMetrics_SkipsAChunkWithNoEventsFacade(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newIndexMetrics(registry, "ns")
	metrics.setSource(func() []*hotchunk.DB { return []*hotchunk.DB{{}, {}} })

	got := gatherIndexMetrics(t, registry)
	assert.Len(t, got, 4)
	assert.InDelta(t, 0, got["ns_events_inmemory_index_terms{ingesting}"], 0)
	assert.InDelta(t, 0, got["ns_events_inmemory_index_terms{completed}"], 0)
}
