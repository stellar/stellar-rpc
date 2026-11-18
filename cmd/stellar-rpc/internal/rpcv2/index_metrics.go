package rpcv2

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

// Values of the state label.
const (
	// stateIngesting is the chunk that ingest writes to.
	stateIngesting = "ingesting"
	// stateCompleted is each other open hot chunk. If more than one is open,
	// the value is the sum.
	stateCompleted = "completed"
)

// indexMetrics is a collector for the in-memory events index of the open hot
// chunks. It reads the chunks at scrape time, so no code has to update it.
type indexMetrics struct {
	terms      *prometheus.Desc
	denseTerms *prometheus.Desc

	// hotChunks returns the open hot chunks, oldest first. Unset until
	// startup opens the query registry.
	hotChunks atomic.Pointer[func() []*hotchunk.DB]
}

func newIndexMetrics(registry *prometheus.Registry, namespace string) *indexMetrics {
	desc := func(name, help string) *prometheus.Desc {
		fqName := prometheus.BuildFQName(namespace, "events_inmemory_index", name)
		return prometheus.NewDesc(fqName, help, []string{"state"}, nil)
	}
	m := &indexMetrics{
		terms: desc("terms",
			"Terms in the in-memory events index. A chunk that waits for close is not counted."),
		denseTerms: desc("dense_terms",
			"Terms in the in-memory events index that are bitmaps and not lists. "+
				"A chunk that waits for close is not counted."),
	}
	registry.MustRegister(m)
	return m
}

func (m *indexMetrics) Describe(ch chan<- *prometheus.Desc) {
	ch <- m.terms
	ch <- m.denseTerms
}

func (m *indexMetrics) Collect(ch chan<- prometheus.Metric) {
	var dbs []*hotchunk.DB
	if source := m.hotChunks.Load(); source != nil {
		dbs = (*source)()
	}
	var ingesting, completed event.IndexStats
	for i, db := range dbs {
		// A read-only chunk has no events index.
		if !db.HasEvents() {
			continue
		}
		stats := db.Events().IndexStats()
		// The last chunk is the chunk that ingest writes to.
		if i == len(dbs)-1 {
			ingesting = stats
			continue
		}
		completed.Terms += stats.Terms
		completed.DenseTerms += stats.DenseTerms
	}
	// Both label values are always exported, at zero if no chunk has them.
	m.send(ch, stateIngesting, ingesting)
	m.send(ch, stateCompleted, completed)
}

func (m *indexMetrics) send(ch chan<- prometheus.Metric, label string, stats event.IndexStats) {
	ch <- prometheus.MustNewConstMetric(m.terms, prometheus.GaugeValue, float64(stats.Terms), label)
	ch <- prometheus.MustNewConstMetric(m.denseTerms, prometheus.GaugeValue, float64(stats.DenseTerms), label)
}

// setSource sets the function that returns the open hot chunks, oldest first.
// A nil *indexMetrics ignores it.
func (m *indexMetrics) setSource(hotChunks func() []*hotchunk.DB) {
	if m != nil {
		m.hotChunks.Store(&hotChunks)
	}
}
