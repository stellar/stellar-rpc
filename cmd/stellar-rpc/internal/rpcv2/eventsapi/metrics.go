package eventsapi

import (
	"github.com/prometheus/client_golang/prometheus"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

// metricsSubsystem is the Prometheus subsystem for the queryEvents metrics.
const metricsSubsystem = "query_events"

// reasonTermBudgetExceeded labels a term-budget rejection. On the wire it is
// an invalid_params error. The metric splits it out, so an operator can see
// how many requests the term_budget setting rejects.
const reasonTermBudgetExceeded = "term_budget_exceeded"

// 15 is the default term budget.
//
//nolint:gochecknoglobals // fixed bucket layout, read-only
var distinctTermsBuckets = []float64{0, 1, 2, 3, 4, 5, 6, 8, 10, 12, 15, 20, 30, 50}

// Page labels for the scanned-ledgers histogram: how full the page that
// the scan produced is. An empty page over many ledgers is the expensive
// query that finds nothing.
const (
	pageEmpty   = "empty"
	pagePartial = "partial"
	pageFull    = "full"
)

// returnedEventsBuckets end at protocol.QueryEventsMaxLimit.
//
//nolint:gochecknoglobals // fixed bucket layout, read-only
var returnedEventsBuckets = []float64{0, 1, 10, 50, 100, 250, 500, protocol.QueryEventsMaxLimit}

// scannedLedgersBuckets end at query.MaxScanLedgers, the most one page scans.
//
//nolint:gochecknoglobals // fixed bucket layout, read-only
var scannedLedgersBuckets = []float64{1, 10, 100, 500, 1000, 2500, 5000, float64(query.MaxScanLedgers)}

// Metrics are the queryEvents request metrics. A nil *Metrics records
// nothing, which is what a server with no registry passes.
type Metrics struct {
	distinctTerms prometheus.Histogram
	rejections    *prometheus.CounterVec

	returnedEvents prometheus.Histogram
	scannedLedgers *prometheus.HistogramVec
}

// NewMetrics builds the queryEvents metrics and registers them on registry.
func NewMetrics(registry *prometheus.Registry, namespace string) *Metrics {
	m := &Metrics{
		distinctTerms: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: metricsSubsystem,
			Name: "distinct_terms",
			Help: "Distinct index terms named by the filters of one queryEvents request. " +
				"Includes requests that the term budget rejects.",
			Buckets: distinctTermsBuckets,
		}),
		rejections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: metricsSubsystem,
			Name: "rejections_total",
			Help: "queryEvents requests rejected because of a client error, by reason. " +
				"A term budget rejection has its own reason and is not in invalid_params. " +
				"Server errors are not counted.",
		}, []string{"reason"}),
		returnedEvents: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: metricsSubsystem,
			Name: "returned_events",
			Help: "Events returned by one queryEvents page. " +
				"Pages that scan nothing are not counted.",
			Buckets: returnedEventsBuckets,
		}),
		scannedLedgers: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: metricsSubsystem,
			Name: "scanned_ledgers",
			Help: "Ledgers scanned by one queryEvents page. " +
				"The page label shows how full the page is: empty, partial or full. " +
				"Pages that scan nothing are not counted.",
			Buckets: scannedLedgersBuckets,
		}, []string{"page"}),
	}
	// Every reason exports from the start, at zero, so a rate over one is
	// never an absent series.
	for _, reason := range []string{
		reasonTermBudgetExceeded,
		protocol.ErrorReasonInvalidParams,
		protocol.ErrorReasonLedgerOutOfRange,
		protocol.ErrorReasonCursorMalformed,
	} {
		m.rejections.WithLabelValues(reason)
	}
	for _, page := range []string{pageEmpty, pagePartial, pageFull} {
		m.scannedLedgers.WithLabelValues(page)
	}
	registry.MustRegister(m.distinctTerms, m.rejections, m.returnedEvents, m.scannedLedgers)
	return m
}

func (m *Metrics) observeDistinctTerms(terms int) {
	if m == nil {
		return
	}
	m.distinctTerms.Observe(float64(terms))
}

func (m *Metrics) countRejection(reason string) {
	if m == nil {
		return
	}
	m.rejections.WithLabelValues(reason).Inc()
}

// observePage records one served page that scanned: its event count and
// the ledgers it read.
func (m *Metrics) observePage(start *query.EventCursor, page *query.EventPage, limit int) {
	if m == nil {
		return
	}
	events := len(page.Events)
	// Ledgers the page finished.
	read := scanWatermark(&page.Next) - scanWatermark(start)
	if start.Scope.Dir == query.Descending {
		read = -read
	}
	// Add the ledger the page stopped inside. The events check is needed:
	// a waiting page returns its cursor unchanged, with an old Position.
	if events > 0 && page.Next.Position != nil {
		read++
	}
	// Skip pages that read nothing, such as polls at the tip.
	if read <= 0 {
		return
	}
	fill := pagePartial
	switch {
	case events == 0:
		fill = pageEmpty
	case events >= limit:
		fill = pageFull
	}
	m.returnedEvents.Observe(float64(events))
	m.scannedLedgers.WithLabelValues(fill).Observe(float64(read))
}
