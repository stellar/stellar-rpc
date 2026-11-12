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

// Stop labels for the scanned-ledgers histogram: why the page stopped.
// The window_edge share shows how often query.MaxScanLedgers ends a page.
const (
	stopPageFilled    = "page_filled"
	stopWindowEdge    = "window_edge"
	stopEndOfRange    = "end_of_range"
	stopTip           = "tip"
	stopOldestReached = "oldest_reached"
)

// returnedEventsBuckets end at protocol.QueryEventsMaxLimit.
//
//nolint:gochecknoglobals // fixed bucket layout, read-only
var returnedEventsBuckets = []float64{0, 1, 10, 50, 100, 250, 500, protocol.QueryEventsMaxLimit}

// scannedLedgersBuckets end at query.MaxScanLedgers, the most one page scans.
// The 0 bucket holds the pages that scan nothing, such as polls at the tip.
//
//nolint:gochecknoglobals // fixed bucket layout, read-only
var scannedLedgersBuckets = []float64{0, 1, 10, 100, 500, 1000, 2500, 5000, float64(query.MaxScanLedgers)}

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
			Help: "Distinct index terms named by the filters of one queryEvents query. " +
				"A request with a cursor continues a query and is not counted. " +
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
				"The stop label shows why the page stopped. " +
				"A page that scans nothing counts as 0. " +
				"A page that fills counts through the ledger of the next match, " +
				"so two pages can count the same ledger.",
			Buckets: scannedLedgersBuckets,
		}, []string{"stop"}),
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
	for _, stop := range []string{
		stopPageFilled, stopWindowEdge, stopEndOfRange, stopTip, stopOldestReached,
	} {
		m.scannedLedgers.WithLabelValues(stop)
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

// observePage records one served page: the ledgers it read and, when it
// read any, its event count.
func (m *Metrics) observePage(page *query.EventPage) {
	if m == nil {
		return
	}
	m.scannedLedgers.WithLabelValues(stopReason(page)).Observe(float64(page.LedgersRead))
	// The event count has no label, so polls at the tip would hide the
	// pages that scanned.
	if page.LedgersRead != 0 {
		m.returnedEvents.Observe(float64(len(page.Events)))
	}
}

// stopReason is why the page stopped. The pager's status gives every
// reason but one: ScanHasMore is a page that filled or a window that ended.
func stopReason(page *query.EventPage) string {
	switch page.Status {
	case query.ScanComplete:
		return stopEndOfRange
	case query.ScanWaitingForLedgers:
		return stopTip
	case query.ScanOldestReached:
		return stopOldestReached
	case query.ScanHasMore:
	}
	if page.WindowEdge {
		return stopWindowEdge
	}
	return stopPageFilled
}
