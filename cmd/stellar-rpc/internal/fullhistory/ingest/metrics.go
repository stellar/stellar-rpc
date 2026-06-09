package ingest

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Data-type labels reported to a MetricSink. These match the per-type
// subdirectory names used on disk.
const (
	dataTypeLedgers = "ledgers"
	dataTypeTxhash  = "txhash"
	dataTypeEvents  = "events"
)

// MetricSink receives ingest timing and volume signals. Ingesters report their
// own per-call latency / item counts / errors (they know the item count); the
// per-tier services report aggregate per-ledger (hot) and per-chunk (cold)
// wall-clock. A sink lets the same ingesters/services feed Prometheus in prod,
// a CSV recorder in benchmarks, or a test recorder — interchangeably.
//
// Implementations must be safe for concurrent use across ALL methods, not just
// HotIngest: the hot fan-out calls HotIngest/HotLedgerTotal from per-ledger
// goroutines, and RunCold drives multiple chunk workers concurrently, so the
// cold methods (ColdIngest, ColdChunkTotal) are likewise called from several
// goroutines at once.
type MetricSink interface {
	// HotIngest reports one hot ingester's per-ledger Ingest: dataType is the
	// data-type label, d the wall-clock, items the number of items written
	// (events, txhashes, or 1 for a ledger), err the Ingest error (nil on
	// success).
	HotIngest(dataType string, d time.Duration, items int, err error)
	// ColdIngest reports one cold ingester's per-chunk total: the summed Ingest
	// wall-clock plus its Finalize, items the total items written for the chunk,
	// err the first error (nil on success).
	ColdIngest(dataType string, d time.Duration, items int, err error)
	// HotLedgerTotal reports the per-ledger wall-clock across all hot ingesters
	// (the HotService.Ingest fan-out duration).
	HotLedgerTotal(d time.Duration)
	// ColdChunkTotal reports the per-chunk wall-clock across all cold ingesters'
	// ingests plus their Finalizes (the ColdService lifetime).
	ColdChunkTotal(d time.Duration)
}

// NopSink is a MetricSink that discards everything. It is the default when a
// caller passes a nil sink to a service or ingester.
type NopSink struct{}

func (NopSink) HotIngest(string, time.Duration, int, error)  {}
func (NopSink) ColdIngest(string, time.Duration, int, error) {}
func (NopSink) HotLedgerTotal(time.Duration)                 {}
func (NopSink) ColdChunkTotal(time.Duration)                 {}

// orNop returns sink, or NopSink{} when sink is nil, so call sites never
// nil-check before reporting.
func orNop(sink MetricSink) MetricSink {
	if sink == nil {
		return NopSink{}
	}
	return sink
}

// hotMetrics emits a single HotIngest signal for one hot ingester's per-ledger
// Ingest. The ingester sets items as it learns the count, then a single deferred
// emit reports the wall-clock since start, the final item count, and the WRAPPED
// error captured from the named return — so every Ingest has exactly one emit
// site regardless of which return path it takes.
//
// Usage:
//
//	func (h *fooHot) Ingest(...) (err error) {
//	    m := newHotMetrics(h.sink, dataTypeFoo)
//	    defer func() { m.emit(err) }()
//	    ...
//	    m.items = len(things)
//	    return nil
//	}
type hotMetrics struct {
	sink     MetricSink
	dataType string
	start    time.Time
	items    int
}

func newHotMetrics(sink MetricSink, dataType string) hotMetrics {
	return hotMetrics{sink: orNop(sink), dataType: dataType, start: time.Now()}
}

// emit reports the single HotIngest signal: the wall-clock since construction,
// the accumulated item count, and the (wrapped) error from the named return.
func (m *hotMetrics) emit(err error) {
	m.sink.HotIngest(m.dataType, time.Since(m.start), m.items, err)
}

// coldMetrics is the per-chunk metric accumulator shared by all three cold
// ingesters. Each ingester accumulates Ingest wall-clock (accum), item count
// (items), and the FIRST error it saw (firstErr) across the chunk, then emits a
// single ColdIngest signal — in Finalize if reached, otherwise in Close (the
// failure path). The emitted flag guards against a double-emit: a successful
// Finalize emits and sets emitted=true so the deferred Close is a no-op, while a
// chunk that errors before Finalize emits exactly once from Close.
//
// This guarantees: failed chunk → one ColdIngest with the error recorded;
// success → exactly one ColdIngest per ingester; never both.
type coldMetrics struct {
	sink     MetricSink
	dataType string
	accum    time.Duration
	items    int
	firstErr error
	emitted  bool
}

func newColdMetrics(sink MetricSink, dataType string) coldMetrics {
	return coldMetrics{sink: orNop(sink), dataType: dataType}
}

// recordErr folds err into firstErr WITHOUT emitting. Used on the
// constructor-rollback path so the subsequent Close emit carries the abort
// error instead of looking like a clean (nil-err, 0-items) success.
func (m *coldMetrics) recordErr(err error) {
	if err != nil {
		m.firstErr = errOrFirst(m.firstErr, err)
	}
}

// observe records one Ingest's wall-clock and (on error) the first error.
func (m *coldMetrics) observe(d time.Duration, items int, err error) {
	m.accum += d
	m.items += items
	if err != nil {
		m.firstErr = errOrFirst(m.firstErr, err)
	}
}

// emit reports the single ColdIngest signal for this ingester, adding extra to
// the accumulated Ingest time (e.g. the Finalize wall-clock) and folding err
// (if non-nil) into firstErr before reporting. It is a no-op after the first
// call, so calling it from both Finalize (success) and Close (deferred cleanup)
// emits exactly once. Pass a nil err when there is no stage error to record.
func (m *coldMetrics) emit(extra time.Duration, err error) {
	if err != nil {
		m.firstErr = errOrFirst(m.firstErr, err)
	}
	if m.emitted {
		return
	}
	m.emitted = true
	m.sink.ColdIngest(m.dataType, m.accum+extra, m.items, m.firstErr)
}

// metricsSubsystem is the Prometheus subsystem for all full-history ingest
// metrics, under the daemon's namespace (interfaces.PrometheusNamespace).
const metricsSubsystem = "fullhistory_ingest"

// PrometheusSink is a MetricSink that records into Prometheus collectors. It is
// constructed via NewPrometheusSink, which registers its collectors under a
// namespace + the fullhistory_ingest subsystem.
//
// NOTE: daemon wiring (constructing this from Daemon.MetricsRegistry() and
// passing it into the ingest drivers) is a follow-up — there is no full-history
// ingest daemon startup path yet. This type only provides the registerable sink.
type PrometheusSink struct {
	// Per-ingester latency (seconds), keyed by data_type + tier (hot|cold).
	ingestDuration *prometheus.HistogramVec
	// Per-ingester items written, keyed by data_type + tier.
	ingestItems *prometheus.CounterVec
	// Per-ingester errors, keyed by data_type + tier.
	ingestErrors *prometheus.CounterVec
	// Aggregate per-tier wall-clock (seconds): hot per-ledger, cold per-chunk.
	tierDuration *prometheus.HistogramVec
}

// NewPrometheusSink builds a PrometheusSink and MustRegisters its collectors on
// registry under namespace + the fullhistory_ingest subsystem. namespace is the
// daemon convention value (interfaces.PrometheusNamespace).
func NewPrometheusSink(registry *prometheus.Registry, namespace string) *PrometheusSink {
	ingestDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name:    "ingest_duration_seconds",
		Help:    "per-ingester Ingest wall-clock (hot=per-ledger, cold=per-chunk incl. Finalize)",
		Buckets: prometheus.DefBuckets,
	}, []string{"data_type", "tier"})

	ingestItems := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name: "items_total",
		Help: "items written per ingester (events, txhashes, or ledgers)",
	}, []string{"data_type", "tier"})

	ingestErrors := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name: "errors_total",
		Help: "ingester Ingest/Finalize errors",
	}, []string{"data_type", "tier"})

	tierDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name:    "tier_duration_seconds",
		Help:    "aggregate per-tier wall-clock (hot=per-ledger fan-out, cold=per-chunk service)",
		Buckets: prometheus.DefBuckets,
	}, []string{"tier"})

	registry.MustRegister(ingestDuration, ingestItems, ingestErrors, tierDuration)

	return &PrometheusSink{
		ingestDuration: ingestDuration,
		ingestItems:    ingestItems,
		ingestErrors:   ingestErrors,
		tierDuration:   tierDuration,
	}
}

func (p *PrometheusSink) perIngester(dataType, tier string, d time.Duration, items int, err error) {
	labels := prometheus.Labels{"data_type": dataType, "tier": tier}
	p.ingestDuration.With(labels).Observe(d.Seconds())
	if items > 0 {
		p.ingestItems.With(labels).Add(float64(items))
	}
	if err != nil {
		p.ingestErrors.With(labels).Inc()
	}
}

func (p *PrometheusSink) HotIngest(dataType string, d time.Duration, items int, err error) {
	p.perIngester(dataType, "hot", d, items, err)
}

func (p *PrometheusSink) ColdIngest(dataType string, d time.Duration, items int, err error) {
	p.perIngester(dataType, "cold", d, items, err)
}

func (p *PrometheusSink) HotLedgerTotal(d time.Duration) {
	p.tierDuration.With(prometheus.Labels{"tier": "hot"}).Observe(d.Seconds())
}

func (p *PrometheusSink) ColdChunkTotal(d time.Duration) {
	p.tierDuration.With(prometheus.Labels{"tier": "cold"}).Observe(d.Seconds())
}
