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

// Histogram buckets per tier. Hot observations are per-ledger
// (milliseconds–seconds), so the Prometheus defaults (5ms…10s) fit. Cold
// observations are whole-chunk wall-clocks — download + decompress + three
// stores + Finalize for a 10,000-ledger chunk — realistically tens of seconds
// to tens of minutes, so they get their own range; sharing the default
// buckets would pin every cold sample in the +Inf bucket and peg
// histogram_quantile at the top finite bucket.
//
//nolint:gochecknoglobals // fixed bucket layouts, read-only
var (
	hotBuckets = prometheus.DefBuckets
	// 1s … ~34min, doubling.
	coldBuckets = prometheus.ExponentialBuckets(1, 2, 12)
)

// ingestCollectors bundles the pre-resolved per-(data_type, tier) children.
// The label space is fixed at construction (three data types × two tiers), so
// resolving the children once removes the per-emit label-map allocation and
// hashed vector lookups from the hot per-ledger path.
type ingestCollectors struct {
	duration prometheus.Observer
	items    prometheus.Counter
	errors   prometheus.Counter
}

func (c ingestCollectors) observe(d time.Duration, items int, err error) {
	c.duration.Observe(d.Seconds())
	if items > 0 {
		c.items.Add(float64(items))
	}
	if err != nil {
		c.errors.Inc()
	}
}

// PrometheusSink is a MetricSink that records into Prometheus collectors. It is
// constructed via NewPrometheusSink, which registers its collectors under a
// namespace + the fullhistory_ingest subsystem.
//
// NOTE: daemon wiring (constructing this from Daemon.MetricsRegistry() and
// passing it into the ingest drivers) is a follow-up — there is no full-history
// ingest daemon startup path yet. This type only provides the registerable sink.
type PrometheusSink struct {
	// Pre-resolved per-ingester children, keyed by data type, one map per
	// tier (the duration histograms have per-tier buckets).
	hot  map[string]ingestCollectors
	cold map[string]ingestCollectors
	// The vectors behind the resolved children, kept for the (unexpected)
	// case of a data type outside the construction-time set — resolved on
	// the fly so no signal is ever silently dropped.
	hotDuration  *prometheus.HistogramVec
	coldDuration *prometheus.HistogramVec
	ingestItems  *prometheus.CounterVec
	ingestErrors *prometheus.CounterVec
	// Aggregate per-tier wall-clock: hot per-ledger fan-out, cold per-chunk
	// service lifetime. Separate histograms so each tier gets fitting buckets.
	hotLedgerTotal prometheus.Observer
	coldChunkTotal prometheus.Observer
}

// NewPrometheusSink builds a PrometheusSink and MustRegisters its collectors on
// registry under namespace + the fullhistory_ingest subsystem. namespace is the
// daemon convention value (interfaces.PrometheusNamespace).
func NewPrometheusSink(registry *prometheus.Registry, namespace string) *PrometheusSink {
	hotDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name:    "hot_ingest_duration_seconds",
		Help:    "per-ingester hot Ingest wall-clock (per ledger)",
		Buckets: hotBuckets,
	}, []string{"data_type"})

	coldDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name:    "cold_ingest_duration_seconds",
		Help:    "per-ingester cold wall-clock (per chunk, incl. Finalize)",
		Buckets: coldBuckets,
	}, []string{"data_type"})

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

	hotLedgerTotal := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name:    "hot_ledger_duration_seconds",
		Help:    "aggregate per-ledger wall-clock across all hot ingesters (HotService fan-out)",
		Buckets: hotBuckets,
	})

	coldChunkTotal := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name:    "cold_chunk_duration_seconds",
		Help:    "aggregate per-chunk wall-clock across all cold ingesters (ColdService lifetime)",
		Buckets: coldBuckets,
	})

	registry.MustRegister(hotDuration, coldDuration, ingestItems, ingestErrors, hotLedgerTotal, coldChunkTotal)

	hot := make(map[string]ingestCollectors, 3)
	cold := make(map[string]ingestCollectors, 3)
	for _, dataType := range []string{dataTypeLedgers, dataTypeTxhash, dataTypeEvents} {
		hot[dataType] = ingestCollectors{
			duration: hotDuration.WithLabelValues(dataType),
			items:    ingestItems.WithLabelValues(dataType, "hot"),
			errors:   ingestErrors.WithLabelValues(dataType, "hot"),
		}
		cold[dataType] = ingestCollectors{
			duration: coldDuration.WithLabelValues(dataType),
			items:    ingestItems.WithLabelValues(dataType, "cold"),
			errors:   ingestErrors.WithLabelValues(dataType, "cold"),
		}
	}

	return &PrometheusSink{
		hot:            hot,
		cold:           cold,
		hotDuration:    hotDuration,
		coldDuration:   coldDuration,
		ingestItems:    ingestItems,
		ingestErrors:   ingestErrors,
		hotLedgerTotal: hotLedgerTotal,
		coldChunkTotal: coldChunkTotal,
	}
}

func (p *PrometheusSink) HotIngest(dataType string, d time.Duration, items int, err error) {
	c, ok := p.hot[dataType]
	if !ok {
		c = ingestCollectors{
			duration: p.hotDuration.WithLabelValues(dataType),
			items:    p.ingestItems.WithLabelValues(dataType, "hot"),
			errors:   p.ingestErrors.WithLabelValues(dataType, "hot"),
		}
	}
	c.observe(d, items, err)
}

func (p *PrometheusSink) ColdIngest(dataType string, d time.Duration, items int, err error) {
	c, ok := p.cold[dataType]
	if !ok {
		c = ingestCollectors{
			duration: p.coldDuration.WithLabelValues(dataType),
			items:    p.ingestItems.WithLabelValues(dataType, "cold"),
			errors:   p.ingestErrors.WithLabelValues(dataType, "cold"),
		}
	}
	c.observe(d, items, err)
}

func (p *PrometheusSink) HotLedgerTotal(d time.Duration) {
	p.hotLedgerTotal.Observe(d.Seconds())
}

func (p *PrometheusSink) ColdChunkTotal(d time.Duration) {
	p.coldChunkTotal.Observe(d.Seconds())
}
