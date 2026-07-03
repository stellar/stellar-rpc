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
	// dataTypeBatch labels the hot per-ledger phases that are ledger-scoped rather
	// than per data type — the shared extract walk and the batch commit — so they
	// share one axis instead of being triple-counted across the three types.
	dataTypeBatch = "batch"
)

// Tier labels reported to a MetricSink.
const (
	tierHot  = "hot"
	tierCold = "cold"
)

// Stage labels reported via MetricSink.IngestStage. These sit at the seams
// the rpc-hack bench collectors measured (per-stage extract / term-index /
// store-write samples plus a per-chunk finish), so a CSV sink can reproduce
// those reports from production ingesters without re-instrumenting.
const (
	stageExtract   = "extract"    // view → payloads / hashes derivation
	stageTermIndex = "term_index" // per-event term derivation + mirror update (events cold)
	stageWrite     = "write"      // store write / pack append (cold) / queue-into-batch (hot)
	stageFinalize  = "finalize"   // per-chunk commit (pack trailer, index build, .bin write)
	stageCommit    = "commit"     // hot: the RocksDB batch write (WAL append + fsync + memtable)
)

// MetricSink receives ingest timing and volume signals. Ingesters report their
// own per-call latency / item counts / errors (they know the item count); the
// per-tier services report aggregate per-ledger (hot) and per-chunk (cold)
// wall-clock. A sink lets the same ingesters/services feed Prometheus in prod,
// a CSV recorder in benchmarks, or a test recorder — interchangeably.
//
// Implementations must be safe for concurrent use across ALL methods: the live
// hot ingestion loop reports HotItems/HotLedgerTotal from its own goroutine
// while the lifecycle may freeze several chunks concurrently (each its own
// WriteColdChunk), so the cold methods (ColdIngest, ColdChunkTotal) can likewise
// be called from several goroutines at once.
type MetricSink interface {
	// HotItems reports the per-type volume of one HotService.Ingest: how many items
	// (events, txhashes, or 1 ledger) that type contributed to the ledger's atomic
	// batch. Emitted on the success path only — a failed atomic batch wrote nothing
	// durably. There is deliberately no per-type hot DURATION or ERROR: the whole
	// ledger commits as ONE synced batch (one fsync), and post-#18 a single shared
	// ExtractLedgerEvents walk feeds both txhash and events, so neither timing nor
	// an extraction failure is attributable per type — both live on HotLedgerTotal.
	HotItems(dataType string, items int)
	// ColdIngest reports one cold ingester's per-chunk total: the summed Ingest
	// wall-clock plus its Finalize, items the total items written for the chunk,
	// err the first error (nil on success).
	ColdIngest(dataType string, d time.Duration, items int, err error)
	// HotLedgerTotal is the ONE batch-level signal per hot ledger: d is the
	// wall-clock of the single atomic synced WriteBatch across all CFs, err its
	// commit outcome (nil on success). It carries the hot tier's only honest
	// per-ledger duration and error — attribution is batch-scoped, not per type.
	HotLedgerTotal(d time.Duration, err error)
	// ColdChunkTotal reports the per-chunk wall-clock across all cold ingesters'
	// ingests plus their Finalizes (the ColdService lifetime).
	ColdChunkTotal(d time.Duration)
	// IngestStage reports one ingester's per-stage wall-clock INSIDE an
	// Ingest/Finalize call: stage is one of the stage* constants (extract,
	// term_index, write, finalize), tier "hot" or "cold", items the stage's
	// natural item count (0 where none applies). The whole-call HotLedgerTotal /
	// ColdIngest signals above cannot be decomposed by a sink after the
	// fact, so the per-stage granularity the bench reports need is exposed
	// as its own signal — a sink that doesn't want it (production
	// Prometheus, optionally) can no-op it.
	IngestStage(dataType, tier, stage string, d time.Duration, items int)
}

// NopSink is a MetricSink that discards everything. It is the default when a
// caller passes a nil sink to a service or ingester.
type NopSink struct{}

func (NopSink) HotItems(string, int)                                   {}
func (NopSink) ColdIngest(string, time.Duration, int, error)           {}
func (NopSink) HotLedgerTotal(time.Duration, error)                    {}
func (NopSink) ColdChunkTotal(time.Duration)                           {}
func (NopSink) IngestStage(string, string, string, time.Duration, int) {}

// orNop returns sink, or NopSink{} when sink is nil, so call sites never
// nil-check before reporting.
func orNop(sink MetricSink) MetricSink {
	if sink == nil {
		return NopSink{}
	}
	return sink
}

// coldMetrics is the per-chunk metric accumulator shared by all three cold
// ingesters. Each ingester accumulates Ingest wall-clock (accum), item count
// (items), and the FIRST error it saw (firstErr) across the chunk, then emits a
// single ColdIngest signal on a TERMINAL step only: Finalize (success or error),
// or an Ingest error (which abandons the chunk). Close NEVER emits — an ingester
// that was built but never ingested/finalized (e.g. a sibling constructor failed
// and the build rolled back) produces NO phantom sample. The emitted flag guards
// against a double-emit so the guarantee holds even if a defensive caller drives
// the terminal steps redundantly.
//
// This guarantees: a chunk that ingested and then failed/finalized → exactly one
// ColdIngest (error recorded on failure); a rolled-back ingester → none.
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

// observe records one Ingest's wall-clock and (on error) the first error. An
// Ingest error is TERMINAL by the ColdIngester contract (the chunk is abandoned
// and the ingester is never reused), so observe emits the single per-ingester
// ColdIngest itself here — callers just observe-and-return, no hand-paired emit.
func (m *coldMetrics) observe(d time.Duration, items int, err error) {
	m.accum += d
	m.items += items
	if err != nil {
		m.firstErr = errOrFirst(m.firstErr, err)
		m.emit(0, nil)
	}
}

// emit reports the single ColdIngest signal for this ingester, adding extra to
// the accumulated Ingest time (e.g. the Finalize wall-clock) and folding err
// (if non-nil) into firstErr before reporting. It is a no-op after the first
// call, so a redundant terminal-step call emits exactly once. Pass a nil err
// when the error is already recorded (an Ingest failure observes it) or there is
// none.
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
	// Cold STAGE observations span per-ledger sub-stages (sub-millisecond
	// extract/append) through the per-chunk finalize (minutes): 1ms … ~70min,
	// ×4 per bucket.
	coldStageBuckets = prometheus.ExponentialBuckets(0.001, 4, 12)
)

// ingestStages is the construction-time stage label set used to pre-resolve
// the per-(data_type, stage) children.
//
//nolint:gochecknoglobals // fixed label set, read-only
var ingestStages = []string{stageExtract, stageTermIndex, stageWrite, stageFinalize}

// hotPhaseKeys is the fixed set of per-ledger phase children the hot ingest path
// reports via IngestStage(dataType, tierHot, stage): the shared extract walk and
// the batch commit under the batch pseudo-type, plus per-type queue-into-batch
// timings under stageWrite. Not the cold cross-product — the hot path has its own
// phase taxonomy — so the sink pre-resolves exactly these keys.
//
//nolint:gochecknoglobals // fixed label set, read-only
var hotPhaseKeys = []struct{ dataType, stage string }{
	{dataTypeBatch, stageExtract},
	{dataTypeLedgers, stageWrite},
	{dataTypeTxhash, stageWrite},
	{dataTypeEvents, stageWrite},
	{dataTypeBatch, stageCommit},
}

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
	// Per-type hot volume counters (HotItems), keyed by data type. The hot tier has
	// no per-type duration or error — one atomic batch, one shared extraction walk —
	// so unlike cold it needs only an item counter per type.
	hotItems map[string]prometheus.Counter
	// hotCommitErrors counts failed hot batch commits (HotLedgerTotal's err); it is
	// batch-scoped (not per data type) because one fsync commits all CFs together.
	hotCommitErrors prometheus.Counter
	// Pre-resolved per-cold-ingester children, keyed by data type (duration
	// histogram, items counter, errors counter). Producers draw their data_type
	// from the same unexported constant set the map is built from, so a lookup can
	// never miss — indexed directly, with no on-the-fly vector fallback.
	cold map[string]ingestCollectors
	// Per-stage durations (IngestStage), pre-resolved per
	// (data_type, stage) with per-tier buckets, keyed "dataType/stage".
	hotStage  map[string]prometheus.Observer
	coldStage map[string]prometheus.Observer
	// Aggregate per-tier wall-clock: hot per-ledger batch, cold per-chunk
	// service lifetime. Separate histograms so each tier gets fitting buckets.
	hotLedgerTotal prometheus.Observer
	coldChunkTotal prometheus.Observer
}

// NewPrometheusSink builds a PrometheusSink and MustRegisters its collectors on
// registry under namespace + the fullhistory_ingest subsystem. namespace is the
// daemon convention value (interfaces.PrometheusNamespace).
func NewPrometheusSink(registry *prometheus.Registry, namespace string) *PrometheusSink {
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
		Help:    "per-ledger wall-clock of one HotService.Ingest (single atomic batch across all CFs)",
		Buckets: hotBuckets,
	})

	hotCommitErrors := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name: "hot_commit_errors_total",
		Help: "failed hot batch commits (batch-scoped: one fsync commits all CFs)",
	})

	coldChunkTotal := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name:    "cold_chunk_duration_seconds",
		Help:    "aggregate per-chunk wall-clock across all cold ingesters (ColdService lifetime)",
		Buckets: coldBuckets,
	})

	hotStageVec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name: "hot_stage_duration_seconds",
		Help: "per-ledger phase wall-clock (batch/extract, {ledgers,txhash,events}/write, " +
			"batch/commit; the phases sum to the per-ledger total)",
		Buckets: hotBuckets,
	}, []string{"data_type", "stage"})

	coldStageVec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name: "cold_stage_duration_seconds",
		Help: "per-stage wall-clock inside a cold Ingest/Finalize " +
			"(extract, term_index, write, finalize; not every data type emits every stage)",
		Buckets: coldStageBuckets,
	}, []string{"data_type", "stage"})

	registry.MustRegister(coldDuration, ingestItems, ingestErrors,
		hotLedgerTotal, hotCommitErrors, coldChunkTotal, hotStageVec, coldStageVec)

	hotItems := make(map[string]prometheus.Counter, 3)
	cold := make(map[string]ingestCollectors, 3)
	coldStage := make(map[string]prometheus.Observer, 3*len(ingestStages))
	for _, dataType := range []string{dataTypeLedgers, dataTypeTxhash, dataTypeEvents} {
		hotItems[dataType] = ingestItems.WithLabelValues(dataType, tierHot)
		cold[dataType] = ingestCollectors{
			duration: coldDuration.WithLabelValues(dataType),
			items:    ingestItems.WithLabelValues(dataType, tierCold),
			errors:   ingestErrors.WithLabelValues(dataType, tierCold),
		}
		for _, stage := range ingestStages {
			coldStage[dataType+"/"+stage] = coldStageVec.WithLabelValues(dataType, stage)
		}
	}
	// Hot phases are a fixed 5-key set (not the cold cross-product).
	hotStage := make(map[string]prometheus.Observer, len(hotPhaseKeys))
	for _, k := range hotPhaseKeys {
		hotStage[k.dataType+"/"+k.stage] = hotStageVec.WithLabelValues(k.dataType, k.stage)
	}

	return &PrometheusSink{
		hotItems:        hotItems,
		hotCommitErrors: hotCommitErrors,
		cold:            cold,
		hotStage:        hotStage,
		coldStage:       coldStage,
		hotLedgerTotal:  hotLedgerTotal,
		coldChunkTotal:  coldChunkTotal,
	}
}

func (p *PrometheusSink) HotItems(dataType string, items int) {
	if items > 0 {
		p.hotItems[dataType].Add(float64(items))
	}
}

func (p *PrometheusSink) ColdIngest(dataType string, d time.Duration, items int, err error) {
	p.cold[dataType].observe(d, items, err)
}

func (p *PrometheusSink) HotLedgerTotal(d time.Duration, err error) {
	p.hotLedgerTotal.Observe(d.Seconds())
	if err != nil {
		p.hotCommitErrors.Inc()
	}
}

func (p *PrometheusSink) ColdChunkTotal(d time.Duration) {
	p.coldChunkTotal.Observe(d.Seconds())
}

// IngestStage records the per-stage duration into the tier's stage histogram.
// The per-stage item counts are not exported to Prometheus (the per-Ingest
// items_total already carries volume); they exist on the interface for the
// CSV bench sink.
func (p *PrometheusSink) IngestStage(dataType, tier, stage string, d time.Duration, _ int) {
	resolved := p.hotStage
	if tier == tierCold {
		resolved = p.coldStage
	}
	resolved[dataType+"/"+stage].Observe(d.Seconds())
}
