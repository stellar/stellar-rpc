package ingest

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
)

// Data-type labels reported to a MetricSink. These match the per-type
// subdirectory names used on disk. (The hot tier keys its per-ledger phases by
// hotchunk.Phase, not by data type — see MetricSink.HotPhase.)
const (
	dataTypeLedgers = "ledgers"
	dataTypeTxhash  = "txhash"
	dataTypeEvents  = "events"
	// dataTypeShared labels the ONE per-ledger ExtractLedgerEvents walk the cold
	// service runs once and shares across txhash + events (issue #836) — it is
	// not a stored data type, only the scope of the shared extract stage, mirroring
	// the hot path's single (data-type-less) extract phase.
	dataTypeShared = "shared"
)

// Cold stage labels reported via MetricSink.IngestStage. These sit at the seams
// the rpc-hack bench collectors measured (per-stage extract / term-index /
// store-write samples plus a per-chunk finish), so a CSV sink can reproduce
// those reports from production ingesters without re-instrumenting.
const (
	stageExtract   = "extract"    // the shared per-ledger ExtractLedgerEvents walk (ColdService)
	stageTermIndex = "term_index" // per-event term derivation + mirror update (events cold)
	stageWrite     = "write"      // store write / pack append
	stageFinalize  = "finalize"   // per-chunk commit (pack trailer, index build, .bin write)
)

// coldStagePairs is the set of (data_type, stage) pairs the cold path actually
// emits — the seven real ones, not a cross-product. A sink pre-resolves exactly
// these, so it registers no series no code path can feed. The extract stage is
// keyed dataTypeShared: after issue #836 the ledger walk is done once by
// ColdService and shared, so it is metered once per ledger rather than once per
// consuming type. txhash's per-ledger work is a cheap truncate-append that folds
// into its ColdIngest total (its stage cost is the finalize sort + .bin write).
//
//nolint:gochecknoglobals // fixed label set, read-only
var coldStagePairs = []struct{ dataType, stage string }{
	{dataTypeShared, stageExtract},
	{dataTypeLedgers, stageWrite},
	{dataTypeLedgers, stageFinalize},
	{dataTypeTxhash, stageFinalize},
	{dataTypeEvents, stageTermIndex},
	{dataTypeEvents, stageWrite},
	{dataTypeEvents, stageFinalize},
}

// MetricSink receives ingest timing and volume signals. Ingesters report their
// own per-call latency / item counts / errors (they know the item count); the
// per-tier services report aggregate per-ledger (hot) and per-chunk (cold)
// wall-clock. A sink lets the same ingesters/services feed Prometheus in prod,
// a CSV recorder in benchmarks, or a test recorder — interchangeably.
//
// Implementations must be safe for concurrent use across ALL methods: the live
// hot ingestion loop reports HotPhase from its own goroutine while the lifecycle
// may freeze several chunks concurrently (each its own WriteColdChunk), so the
// cold methods (ColdIngest, ColdChunkTotal, IngestStage) can likewise be called
// from several goroutines at once.
type MetricSink interface {
	// HotPhase reports ONE phase of one hot ledger ingest — the single hot-tier
	// signal family. It carries that phase's wall-clock, its item count (0 for the
	// extract/commit/apply phases, the per-type write volume for the write phases, on
	// the success path), and its outcome (err is non-nil only on the phase that
	// failed, so a decode failure lands on PhaseExtract and a commit failure on
	// PhaseCommit by construction; the post-commit PhaseApply runs on success only, so
	// it never carries an error). The per-ledger total is the sum of the phase
	// durations; the caller emits phases [0, Failed] on error and all phases on success.
	HotPhase(phase hotchunk.Phase, d time.Duration, items int, err error)
	// ColdIngest reports one cold ingester's per-chunk total: the summed Ingest
	// wall-clock plus its Finalize, items the total items written for the chunk,
	// err the first error (nil on success).
	ColdIngest(dataType string, d time.Duration, items int, err error)
	// ColdChunkTotal reports the per-chunk aggregate wall-clock: the whole
	// ColdService lifetime, from construction through the drain of the source stream
	// and every Ingest and Finalize, to the single emit.
	ColdChunkTotal(d time.Duration)
	// IngestStage reports one COLD ingester's per-stage wall-clock inside an
	// Ingest/Finalize call: stage is one of the stage* constants (extract,
	// term_index, write, finalize), items the stage's natural item count (0 where
	// none applies). The whole-call ColdIngest signal cannot be decomposed by a
	// sink after the fact, so the per-stage granularity the bench reports need is
	// exposed as its own signal — a sink that doesn't want it can no-op it.
	IngestStage(dataType, stage string, d time.Duration, items int)
}

// NopSink is a MetricSink that discards everything. It is the default when a
// caller passes a nil sink to a service or ingester.
type NopSink struct{}

func (NopSink) HotPhase(hotchunk.Phase, time.Duration, int, error) {}
func (NopSink) ColdIngest(string, time.Duration, int, error)       {}
func (NopSink) ColdChunkTotal(time.Duration)                       {}
func (NopSink) IngestStage(string, string, time.Duration, int)     {}

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

// ingestCollectors bundles the pre-resolved per-cold-data-type children. The
// label space is fixed at construction, so resolving the children once removes
// the per-emit label-map allocation and hashed vector lookup.
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
// namespace + the fullhistory_ingest subsystem. The daemon's buildSinks constructs
// it on the shared registry and wires it into both the cold and hot ingest paths.
type PrometheusSink struct {
	// Hot per-ledger phases — the single hot signal family, one set of children per
	// hotchunk.Phase, indexed by the phase value into a fixed-size ARRAY (not a map),
	// so an out-of-table phase is a bounds panic at the index rather than a silent
	// nil-map emit. The per-ledger total is the sum of hotPhaseDur; commit errors are
	// hotPhaseErrs[PhaseCommit]; decode errors hotPhaseErrs[PhaseExtract].
	hotPhaseDur   [hotchunk.NumPhases]prometheus.Observer
	hotPhaseItems [hotchunk.NumPhases]prometheus.Counter
	hotPhaseErrs  [hotchunk.NumPhases]prometheus.Counter
	// Pre-resolved per-cold-ingester children, keyed by data type. Producers draw
	// their data_type from the same constant set the map is built from, so a lookup
	// can never miss — indexed directly, no on-the-fly vector fallback.
	cold map[string]ingestCollectors
	// Per-cold-stage durations, pre-resolved for the eight real (data_type, stage)
	// pairs only (coldStagePairs), keyed "dataType/stage".
	coldStage map[string]prometheus.Observer
	// Aggregate per-chunk cold wall-clock (ColdService lifetime).
	coldChunkTotal prometheus.Observer
}

// NewPrometheusSink builds a PrometheusSink and MustRegisters its collectors on
// registry under namespace + the fullhistory_ingest subsystem. namespace is the
// daemon convention value (interfaces.PrometheusNamespace).
func NewPrometheusSink(registry *prometheus.Registry, namespace string) *PrometheusSink {
	hotPhaseDurVec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name: "hot_phase_duration_seconds",
		Help: "per-ledger phase wall-clock (extract/ledgers/txhash/events/commit/apply; " +
			"phases sum to the per-ledger total)",
		Buckets: hotBuckets,
	}, []string{"phase"})

	hotPhaseItemsVec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name: "hot_phase_items_total",
		Help: "items written per hot phase (the write phases carry per-type volume; extract/commit/apply are 0)",
	}, []string{"phase"})

	hotPhaseErrsVec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name: "hot_phase_errors_total",
		Help: "hot ledger failures by the phase that failed (decode->extract, commit->commit, by construction)",
	}, []string{"phase"})

	coldDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name:    "cold_ingest_duration_seconds",
		Help:    "per-ingester cold wall-clock (per chunk, incl. Finalize)",
		Buckets: coldBuckets,
	}, []string{"data_type"})

	coldItems := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name: "cold_items_total",
		Help: "items written per cold ingester (events, txhashes, or ledgers)",
	}, []string{"data_type"})

	coldErrors := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name: "cold_errors_total",
		Help: "cold ingester Ingest/Finalize errors",
	}, []string{"data_type"})

	coldChunkTotal := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name:    "cold_chunk_duration_seconds",
		Help:    "aggregate per-chunk wall-clock across all cold ingesters (ColdService lifetime)",
		Buckets: coldBuckets,
	})

	coldStageVec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name: "cold_stage_duration_seconds",
		Help: "per-stage wall-clock inside a cold Ingest/Finalize " +
			"(extract, term_index, write, finalize; not every data type emits every stage)",
		Buckets: coldStageBuckets,
	}, []string{"data_type", "stage"})

	registry.MustRegister(hotPhaseDurVec, hotPhaseItemsVec, hotPhaseErrsVec,
		coldDuration, coldItems, coldErrors, coldChunkTotal, coldStageVec)

	sink := &PrometheusSink{
		cold:           make(map[string]ingestCollectors, 3),
		coldStage:      make(map[string]prometheus.Observer, len(coldStagePairs)),
		coldChunkTotal: coldChunkTotal,
	}
	// Hot phases: one child per phase, indexed by the phase value.
	for p := range hotchunk.NumPhases {
		sink.hotPhaseDur[p] = hotPhaseDurVec.WithLabelValues(p.String())
		sink.hotPhaseItems[p] = hotPhaseItemsVec.WithLabelValues(p.String())
		sink.hotPhaseErrs[p] = hotPhaseErrsVec.WithLabelValues(p.String())
	}
	for _, dataType := range []string{dataTypeLedgers, dataTypeTxhash, dataTypeEvents} {
		sink.cold[dataType] = ingestCollectors{
			duration: coldDuration.WithLabelValues(dataType),
			items:    coldItems.WithLabelValues(dataType),
			errors:   coldErrors.WithLabelValues(dataType),
		}
	}
	// Cold stages: only the eight real (data_type, stage) pairs.
	for _, k := range coldStagePairs {
		sink.coldStage[k.dataType+"/"+k.stage] = coldStageVec.WithLabelValues(k.dataType, k.stage)
	}
	return sink
}

func (p *PrometheusSink) HotPhase(phase hotchunk.Phase, d time.Duration, items int, err error) {
	p.hotPhaseDur[phase].Observe(d.Seconds())
	if items > 0 {
		p.hotPhaseItems[phase].Add(float64(items))
	}
	if err != nil {
		p.hotPhaseErrs[phase].Inc()
	}
}

func (p *PrometheusSink) ColdIngest(dataType string, d time.Duration, items int, err error) {
	p.cold[dataType].observe(d, items, err)
}

func (p *PrometheusSink) ColdChunkTotal(d time.Duration) {
	p.coldChunkTotal.Observe(d.Seconds())
}

// IngestStage records the per-stage cold duration. The per-stage item counts are
// not exported to Prometheus (cold_items_total already carries volume); they exist
// on the interface for the CSV bench sink.
func (p *PrometheusSink) IngestStage(dataType, stage string, d time.Duration, _ int) {
	p.coldStage[dataType+"/"+stage].Observe(d.Seconds())
}
