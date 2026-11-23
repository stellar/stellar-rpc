package ingest

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

// Data-type labels reported to a MetricSink: the data_type label values. They
// name what is counted, so txhashes is plural like the phase label and is not
// the on-disk "txhash" directory name. (The hot tier reports its per-ledger
// phases by hotchunk.Phase — see MetricSink.HotPhase. PrometheusSink maps the
// three write phases to these labels for its item counter.)
const (
	dataTypeLedgers = "ledgers"
	dataTypeTxhash  = "txhashes"
	dataTypeEvents  = "events"
)

// Cold stage labels reported via MetricSink.IngestStage. These sit at the seams
// the rpc-hack bench collectors measured (per-stage term-index / store-write
// samples plus a per-chunk finish), so a CSV sink can reproduce those reports
// from production writers without re-instrumenting. The shared per-ledger
// ExtractLedgerTxParts walk is NOT a stage: it belongs to no single data type,
// so it is its own ledger-scoped signal (MetricSink.ColdExtract), mirroring the
// hot path's type-less extract phase.
const (
	stageTermIndex = "term_index" // per-event term derivation + mirror update (events cold)
	stageWrite     = "write"      // store write / pack append
	stageFinalize  = "finalize"   // per-chunk commit (pack trailer, index build, .bin write)
)

// coldStagePairs is the set of (data_type, stage) pairs the cold path actually
// emits — the six real ones, not a cross-product. A sink pre-resolves exactly
// these, so it registers no series no code path can feed. txhash's per-ledger
// work is a cheap truncate-append that folds into its ColdIngest total (its
// stage cost is the finalize sort + .bin write).
//
//nolint:gochecknoglobals // fixed label set, read-only
var coldStagePairs = []struct{ dataType, stage string }{
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
	// HotPhase reports ONE phase of one hot ledger ingest. It carries that
	// phase's wall-clock, its item count (0 for the extract/commit/apply
	// phases, the per-type write volume for the write phases, on
	// the success path), and its outcome (err is non-nil only on the phase that
	// failed, so a decode failure lands on PhaseExtract and a commit failure on
	// PhaseCommit by construction; the post-commit PhaseApply runs on success only, so
	// it never carries an error). The caller emits phases [0, Failed] on error and
	// all phases on success. The phases do not include the fee append:
	// HotLedger is the per-ledger total.
	HotPhase(phase hotchunk.Phase, d time.Duration, items int, err error)
	// HotLedger reports the whole wall-clock of one hot ledger that Ingest
	// completed: the phases plus the fee append. A ledger that committed but
	// failed its fee append gets none.
	HotLedger(d time.Duration)
	// ColdIngest reports one cold ingester's per-chunk total: the summed Ingest
	// wall-clock plus its Finalize, items the total items written for the chunk,
	// err the first error (nil on success).
	ColdIngest(dataType string, d time.Duration, items int, err error)
	// ColdChunkTotal reports the per-chunk aggregate wall-clock: the whole
	// WriteColdChunk attempt, from just after config validation through open,
	// the drain of the source stream, every ingest and finalize, to the
	// deferred close. err is the attempt's outcome (nil on success).
	ColdChunkTotal(d time.Duration, err error)
	// ColdExtract reports the ONE shared per-ledger ExtractLedgerTxParts walk the
	// cold chunk runs and both txhash and events read (issue #836) — ledger-scoped,
	// not per data type, mirroring the hot path's type-less extract phase. items
	// is the walk's transaction count (0 on failure); err is non-nil when the
	// walk itself failed, which aborts the ledger before any writer runs — the
	// duration then covers the partial walk, mirroring hot's PhaseExtract error
	// signal. Since the walk left the per-type writers, their ColdIngest totals
	// are post-extraction figures and the per-type stages a partial breakdown of
	// them.
	ColdExtract(d time.Duration, items int, err error)
	// IngestStage reports one COLD writer's per-stage wall-clock inside an
	// ingest/finalize call: stage is one of the stage* constants (term_index,
	// write, finalize), items the stage's natural item count (0 where none
	// applies). The whole-call ColdIngest signal cannot be decomposed by a
	// sink after the fact, so the per-stage granularity the bench reports need is
	// exposed as its own signal — a sink that doesn't want it can no-op it.
	IngestStage(dataType, stage string, d time.Duration, items int)
}

// NopSink is a MetricSink that discards everything. It is the default when a
// caller passes a nil sink to a service or ingester.
type NopSink struct{}

func (NopSink) HotPhase(hotchunk.Phase, time.Duration, int, error) {}
func (NopSink) HotLedger(time.Duration)                            {}
func (NopSink) ColdIngest(string, time.Duration, int, error)       {}
func (NopSink) ColdChunkTotal(time.Duration, error)                {}
func (NopSink) ColdExtract(time.Duration, int, error)              {}
func (NopSink) IngestStage(string, string, time.Duration, int)     {}

// orNop returns sink, or NopSink{} when sink is nil, so call sites never
// nil-check before reporting.
func orNop(sink MetricSink) MetricSink {
	if sink == nil {
		return NopSink{}
	}
	return sink
}

// errOrFirst returns prev if it is non-nil, else cur. Used to retain the FIRST
// error a cold writer saw across its ingest/finalize lifetime for the sink's
// per-chunk err report.
func errOrFirst(prev, cur error) error {
	if prev != nil {
		return prev
	}
	return cur
}

// coldMetrics is the per-chunk metric accumulator shared by all three cold
// writers. Each ingester accumulates Ingest wall-clock (accum), item count
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
// write error is TERMINAL by the coldChunk contract (the chunk is abandoned
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

// metricsSubsystem is the Prometheus subsystem for all ingest metrics, under
// host.StellarPrometheusNamespace. The live (hot) metrics are per ledger and
// have no prefix; the cold chunk write metrics start with chunk_.
const metricsSubsystem = "ingest"

// labelDataType is the label name of the per-data-type metrics.
const labelDataType = "data_type"

// status label values of chunk_duration_seconds.
const (
	statusOK    = "ok"
	statusError = "error"
)

// Histogram buckets per tier. Hot observations are per-ledger: a ledger takes
// about 10ms on pubnet and most phases 1–5ms, so the Prometheus defaults
// (5ms…10s) get four more buckets where the samples are. Cold
// observations are whole-chunk wall-clocks — download + decompress + three
// stores + Finalize for a 10,000-ledger chunk — realistically tens of seconds
// to tens of minutes, so they get their own range; sharing the default
// buckets would pin every cold sample in the +Inf bucket and peg
// histogram_quantile at the top finite bucket.
//
//nolint:gochecknoglobals // fixed bucket layouts, read-only
var (
	// 1ms … 10s: the defaults plus 1ms, 2.5ms, 7.5ms and 15ms.
	hotBuckets = []float64{.001, .0025, .005, .0075, .01, .015, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}
	// 1s … ~34min, doubling.
	coldBuckets = prometheus.ExponentialBuckets(1, 2, 12)
	// Cold STAGE observations (also the shared extract walk) span per-ledger
	// sub-stages (sub-millisecond appends) through the per-chunk finalize
	// (minutes): 1ms … ~70min, ×4 per bucket.
	coldStageBuckets = prometheus.ExponentialBuckets(0.001, 4, 12)
)

// PrometheusSink is a MetricSink that records into Prometheus collectors. It is
// constructed via NewPrometheusSink. The daemon's buildSinks constructs it on the
// shared registry and wires it into both the cold and hot ingest paths.
type PrometheusSink struct {
	// Hot per-ledger phase durations, one child per hotchunk.Phase, indexed by the
	// phase value into a fixed-size ARRAY (not a map), so an out-of-table phase is a
	// bounds panic at the index rather than a silent nil-map emit.
	hotPhaseDur [hotchunk.NumPhases]prometheus.Observer
	// Hot item counters by data type, indexed by the write phase that carries the
	// volume. The entries for extract, commit and apply are nil: those phases
	// write no items.
	hotItems [hotchunk.NumPhases]prometheus.Counter
	// Whole per-ledger wall-clock (the phases plus the fee append).
	hotLedger prometheus.Observer
	// Pre-resolved per-cold-ingester durations, keyed by data type. Producers draw
	// their data_type from the same constant set the map is built from, so a lookup
	// can never miss — indexed directly, no on-the-fly vector fallback.
	cold map[string]prometheus.Observer
	// Item counters of the cold writers, keyed by data type like cold.
	coldItems map[string]prometheus.Counter
	// Per-cold-stage durations, pre-resolved for the six real (data_type, stage)
	// pairs only (coldStagePairs), keyed "dataType/stage".
	coldStage map[string]prometheus.Observer
	// Aggregate per-chunk cold wall-clock (the WriteColdChunk attempt), one
	// child per status.
	coldChunkOK, coldChunkError prometheus.Observer
	// The shared per-ledger extract walk — ledger-scoped, label-less.
	coldExtract prometheus.Observer
}

// NewPrometheusSink builds a PrometheusSink and MustRegisters its collectors on
// registry under namespace + the ingest subsystem. namespace is
// host.StellarPrometheusNamespace.
func NewPrometheusSink(registry *prometheus.Registry, namespace string) *PrometheusSink {
	hotLedger := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name:    "ledger_duration_seconds",
		Help:    "live ingestion wall-clock of one fully ingested ledger (all phases plus the fee update)",
		Buckets: hotBuckets,
	})

	hotPhaseDurVec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name:    "phase_duration_seconds",
		Help:    "live ingestion per-ledger phase wall-clock (extract/ledger/txhashes/events/commit/apply)",
		Buckets: hotBuckets,
	}, []string{"phase"})

	hotItemsVec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name: "items_total",
		Help: "items live ingestion committed to the hot chunk, by data type " +
			"(txhashes counts hash entries: a fee-bump transaction adds two)",
	}, []string{labelDataType})

	coldDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name: "chunk_writer_duration_seconds",
		Help: "wall-clock one data type's writer spent on one chunk (all its ledger writes plus " +
			"finalize; excludes the ledger read and chunk_extract_duration_seconds)",
		Buckets: coldBuckets,
	}, []string{labelDataType})

	coldItemsVec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name: "chunk_items_total",
		Help: "items reported by the chunk writers, by data type (a writer reports when it finalizes " +
			"or its own write fails, so a failed attempt adds the items of each writer that got that far " +
			"and the retry adds them again; a canceled attempt can count though " +
			"chunk_duration_seconds does not record it)",
	}, []string{labelDataType})

	coldChunkTotal := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name: "chunk_duration_seconds",
		Help: "wall-clock of one attempt to write the immutable files of one chunk, during " +
			"backfill or freeze (ledger read, extract, all writers; not the fsync and catalog " +
			"update that follow), by status (ok, error; a canceled attempt is not recorded)",
		Buckets: coldBuckets,
	}, []string{"status"})

	coldStageVec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name: "chunk_stage_duration_seconds",
		Help: "wall-clock of one stage of a chunk writer (write and term_index: one sample per " +
			"ledger; finalize: one per chunk; a partial breakdown of chunk_writer_duration_seconds)",
		Buckets: coldStageBuckets,
	}, []string{labelDataType, "stage"})

	coldExtract := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: metricsSubsystem,
		Name: "chunk_extract_duration_seconds",
		Help: "wall-clock to read the transaction parts out of one ledger during a chunk write " +
			"(one sample per ledger; shared by the txhash and events writers)",
		Buckets: coldStageBuckets,
	})

	registry.MustRegister(hotLedger, hotPhaseDurVec, hotItemsVec,
		coldDuration, coldItemsVec, coldChunkTotal, coldStageVec, coldExtract)

	sink := &PrometheusSink{
		cold:      make(map[string]prometheus.Observer, 3),
		coldItems: make(map[string]prometheus.Counter, 3),
		coldStage: make(map[string]prometheus.Observer, len(coldStagePairs)),
		// Both status series exist from startup, so the error series reads 0
		// before the first failure.
		coldChunkOK:    coldChunkTotal.WithLabelValues(statusOK),
		coldChunkError: coldChunkTotal.WithLabelValues(statusError),
		coldExtract:    coldExtract,
		hotLedger:      hotLedger,
	}
	// Hot phases: one child per phase, indexed by the phase value.
	for p := range hotchunk.NumPhases {
		sink.hotPhaseDur[p] = hotPhaseDurVec.WithLabelValues(p.String())
	}
	sink.hotItems[hotchunk.PhaseLedgers] = hotItemsVec.WithLabelValues(dataTypeLedgers)
	sink.hotItems[hotchunk.PhaseTxhash] = hotItemsVec.WithLabelValues(dataTypeTxhash)
	sink.hotItems[hotchunk.PhaseEvents] = hotItemsVec.WithLabelValues(dataTypeEvents)
	for _, dataType := range []string{dataTypeLedgers, dataTypeTxhash, dataTypeEvents} {
		sink.cold[dataType] = coldDuration.WithLabelValues(dataType)
		sink.coldItems[dataType] = coldItemsVec.WithLabelValues(dataType)
	}
	// Cold stages: only the six real (data_type, stage) pairs.
	for _, k := range coldStagePairs {
		sink.coldStage[k.dataType+"/"+k.stage] = coldStageVec.WithLabelValues(k.dataType, k.stage)
	}
	return sink
}

// HotPhase records the phase duration and the write phases' item volume. It
// counts no errors: a failed hot ledger stops the daemon, so the error is in
// the exit log and no scrape would see a counter.
func (p *PrometheusSink) HotPhase(phase hotchunk.Phase, d time.Duration, items int, _ error) {
	p.hotPhaseDur[phase].Observe(d.Seconds())
	if c := p.hotItems[phase]; c != nil && items > 0 {
		c.Add(float64(items))
	}
}

func (p *PrometheusSink) HotLedger(d time.Duration) { p.hotLedger.Observe(d.Seconds()) }

// ColdIngest records the writer's duration and the items it wrote. A failed
// writer's items count, and the chunk's retry counts them again; a writer that
// never reached a terminal step (the attempt failed elsewhere) reports nothing.
// The error itself is not exported: a failed file write shows in
// chunk_duration_seconds{status="error"} and in the retry log. A failure of
// the caller's later fsync or catalog flip shows only in the retry counter and
// log.
func (p *PrometheusSink) ColdIngest(dataType string, d time.Duration, items int, _ error) {
	p.cold[dataType].Observe(d.Seconds())
	if items > 0 {
		p.coldItems[dataType].Add(float64(items))
	}
}

// ColdChunkTotal records the attempt under its status. A canceled attempt is
// not recorded: it is fallout from a shutdown or a sibling chunk's failure, not
// a failure of this chunk (the same rule as backfill's chunkFailed log).
func (p *PrometheusSink) ColdChunkTotal(d time.Duration, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	if err != nil {
		p.coldChunkError.Observe(d.Seconds())
		return
	}
	p.coldChunkOK.Observe(d.Seconds())
}

// ColdExtract records the shared per-ledger walk's duration (the partial walk
// on failure — a phase that blocked then failed is signal). The item count and
// the error are not exported to Prometheus; they exist on the interface for the
// CSV bench sink and tests.
func (p *PrometheusSink) ColdExtract(d time.Duration, _ int, _ error) {
	p.coldExtract.Observe(d.Seconds())
}

// IngestStage records the per-stage cold duration. The per-stage item counts are
// not exported to Prometheus; they exist on the interface for the CSV bench sink.
func (p *PrometheusSink) IngestStage(dataType, stage string, d time.Duration, _ int) {
	p.coldStage[dataType+"/"+stage].Observe(d.Seconds())
}
