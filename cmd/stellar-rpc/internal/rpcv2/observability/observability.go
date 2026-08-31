package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
)

// Metrics is the daemon's control-plane sink — the derived-progress gauges plus
// per-phase wall-clock timings; distinct from the per-data-type ingest.MetricSink.
// All methods must be safe for concurrent use.
type Metrics interface {
	// LastCommitted sets the derived last-committed ledger gauge. Owned by the two
	// call sites that know the TRUE value: startup/backfill (as history advances)
	// and the ingestion loop (one atomic gauge set per committed ledger). The tick
	// must NOT set it — its chunk-aligned lastChunk.LastLedger() would regress the
	// gauge below a mid-chunk refined last-committed ledger on every restart.
	LastCommitted(lastCommitted uint32)

	// RetentionFloor sets the effective retention floor gauge (lowest in-window
	// ledger). Owned by startup/backfill and the lifecycle tick; the floor depends
	// only on the last complete chunk, so it does not regress in the tick's window.
	RetentionFloor(retentionFloor uint32)

	// ChunkBoundary counts one ingestion chunk-boundary handoff. The closed chunk
	// id is logged at the call site; this metric is a plain counter.
	ChunkBoundary()

	// LiveHotChunks sets the count of hot-chunk DBs currently on disk (the
	// hot:chunk key count). Reported by every lifecycle tick after the discard
	// stage so the gauge tracks the live + awaiting-discard set.
	LiveHotChunks(count int)

	// BackfillPass records one completed backfill pass's wall-clock.
	BackfillPass(d time.Duration)
	// Freeze records one freeze (plan-and-execute) stage's wall-clock.
	Freeze(d time.Duration)
	// Rebuild records one index rebuild's wall-clock, spanning all retry attempts
	// (up to MaxRetries+1) and their inter-attempt backoff sleeps in one sample.
	Rebuild(d time.Duration)
	// Discard counts the hot DBs a tick retired and records the stage wall-clock.
	Discard(count int, d time.Duration)
	// Prune counts swept artifacts and records the sweep's wall-clock.
	Prune(count int, d time.Duration)

	// FailedDestroy counts one deferred destroy that failed and was left for the
	// next lifecycle run's scan — most commonly a hot handle whose close found a
	// reader still in flight. A handle stuck forever recounts every run, so a
	// climbing counter is the stuck-handle alarm the logs alone cannot raise.
	FailedDestroy()

	// TruncatedEventWindow counts one v1 getEvents page that stopped before
	// covering its window. The v1 scan window is sized to fit inside one
	// pager page, so the handler refuses such a page rather than mint a
	// cursor that skips the unscanned remainder. Any count means that
	// sizing no longer holds and the endpoint is failing requests.
	TruncatedEventWindow()

	// TxIndexInconsistency counts one lookup where an exact (hot) transaction
	// index disagreed with the ledger store — the index named a ledger that
	// lacks the transaction or cannot be served. Any count means corruption
	// operators should see.
	TxIndexInconsistency()
}

// NopMetrics discards every signal — the default when a config carries no Metrics.
type NopMetrics struct{}

func (NopMetrics) LastCommitted(uint32)       {}
func (NopMetrics) RetentionFloor(uint32)      {}
func (NopMetrics) ChunkBoundary()             {}
func (NopMetrics) LiveHotChunks(int)          {}
func (NopMetrics) BackfillPass(time.Duration) {}
func (NopMetrics) Freeze(time.Duration)       {}
func (NopMetrics) Rebuild(time.Duration)      {}
func (NopMetrics) Discard(int, time.Duration) {}
func (NopMetrics) Prune(int, time.Duration)   {}
func (NopMetrics) FailedDestroy()             {}
func (NopMetrics) TxIndexInconsistency()      {}
func (NopMetrics) TruncatedEventWindow()      {}

// MetricsOrNop returns m, or NopMetrics{} when nil, so call sites never nil-check.
func MetricsOrNop(m Metrics) Metrics {
	if m == nil {
		return NopMetrics{}
	}
	return m
}

// subsystem is the Prometheus subsystem for the daemon's control-plane metrics,
// distinct from ingest's so the families never collide in one registry.
const subsystem = "fullhistory_streaming"

// phaseBuckets time the daemon's phase actions (1ms … ~70min, ×4 per bucket) —
// same span as ingest's coldStageBuckets so one dashboard renders both.
//
//nolint:gochecknoglobals // fixed bucket layout, read-only
var phaseBuckets = prometheus.ExponentialBuckets(0.001, 4, 12)

// PrometheusMetrics is the production Metrics sink (constructed via NewPrometheusMetrics).
type PrometheusMetrics struct {
	// Gauges — absolute, last-write-wins.
	lastCommitted  prometheus.Gauge
	retentionFloor prometheus.Gauge
	liveHotChunks  prometheus.Gauge

	// Counters — monotonic tallies.
	chunkBoundaries        prometheus.Counter
	discarded              prometheus.Counter
	pruned                 prometheus.Counter
	failedDestroys         prometheus.Counter
	txIndexInconsistencies prometheus.Counter
	truncatedEventWindows  prometheus.Counter

	// Durations — per-phase wall-clock histogram, keyed by phase label.
	phaseDuration *prometheus.HistogramVec
}

// Phase labels for the per-phase duration histogram.
const (
	phaseBackfillPass = "backfill_pass"
	phaseFreeze       = "freeze"
	phaseRebuild      = "rebuild"
	phaseDiscard      = "discard"
	phasePrune        = "prune"
)

// NewPrometheusMetrics builds a PrometheusMetrics and MustRegisters its collectors.
func NewPrometheusMetrics(registry *prometheus.Registry, namespace string) *PrometheusMetrics {
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: subsystem, Name: name, Help: help,
		})
	}
	counter := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystem, Name: name, Help: help,
		})
	}

	m := &PrometheusMetrics{
		lastCommitted:   gauge("last_committed_ledger", "highest ledger durably committed"),
		retentionFloor:  gauge("retention_floor_ledger", "effective retention floor — lowest in-window ledger"),
		liveHotChunks:   gauge("live_hot_chunks", "count of hot-chunk DBs currently on disk"),
		chunkBoundaries: counter("chunk_boundaries_total", "ingestion chunk-boundary handoffs"),
		discarded: counter("discarded_hot_chunks_total", "hot DBs demoted by the discard stage "+
			"(a chunk whose deferred destroy stays reader-busy recounts on later runs)"),
		pruned: counter("pruned_artifacts_total", "artifacts swept by the prune stage (below-floor artifacts, "+
			"transient index debris, in-retention demotions, and redundant txhash keys)"),
		failedDestroys: counter("failed_destroys_total", "deferred destroys that failed and were left for the "+
			"next lifecycle run (a stuck hot handle recounts every run)"),
		txIndexInconsistencies: counter("tx_index_inconsistencies_total", "lookups where an exact hot tx index "+
			"disagreed with the ledger store (any count means corruption)"),
		truncatedEventWindows: counter("truncated_event_windows_total",
			"v1 getEvents pages that stopped before covering their window "+
				"(the window is sized to fit one pager page, so any count is an alarm)"),
		phaseDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: subsystem,
			Name: "phase_duration_seconds", Help: "wall-clock of a daemon phase action",
			Buckets: phaseBuckets,
		}, []string{"phase"}),
	}

	// Three serving-invariant counters are tallied where their condition is
	// detected — deep in the store and routing packages, below any metrics
	// plumbing — as package-level atomics. CounterFuncs export them; each
	// should flatline at zero, so one alert rule per family is "rate > 0".
	counterFunc := func(name, help string, read func() uint64) prometheus.CounterFunc {
		return prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystem, Name: name, Help: help,
		}, func() float64 { return float64(read()) })
	}

	registry.MustRegister(
		m.lastCommitted, m.retentionFloor, m.liveHotChunks,
		m.chunkBoundaries, m.discarded, m.pruned,
		m.failedDestroys, m.txIndexInconsistencies, m.truncatedEventWindows,
		m.phaseDuration,
		counterFunc("store_ops_after_deferred_close_total",
			"operations refused because deferred deletion had closed the store "+
				"(each one is a caller that outlived the deletion grace period)",
			rocksdb.DeferredCloseOps),
		counterFunc("unavailable_chunk_resolves_total",
			"reads that found no serving store for a chunk "+
				"(unreachable within the serving model; any count is an alarm)",
			query.UnavailableResolves),
		counterFunc("missing_cold_pack_opens_total",
			"cold ledger packs whose file was gone on first read "+
				"(routing only opens packs the catalog snapshot holds; any count is an alarm)",
			ledger.MissingPackOpens),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "open_snapshots",
			Help: "RocksDB snapshots currently held, across all stores " +
				"(request-scoped, so a floor that stops returning to zero is a leaked read view)",
		}, func() float64 { return float64(rocksdb.OpenSnapshots()) }),
	)
	return m
}

func (m *PrometheusMetrics) LastCommitted(lastCommitted uint32) {
	m.lastCommitted.Set(float64(lastCommitted))
}

func (m *PrometheusMetrics) RetentionFloor(retentionFloor uint32) {
	m.retentionFloor.Set(float64(retentionFloor))
}

func (m *PrometheusMetrics) ChunkBoundary() { m.chunkBoundaries.Inc() }

func (m *PrometheusMetrics) LiveHotChunks(count int) { m.liveHotChunks.Set(float64(count)) }

func (m *PrometheusMetrics) BackfillPass(d time.Duration) {
	m.phaseDuration.WithLabelValues(phaseBackfillPass).Observe(d.Seconds())
}

func (m *PrometheusMetrics) Freeze(d time.Duration) {
	m.phaseDuration.WithLabelValues(phaseFreeze).Observe(d.Seconds())
}

func (m *PrometheusMetrics) Rebuild(d time.Duration) {
	m.phaseDuration.WithLabelValues(phaseRebuild).Observe(d.Seconds())
}

func (m *PrometheusMetrics) Discard(count int, d time.Duration) {
	if count > 0 {
		m.discarded.Add(float64(count))
	}
	m.phaseDuration.WithLabelValues(phaseDiscard).Observe(d.Seconds())
}

func (m *PrometheusMetrics) FailedDestroy() { m.failedDestroys.Inc() }

func (m *PrometheusMetrics) TxIndexInconsistency() { m.txIndexInconsistencies.Inc() }

func (m *PrometheusMetrics) TruncatedEventWindow() { m.truncatedEventWindows.Inc() }

func (m *PrometheusMetrics) Prune(count int, d time.Duration) {
	if count > 0 {
		m.pruned.Add(float64(count))
	}
	m.phaseDuration.WithLabelValues(phasePrune).Observe(d.Seconds())
}

// compile-time interface check.
var _ Metrics = (*PrometheusMetrics)(nil)
