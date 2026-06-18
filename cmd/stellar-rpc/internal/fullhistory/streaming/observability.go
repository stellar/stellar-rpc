package streaming

import (
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Observability for the streaming daemon's own control plane — distinct from the
// per-data-type ingest metrics (ingest.MetricSink / ingest.PrometheusSink), which
// time the cold/hot ingesters themselves. THIS sink times and counts the daemon's
// PHASES: the ingestion loop's chunk-boundary handoffs, catch-up backfill passes,
// the three lifecycle-tick stages (freeze / discard / prune), and surgical
// recovery — plus the derived progress gauges (ingestion lag, watermark, the
// effective retention floor, live hot-chunk count, cold-tier footprint) that no
// per-ingester sink can see because they are properties of the whole catalog.
//
// It is a SMALL interface so it is trivially testable: a test passes a recorder
// (recordingMetrics in the tests) and asserts the daemon drove the expected
// signals at the right phase boundaries, without standing up Prometheus. Every
// call site reads cfg's Metrics through metricsOrNop, so a nil sink is a no-op and
// no phase ever nil-checks.
//
// All methods MUST be safe for concurrent use: the ingestion loop, the lifecycle
// goroutine, and (during catch-up) the worker pool all report concurrently.
type Metrics interface {
	// --- gauges (absolute, last-write-wins) ---

	// IngestionLag sets the live lag in ledgers: networkTip - lastCommitted. The
	// ingestion loop reports it at each chunk boundary against captive core's tip;
	// catch-up reports it each pass against the bulk tip. networkTip is the best
	// tip currently known; lastCommitted the highest durably committed ledger.
	IngestionLag(networkTip, lastCommitted uint32)

	// Watermark sets the derived watermark (the highest durably committed ledger,
	// deriveWatermark's result) and the effective retention floor (the lowest
	// ledger inside the retention window). Reported by startStreaming after
	// derivation and by every lifecycle tick.
	Watermark(lastCommitted, retentionFloor uint32)

	// CatchupProgress sets catch-up's position: the last ledger backfilled so far
	// and the target (the tip-anchored upper bound of the catch-up window). Equal
	// values mean catch-up has converged.
	CatchupProgress(backfilledThrough, target uint32)

	// LiveHotChunks sets the count of hot-chunk DBs currently on disk (the
	// hot:chunk key count). Reported by every lifecycle tick after the discard
	// stage so the gauge tracks the live + awaiting-discard set.
	LiveHotChunks(count int)

	// ColdTierBytes sets the cold-tier on-disk footprint in bytes (the summed size
	// of the ledgers/events/txhash trees). Reported by every lifecycle tick after
	// the prune stage.
	ColdTierBytes(bytes int64)

	// --- counters + durations (one call per completed phase action) ---

	// ChunkBoundary counts one ingestion chunk-boundary handoff (a chunk filled,
	// its DB closed, the next chunk's DB opened). closedChunk is the just-filled
	// chunk's id.
	ChunkBoundary(closedChunk uint32)

	// CatchupPass counts one completed catch-up backfill pass over [lo, hi] and
	// records its wall-clock. A pass that backfilled nothing (converged) is not
	// reported — only passes that ran runBackfill.
	CatchupPass(lo, hi uint32, d time.Duration)

	// Freeze counts one lifecycle-tick plan-and-execute stage (the freeze + index
	// fold) and records its wall-clock. chunkBuilds / indexBuilds are the plan's
	// sizes — 0/0 when the tick had no producible range (the stage still reports,
	// with a zero count, so the rate of empty ticks is observable).
	Freeze(chunkBuilds, indexBuilds int, d time.Duration)

	// Rebuild records the burst throughput of an index rebuild: chunks folded into
	// one .idx over a wall-clock. It is the per-IndexBuild signal the Freeze
	// aggregate cannot decompose; emitted once per index build executePlan ran.
	Rebuild(chunks int, d time.Duration)

	// Discard counts the hot DBs a tick retired and records the stage wall-clock.
	Discard(count int, d time.Duration)

	// Prune counts the prune-stage sweep ops a tick ran and records the stage
	// wall-clock.
	Prune(count int, d time.Duration)

	// Recovery counts one surgical-recovery apply and records how many keys it
	// demoted across the cold/index/hot tiers.
	Recovery(coldKeys, indexKeys, hotKeys int, d time.Duration)
}

// nopMetrics discards every signal. It is the default when a config carries no
// Metrics, so every phase reports unconditionally without a nil-check.
type nopMetrics struct{}

func (nopMetrics) IngestionLag(uint32, uint32)               {}
func (nopMetrics) Watermark(uint32, uint32)                  {}
func (nopMetrics) CatchupProgress(uint32, uint32)            {}
func (nopMetrics) LiveHotChunks(int)                         {}
func (nopMetrics) ColdTierBytes(int64)                       {}
func (nopMetrics) ChunkBoundary(uint32)                      {}
func (nopMetrics) CatchupPass(uint32, uint32, time.Duration) {}
func (nopMetrics) Freeze(int, int, time.Duration)            {}
func (nopMetrics) Rebuild(int, time.Duration)                {}
func (nopMetrics) Discard(int, time.Duration)                {}
func (nopMetrics) Prune(int, time.Duration)                  {}
func (nopMetrics) Recovery(int, int, int, time.Duration)     {}

// metricsOrNop returns m, or nopMetrics{} when m is nil, so call sites never
// nil-check before reporting a phase signal.
func metricsOrNop(m Metrics) Metrics {
	if m == nil {
		return nopMetrics{}
	}
	return m
}

// streamingSubsystem is the Prometheus subsystem for all streaming control-plane
// metrics, under the daemon's namespace (interfaces.PrometheusNamespace). It is
// distinct from ingest.metricsSubsystem ("fullhistory_ingest") so the two metric
// families never collide in one registry.
const streamingSubsystem = "fullhistory_streaming"

// phaseBuckets time the daemon's phase actions: a chunk-boundary handoff is
// sub-millisecond, a freeze/rebuild over a full chunk is seconds to minutes, a
// catch-up pass over many chunks longer still. 1ms … ~70min, ×4 per bucket — the
// same wide span ingest's coldStageBuckets use, so a single dashboard renders
// both families on one axis.
//
//nolint:gochecknoglobals // fixed bucket layout, read-only
var phaseBuckets = prometheus.ExponentialBuckets(0.001, 4, 12)

// PrometheusMetrics is the production Metrics sink: it records the streaming
// daemon's phase signals into Prometheus collectors. Constructed via
// NewPrometheusMetrics, which MustRegisters its collectors under a namespace +
// the fullhistory_streaming subsystem — the same daemon convention
// ingest.NewPrometheusSink follows.
type PrometheusMetrics struct {
	// Gauges — absolute, last-write-wins.
	ingestionLag      prometheus.Gauge
	watermark         prometheus.Gauge
	retentionFloor    prometheus.Gauge
	catchupBackfilled prometheus.Gauge
	catchupTarget     prometheus.Gauge
	liveHotChunks     prometheus.Gauge
	coldTierBytes     prometheus.Gauge

	// Counters — monotonic event tallies.
	chunkBoundaries prometheus.Counter
	catchupPasses   prometheus.Counter
	freezeChunks    prometheus.Counter
	freezeIndexes   prometheus.Counter
	rebuiltChunks   prometheus.Counter
	discarded       prometheus.Counter
	pruned          prometheus.Counter
	recoveries      prometheus.Counter
	recoveredKeys   *prometheus.CounterVec // by tier

	// Durations — per-phase wall-clock histograms, keyed by phase label.
	phaseDuration *prometheus.HistogramVec
	// Rebuild burst throughput (chunks folded per .idx) as its own histogram.
	rebuildChunksPerIdx prometheus.Histogram
}

// Phase labels for the per-phase duration histogram.
const (
	phaseCatchupPass = "catchup_pass"
	phaseFreeze      = "freeze"
	phaseRebuild     = "rebuild"
	phaseDiscard     = "discard"
	phasePrune       = "prune"
	phaseRecovery    = "recovery"
)

// NewPrometheusMetrics builds a PrometheusMetrics and MustRegisters its
// collectors on registry under namespace + the fullhistory_streaming subsystem.
// namespace is the daemon convention value (interfaces.PrometheusNamespace).
func NewPrometheusMetrics(registry *prometheus.Registry, namespace string) *PrometheusMetrics {
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: streamingSubsystem, Name: name, Help: help,
		})
	}
	counter := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: streamingSubsystem, Name: name, Help: help,
		})
	}

	m := &PrometheusMetrics{
		ingestionLag:      gauge("ingestion_lag_ledgers", "network tip minus last committed ledger"),
		watermark:         gauge("watermark_ledger", "derived watermark — highest durably committed ledger"),
		retentionFloor:    gauge("retention_floor_ledger", "effective retention floor — lowest in-window ledger"),
		catchupBackfilled: gauge("catchup_backfilled_ledger", "last ledger catch-up has backfilled through"),
		catchupTarget:     gauge("catchup_target_ledger", "catch-up target — tip-anchored upper bound"),
		liveHotChunks:     gauge("live_hot_chunks", "count of hot-chunk DBs currently on disk"),
		coldTierBytes:     gauge("cold_tier_bytes", "cold-tier on-disk footprint in bytes"),

		chunkBoundaries: counter("chunk_boundaries_total", "ingestion chunk-boundary handoffs"),
		catchupPasses:   counter("catchup_passes_total", "completed catch-up backfill passes"),
		freezeChunks:    counter("freeze_chunks_total", "chunks frozen by the lifecycle freeze stage"),
		freezeIndexes:   counter("freeze_indexes_total", "indexes built by the lifecycle freeze stage"),
		rebuiltChunks:   counter("rebuilt_chunks_total", "chunks folded into rebuilt indexes"),
		discarded:       counter("discarded_hot_chunks_total", "hot DBs retired by the discard stage"),
		pruned:          counter("pruned_ops_total", "prune-stage sweep ops"),
		recoveries:      counter("recoveries_total", "surgical-recovery applies"),
		recoveredKeys: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: streamingSubsystem,
			Name: "recovered_keys_total", Help: "keys demoted by surgical recovery, by tier",
		}, []string{"tier"}),

		phaseDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: streamingSubsystem,
			Name: "phase_duration_seconds", Help: "wall-clock of a daemon phase action",
			Buckets: phaseBuckets,
		}, []string{"phase"}),
		rebuildChunksPerIdx: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: streamingSubsystem,
			Name: "rebuild_chunks_per_index", Help: "chunks folded into one index rebuild (burst throughput)",
			// 1 … ~4096 chunks, doubling.
			Buckets: prometheus.ExponentialBuckets(1, 2, 13),
		}),
	}

	registry.MustRegister(
		m.ingestionLag, m.watermark, m.retentionFloor, m.catchupBackfilled, m.catchupTarget,
		m.liveHotChunks, m.coldTierBytes,
		m.chunkBoundaries, m.catchupPasses, m.freezeChunks, m.freezeIndexes, m.rebuiltChunks,
		m.discarded, m.pruned, m.recoveries, m.recoveredKeys,
		m.phaseDuration, m.rebuildChunksPerIdx,
	)
	return m
}

func (m *PrometheusMetrics) IngestionLag(networkTip, lastCommitted uint32) {
	// Signed lag: a lagging bulk tip below the watermark yields 0, not a wrap.
	lag := int64(networkTip) - int64(lastCommitted)
	if lag < 0 {
		lag = 0
	}
	m.ingestionLag.Set(float64(lag))
}

func (m *PrometheusMetrics) Watermark(lastCommitted, retentionFloor uint32) {
	m.watermark.Set(float64(lastCommitted))
	m.retentionFloor.Set(float64(retentionFloor))
}

func (m *PrometheusMetrics) CatchupProgress(backfilledThrough, target uint32) {
	m.catchupBackfilled.Set(float64(backfilledThrough))
	m.catchupTarget.Set(float64(target))
}

func (m *PrometheusMetrics) LiveHotChunks(count int) { m.liveHotChunks.Set(float64(count)) }

func (m *PrometheusMetrics) ColdTierBytes(bytes int64) { m.coldTierBytes.Set(float64(bytes)) }

func (m *PrometheusMetrics) ChunkBoundary(uint32) { m.chunkBoundaries.Inc() }

func (m *PrometheusMetrics) CatchupPass(_, _ uint32, d time.Duration) {
	m.catchupPasses.Inc()
	m.phaseDuration.WithLabelValues(phaseCatchupPass).Observe(d.Seconds())
}

func (m *PrometheusMetrics) Freeze(chunkBuilds, indexBuilds int, d time.Duration) {
	if chunkBuilds > 0 {
		m.freezeChunks.Add(float64(chunkBuilds))
	}
	if indexBuilds > 0 {
		m.freezeIndexes.Add(float64(indexBuilds))
	}
	m.phaseDuration.WithLabelValues(phaseFreeze).Observe(d.Seconds())
}

func (m *PrometheusMetrics) Rebuild(chunks int, d time.Duration) {
	if chunks > 0 {
		m.rebuiltChunks.Add(float64(chunks))
	}
	m.rebuildChunksPerIdx.Observe(float64(chunks))
	m.phaseDuration.WithLabelValues(phaseRebuild).Observe(d.Seconds())
}

func (m *PrometheusMetrics) Discard(count int, d time.Duration) {
	if count > 0 {
		m.discarded.Add(float64(count))
	}
	m.phaseDuration.WithLabelValues(phaseDiscard).Observe(d.Seconds())
}

func (m *PrometheusMetrics) Prune(count int, d time.Duration) {
	if count > 0 {
		m.pruned.Add(float64(count))
	}
	m.phaseDuration.WithLabelValues(phasePrune).Observe(d.Seconds())
}

func (m *PrometheusMetrics) Recovery(coldKeys, indexKeys, hotKeys int, d time.Duration) {
	m.recoveries.Inc()
	if coldKeys > 0 {
		m.recoveredKeys.WithLabelValues("cold").Add(float64(coldKeys))
	}
	if indexKeys > 0 {
		m.recoveredKeys.WithLabelValues("index").Add(float64(indexKeys))
	}
	if hotKeys > 0 {
		m.recoveredKeys.WithLabelValues("hot").Add(float64(hotKeys))
	}
	m.phaseDuration.WithLabelValues(phaseRecovery).Observe(d.Seconds())
}

// compile-time assertion: the production sink satisfies the interface.
var _ Metrics = (*PrometheusMetrics)(nil)

// coldTierBytes sums the on-disk footprint of the cold tier — the
// ledgers/events/txhash-raw/txhash-index trees (the hot tier and the meta store
// are excluded: the hot tier is transient, the meta store tiny). It walks each
// tree's roots once, ignoring missing trees (a frontfill deployment may not have
// materialized any). A walk error on a single tree is non-fatal to the others —
// the lifecycle caller treats a returned error as "skip the gauge this tick"
// rather than failing the tick, so a transient FS hiccup never aborts the daemon.
func coldTierBytes(layout Layout) (int64, error) {
	var total int64
	var firstErr error
	for _, root := range []string{
		layout.LedgersRoot(),
		layout.EventsRoot(),
		layout.TxHashRawRoot(),
		layout.TxHashIndexRoot(),
	} {
		err := filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
			if err != nil {
				if os.IsNotExist(err) {
					return nil // an un-materialized tree contributes nothing
				}
				return err
			}
			if d.IsDir() {
				return nil
			}
			info, ierr := d.Info()
			if ierr != nil {
				if os.IsNotExist(ierr) {
					return nil // raced with a prune unlink — count it as gone
				}
				return ierr
			}
			total += info.Size()
			return nil
		})
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return total, firstErr
}
