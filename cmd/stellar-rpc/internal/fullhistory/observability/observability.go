package observability

import (
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
)

// Metrics is the daemon's control-plane sink — times/counts the daemon's phases
// (catch-up, freeze/rebuild, prune) plus derived progress gauges; distinct from
// per-data-type ingest.MetricSink. All methods must be safe for concurrent use.
// LastCommitted and ChunkBoundary await the Phase-2 live-ingestion loop; the rest are wired.
type Metrics interface {
	// --- gauges (absolute, last-write-wins) ---

	// IngestionLag sets the lag in ledgers (networkTip - lastCommitted); catch-up
	// only, freezes at its final value once catch-up converges.
	IngestionLag(networkTip, lastCommitted uint32)

	// LastCommitted sets the highest durably synced ledger (per-ledger liveness signal).
	LastCommitted(seq uint32)

	// Watermark sets the derived watermark and the effective retention floor.
	Watermark(lastCommitted, retentionFloor uint32)

	// CatchupProgress sets catch-up's position; equal values mean converged.
	CatchupProgress(backfilledThrough, target uint32)

	// ColdTierBytes sets the cold-tier on-disk footprint.
	ColdTierBytes(bytes int64)

	// --- counters + durations (one call per completed phase action) ---

	// ChunkBoundary counts one ingestion chunk-boundary handoff (closedChunk = just-filled chunk id).
	ChunkBoundary(closedChunk uint32)

	// CatchupPass counts one completed catch-up pass over [lo, hi] and records its wall-clock.
	CatchupPass(lo, hi uint32, d time.Duration)

	// Freeze counts one plan-and-execute stage and records its wall-clock;
	// chunkBuilds/indexBuilds are plan sizes, 0/0 still reported.
	Freeze(chunkBuilds, indexBuilds int, d time.Duration)

	// Rebuild records one index rebuild's burst throughput (chunks folded per .idx).
	Rebuild(chunks int, d time.Duration)

	// Prune counts swept artifacts and records the sweep's wall-clock.
	Prune(count int, d time.Duration)
}

// NopMetrics discards every signal — the default when a config carries no Metrics.
type NopMetrics struct{}

func (NopMetrics) IngestionLag(uint32, uint32)               {}
func (NopMetrics) LastCommitted(uint32)                      {}
func (NopMetrics) Watermark(uint32, uint32)                  {}
func (NopMetrics) CatchupProgress(uint32, uint32)            {}
func (NopMetrics) ColdTierBytes(int64)                       {}
func (NopMetrics) ChunkBoundary(uint32)                      {}
func (NopMetrics) CatchupPass(uint32, uint32, time.Duration) {}
func (NopMetrics) Freeze(int, int, time.Duration)            {}
func (NopMetrics) Rebuild(int, time.Duration)                {}
func (NopMetrics) Prune(int, time.Duration)                  {}

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
	ingestionLag      prometheus.Gauge
	lastCommitted     prometheus.Gauge
	watermark         prometheus.Gauge
	retentionFloor    prometheus.Gauge
	catchupBackfilled prometheus.Gauge
	catchupTarget     prometheus.Gauge
	coldTierBytes     prometheus.Gauge

	// Counters — monotonic event tallies.
	chunkBoundaries prometheus.Counter
	catchupPasses   prometheus.Counter
	freezeChunks    prometheus.Counter
	freezeIndexes   prometheus.Counter
	rebuiltChunks   prometheus.Counter
	pruned          prometheus.Counter

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
	phasePrune       = "prune"
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
		ingestionLag:      gauge("ingestion_lag_ledgers", "catch-up only: network tip minus last committed ledger"),
		lastCommitted:     gauge("last_committed_ledger", "highest ledger the ingestion loop has durably synced (per-ledger liveness)"),
		watermark:         gauge("watermark_ledger", "derived watermark — highest durably committed ledger"),
		retentionFloor:    gauge("retention_floor_ledger", "effective retention floor — lowest in-window ledger"),
		catchupBackfilled: gauge("catchup_backfilled_ledger", "last ledger catch-up has backfilled through"),
		catchupTarget:     gauge("catchup_target_ledger", "catch-up target — tip-anchored upper bound"),
		coldTierBytes:     gauge("cold_tier_bytes", "cold-tier on-disk footprint in bytes"),

		chunkBoundaries: counter("chunk_boundaries_total", "ingestion chunk-boundary handoffs"),
		catchupPasses:   counter("catchup_passes_total", "completed catch-up backfill passes"),
		freezeChunks:    counter("freeze_chunks_total", "chunks frozen by the freeze stage"),
		freezeIndexes:   counter("freeze_indexes_total", "indexes built by the freeze stage"),
		rebuiltChunks:   counter("rebuilt_chunks_total", "chunks folded into rebuilt indexes"),
		pruned:          counter("pruned_ops_total", "artifacts swept after an index build"),

		phaseDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: subsystem,
			Name: "phase_duration_seconds", Help: "wall-clock of a daemon phase action",
			Buckets: phaseBuckets,
		}, []string{"phase"}),
		rebuildChunksPerIdx: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: subsystem,
			Name: "rebuild_chunks_per_index", Help: "chunks folded into one index rebuild (burst throughput)",
			// 1 … ~4096 chunks, doubling.
			Buckets: prometheus.ExponentialBuckets(1, 2, 13),
		}),
	}

	registry.MustRegister(
		m.ingestionLag, m.lastCommitted, m.watermark, m.retentionFloor, m.catchupBackfilled, m.catchupTarget,
		m.coldTierBytes,
		m.chunkBoundaries, m.catchupPasses, m.freezeChunks, m.freezeIndexes, m.rebuiltChunks,
		m.pruned,
		m.phaseDuration, m.rebuildChunksPerIdx,
	)
	return m
}

func (m *PrometheusMetrics) IngestionLag(networkTip, lastCommitted uint32) {
	// Signed: a lagging bulk tip below the watermark yields 0, not a wrap.
	lag := int64(networkTip) - int64(lastCommitted)
	if lag < 0 {
		lag = 0
	}
	m.ingestionLag.Set(float64(lag))
}

func (m *PrometheusMetrics) LastCommitted(seq uint32) { m.lastCommitted.Set(float64(seq)) }

func (m *PrometheusMetrics) Watermark(lastCommitted, retentionFloor uint32) {
	m.watermark.Set(float64(lastCommitted))
	m.retentionFloor.Set(float64(retentionFloor))
}

func (m *PrometheusMetrics) CatchupProgress(backfilledThrough, target uint32) {
	m.catchupBackfilled.Set(float64(backfilledThrough))
	m.catchupTarget.Set(float64(target))
}

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

func (m *PrometheusMetrics) Prune(count int, d time.Duration) {
	if count > 0 {
		m.pruned.Add(float64(count))
	}
	m.phaseDuration.WithLabelValues(phasePrune).Observe(d.Seconds())
}

// compile-time interface check.
var _ Metrics = (*PrometheusMetrics)(nil)

// MeasureColdTierBytes sums the cold tier's on-disk footprint (ledgers/events/txhash-raw/
// txhash-index trees; hot tier and meta store excluded), walking each root once and
// ignoring missing trees. A per-tree error is non-fatal to the others (caller skips the gauge).
func MeasureColdTierBytes(layout geometry.Layout) (int64, error) {
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
