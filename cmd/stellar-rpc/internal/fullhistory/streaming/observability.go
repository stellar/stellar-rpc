package streaming

import (
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics is the streaming daemon's control-plane sink — distinct from the
// per-data-type ingest metrics (ingest.MetricSink), which time the cold
// ingesters. THIS sink times and counts the daemon's PHASES (catch-up passes,
// freeze/rebuild, prune) plus the derived progress gauges (lag, watermark,
// retention floor, cold-tier footprint) that are properties of the whole catalog.
//
// A small interface so a test recorder asserts the phase signals without
// Prometheus; every call site reads it via metricsOrNop, so a nil sink no-ops.
// All methods MUST be safe for concurrent use (the catch-up pool reports concurrently).
//
// In cold-only Phase 1 only the catch-up signals have production callers:
// IngestionLag/Watermark/CatchupProgress/CatchupPass (startStreaming + catchUp)
// and Rebuild (executePlan). LastCommitted, ChunkBoundary, Freeze, Prune, and
// ColdTierBytes are part of this stable interface but are wired by Phase 2's
// live-ingestion + lifecycle loops, so they have no call site here yet — don't
// go hunting for one.
type Metrics interface {
	// --- gauges (absolute, last-write-wins) ---

	// IngestionLag sets the lag in ledgers (networkTip - lastCommitted). CATCH-UP
	// ONLY: the steady-state loop has no independent tip to compare against and
	// uses LastCommitted for liveness instead. After catch-up converges this gauge
	// freezes at its final value by design — not a live steady-state health metric.
	IngestionLag(networkTip, lastCommitted uint32)

	// LastCommitted sets the highest durably synced ledger — the per-ledger
	// steady-state liveness signal (refreshed after every synced WriteBatch, so a
	// slow ingester is detectable between the ≈LedgersPerChunk-apart watermark ticks).
	LastCommitted(seq uint32)

	// Watermark sets the derived watermark and the effective retention floor
	// (lowest in-window ledger). Reported by startStreaming and every lifecycle tick.
	Watermark(lastCommitted, retentionFloor uint32)

	// CatchupProgress sets catch-up's position: last ledger backfilled and the
	// tip-anchored target. Equal values mean catch-up has converged.
	CatchupProgress(backfilledThrough, target uint32)

	// ColdTierBytes sets the cold-tier on-disk footprint (ledgers/events/txhash
	// trees). Reported by every lifecycle tick after the prune stage.
	ColdTierBytes(bytes int64)

	// --- counters + durations (one call per completed phase action) ---

	// ChunkBoundary counts one ingestion chunk-boundary handoff; closedChunk is
	// the just-filled chunk's id.
	ChunkBoundary(closedChunk uint32)

	// CatchupPass counts one completed catch-up pass over [lo, hi] and records its
	// wall-clock. A converged (no-op) pass is not reported.
	CatchupPass(lo, hi uint32, d time.Duration)

	// Freeze counts one lifecycle plan-and-execute stage and records its wall-clock.
	// chunkBuilds/indexBuilds are the plan's sizes — 0/0 for an empty tick (still
	// reported, so the rate of empty ticks is observable).
	Freeze(chunkBuilds, indexBuilds int, d time.Duration)

	// Rebuild records one index rebuild's burst throughput (chunks folded per .idx)
	// — the per-IndexBuild signal the Freeze aggregate can't decompose.
	Rebuild(chunks int, d time.Duration)

	// Prune counts the prune-stage sweep ops a tick ran and records its wall-clock.
	Prune(count int, d time.Duration)
}

// nopMetrics discards every signal — the default when a config carries no
// Metrics, so every phase reports without a nil-check.
type nopMetrics struct{}

func (nopMetrics) IngestionLag(uint32, uint32)               {}
func (nopMetrics) LastCommitted(uint32)                      {}
func (nopMetrics) Watermark(uint32, uint32)                  {}
func (nopMetrics) CatchupProgress(uint32, uint32)            {}
func (nopMetrics) ColdTierBytes(int64)                       {}
func (nopMetrics) ChunkBoundary(uint32)                      {}
func (nopMetrics) CatchupPass(uint32, uint32, time.Duration) {}
func (nopMetrics) Freeze(int, int, time.Duration)            {}
func (nopMetrics) Rebuild(int, time.Duration)                {}
func (nopMetrics) Prune(int, time.Duration)                  {}

// metricsOrNop returns m, or nopMetrics{} when nil, so call sites never nil-check.
func metricsOrNop(m Metrics) Metrics {
	if m == nil {
		return nopMetrics{}
	}
	return m
}

// streamingSubsystem is the Prometheus subsystem for the streaming control-plane
// metrics, distinct from ingest's ("fullhistory_ingest") so the families never
// collide in one registry.
const streamingSubsystem = "fullhistory_streaming"

// phaseBuckets time the daemon's phase actions: 1ms … ~70min, ×4 per bucket —
// the same span as ingest's coldStageBuckets, so one dashboard renders both.
//
//nolint:gochecknoglobals // fixed bucket layout, read-only
var phaseBuckets = prometheus.ExponentialBuckets(0.001, 4, 12)

// PrometheusMetrics is the production Metrics sink, recording the daemon's phase
// signals into Prometheus collectors (constructed via NewPrometheusMetrics).
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

// NewPrometheusMetrics builds a PrometheusMetrics and MustRegisters its
// collectors under namespace + the fullhistory_streaming subsystem.
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
		ingestionLag:      gauge("ingestion_lag_ledgers", "catch-up only: network tip minus last committed ledger"),
		lastCommitted:     gauge("last_committed_ledger", "highest ledger the ingestion loop has durably synced (per-ledger liveness)"),
		watermark:         gauge("watermark_ledger", "derived watermark — highest durably committed ledger"),
		retentionFloor:    gauge("retention_floor_ledger", "effective retention floor — lowest in-window ledger"),
		catchupBackfilled: gauge("catchup_backfilled_ledger", "last ledger catch-up has backfilled through"),
		catchupTarget:     gauge("catchup_target_ledger", "catch-up target — tip-anchored upper bound"),
		coldTierBytes:     gauge("cold_tier_bytes", "cold-tier on-disk footprint in bytes"),

		chunkBoundaries: counter("chunk_boundaries_total", "ingestion chunk-boundary handoffs"),
		catchupPasses:   counter("catchup_passes_total", "completed catch-up backfill passes"),
		freezeChunks:    counter("freeze_chunks_total", "chunks frozen by the lifecycle freeze stage"),
		freezeIndexes:   counter("freeze_indexes_total", "indexes built by the lifecycle freeze stage"),
		rebuiltChunks:   counter("rebuilt_chunks_total", "chunks folded into rebuilt indexes"),
		pruned:          counter("pruned_ops_total", "prune-stage sweep ops"),

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

// compile-time assertion: the production sink satisfies the interface.
var _ Metrics = (*PrometheusMetrics)(nil)

// coldTierBytes sums the cold tier's on-disk footprint — the
// ledgers/events/txhash-raw/txhash-index trees (the transient hot tier and the
// tiny meta store are excluded). It walks each root once, ignoring missing trees.
// A per-tree walk error is non-fatal to the others; the lifecycle caller treats a
// returned error as "skip the gauge this tick", so an FS hiccup never aborts the daemon.
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
