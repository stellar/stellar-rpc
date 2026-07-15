// Package latencytrack records durations and reports exact-enough quantiles
// in-process — the layer Prometheus histograms cannot provide (their quantiles
// are bucket-limited server-side estimates, and summaries cannot be aggregated
// or extended with a max-ever). Count, sum (so Avg), and Max are exact;
// P50/P75/P90/P99 come from log-spaced buckets with ~6% relative error.
//
// A Tracker is one named series (e.g. one per ingest phase or RPC method);
// a Set is a named collection of them. Both are safe for concurrent use, and
// both are nil-receiver safe: an unwired *Set (or the nil *Tracker it hands
// out) silently drops every Record, so call sites never nil-check.
package latencytrack

import (
	"encoding/json"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// The bucket grid: geometric from 10µs, ×1.12 per bucket, 160 buckets — the
// top bound lands at ~11 minutes. A duration in bucket i is estimated by
// linear interpolation inside (bounds[i-1], bounds[i]], so the worst-case
// relative error is the bucket width (12%), typically about half that.
// Durations past the top bound clamp into the last bucket; Max stays exact.
const (
	bucketBase   = 10 * time.Microsecond
	bucketFactor = 1.12
	numBuckets   = 160
)

//nolint:gochecknoglobals // fixed bucket grid, computed once, read-only
var bucketBounds = func() [numBuckets]int64 {
	var b [numBuckets]int64
	bound := float64(bucketBase.Nanoseconds())
	for i := range b {
		b[i] = int64(math.Round(bound))
		bound *= bucketFactor
	}
	return b
}()

// bucketFor returns the index of the bucket holding ns: the first bucket whose
// upper bound is >= ns, or the last bucket when ns is past the top bound.
func bucketFor(ns int64) int {
	idx := sort.Search(numBuckets, func(i int) bool { return ns <= bucketBounds[i] })
	if idx == numBuckets {
		return numBuckets - 1
	}
	return idx
}

// Tracker records durations for one series. Concurrent-safe; Record is
// lock-free (atomic counters), so it is cheap enough for per-ledger and
// per-request call sites.
type Tracker struct {
	count    atomic.Uint64
	sumNanos atomic.Int64
	maxNanos atomic.Int64
	buckets  [numBuckets]atomic.Uint64
}

// Record adds one duration to the series. Negative durations count as zero.
// A nil receiver drops the sample (see the package doc).
func (t *Tracker) Record(d time.Duration) {
	if t == nil {
		return
	}
	ns := d.Nanoseconds()
	if ns < 0 {
		ns = 0
	}
	t.count.Add(1)
	t.sumNanos.Add(ns)
	for {
		cur := t.maxNanos.Load()
		if ns <= cur || t.maxNanos.CompareAndSwap(cur, ns) {
			break
		}
	}
	t.buckets[bucketFor(ns)].Add(1)
}

// Snapshot summarizes everything recorded so far. It reads the counters
// without stopping writers, so a snapshot taken mid-Record may lag that one
// sample; a series with no samples returns the zero Stats.
func (t *Tracker) Snapshot() Stats {
	if t == nil {
		return Stats{}
	}
	count := t.count.Load()
	if count == 0 {
		return Stats{}
	}
	stats := Stats{
		Count: count,
		Avg:   time.Duration(t.sumNanos.Load() / int64(count)), //nolint:gosec // count > 0 checked above
		Max:   time.Duration(t.maxNanos.Load()),
	}

	var counts [numBuckets]uint64
	var total uint64
	for i := range t.buckets {
		c := t.buckets[i].Load()
		counts[i] = c
		total += c
	}
	if total == 0 {
		return stats
	}
	maxNs := stats.Max.Nanoseconds()
	stats.P50 = quantile(&counts, total, 0.50, maxNs)
	stats.P75 = quantile(&counts, total, 0.75, maxNs)
	stats.P90 = quantile(&counts, total, 0.90, maxNs)
	stats.P99 = quantile(&counts, total, 0.99, maxNs)
	return stats
}

// quantile estimates the q-quantile (q in (0,1)) from the bucket counts by
// linear interpolation inside the bucket holding rank q*total. The exact
// observed max bounds the estimate: it replaces the upper bound of the bucket
// the max falls in, and extends the last bucket when clamped overflows sit
// there, so no estimate ever exceeds the true max.
func quantile(counts *[numBuckets]uint64, total uint64, q float64, maxNs int64) time.Duration {
	rank := q * float64(total)
	cum := 0.0
	for i, c := range counts {
		if c == 0 {
			continue
		}
		cum += float64(c)
		if cum < rank {
			continue
		}
		lo := 0.0
		if i > 0 {
			lo = float64(bucketBounds[i-1])
		}
		hi := float64(bucketBounds[i])
		if m := float64(maxNs); (m > lo && m < hi) || (i == numBuckets-1 && m > hi) {
			hi = m
		}
		frac := (rank - (cum - float64(c))) / float64(c)
		frac = math.Max(0, math.Min(1, frac))
		return time.Duration(int64(lo + frac*(hi-lo)))
	}
	return time.Duration(maxNs)
}

// Stats is one series' summary. Count, Avg, and Max are exact; the quantiles
// are bucket estimates (see the package doc). The zero value means "no
// samples".
type Stats struct {
	Count              uint64
	Avg, Max           time.Duration
	P50, P75, P90, P99 time.Duration
}

// MarshalJSON emits every duration in seconds as a float — the wire form the
// admin /latency.json endpoint and the metrics JSON-RPC method serve.
func (s Stats) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Count uint64  `json:"count"`
		Avg   float64 `json:"avg"`
		Max   float64 `json:"max"`
		P50   float64 `json:"p50"`
		P75   float64 `json:"p75"`
		P90   float64 `json:"p90"`
		P99   float64 `json:"p99"`
	}{s.Count, s.Avg.Seconds(), s.Max.Seconds(), s.P50.Seconds(), s.P75.Seconds(), s.P90.Seconds(), s.P99.Seconds()})
}

// Set is a named collection of Trackers, created on first use. The zero value
// is ready to use; a nil *Set hands out nil Trackers, so an unwired Set drops
// every Record (see the package doc).
type Set struct {
	mu       sync.Mutex
	trackers map[string]*Tracker
}

// Tracker returns the series with the given name, creating it on first use.
// Resolve once and reuse the returned Tracker on hot paths — each call takes
// the Set's lock.
func (s *Set) Tracker(name string) *Tracker {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.trackers == nil {
		s.trackers = make(map[string]*Tracker)
	}
	t := s.trackers[name]
	if t == nil {
		t = &Tracker{}
		s.trackers[name] = t
	}
	return t
}

// SnapshotAll snapshots every series, keyed by name. encoding/json marshals
// map keys in sorted order, so the JSON form is stably ordered for free.
func (s *Set) SnapshotAll() map[string]Stats {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	named := make(map[string]*Tracker, len(s.trackers))
	for name, t := range s.trackers {
		named[name] = t
	}
	s.mu.Unlock()

	out := make(map[string]Stats, len(named))
	for name, t := range named {
		out[name] = t.Snapshot()
	}
	return out
}
