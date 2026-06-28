package backfill

import (
	"sync"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/observability"
)

// recordingMetrics is a concurrency-safe Metrics sink for executor tests: records
// Rebuild bursts, Freeze stages, and Prune sweeps; no-ops the rest.
type recordingMetrics struct {
	mu      sync.Mutex
	rebuild []rebuildRec
	freeze  []freezeRec
	prune   []pruneRec
}

type rebuildRec struct {
	chunks int
	d      time.Duration
}

type freezeRec struct {
	chunks, indexes int
	d               time.Duration
}

type pruneRec struct {
	count int
	d     time.Duration
}

func newRecordingMetrics() *recordingMetrics { return &recordingMetrics{} }

func (r *recordingMetrics) Rebuild(chunks int, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rebuild = append(r.rebuild, rebuildRec{chunks, d})
}

func (r *recordingMetrics) Freeze(chunks, indexes int, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.freeze = append(r.freeze, freezeRec{chunks, indexes, d})
}

func (r *recordingMetrics) Prune(count int, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prune = append(r.prune, pruneRec{count, d})
}

func (*recordingMetrics) IngestionLag(uint32, uint32)               {}
func (*recordingMetrics) LastCommitted(uint32)                      {}
func (*recordingMetrics) Watermark(uint32, uint32)                  {}
func (*recordingMetrics) CatchupProgress(uint32, uint32)            {}
func (*recordingMetrics) ColdTierBytes(int64)                       {}
func (*recordingMetrics) ChunkBoundary(uint32)                      {}
func (*recordingMetrics) CatchupPass(uint32, uint32, time.Duration) {}

var _ observability.Metrics = (*recordingMetrics)(nil)
