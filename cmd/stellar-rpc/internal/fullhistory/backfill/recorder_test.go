package backfill

import (
	"sync"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/observability"
)

// recordingMetrics is a concurrency-safe Metrics sink for executor tests: records
// only Rebuild bursts, no-ops the rest.
type recordingMetrics struct {
	mu      sync.Mutex
	rebuild []rebuildRec
}

type rebuildRec struct {
	chunks int
	d      time.Duration
}

func newRecordingMetrics() *recordingMetrics { return &recordingMetrics{} }

func (r *recordingMetrics) Rebuild(chunks int, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rebuild = append(r.rebuild, rebuildRec{chunks, d})
}

func (*recordingMetrics) IngestionLag(uint32, uint32)               {}
func (*recordingMetrics) LastCommitted(uint32)                      {}
func (*recordingMetrics) Watermark(uint32, uint32)                  {}
func (*recordingMetrics) CatchupProgress(uint32, uint32)            {}
func (*recordingMetrics) ColdTierBytes(int64)                       {}
func (*recordingMetrics) ChunkBoundary(uint32)                      {}
func (*recordingMetrics) CatchupPass(uint32, uint32, time.Duration) {}
func (*recordingMetrics) Freeze(int, int, time.Duration)            {}
func (*recordingMetrics) Prune(int, time.Duration)                  {}

var _ observability.Metrics = (*recordingMetrics)(nil)
