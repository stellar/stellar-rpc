package backfill

import (
	"sync"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/observability"
)

// recordingMetrics is a concurrency-safe Metrics sink for executor tests: records
// one entry per Rebuild stage, Freeze stage, and Prune sweep; no-ops the rest.
type recordingMetrics struct {
	mu      sync.Mutex
	rebuild []time.Duration
	freeze  []time.Duration
	prune   []pruneRec
}

type pruneRec struct {
	count int
	d     time.Duration
}

func newRecordingMetrics() *recordingMetrics { return &recordingMetrics{} }

func (r *recordingMetrics) Rebuild(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rebuild = append(r.rebuild, d)
}

func (r *recordingMetrics) Freeze(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.freeze = append(r.freeze, d)
}

func (r *recordingMetrics) Prune(count int, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prune = append(r.prune, pruneRec{count, d})
}

func (*recordingMetrics) LastCommitted(uint32, uint32) {}
func (*recordingMetrics) LedgerCommitted(uint32)       {}
func (*recordingMetrics) ChunkBoundary(uint32)         {}
func (*recordingMetrics) BackfillPass(time.Duration)   {}
func (*recordingMetrics) LiveHotChunks(int)            {}
func (*recordingMetrics) ColdTierBytes(int64)          {}
func (*recordingMetrics) Discard(int, time.Duration)   {}

var _ observability.Metrics = (*recordingMetrics)(nil)
