package backfill

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
)

// progress reports what a backfill pass is doing while it runs, so a healthy
// pass and a wedged one do not look the same for a day.
//
// Counts and measured bytes, no percentage and no ETA: chunk cost varies
// roughly twentyfold from genesis to the tip, so extrapolating from what is
// done is wrong for most of the run. Index builds get their own lines — they
// hold no worker slot while they wait and then all land at the tail.
type progress struct {
	logger  *supportlog.Entry
	metrics observability.Metrics
	layout  geometry.Layout
	chunks  int
	indexes int
	start   time.Time

	mu         sync.Mutex
	chunksDone int
	indexDone  int
	bytes      int64
	inFlight   int
}

func newProgress(cfg ExecConfig, plan Plan, start time.Time) *progress {
	// Only a pass with work republishes. A steady-state tick resolves an empty
	// plan, and zeroing both gauges there would erase the finished backfill.
	if len(plan.ChunkBuilds) > 0 {
		cfg.metrics().BackfillPlanned(len(plan.ChunkBuilds))
		cfg.metrics().BackfillCompleted(0)
	}
	return &progress{
		logger:  cfg.Logger,
		metrics: cfg.metrics(),
		layout:  cfg.Catalog.Layout(),
		chunks:  len(plan.ChunkBuilds),
		indexes: len(plan.IndexBuilds),
		start:   start,
	}
}

func (p *progress) chunkStarted() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inFlight++
}

// chunkEnded pairs with chunkStarted on EVERY exit, so a failed build does not
// leave in_flight inflated for the rest of the pass.
func (p *progress) chunkEnded() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inFlight--
}

// chunkFrozen reports one finished chunk build, sizing it from the artifacts
// just written.
func (p *progress) chunkFrozen(cb ChunkBuild) {
	var size int64
	for _, kind := range cb.Artifacts.Kinds() {
		for _, path := range p.layout.ArtifactPaths(cb.Chunk, kind) {
			if fi, err := os.Stat(path); err == nil {
				size += fi.Size()
			}
		}
	}
	p.mu.Lock()
	p.chunksDone, p.bytes = p.chunksDone+1, p.bytes+size
	done, total, written, flight := p.chunksDone, p.chunks, p.bytes, p.inFlight
	// Under the lock: two workers finishing together could otherwise Set in the
	// opposite order to the one they counted in. Not a race the detector sees.
	p.metrics.BackfillCompleted(done)
	p.mu.Unlock()

	elapsed := time.Since(p.start)
	fields := supportlog.F{
		"chunk":       cb.Chunk.String(),
		"done":        done,
		"of":          total,
		"chunk_bytes": size,
		"written":     written,
		"in_flight":   flight - 1, // this chunk is still counted; report the others
		"elapsed":     elapsed.Round(time.Second).String(),
	}
	// Needs a measurable interval: int64(+Inf) is undefined in Go.
	if elapsed >= time.Second {
		fields["bytes_per_sec"] = int64(float64(written) / elapsed.Seconds())
	}
	p.logger.WithFields(fields).Info("chunk frozen")
}

// chunkFailed reports a chunk build that exhausted its retries. A cancellation
// is fallout from a shutdown or a sibling's failure, not a cause, so it is not
// warned about; indexBuilt applies the same rule.
func (p *progress) chunkFailed(cb ChunkBuild, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	p.logger.WithField("chunk", cb.Chunk.String()).WithError(err).
		Warn("chunk build failed after every attempt")
}

// indexStarted reports one coverage entering its build. No done count: builds
// run concurrently, so they would all print the same number. indexBuilt has it.
func (p *progress) indexStarted(b IndexBuild) {
	p.logger.WithFields(supportlog.F{
		"coverage_lo": b.Lo.String(),
		"coverage_hi": b.Hi.String(),
		"of":          p.indexes,
	}).Info("txhash index build starting")
}

func (p *progress) indexBuilt(b IndexBuild, dur time.Duration, err error) {
	fields := supportlog.F{
		"coverage_lo": b.Lo.String(),
		"coverage_hi": b.Hi.String(),
		"duration":    dur.Round(time.Millisecond).String(),
	}
	if err == nil {
		// Only a build that produced an index is progress.
		p.mu.Lock()
		p.indexDone++
		fields["done"], fields["of"] = p.indexDone, p.indexes
		p.mu.Unlock()
	}
	switch {
	case errors.Is(err, context.Canceled):
		// Same rule as chunkFailed: cancellation is fallout, not a cause.
		p.logger.WithFields(fields).Info("txhash index build canceled")
		return
	case err != nil:
		p.logger.WithFields(fields).WithError(err).Warn("txhash index build failed")
		return
	}
	p.logger.WithFields(fields).Info("txhash index build complete")
}
