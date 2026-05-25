package main

import (
	"math/rand/v2"
	"sync"
	"time"
)

// concurrentResult is one cell of a workers sweep: aggregated latency
// stats across all workers' iterations, the error count, and the raw
// per-iter durations in completion order (useful for downstream
// re-aggregation by caller).
type concurrentResult struct {
	stats     latencyStats
	totalErrs int
	durs      []time.Duration
}

// iterOp is the per-iter closure passed to the sweep runners. `rng`
// is the worker-local PCG RNG. `measured` is true for measured
// iterations and false during warmup; closures that maintain side
// effects (CSV writes, per-cell accumulator slices) MUST gate those
// on `measured` so warmup iters don't pollute either output.
type iterOp func(rng *rand.Rand, measured bool) (time.Duration, error)

// runConcurrentSweep fans `op` out across `workers` goroutines, each
// performing `itersPerWorker` iterations with its own PCG RNG seeded
// from (baseSeed, workers, id). The op closure owns all per-iter
// setup, work, and teardown (eviction, file opens, store handles,
// CSV writes); the runner only handles fan-out, RNG seeding, error
// counting, and percentile aggregation.
//
// Methodology note for cold benches: when two workers happen to pick
// the same chunk in overlapping windows, one's FADV_DONTNEED evicts
// pages the other has already faulted in. With C chunks available
// and W workers, per-iter collision probability is ~1-(1-1/C)^(W-1).
// For C=10, W=8 that's ~50%. Treat the workers≫chunks regime as
// "warm-pagecache contention measurement" rather than "cold-fault
// per-request measurement." Prefer C ≫ W (or single-worker) for
// pure cold-fault numbers.
//
// Per-iter CSV writes inside op MUST be serialized externally (the
// caller provides the mutex). The runner itself does no I/O.
//
// `opsPerSec` in the returned stats is wall-clock based: total
// successful (non-errored) iterations divided by the sweep's wall
// duration. Errored iters are excluded from the numerator but still
// run during the wall window, so a high error rate undershoots ops/sec
// relative to a perfectly successful run — intentional for a bench.
//
// computeStats's sum-of-latencies opsPerSec is overwritten with
// wall-clock-based ops/sec because the sum form is meaningless for
// parallel runs (the durations overlap in real time).
//
// For hot benches that need a RocksDB block-cache warmup phase, use
// runConcurrentSweepWithWarmup — this entry point is for the
// no-warmup (cold) shape and always passes measured=true to op.
func runConcurrentSweep(
	workers, itersPerWorker int,
	baseSeed int64,
	op iterOp,
) concurrentResult {
	rngs := newWorkerRNGs(workers, baseSeed)
	return runConcurrentSweepWithRNGs(rngs, itersPerWorker, true, op)
}

// runConcurrentSweepWithWarmup is the hot-bench entry point: it runs
// `warmupIters` warmup iterations per worker (op invoked with
// measured=false), then `itersPerWorker` measured iterations
// (measured=true), with each worker reusing the same RNG across both
// phases. RNG continuity matters here: re-seeding for the measured
// phase would replay the warmup's draws and the measured iters would
// be hitting already-warmed cache lines.
//
// `opsPerSec` covers only the measured phase — the wall-clock timer
// starts after warmup completes.
func runConcurrentSweepWithWarmup(
	workers, warmupIters, itersPerWorker int,
	baseSeed int64,
	op iterOp,
) concurrentResult {
	rngs := newWorkerRNGs(workers, baseSeed)
	if warmupIters > 0 {
		_ = runConcurrentSweepWithRNGs(rngs, warmupIters, false, op)
	}
	return runConcurrentSweepWithRNGs(rngs, itersPerWorker, true, op)
}

// newWorkerRNGs seeds one PCG RNG per worker with the (baseSeed,
// workers, id) triple mixed in so independent worker streams don't
// share prefixes across different workers-list entries. Without
// `workers` in the seed, rngs[0] for the w=1 cell would draw the
// same sequence as rngs[0] for the w=16 cell — high-w cells would
// then have the low-w cell's exact draws as a subsequence, which
// biases page-cache state across cells in a sweep.
func newWorkerRNGs(workers int, baseSeed int64) []*rand.Rand {
	rngs := make([]*rand.Rand, workers)
	for id := range workers {
		rngs[id] = rand.New(rand.NewPCG(
			uint64(baseSeed)+uint64(id)+uint64(workers)*1000003,
			uint64(baseSeed*7919)+uint64(id)+uint64(workers)*73,
		))
	}
	return rngs
}

// runConcurrentSweepWithRNGs is the worker fan-out core. The caller
// owns the per-worker RNGs so multiple passes (warmup then measured)
// can share state across them. `measured` is plumbed straight through
// to every op call within this invocation.
func runConcurrentSweepWithRNGs(
	rngs []*rand.Rand,
	itersPerWorker int,
	measured bool,
	op iterOp,
) concurrentResult {
	workers := len(rngs)
	type workerResult struct {
		durs []time.Duration
		errs int
	}
	results := make([]workerResult, workers)

	var wg sync.WaitGroup
	wg.Add(workers)
	tStart := time.Now()
	for id := range workers {
		go func(id int) {
			defer wg.Done()
			rng := rngs[id]
			durs := make([]time.Duration, 0, itersPerWorker)
			var errs int
			for range itersPerWorker {
				d, err := op(rng, measured)
				if err != nil {
					errs++
					continue
				}
				durs = append(durs, d)
			}
			results[id] = workerResult{durs: durs, errs: errs}
		}(id)
	}
	wg.Wait()
	wall := time.Since(tStart)

	all := make([]time.Duration, 0, workers*itersPerWorker)
	var totalErrs int
	for _, r := range results {
		all = append(all, r.durs...)
		totalErrs += r.errs
	}
	stats := computeStats(all)
	stats.opsPerSec = float64(len(all)) / wall.Seconds()
	return concurrentResult{stats: stats, totalErrs: totalErrs, durs: all}
}
