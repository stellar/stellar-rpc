package bench

import (
	"math/rand/v2"
	"sync"
	"time"
)

// This file is the query bench's fan-out core: it runs one (query type,
// concurrency) cell by fanning a per-request closure across W goroutines and
// collecting every request's latency. It is deliberately ignorant of what the
// requests do — the per-type bodies in query_types.go own that — and it does no
// I/O of its own, so the only thing between the timer and the store is the
// query itself.

// cellSample is one measured request: its latency, the number of items the
// response carried, and the sub-stage it belongs to. An empty stage means the
// sample belongs only to the type's blended row; txhash sets it so found and
// not-found lookups are also reported apart (see recordCell).
type cellSample struct {
	d     time.Duration
	items int
	stage string
}

// queryRequest issues one request against the fixture and reports what the
// response carried. It runs on a sweep worker's goroutine with that worker's
// RNG, so an implementation must treat its corpus as read-only and must not
// share mutable state between workers.
//
// The whole request is inside the caller's timer, read-view acquisition
// included: a served request pays for its own view, and leaving that out would
// flatter every number.
type queryRequest func(rng *rand.Rand) (cellSample, error)

// sweepResult is one cell's outcome: every measured request in completion
// order, the wall-clock the measured pass took, and how many requests failed.
// Failed requests contribute no sample — a latency for a request that did not
// answer is not a latency — but they do consume wall-clock, so a cell with
// errors reports a throughput below what a clean run would.
type sweepResult struct {
	samples []cellSample
	wall    time.Duration
	errs    int
}

// runSweep fans req out across workers goroutines, each issuing itersPerWorker
// measured requests after warmup unmeasured ones. Each worker draws from its
// own RNG so the workers pick independent work, and the warmup and measured
// passes share that RNG rather than reseeding: reseeding would replay the
// warmup's draws and the measured pass would read what it just warmed.
//
// A cell's total request count is workers × itersPerWorker. Iterations are
// split evenly up front rather than pulled off a shared queue, which keeps the
// hot loop free of coordination — the point is to measure the store, not a work
// queue.
//
// Warmup matters for the hot tier, whose steady state is a warm RocksDB block
// cache. A cold cell passes warmup 0 and evicts the page cache before the pass
// instead (see evictColdArtifacts): warming it would measure the opposite of
// what a cold read is.
func runSweep(workers, warmup, itersPerWorker int, seed int64, req queryRequest) sweepResult {
	rngs := workerRNGs(workers, seed)
	if warmup > 0 {
		runSweepPass(rngs, warmup, false, req)
	}
	return runSweepPass(rngs, itersPerWorker, true, req)
}

// workerRNGs seeds one RNG per worker, mixing the worker count into the seed
// alongside the worker id. Without the worker count, worker 0 of the c1 cell
// and worker 0 of the c16 cell would draw the same sequence, so the c16 cell
// would re-read exactly what c1 just read and inherit its page-cache state —
// the sweep would measure a warming curve instead of a concurrency curve.
func workerRNGs(workers int, seed int64) []*rand.Rand {
	rngs := make([]*rand.Rand, workers)
	for id := range workers {
		//nolint:gosec // seed mixing, not cryptography
		rngs[id] = rand.New(rand.NewPCG(
			uint64(seed)+uint64(id)+uint64(workers)*1000003,
			uint64(seed*7919)+uint64(id)+uint64(workers)*73,
		))
	}
	return rngs
}

// runSweepPass is the fan-out itself: one goroutine per RNG, each issuing iters
// requests. An unmeasured pass discards its samples but still runs every
// request, which is the whole point of a warmup.
func runSweepPass(rngs []*rand.Rand, iters int, measured bool, req queryRequest) sweepResult {
	type workerResult struct {
		samples []cellSample
		errs    int
	}
	results := make([]workerResult, len(rngs))

	var wg sync.WaitGroup
	wg.Add(len(rngs))
	start := time.Now()
	for id, rng := range rngs {
		go func(id int, rng *rand.Rand) {
			defer wg.Done()
			var r workerResult
			if measured {
				r.samples = make([]cellSample, 0, iters)
			}
			for range iters {
				s, err := req(rng)
				if err != nil {
					r.errs++
					continue
				}
				if measured {
					r.samples = append(r.samples, s)
				}
			}
			results[id] = r
		}(id, rng)
	}
	wg.Wait()
	wall := time.Since(start)

	out := sweepResult{wall: wall}
	if measured {
		out.samples = make([]cellSample, 0, len(rngs)*iters)
	}
	for _, r := range results {
		out.samples = append(out.samples, r.samples...)
		out.errs += r.errs
	}
	return out
}

// timed runs fn and returns a sample carrying its latency, so a per-type body
// times exactly the request and nothing around it.
func timed(stage string, fn func() (int, error)) (cellSample, error) {
	start := time.Now()
	items, err := fn()
	d := time.Since(start)
	if err != nil {
		return cellSample{}, err
	}
	return cellSample{d: d, items: items, stage: stage}, nil
}
