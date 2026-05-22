package main

import (
	"math/rand/v2"
	"sync"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// coldWorkload is the per-iteration body of a cold-concurrent run.
// runColdConcurrent handles chunk selection, eviction, reader open/close,
// and timing; the workload picks the in-chunk target and does the read.
// Returning an error increments the error counter; the iteration's
// elapsed time is discarded.
type coldWorkload func(r *ledger.ColdStoreReader, rng *rand.Rand, chunkID uint32) error

type concurrentResult struct {
	stats     latencyStats
	totalErrs int
	durs      []time.Duration // per-iteration latencies, in completion order
}

// runColdConcurrent runs N worker goroutines, each looping over
// itersPerWorker iterations of: pick chunk in [chunkLo, chunkLo+chunkSpan),
// FADV_DONTNEED evict, open ColdStoreReader, invoke op, close.
//
// Per-worker RNG and decompressor avoid cross-worker contention so any
// measured contention is at the kernel layer (CPU sched + NVMe), not
// in user space. Wall-clock-aggregate ops/sec is computed from
// successful iterations only.
func runColdConcurrent(
	logger *supportlog.Entry,
	coldDir string,
	chunkLo, chunkSpan uint32,
	workers, itersPerWorker int,
	baseSeed int64,
	op coldWorkload,
) concurrentResult {
	type workerResult struct {
		durs []time.Duration
		errs int
	}
	results := make([]workerResult, workers)

	var wg sync.WaitGroup
	wg.Add(workers)
	tStart := time.Now()
	for wID := range workers {
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(
				uint64(baseSeed)+uint64(id),
				uint64(baseSeed*7919)+uint64(id),
			))
			durs := make([]time.Duration, 0, itersPerWorker)
			var errs int

			for range itersPerWorker {
				c := chunkLo + rng.Uint32N(chunkSpan)
				path := packPath(coldDir, c)

				if err := evictFile(path); err != nil {
					logger.WithError(err).Warnf("w%d evict %s", id, path)
					errs++
					continue
				}

				t0 := time.Now()
				r, err := ledger.NewColdStoreReader(path)
				if err != nil {
					logger.WithError(err).Warnf("w%d open %s", id, path)
					errs++
					continue
				}
				err = op(r, rng, c)
				d := time.Since(t0)
				_ = r.Close()
				if err != nil {
					errs++
					continue
				}
				durs = append(durs, d)
			}
			results[id] = workerResult{durs: durs, errs: errs}
		}(wID)
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
