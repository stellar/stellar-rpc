package bench

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"syscall"
	"time"
)

// This file is the bench-query open-loop load generator. It starts requests
// on a fixed schedule and never waits for a response before it starts
// the next request. See README.md for the terms and formulas.

// requestTiming holds the timings of one successful request.
type requestTiming struct {
	// latency spans request start to response.
	latency time.Duration
	// latencyFromDue spans the iteration's due time to response: start delay
	// plus latency.
	latencyFromDue time.Duration
	// items counts what the response carried.
	items int
	// outcome is the txhash lookup result, or outcomeNone.
	outcome lookupOutcome
}

// lookupOutcome splits txhash latencies by lookup result.
type lookupOutcome uint8

const (
	outcomeNone lookupOutcome = iota

	outcomeFound
	outcomeNotFound
)

// queryRequest sends one request. Calls run concurrently on separate
// goroutines; rng is per call. Read-view acquisition is inside the timer. ctx
// is the scenario's context. A request must return soon after ctx is done,
// because a canceled scenario waits for every running request before it
// returns.
type queryRequest func(ctx context.Context, rng *rand.Rand) (requestTiming, error)

// maxConcurrent caps a scenario's running requests. An iteration that comes
// due while maxConcurrent requests run is dropped, so a slow store cannot grow
// the goroutine count without bound.
const maxConcurrent = 512

// maxRPS is the highest target rate: one iteration per nanosecond.
const maxRPS = float64(time.Second)

// maxIterations caps a scenario's iterations, warmup included: about 2.7
// hours at 10k rps. At the cap the scenario holds about 4 GB of timings until
// it ends.
const maxIterations = 100_000_000

// phaseCounts counts one phase's iterations. Every iteration is started or
// dropped. failed counts the started requests that returned an error, and
// firstErr is nil when failed is zero.
type phaseCounts struct {
	started  int
	dropped  int
	failed   int
	firstErr error
}

// scenarioRecord holds what a scenario records as it runs. The generator
// goroutine writes startDelays and each phase's started and dropped without
// the lock. The request goroutines write timings and each phase's failed and
// firstErr under scenarioRun.mu.
type scenarioRecord struct {
	// startDelays has one entry per measured iteration, dropped ones included.
	startDelays []time.Duration
	// timings has one entry per measured request that succeeded.
	timings []requestTiming

	warmup   phaseCounts
	measured phaseCounts
}

// scenarioResult is one scenario's outcome. The fields hold the measured phase
// only.
type scenarioResult struct {
	scenarioRecord

	// planned counts the measured iterations: round(rps × duration), or the
	// iterations reached before a cancel.
	planned int
	// schedule is planned × interval, the length of the schedule.
	schedule time.Duration
	// elapsed spans the first measured due time to the last measured response,
	// and is at least schedule. Failed requests count as responses.
	elapsed time.Duration
	// overrun is elapsed − schedule: how long the last response arrived after
	// the schedule ended.
	overrun time.Duration
	// processCPU is the CPU time of the whole process, user plus system, from
	// the start of the first measured iteration to the end of the scenario. It
	// includes the store's work. Zero when the measured phase did not start.
	processCPU time.Duration
}

// succeeded counts the measured requests that succeeded.
func (r scenarioResult) succeeded() int { return len(r.timings) }

// scenarioClock is the load generator's time source.
type scenarioClock interface {
	now() time.Time
	// waitUntil returns nil at or after t, or ctx.Err() once ctx is done. It
	// never returns nil while ctx is done.
	waitUntil(ctx context.Context, t time.Time) error
}

// timerClock is the real scenarioClock. It waits on a Go timer, which can end
// its wait up to about 1 ms late. That delay shows in start_delay and
// latency_from_due, not in latency.
type timerClock struct{}

func (timerClock) now() time.Time { return time.Now() }

func (timerClock) waitUntil(ctx context.Context, t time.Time) error {
	// contextSleep returns at once for a past t and does not read ctx.
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := contextSleep(ctx, time.Until(t)); err != nil {
		return err
	}
	// The timer and the cancel can be ready together; the cancel wins.
	return ctx.Err()
}

// scenarioRun is one scenario's state while it runs.
type scenarioRun struct {
	scenarioRecord

	clock  scenarioClock
	req    queryRequest
	rngKey uint64
	wg     sync.WaitGroup
	slots  chan struct{}

	// mu guards lastResponse and the scenarioRecord fields of the request
	// goroutines.
	mu           sync.Mutex
	lastResponse time.Time
}

func newScenarioRun(clock scenarioClock, req queryRequest, seed int64, rps float64, planned int) *scenarioRun {
	return &scenarioRun{
		clock:  clock,
		req:    req,
		rngKey: scenarioRNGKey(seed, rps),
		slots:  make(chan struct{}, maxConcurrent),
		scenarioRecord: scenarioRecord{
			startDelays: make([]time.Duration, 0, planned),
			timings:     make([]requestTiming, 0, planned),
		},
	}
}

func (r *scenarioRun) recordTiming(t requestTiming, done time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.timings = append(r.timings, t)
	if done.After(r.lastResponse) {
		r.lastResponse = done
	}
}

// recordFailure counts a failed request in phase p. Only a measured failure
// moves lastResponse, which bounds elapsed.
func (r *scenarioRun) recordFailure(p *phaseCounts, err error, done time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p.failed++
	if p.firstErr == nil {
		p.firstErr = err
	}
	if p == &r.measured && done.After(r.lastResponse) {
		r.lastResponse = done
	}
}

// result assembles the scenario's outcome once every request has returned.
// Each measured iteration reached has one start delay, so len(startDelays) is
// planned.
func (r *scenarioRun) result(firstDue time.Time, interval, processCPU time.Duration) scenarioResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := scenarioResult{
		scenarioRecord: r.scenarioRecord,
		planned:        len(r.startDelays),
		processCPU:     processCPU,
	}
	out.schedule = time.Duration(out.planned) * interval
	out.elapsed = out.schedule
	if !r.lastResponse.IsZero() {
		out.elapsed = max(out.schedule, r.lastResponse.Sub(firstDue))
	}
	out.overrun = out.elapsed - out.schedule
	return out
}

// scenarioInterval is the time between two due times at rps.
func scenarioInterval(rps float64) time.Duration {
	return time.Duration(math.Round(float64(time.Second) / rps))
}

// validateScenario checks a scenario's arguments and returns its interval and
// planned iteration count. warmup must be non-negative.
func validateScenario(rps float64, duration time.Duration, warmup int) (time.Duration, int, error) {
	if rps <= 0 || math.IsNaN(rps) || math.IsInf(rps, 0) {
		return 0, 0, fmt.Errorf("scenario needs a positive finite rate, got %v", rps)
	}
	if rps > maxRPS {
		return 0, 0, fmt.Errorf("scenario rate %v is too high: its interval is less than 1ns", rps)
	}
	if float64(time.Second)/rps >= math.MaxInt64 {
		return 0, 0, fmt.Errorf("scenario rate %v is too low: its interval overflows a Duration", rps)
	}
	if duration <= 0 {
		return 0, 0, fmt.Errorf("scenario needs a positive duration, got %v", duration)
	}
	if rps*duration.Seconds() > maxIterations {
		return 0, 0, fmt.Errorf("scenario at %v rps for %v plans more than %d iterations",
			rps, duration, maxIterations)
	}
	planned := int(math.Round(rps * duration.Seconds()))
	if planned < 1 {
		return 0, 0, fmt.Errorf(
			"scenario at %v rps for %v plans no measured iteration; raise --duration or --target-rps",
			rps, duration)
	}
	if warmup > maxIterations-planned {
		return 0, 0, fmt.Errorf(
			"scenario plans more than %d iterations: %d warmup plus %d measured",
			maxIterations, warmup, planned)
	}
	interval := scenarioInterval(rps)
	if int64(warmup+planned) > math.MaxInt64/int64(interval) {
		return 0, 0, errors.New("scenario schedule overflows a Duration")
	}
	return interval, planned, nil
}

// runConstantArrivalRate runs one scenario: it starts req at rps iterations
// per second, an open-loop load. Iteration i is due at start + i×interval,
// where start is clock.now() when the scenario begins. Iterations 0 to
// warmup-1 are warmup; the round(rps × duration) iterations after them are
// measured. A negative warmup counts as zero.
//
// A failed request is counted and does not end the scenario. A non-nil error
// is a bad argument, with an empty result, or a context error, with a result
// that covers the iterations reached before the cancel. Requests that the
// cancel stops count as failed.
func runConstantArrivalRate(
	ctx context.Context, clock scenarioClock, rps float64, duration time.Duration, warmup int, seed int64,
	req queryRequest,
) (scenarioResult, error) {
	warmup = max(warmup, 0)
	interval, planned, err := validateScenario(rps, duration, warmup)
	if err != nil {
		return scenarioResult{}, err
	}

	run := newScenarioRun(clock, req, seed, rps, planned)
	start := clock.now()
	due := func(i int) time.Time { return start.Add(time.Duration(i) * interval) }
	var cpuAtFirstStart time.Duration
	for i := range warmup + planned {
		if err = clock.waitUntil(ctx, due(i)); err != nil {
			break
		}
		if i == warmup {
			cpuAtFirstStart = processCPUTime()
		}
		run.start(ctx, i, due(i), clock.now(), i >= warmup)
	}

	run.wg.Wait()
	var processCPU time.Duration
	if len(run.startDelays) > 0 {
		processCPU = processCPUTime() - cpuAtFirstStart
	}
	if err == nil {
		// A cancel while the last requests finish still ends the scenario as
		// canceled.
		err = ctx.Err()
	}
	return run.result(due(warmup), interval, processCPU), err
}

// start runs iteration i's request on its own goroutine when fewer than
// maxConcurrent requests run, and drops the iteration otherwise. A measured
// iteration records its start delay, now minus due, whether or not it is
// dropped. Called from the generator goroutine only.
func (r *scenarioRun) start(ctx context.Context, i int, due, now time.Time, measured bool) {
	phase := &r.warmup
	if measured {
		phase = &r.measured
		r.startDelays = append(r.startDelays, max(now.Sub(due), 0))
	}
	select {
	case r.slots <- struct{}{}:
	default:
		phase.dropped++
		return
	}
	phase.started++
	r.wg.Go(func() {
		t, err := r.req(ctx, requestRNG(r.rngKey, i))
		done := r.clock.now()
		<-r.slots
		switch {
		case err != nil:
			r.recordFailure(phase, err, done)
		case measured:
			t.latencyFromDue = done.Sub(due)
			r.recordTiming(t, done)
		}
	})
}

// processCPUTime returns the process's user plus system CPU time, or zero if
// the kernel does not report it.
func processCPUTime() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// scenarioRNGKey mixes the run seed and the scenario rate into one key.
// Scenarios at different rates get different keys.
func scenarioRNGKey(seed int64, rps float64) uint64 {
	return splitmix64(uint64(seed) ^ splitmix64(math.Float64bits(rps))) //nolint:gosec // seed mixing, not cryptography
}

// requestRNG returns the RNG of iteration i in the scenario with key. Distinct
// iterations or keys give distinct PCG states, so distinct streams.
func requestRNG(key uint64, i int) *rand.Rand {
	return rand.New(rand.NewPCG(key, splitmix64(key^uint64(i)))) //nolint:gosec // seed mixing, not cryptography
}

// splitmix64 is the SplitMix64 finalizer, a bijection on uint64.
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// timed runs fn and returns its run time as latency.
//
//nolint:unparam // outcome is set by the txhash body in bench-query/02-read-path, the next PR in this stack
func timed(outcome lookupOutcome, fn func() (int, error)) (requestTiming, error) {
	start := time.Now()
	items, err := fn()
	latency := time.Since(start)
	if err != nil {
		return requestTiming{}, err
	}
	return requestTiming{latency: latency, items: items, outcome: outcome}, nil
}
