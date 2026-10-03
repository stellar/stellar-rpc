package bench

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeScenarioClock is a scenarioClock that never sleeps. waitUntil(t) calls onWait
// with the wait's index, when set, and then returns ctx.Err() if ctx is done.
// Otherwise it moves the time to t plus lateness[index], when given; the time
// never moves back.
type fakeScenarioClock struct {
	lateness []time.Duration
	onWait   func(i int)

	mu    sync.Mutex
	t     time.Time
	waits []time.Time
}

func (c *fakeScenarioClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeScenarioClock) waitUntil(ctx context.Context, t time.Time) error {
	c.mu.Lock()
	i := len(c.waits)
	c.waits = append(c.waits, t)
	c.mu.Unlock()
	if c.onWait != nil {
		c.onWait(i)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if i < len(c.lateness) {
		t = t.Add(c.lateness[i])
	}
	if t.After(c.t) {
		c.t = t
	}
	return nil
}

// cancelAtWait returns an onWait hook that calls cancel at wait k, so the
// generator sees the cancel before iteration k.
func cancelAtWait(cancel context.CancelFunc, k int) func(int) {
	return func(i int) {
		if i == k {
			cancel()
		}
	}
}

// countingRequest is a queryRequest that counts calls, warmup included, and
// sleeps countingRequestLatency so latency is never zero.
type countingRequest struct {
	calls atomic.Int64
}

const countingRequestLatency = 50 * time.Microsecond

func (c *countingRequest) run(context.Context, *rand.Rand) (requestTiming, error) {
	c.calls.Add(1)
	return timed(func() (int, error) {
		time.Sleep(countingRequestLatency)
		return 1, nil
	})
}

// okRequest is a queryRequest that succeeds at once with one item.
func okRequest(context.Context, *rand.Rand) (requestTiming, error) {
	return requestTiming{latency: time.Microsecond, items: 1}, nil
}

// TestScenarioPlannedCount: a clean scenario measures round(rps × duration)
// iterations, runs warmup iterations without timing them, and drops nothing.
func TestScenarioPlannedCount(t *testing.T) {
	const rps = 200.0
	fake := &countingRequest{}
	res, err := runConstantArrivalRate(t.Context(), &fakeScenarioClock{}, rps, 100*time.Millisecond, 5, 42, fake.run)
	require.NoError(t, err)

	assert.Equal(t, 20, res.planned)
	assert.Equal(t, 20, res.measured.started)
	assert.Len(t, res.timings, 20)
	assert.Len(t, res.startDelays, 20)
	assert.Equal(t, 0, res.measured.dropped)
	assert.Equal(t, 0, res.measured.failed)
	assert.Equal(t, int64(25), fake.calls.Load(), "5 warmup plus 20 measured iterations each call the request once")
	assert.Equal(t, wantSchedule(rps, res), res.schedule)
	assert.GreaterOrEqual(t, res.elapsed, res.schedule)
	assert.Equal(t, res.elapsed-res.schedule, res.overrun)

	for i, timing := range res.timings {
		assert.Positive(t, timing.latency, "request %d", i)
		assert.GreaterOrEqual(t, timing.latencyFromDue, time.Duration(0), "request %d", i)
		assert.Equal(t, 1, timing.items, "request %d", i)
	}
	for i, delay := range res.startDelays {
		assert.Zero(t, delay, "start delay %d", i)
	}
}

// drawRecorder is a queryRequest that records each request's first RNG draw.
type drawRecorder struct {
	mu    sync.Mutex
	draws []uint64
}

func (d *drawRecorder) run(_ context.Context, rng *rand.Rand) (requestTiming, error) {
	v := rng.Uint64()
	d.mu.Lock()
	d.draws = append(d.draws, v)
	d.mu.Unlock()
	return timed(func() (int, error) { return 1, nil })
}

// TestScenarioRNGIndependence: requests of one scenario, and scenarios at
// different rates, draw distinct values.
func TestScenarioRNGIndependence(t *testing.T) {
	first := &drawRecorder{}
	_, err := runConstantArrivalRate(t.Context(), &fakeScenarioClock{}, 500, 40*time.Millisecond, 0, 7, first.run)
	require.NoError(t, err)
	require.Len(t, first.draws, 20)

	seen := make(map[uint64]bool, len(first.draws))
	for _, v := range first.draws {
		assert.False(t, seen[v], "two requests of one scenario drew %d", v)
		seen[v] = true
	}

	second := &drawRecorder{}
	_, err = runConstantArrivalRate(t.Context(), &fakeScenarioClock{}, 250, 80*time.Millisecond, 0, 7, second.run)
	require.NoError(t, err)
	require.Len(t, second.draws, 20)
	for _, v := range second.draws {
		assert.False(t, seen[v], "a scenario at another rate redrew %d", v)
	}
}

// blockingRequest is a queryRequest that blocks until release is closed.
type blockingRequest struct {
	release chan struct{}
	running atomic.Int64
	calls   atomic.Int64
}

func (b *blockingRequest) run(context.Context, *rand.Rand) (requestTiming, error) {
	b.calls.Add(1)
	b.running.Add(1)
	defer b.running.Add(-1)
	return timed(func() (int, error) {
		<-b.release
		return 1, nil
	})
}

// TestScenarioDrops: with every slot held, the first maxConcurrent measured
// iterations start and the rest are dropped; start delays cover every
// measured iteration. The scenario plans one more iteration than the test
// reaches: the wait for it cancels the scenario and releases the requests, so
// every slot is held until the last reached iteration.
func TestScenarioDrops(t *testing.T) {
	const reached = 1000
	fake := &blockingRequest{release: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	clock := &fakeScenarioClock{onWait: func(i int) {
		if i == reached {
			cancel()
			close(fake.release)
		}
	}}

	res, err := runConstantArrivalRate(ctx, clock, 1000, (reached+1)*time.Millisecond, 0, 3, fake.run)
	require.ErrorIs(t, err, context.Canceled)

	assert.Equal(t, maxConcurrent, res.measured.started)
	assert.Equal(t, reached-maxConcurrent, res.measured.dropped)
	assert.Equal(t, reached, res.planned)
	assert.Equal(t, res.planned, res.measured.started+res.measured.dropped)
	assert.Equal(t, res.measured.started, res.succeeded()+res.measured.failed)
	assert.Len(t, res.timings, maxConcurrent)
	assert.Len(t, res.startDelays, reached, "every measured iteration has a start delay")
	assert.Equal(t, int64(maxConcurrent), fake.calls.Load())
	assert.Equal(t, int64(0), fake.running.Load())
}

// TestScenarioCountsFailures: a failed request is counted, leaves no timing
// and does not end the scenario.
func TestScenarioCountsFailures(t *testing.T) {
	const rps = 200.0
	var ordinal atomic.Int64
	fail := errors.New("request failed")
	req := func(context.Context, *rand.Rand) (requestTiming, error) {
		if ordinal.Add(1)%2 == 0 {
			return requestTiming{}, fail
		}
		return timed(func() (int, error) { return 1, nil })
	}

	res, err := runConstantArrivalRate(t.Context(), &fakeScenarioClock{}, rps, 100*time.Millisecond, 0, 11, req)
	require.NoError(t, err)

	assert.Equal(t, 20, res.measured.started)
	assert.Equal(t, res.planned, res.measured.started+res.measured.dropped)
	assert.Equal(t, res.measured.started, res.succeeded()+res.measured.failed)
	assert.Equal(t, 10, res.measured.failed)
	assert.ErrorIs(t, res.measured.firstErr, fail)
	assert.Len(t, res.timings, 10)
	assert.Len(t, res.startDelays, 20)
	assert.Equal(t, 0, res.measured.dropped)
	assert.Equal(t, wantSchedule(rps, res), res.schedule)
}

// wantSchedule is the expected scenarioResult.schedule of a scenario at rps.
func wantSchedule(rps float64, res scenarioResult) time.Duration {
	return time.Duration(res.measured.started+res.measured.dropped) * scenarioInterval(rps)
}

// TestScenarioRoundsTheInterval: the schedule uses the rounded interval.
func TestScenarioRoundsTheInterval(t *testing.T) {
	assert.Equal(t, 142857143*time.Nanosecond, scenarioInterval(7))

	clock := &fakeScenarioClock{}
	res, err := runConstantArrivalRate(t.Context(), clock, 7, 300*time.Millisecond, 0, 1, okRequest)
	require.NoError(t, err)
	assert.Equal(t, 2, res.planned)
	assert.Equal(t, 2*142857143*time.Nanosecond, res.schedule)
	assert.Equal(t, []time.Time{{}, time.Time{}.Add(142857143 * time.Nanosecond)}, clock.waits)
}

// TestScenarioContextCancel: a canceled context ends the scenario with the
// context's error and no request running. The result covers the iterations
// reached before the cancel, at any rate.
func TestScenarioContextCancel(t *testing.T) {
	for _, tc := range []struct {
		name    string
		rps     float64
		reached int
	}{
		{"low rate", 2, 1},
		{"high rate", 5000, 250},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &countingRequest{}
			var running atomic.Int64
			req := func(ctx context.Context, rng *rand.Rand) (requestTiming, error) {
				running.Add(1)
				defer running.Add(-1)
				return fake.run(ctx, rng)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			clock := &fakeScenarioClock{onWait: cancelAtWait(cancel, tc.reached)}

			res, err := runConstantArrivalRate(ctx, clock, tc.rps, 10*time.Second, 0, 5, req)
			require.ErrorIs(t, err, context.Canceled)
			assert.Equal(t, int64(0), running.Load())
			assert.Len(t, clock.waits, tc.reached+1, "no wait after the cancel")
			assert.Equal(t, tc.reached, res.planned)
			assert.Equal(t, tc.reached, res.measured.started)
			assert.Len(t, res.timings, tc.reached)
			assert.Len(t, res.startDelays, tc.reached)
			assert.Equal(t, time.Duration(tc.reached)*scenarioInterval(tc.rps), res.schedule)
		})
	}
}

// TestScenarioAlreadyCanceled: a context that is done before the scenario
// starts plans and starts nothing.
func TestScenarioAlreadyCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var calls atomic.Int64
	req := func(context.Context, *rand.Rand) (requestTiming, error) {
		calls.Add(1)
		return requestTiming{items: 1}, nil
	}

	res, err := runConstantArrivalRate(ctx, &fakeScenarioClock{}, 100, time.Second, 5, 1, req)
	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, calls.Load())
	assert.Zero(t, res.planned)
	assert.Zero(t, res.measured.started)
	assert.Zero(t, res.warmup.started)
	assert.Zero(t, res.schedule)
	assert.Zero(t, res.elapsed)
	assert.Zero(t, res.processCPU)
}

// TestScenarioCancelWhileRequestsRun: a cancel after the last iteration
// started, while a request still runs, returns the context error.
func TestScenarioCancelWhileRequestsRun(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var calls atomic.Int64
	req := func(context.Context, *rand.Rand) (requestTiming, error) {
		if calls.Add(1) == 3 {
			cancel()
		}
		return requestTiming{items: 1}, nil
	}

	res, err := runConstantArrivalRate(ctx, &fakeScenarioClock{}, 1000, 3*time.Millisecond, 0, 1, req)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 3, res.planned)
	assert.Len(t, res.timings, 3)
}

// TestScenarioCancelReachesRequests: a request gets the scenario's context, so
// a cancel ends a running request, and the request counts as failed because
// it returns the context's error.
func TestScenarioCancelReachesRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var calls atomic.Int64
	req := func(ctx context.Context, _ *rand.Rand) (requestTiming, error) {
		if calls.Add(1) == 3 {
			cancel()
		}
		select {
		case <-ctx.Done():
			return requestTiming{}, ctx.Err()
		case <-time.After(time.Minute):
			return requestTiming{items: 1}, nil
		}
	}

	res, err := runConstantArrivalRate(ctx, &fakeScenarioClock{}, 1000, 3*time.Millisecond, 0, 1, req)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 3, res.measured.started)
	assert.Equal(t, 3, res.measured.failed)
	assert.ErrorIs(t, res.measured.firstErr, context.Canceled)
	assert.Empty(t, res.timings)
}

// TestScenarioStartsWithoutWaitingForResponses: the generator starts each
// iteration at its due time while earlier requests still run. No request
// returns until the last one has started, so a generator that waited for a
// response would never finish.
func TestScenarioStartsWithoutWaitingForResponses(t *testing.T) {
	const iterations = 50
	release := make(chan struct{})
	var calls atomic.Int64
	req := func(context.Context, *rand.Rand) (requestTiming, error) {
		if calls.Add(1) == iterations {
			close(release)
		}
		<-release
		return requestTiming{items: 1}, nil
	}

	res, err := runConstantArrivalRate(t.Context(), &fakeScenarioClock{}, 1000, iterations*time.Millisecond, 0, 13, req)
	require.NoError(t, err)
	assert.Equal(t, iterations, res.measured.started)
	assert.Zero(t, res.measured.dropped)
	assert.Equal(t, make([]time.Duration, iterations), res.startDelays, "every start is on time")
}

// TestScenarioChargesLateStart: a late start shows in latency_from_due and in
// the start delay, not in latency.
func TestScenarioChargesLateStart(t *testing.T) {
	const late = 50 * time.Millisecond
	clock := &fakeScenarioClock{t: time.Unix(100, 0)}
	run := newScenarioRun(clock, okRequest, 1, 1, 1)
	due := clock.now().Add(-late)
	run.start(t.Context(), 0, due, clock.now(), true)
	run.wg.Wait()

	res := run.result(due, scenarioInterval(1), 0)
	require.Len(t, res.timings, 1)
	require.Len(t, res.startDelays, 1)
	assert.Equal(t, late, res.timings[0].latencyFromDue, "the client waited from the due time")
	assert.Equal(t, time.Microsecond, res.timings[0].latency, "the request itself took none of that wait")
	assert.Equal(t, late, res.startDelays[0], "the start delay is charged too")
}

// TestScenarioRejectsBadArguments: a bad rate, duration or warmup count is an
// error.
func TestScenarioRejectsBadArguments(t *testing.T) {
	clock := &fakeScenarioClock{}
	_, err := runConstantArrivalRate(t.Context(), clock, 0, time.Second, 0, 1, okRequest)
	assert.Error(t, err)
	_, err = runConstantArrivalRate(t.Context(), clock, 10, 0, 0, 1, okRequest)
	assert.Error(t, err)
	// At 1e-12 rps the interval does not fit in a Duration.
	_, err = runConstantArrivalRate(t.Context(), clock, 1e-12, time.Second, 0, 1, okRequest)
	assert.ErrorContains(t, err, "too low")
	// 1e6 rps over a century is more iterations than the scenario cap allows.
	_, err = runConstantArrivalRate(t.Context(), clock, 1e6, 100*365*24*time.Hour, 0, 1, okRequest)
	assert.ErrorContains(t, err, "more than")
	// 0.01 rps for a second rounds to no measured iteration at all.
	_, err = runConstantArrivalRate(t.Context(), clock, 0.01, time.Second, 0, 1, okRequest)
	assert.ErrorContains(t, err, "no measured iteration")
	// A warmup at the scenario cap leaves no room for a measured iteration.
	_, err = runConstantArrivalRate(t.Context(), clock, 10, time.Second, maxIterations, 1, okRequest)
	assert.ErrorContains(t, err, "warmup plus")
	assert.Empty(t, clock.waits, "a rejected scenario never waits")
}

// TestScenarioNegativeWarmup: a negative warmup counts as zero.
func TestScenarioNegativeWarmup(t *testing.T) {
	res, err := runConstantArrivalRate(t.Context(), &fakeScenarioClock{}, 1000, 10*time.Millisecond, -3, 1, okRequest)
	require.NoError(t, err)
	assert.Zero(t, res.warmup.started+res.warmup.dropped)
	assert.Equal(t, 10, res.planned)
	assert.Equal(t, 10, res.measured.started+res.measured.dropped)
}

// TestScenarioIterationCapRounds: the cap applies to the rounded count, so a
// duration that rounds down to the cap passes and one that rounds above it
// fails.
func TestScenarioIterationCapRounds(t *testing.T) {
	seconds := func(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }

	_, planned, err := validateScenario(1, seconds(maxIterations+0.4), 0)
	require.NoError(t, err)
	assert.Equal(t, maxIterations, planned)

	_, _, err = validateScenario(1, seconds(maxIterations+0.5), 0)
	require.ErrorContains(t, err, "more than 100000000 iterations")
}

// TestScenarioRejectsOutOfRangeArguments: a rate, duration or warmup count past
// a Duration or iteration limit is an error with an empty result.
func TestScenarioRejectsOutOfRangeArguments(t *testing.T) {
	for _, tc := range []struct {
		name     string
		rps      float64
		duration time.Duration
		warmup   int
		want     string
	}{
		{"sub-nanosecond interval", 1e9 + 1, time.Nanosecond, 0, "less than 1ns"},
		{"iteration count", 1, time.Second, math.MaxInt, "more than 100000000 iterations"},
		{"warmup count", 0.1, 10 * time.Second, int(math.MaxInt32), "more than 100000000 iterations"},
		{"schedule length", 0.001, 1000 * time.Second, 10_000_000, "schedule overflows"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := runConstantArrivalRate(t.Context(), &fakeScenarioClock{}, tc.rps, tc.duration, tc.warmup, 1, nil)
			require.ErrorContains(t, err, tc.want)
			assert.Equal(t, scenarioResult{}, res)
		})
	}
}

// TestScenarioLowRate: a rate below one request per second works.
func TestScenarioLowRate(t *testing.T) {
	assert.Equal(t, 2000*time.Second, scenarioInterval(0.0005))
	_, planned, err := validateScenario(0.0005, 10000*time.Second, 0)
	require.NoError(t, err)
	assert.Equal(t, 5, planned)
}

// TestScenarioNanosecondInterval: a rate that rounds to a 1ns interval plans
// and runs one iteration.
func TestScenarioNanosecondInterval(t *testing.T) {
	for _, rps := range []float64{math.Nextafter(1e9, 0), 1e9} {
		t.Run(formatRPS(rps), func(t *testing.T) {
			res, err := runConstantArrivalRate(t.Context(), &fakeScenarioClock{}, rps, time.Nanosecond, 0, 1, okRequest)
			require.NoError(t, err)
			assert.Equal(t, 1, res.planned)
			assert.Equal(t, 1, res.measured.started)
			assert.Len(t, res.timings, 1)
			assert.Zero(t, res.measured.dropped)
			assert.Zero(t, res.measured.failed)
			assert.Equal(t, time.Nanosecond, res.schedule)
		})
	}
}

// TestScenarioTimeWindows: elapsed is the larger of the schedule and the last
// measured response, and overrun is elapsed minus the schedule.
func TestScenarioTimeWindows(t *testing.T) {
	const interval = 10 * time.Millisecond
	firstDue := time.Unix(1, 0)
	for _, tc := range []struct {
		name             string
		success, fail    time.Duration
		elapsed, overrun time.Duration
	}{
		{"before end", 15 * time.Millisecond, 0, 20 * time.Millisecond, 0},
		{"at end", 20 * time.Millisecond, 0, 20 * time.Millisecond, 0},
		{"after end", 35 * time.Millisecond, 0, 35 * time.Millisecond, 15 * time.Millisecond},
		{"failure finishes last", 15 * time.Millisecond, 40 * time.Millisecond, 40 * time.Millisecond, 20 * time.Millisecond},
		{"success finishes last", 40 * time.Millisecond, 15 * time.Millisecond, 40 * time.Millisecond, 20 * time.Millisecond},
		{"all failed", 0, 40 * time.Millisecond, 40 * time.Millisecond, 20 * time.Millisecond},
		{"all dropped", 0, 0, 20 * time.Millisecond, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := newScenarioRun(&fakeScenarioClock{}, nil, 1, 100, 2)
			if tc.success > 0 {
				run.recordTiming(requestTiming{}, firstDue.Add(tc.success))
				run.startDelays = append(run.startDelays, 0)
				run.measured.started++
			}
			if tc.fail > 0 {
				for run.measured.started < 2 {
					run.recordFailure(&run.measured, errors.New("request failed"), firstDue.Add(tc.fail))
					run.startDelays = append(run.startDelays, 0)
					run.measured.started++
				}
			}
			// Fill all slots so the remaining iterations are dropped.
			for range maxConcurrent {
				run.slots <- struct{}{}
			}
			for i := run.measured.started; i < 2; i++ {
				run.start(t.Context(), i, firstDue.Add(time.Duration(i)*interval), firstDue, true)
			}
			res := run.result(firstDue, interval, 0)
			assert.Equal(t, 2, res.planned)
			assert.Equal(t, res.planned, res.measured.started+res.measured.dropped)
			assert.Equal(t, res.measured.started, res.succeeded()+res.measured.failed)
			assert.Equal(t, 2*interval, res.schedule)
			assert.Equal(t, tc.elapsed, res.elapsed)
			assert.Equal(t, tc.overrun, res.overrun)
		})
	}
}

// TestScenarioWarmupDoesNotMoveElapsed: a warmup response after the last
// measured response leaves elapsed and overrun unchanged.
func TestScenarioWarmupDoesNotMoveElapsed(t *testing.T) {
	const interval = 10 * time.Millisecond
	firstDue := time.Unix(1, 0)
	run := newScenarioRun(&fakeScenarioClock{}, nil, 1, 100, 1)
	run.recordTiming(requestTiming{}, firstDue.Add(15*time.Millisecond))
	run.startDelays = append(run.startDelays, 0)
	run.measured.started++
	run.recordFailure(&run.warmup, errors.New("warmup failed"), firstDue.Add(50*time.Millisecond))

	res := run.result(firstDue, interval, 0)
	assert.Equal(t, 1, res.warmup.failed)
	assert.Equal(t, 15*time.Millisecond, res.elapsed)
	assert.Equal(t, 5*time.Millisecond, res.overrun)
}

// TestScenarioWarmupDrop: a warmup iteration reached while every slot is held
// is dropped and counted in the warmup phase only.
func TestScenarioWarmupDrop(t *testing.T) {
	run := newScenarioRun(&fakeScenarioClock{}, nil, 1, 100, 1)
	for range maxConcurrent {
		run.slots <- struct{}{}
	}
	now := time.Unix(1, 0)
	run.start(t.Context(), 0, now, now, false)
	assert.Equal(t, 1, run.warmup.dropped)
	assert.Zero(t, run.warmup.started)
	assert.Equal(t, phaseCounts{}, run.measured)
	assert.Empty(t, run.startDelays)
}

// TestScenarioCancelDuringWarmup: a cancel before the first measured iteration
// leaves the reached warmup iterations and no measured phase.
func TestScenarioCancelDuringWarmup(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	clock := &fakeScenarioClock{t: time.Unix(100, 0), onWait: cancelAtWait(cancel, 3)}

	res, err := runConstantArrivalRate(ctx, clock, 1000, 10*time.Millisecond, 5, 1, okRequest)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 3, res.warmup.started+res.warmup.dropped)
	assert.Zero(t, res.planned)
	assert.Zero(t, res.measured.started)
	assert.Zero(t, res.schedule)
	assert.Zero(t, res.processCPU)
}

// TestScenarioAbsoluteDueTimes: iteration i is due at start + i×interval,
// whatever the lateness of earlier iterations. Every request completes at the
// final fake time, so each latency_from_due is that time minus its due time.
func TestScenarioAbsoluteDueTimes(t *testing.T) {
	const (
		rps      = 1000
		interval = time.Millisecond
		warmup   = 3
		measured = 6
		total    = warmup + measured
	)
	start := time.Unix(100, 0)
	clock := &fakeScenarioClock{
		t: start,
		lateness: []time.Duration{
			0, 300 * time.Microsecond, 0,
			50 * time.Microsecond, 0, 700 * time.Microsecond, 10 * time.Microsecond, 0, 250 * time.Microsecond,
		},
	}
	due := func(i int) time.Time { return start.Add(time.Duration(i) * interval) }

	// The request of the last iteration releases all of them, so every request
	// reads the time after the last waitUntil.
	release := make(chan struct{})
	var calls atomic.Int64
	req := func(context.Context, *rand.Rand) (requestTiming, error) {
		if calls.Add(1) == total {
			close(release)
		}
		<-release
		return requestTiming{items: 1}, nil
	}

	res, err := runConstantArrivalRate(t.Context(), clock, rps, measured*interval, warmup, 1, req)
	require.NoError(t, err)

	wantWaits := make([]time.Time, total)
	for i := range total {
		wantWaits[i] = due(i)
	}
	assert.Equal(t, wantWaits, clock.waits)

	done := due(total - 1).Add(clock.lateness[total-1])
	wantFromDue := make([]time.Duration, 0, measured)
	for i := warmup; i < total; i++ {
		wantFromDue = append(wantFromDue, done.Sub(due(i)))
	}
	gotFromDue := make([]time.Duration, 0, len(res.timings))
	for _, timing := range res.timings {
		gotFromDue = append(gotFromDue, timing.latencyFromDue)
	}
	slices.Sort(wantFromDue)
	slices.Sort(gotFromDue)

	assert.Equal(t, clock.lateness[warmup:], res.startDelays)
	assert.Equal(t, wantFromDue, gotFromDue)
	assert.Equal(t, measured, res.planned)
	assert.Equal(t, measured*interval, res.schedule)
	assert.Equal(t, max(res.schedule, done.Sub(due(warmup))), res.elapsed)
}

// cpuRequest is a queryRequest that keeps the CPU busy for about 1ms.
func cpuRequest(context.Context, *rand.Rand) (requestTiming, error) {
	return timed(func() (int, error) {
		deadline := time.Now().Add(time.Millisecond)
		for time.Now().Before(deadline) {
		}
		return 1, nil
	})
}

// TestTimerClock is a smoke test of the real clock: each latency_from_due is at
// least its latency, and processCPU counts the requests' CPU time. Its bounds
// are generous, so a busy machine does not fail it.
func TestTimerClock(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, timerClock{}.waitUntil(ctx, time.Now().Add(time.Hour)), context.Canceled)
	require.ErrorIs(t, timerClock{}.waitUntil(ctx, time.Now().Add(-time.Hour)), context.Canceled)

	start := time.Now()
	res, err := runConstantArrivalRate(t.Context(), timerClock{}, 1000, 20*time.Millisecond, 0, 1, cpuRequest)
	require.NoError(t, err)
	assert.Equal(t, 20, res.planned)
	assert.Equal(t, res.planned, res.measured.started+res.measured.dropped)
	assert.GreaterOrEqual(t, time.Since(start), 19*time.Millisecond, "the clock waits for the last due time")
	assert.Less(t, time.Since(start), 10*time.Second)
	assert.Positive(t, res.processCPU)
	for i, timing := range res.timings {
		assert.GreaterOrEqual(t, timing.latencyFromDue, timing.latency, "request %d", i)
	}
}
