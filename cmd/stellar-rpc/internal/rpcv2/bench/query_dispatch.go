package bench

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// This file is the query bench's dispatcher: it issues a per-request closure at
// a fixed arrival rate and collects what every request observed. What a request
// does is the business of the per-type bodies in query_types.go; the dispatcher
// only paces, times and counts, and it does no I/O of its own, so the only thing
// between the timer and the store is the query itself.

// cellSample is one measured request: what it cost, what its response carried,
// and which sub-stage of its query type it belongs to.
type cellSample struct {
	// service is how long the request body itself ran, measured by timed.
	service time.Duration
	// scheduled is the span from the request's due time to its completion,
	// stamped by the dispatcher. It carries the dispatcher's lag behind
	// schedule on top of the service time, so it is the latency a client
	// sending at the target rate observes.
	scheduled time.Duration
	// items counts what the response carried: ledgers, transactions or
	// events, depending on the query type.
	items int
	// stage names a sub-stage of the query type, so these samples are also
	// reported apart from the type's blended row. Only txhash sets it,
	// splitting found lookups from not-found ones (see recordLeg).
	stage string
}

// queryRequest issues one request against the fixture and reports what the
// response carried. It runs on its own goroutine with an RNG the dispatcher
// hands it, so an implementation must treat its corpus as read-only and must
// not share mutable state between concurrent requests.
//
// The whole request is inside the caller's timer, read-view acquisition
// included, because a served request pays for its own view.
type queryRequest func(rng *rand.Rand) (cellSample, error)

// maxInFlight is the number of requests a paced leg may have outstanding at
// once. A request that arrives while the cap is full is shed and counted, so a
// target rate the store cannot keep up with shows up as a shed count and the
// leg's goroutine count stays bounded — what it measures stays the store.
const maxInFlight = 512

// Mixing constants for the per-request seeds. They are odd and mutually prime
// so that the seed, the position and the rate each move both PCG words.
const (
	legSeedRateHi = 1000003
	legSeedRateLo = 73
	legSeedStride = 7919
)

// legResult is one paced leg's outcome.
type legResult struct {
	// samples holds the measured requests that answered. A request that
	// failed contributes no sample, since it has no latency to report.
	samples []cellSample
	// lags holds one dispatch lag per measured position, in schedule order,
	// shed positions included. A zero is a real observation — a dispatch that
	// was on time — so zeros are kept, and a shed position's lag shows how far
	// behind schedule the dispatcher ran while it was shedding.
	lags []time.Duration
	// offered is the span of arrivals the leg offered the store: its measured
	// positions times the arrival interval. It is the denominator of the
	// leg's achieved rate.
	offered time.Duration
	// wall spans the first measured due time to the last measured completion,
	// so it includes the tail the leg spent draining its last requests.
	wall time.Duration
	// dispatched counts the measured requests that got a slot and ran.
	dispatched int
	// shed counts the measured requests dropped because maxInFlight was full.
	shed int
	// errs counts the measured requests that returned an error.
	errs int
}

// legCollector gathers a paced leg's outcome from the dispatch loop and the
// request goroutines, which write to it concurrently.
type legCollector struct {
	mu         sync.Mutex
	samples    []cellSample
	lags       []time.Duration
	lastDone   time.Time
	dispatched int
	shed       int
	errs       int
}

// recordLag notes how far behind its due time a measured position was reached.
// Every measured position is charged one lag, whether its request ran or was
// shed.
func (c *legCollector) recordLag(lag time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lags = append(c.lags, lag)
}

// recordDispatch notes that a measured request got a slot and started.
func (c *legCollector) recordDispatch() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dispatched++
}

// recordShed notes that a measured request was dropped because the in-flight
// cap was full.
func (c *legCollector) recordShed() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shed++
}

// recordSample stores a measured request's sample and keeps the leg's latest
// completion time, which is where its wall ends.
func (c *legCollector) recordSample(s cellSample, done time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.samples = append(c.samples, s)
	if done.After(c.lastDone) {
		c.lastDone = done
	}
}

// recordError notes that a measured request failed at the given time. A failed
// request occupied the leg like any other, so its completion counts toward the
// wall.
func (c *legCollector) recordError(done time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs++
	if done.After(c.lastDone) {
		c.lastDone = done
	}
}

// result assembles the leg's outcome, measuring the wall from firstDue. A leg
// in which nothing completed reports a zero wall.
func (c *legCollector) result(firstDue time.Time) legResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := legResult{
		samples:    c.samples,
		lags:       c.lags,
		dispatched: c.dispatched,
		shed:       c.shed,
		errs:       c.errs,
	}
	if !c.lastDone.IsZero() {
		out.wall = c.lastDone.Sub(firstDue)
	}
	return out
}

// runPacedLeg issues req at a fixed arrival rate of rps requests per second and
// reports what the leg observed. Position i is due at anchor + i×interval, where
// interval is one second divided by rps and the anchor is the first position's
// dispatch. Due times are absolute, so a slow request delays no later one and
// the leg keeps its rate whatever the store does with it.
//
// Positions 0 to warmup-1 run at the leg's rate and record nothing; the
// round(rps × duration) positions after them are measured. Their due times run
// from warmup×interval to (warmup+measured-1)×interval, one interval short of
// duration, and the leg offers the store measured×interval of arrivals. Each
// request runs on its own goroutine with its own RNG, and the in-flight count
// is capped at maxInFlight.
//
// The returned error is a context error: it means the leg was cut short, so
// discard its result. A request that fails is counted in errs and does not end
// the leg.
func runPacedLeg(
	ctx context.Context, rps float64, duration time.Duration, warmup int, seed int64, req queryRequest,
) (legResult, error) {
	if rps <= 0 || math.IsNaN(rps) || math.IsInf(rps, 0) {
		return legResult{}, fmt.Errorf("paced leg needs a positive finite rate, got %v", rps)
	}
	if duration <= 0 {
		return legResult{}, fmt.Errorf("paced leg needs a positive duration, got %v", duration)
	}
	warmup = max(warmup, 0)
	measured := measuredRequests(rps, duration)
	interval := time.Duration(float64(time.Second) / rps)
	schedule := newPaceSchedule(interval, 0)

	collector := &legCollector{}
	slots := make(chan struct{}, maxInFlight)
	var wg sync.WaitGroup
	for pos := range warmup + measured {
		if err := ctx.Err(); err != nil {
			wg.Wait()
			return legResult{}, err
		}
		due := schedule.dueForPos(pos)
		if err := contextSleep(ctx, due.Sub(schedule.clock())); err != nil {
			wg.Wait()
			return legResult{}, err
		}
		launchPacedRequest(&wg, slots, collector, req, legRNG(seed, pos, rps), due, pos >= warmup)
	}
	wg.Wait()
	res := collector.result(schedule.dueForPos(warmup))
	res.offered = time.Duration(measured) * interval
	return res, nil
}

// launchPacedRequest starts one request of a paced leg on its own goroutine,
// taking a slot from slots first. With no slot free the request is shed: it is
// counted and it runs nothing. A measured position is charged its dispatch lag
// either way, so the lag distribution covers the whole leg. A warmup request
// runs in full and records nothing.
func launchPacedRequest(
	wg *sync.WaitGroup, slots chan struct{}, collector *legCollector,
	req queryRequest, rng *rand.Rand, due time.Time, measured bool,
) {
	if measured {
		collector.recordLag(max(time.Since(due), 0))
	}
	select {
	case slots <- struct{}{}:
	default:
		if measured {
			collector.recordShed()
		}
		return
	}
	if measured {
		collector.recordDispatch()
	}
	wg.Go(func() {
		defer func() { <-slots }()
		s, err := req(rng)
		done := time.Now()
		switch {
		case !measured:
		case err != nil:
			collector.recordError(done)
		default:
			s.scheduled = done.Sub(due)
			collector.recordSample(s, done)
		}
	})
}

// legRNG returns the random source for the request at position pos of the leg
// at rate rps, mixing the rate and the position into the run's seed. The rate
// keeps two legs of one query type drawing different sequences, and the
// position keeps two requests of one leg drawing different work.
func legRNG(seed int64, pos int, rps float64) *rand.Rand {
	rateBits := math.Float64bits(rps)
	//nolint:gosec // seed mixing, not cryptography
	return rand.New(rand.NewPCG(
		uint64(seed)+uint64(pos)+rateBits*legSeedRateHi,
		uint64(seed*legSeedStride)+uint64(pos)+rateBits*legSeedRateLo,
	))
}

// measuredRequests returns how many measured requests a leg at rate rps issues
// over duration: the rounded product of the two, and never fewer than one, so a
// rate slow enough to schedule less than one request in the leg still measures
// something.
func measuredRequests(rps float64, duration time.Duration) int {
	return max(1, int(math.Round(rps*duration.Seconds())))
}

// timed runs fn and returns a sample carrying how long fn took as its service
// time, so a per-type body times exactly the request and nothing around it.
func timed(stage string, fn func() (int, error)) (cellSample, error) {
	start := time.Now()
	items, err := fn()
	service := time.Since(start)
	if err != nil {
		return cellSample{}, err
	}
	return cellSample{service: service, items: items, stage: stage}, nil
}
