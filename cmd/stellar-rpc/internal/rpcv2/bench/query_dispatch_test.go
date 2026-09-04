package bench

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingRequest is a queryRequest that counts every call, so a test can tell
// how many requests a leg actually issued — warmup requests included, which
// leave no sample behind. Its body sleeps for countingRequestService so that
// the service time it reports is never zero: an empty body can start and finish
// inside one clock tick, and timed would then read the same value twice.
type countingRequest struct {
	calls atomic.Int64
}

// countingRequestService is the work countingRequest does, long enough for the
// clock to move and short enough to leave the leg's pacing alone.
const countingRequestService = 50 * time.Microsecond

func (c *countingRequest) run(*rand.Rand) (cellSample, error) {
	c.calls.Add(1)
	return timed("", func() (int, error) {
		time.Sleep(countingRequestService)
		return 1, nil
	})
}

// TestRunPacedLegMeasuredCount pins the shape of a clean leg: the number of
// measured requests is round(rps × duration), warmup requests run at the leg's
// rate but leave no sample, the offered window is the measured positions times
// the arrival interval, every measured request that answered carries a
// scheduled latency at least as long as its service time, and nothing is shed
// or fails.
func TestRunPacedLegMeasuredCount(t *testing.T) {
	const rps = 200.0
	fake := &countingRequest{}
	res, err := runPacedLeg(t.Context(), rps, 100*time.Millisecond, 5, 42, fake.run)
	require.NoError(t, err)

	assert.Equal(t, 20, res.dispatched)
	assert.Len(t, res.samples, 20)
	assert.Len(t, res.lags, 20)
	assert.Equal(t, 0, res.shed)
	assert.Equal(t, 0, res.errs)
	assert.Equal(t, int64(25), fake.calls.Load(), "warmup requests run at the leg's rate")
	assert.Positive(t, res.wall)
	assert.Equal(t, offeredWindow(rps, res), res.offered)

	for i, s := range res.samples {
		assert.Positive(t, s.service, "sample %d", i)
		assert.GreaterOrEqual(t, s.scheduled, s.service, "sample %d", i)
		assert.Equal(t, 1, s.items, "sample %d", i)
	}
	for i, lag := range res.lags {
		assert.GreaterOrEqual(t, lag, time.Duration(0), "lag %d", i)
	}
}

// drawRecorder is a queryRequest that answers immediately and records the first
// value each request draws from the RNG it was handed.
type drawRecorder struct {
	mu    sync.Mutex
	draws []uint64
}

func (d *drawRecorder) run(rng *rand.Rand) (cellSample, error) {
	v := rng.Uint64()
	d.mu.Lock()
	d.draws = append(d.draws, v)
	d.mu.Unlock()
	return timed("", func() (int, error) { return 1, nil })
}

// TestRunPacedLegRNGIndependence pins that every request of a leg draws its own
// sequence and that two legs of one seed at different rates draw different
// sequences, so a later leg does not repeat the work an earlier one warmed.
func TestRunPacedLegRNGIndependence(t *testing.T) {
	first := &drawRecorder{}
	_, err := runPacedLeg(t.Context(), 500, 40*time.Millisecond, 0, 7, first.run)
	require.NoError(t, err)
	require.Len(t, first.draws, 20)

	seen := make(map[uint64]bool, len(first.draws))
	for _, v := range first.draws {
		assert.False(t, seen[v], "two requests of one leg drew %d", v)
		seen[v] = true
	}

	second := &drawRecorder{}
	_, err = runPacedLeg(t.Context(), 250, 80*time.Millisecond, 0, 7, second.run)
	require.NoError(t, err)
	require.Len(t, second.draws, 20)
	for _, v := range second.draws {
		assert.False(t, seen[v], "a leg at another rate redrew %d", v)
	}
}

// blockingRequest is a queryRequest that holds its slot until release is
// closed, so a test can fill the in-flight cap and keep it full.
type blockingRequest struct {
	release  chan struct{}
	inFlight atomic.Int64
	calls    atomic.Int64
}

func (b *blockingRequest) run(*rand.Rand) (cellSample, error) {
	b.calls.Add(1)
	b.inFlight.Add(1)
	defer b.inFlight.Add(-1)
	return timed("", func() (int, error) {
		<-b.release
		return 1, nil
	})
}

// TestRunPacedLegSheds pins what a leg does when the store cannot keep up: the
// first maxInFlight measured requests get a slot and every later one is shed,
// no position is silently skipped, the shed requests leave no sample, and the
// lag row still covers every measured position.
func TestRunPacedLegSheds(t *testing.T) {
	fake := &blockingRequest{release: make(chan struct{})}
	// The leg's schedule spans 100ms and a shed position costs nothing, so the
	// dispatch loop is over well before the release fires. Releasing only then
	// keeps every slot occupied for the whole loop.
	timer := time.AfterFunc(500*time.Millisecond, func() { close(fake.release) })
	defer timer.Stop()

	res, err := runPacedLeg(t.Context(), 10000, 100*time.Millisecond, 0, 3, fake.run)
	require.NoError(t, err)

	assert.Equal(t, maxInFlight, res.dispatched)
	assert.Equal(t, 1000-maxInFlight, res.shed)
	assert.Equal(t, 1000, res.dispatched+res.shed)
	assert.Len(t, res.samples, maxInFlight)
	assert.Len(t, res.lags, 1000, "every measured position is charged a dispatch lag")
	assert.Equal(t, int64(maxInFlight), fake.calls.Load())
	assert.Equal(t, int64(0), fake.inFlight.Load())
}

// TestRunPacedLegCountsErrors pins that a failed request is counted and leaves
// no sample, that it does not end the leg, and that a failure does not change
// the window the leg offered.
func TestRunPacedLegCountsErrors(t *testing.T) {
	const rps = 200.0
	var ordinal atomic.Int64
	fail := errors.New("request failed")
	req := func(*rand.Rand) (cellSample, error) {
		if ordinal.Add(1)%2 == 0 {
			return cellSample{}, fail
		}
		return timed("", func() (int, error) { return 1, nil })
	}

	res, err := runPacedLeg(t.Context(), rps, 100*time.Millisecond, 0, 11, req)
	require.NoError(t, err)

	assert.Equal(t, 20, res.dispatched)
	assert.Equal(t, 10, res.errs)
	assert.Len(t, res.samples, 10)
	assert.Len(t, res.lags, 20)
	assert.Equal(t, 0, res.shed)
	assert.Equal(t, offeredWindow(rps, res), res.offered)
}

// offeredWindow is the span of arrivals a leg at rate rps offered: one arrival
// interval per measured position, shed positions included.
func offeredWindow(rps float64, res legResult) time.Duration {
	return time.Duration(res.dispatched+res.shed) * time.Duration(float64(time.Second)/rps)
}

// TestRunPacedLegContextCancel pins that a canceled context ends the leg at its
// next due time, reports the context's error, and leaves no request running.
func TestRunPacedLegContextCancel(t *testing.T) {
	fake := &countingRequest{}
	inFlight := &atomic.Int64{}
	req := func(rng *rand.Rand) (cellSample, error) {
		inFlight.Add(1)
		defer inFlight.Add(-1)
		return fake.run(rng)
	}

	ctx, cancel := context.WithCancel(t.Context())
	timer := time.AfterFunc(100*time.Millisecond, cancel)
	defer timer.Stop()

	start := time.Now()
	_, err := runPacedLeg(ctx, 2, 10*time.Second, 0, 5, req)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.Canceled)
	assert.Less(t, elapsed, time.Second, "the leg ends at the cancel, not at its full duration")
	assert.Equal(t, int64(0), inFlight.Load())
}

// TestRunPacedLegLagStaysSmall pins that the dispatcher keeps its schedule when
// requests take longer than the arrival interval. Requests run concurrently, so
// a slow request holds up no later dispatch: the leg reports one lag per
// dispatched request, the typical lag is a small fraction of the service time,
// and the whole leg finishes in about its scheduled span rather than in the sum
// of its requests.
//
// The assertions are on the median lag and the wall rather than on the worst
// lag, because a single dispatch can lose the CPU for tens of milliseconds on a
// busy machine while the dispatcher is still on schedule overall.
func TestRunPacedLegLagStaysSmall(t *testing.T) {
	const service = 20 * time.Millisecond
	req := func(*rand.Rand) (cellSample, error) {
		return timed("", func() (int, error) {
			time.Sleep(service)
			return 1, nil
		})
	}

	res, err := runPacedLeg(t.Context(), 1000, 50*time.Millisecond, 0, 13, req)
	require.NoError(t, err)

	assert.Equal(t, 50, res.dispatched)
	assert.Len(t, res.lags, res.dispatched)
	for i, lag := range res.lags {
		assert.GreaterOrEqual(t, lag, time.Duration(0), "lag %d", i)
	}

	sorted := slices.Clone(res.lags)
	slices.Sort(sorted)
	assert.Less(t, sorted[len(sorted)/2], service, "median dispatch lag")
	// A serialized leg would run 50 requests of 20ms back to back, so it could
	// not finish inside half a second.
	assert.Less(t, res.wall, 500*time.Millisecond, "leg wall")
}

// TestLaunchPacedRequestChargesLateDispatch pins that a dispatch the loop
// reached late is charged to the scheduled latency a client sees rather than
// hidden in the service time, which is the property the open-loop mode exists
// for. The request itself answers immediately, so everything the scheduled
// latency holds beyond it is the dispatcher's own lateness.
func TestLaunchPacedRequestChargesLateDispatch(t *testing.T) {
	const late = 50 * time.Millisecond
	req := func(*rand.Rand) (cellSample, error) {
		return timed("", func() (int, error) { return 1, nil })
	}

	collector := &legCollector{}
	slots := make(chan struct{}, 1)
	var wg sync.WaitGroup
	due := time.Now().Add(-late)
	launchPacedRequest(&wg, slots, collector, req, legRNG(1, 0, 1), due, true)
	wg.Wait()

	res := collector.result(due)
	require.Len(t, res.samples, 1)
	require.Len(t, res.lags, 1)
	assert.GreaterOrEqual(t, res.samples[0].scheduled, late, "the client waited from the due time")
	assert.Less(t, res.samples[0].service, res.samples[0].scheduled-40*time.Millisecond,
		"the request itself took almost none of that wait")
	assert.GreaterOrEqual(t, res.lags[0], late, "the dispatch lag is charged too")
}

// TestRunPacedLegRejectsBadArguments pins that a leg with no rate or no
// duration is refused rather than run as an empty measurement.
func TestRunPacedLegRejectsBadArguments(t *testing.T) {
	req := func(*rand.Rand) (cellSample, error) {
		return timed("", func() (int, error) { return 1, nil })
	}
	_, err := runPacedLeg(t.Context(), 0, time.Second, 0, 1, req)
	assert.Error(t, err)
	_, err = runPacedLeg(t.Context(), 10, 0, 0, 1, req)
	assert.Error(t, err)
}
