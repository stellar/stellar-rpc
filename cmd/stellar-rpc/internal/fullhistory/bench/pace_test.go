package bench

import (
	"context"
	"errors"
	"iter"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
)

// fakeClock is a manually advanced time source, so a test can drive due times
// and pace lag deterministically without sleeping. Tests use it single-threaded
// (the pacer and its consumer run in the test goroutine), so it needs no lock.
type fakeClock struct {
	t time.Time
}

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// staticStream yields n one-byte ledgers, then an optional trailing error. It
// ignores the requested range — the pacer under test paces whatever the inner
// stream produces.
type staticStream struct {
	n   int
	err error
}

func (s staticStream) RawLedgers(
	_ context.Context, _ ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for i := range s.n {
			if !yield([]byte{byte(i)}, nil) {
				return
			}
		}
		if s.err != nil {
			yield(nil, s.err)
		}
	}
}

// TestPacingStreamOnPace: with no work between yields the clock only moves when
// the pacer sleeps, so every ledger after the first waits exactly one interval —
// steady-state, on pace.
func TestPacingStreamOnPace(t *testing.T) {
	const interval = 100 * time.Millisecond
	clock := &fakeClock{t: time.Unix(0, 0)}
	var slept []time.Duration
	// Sleeping advances the clock, modeling a real idle wait to the due time.
	sleep := func(_ context.Context, d time.Duration) error {
		slept = append(slept, d)
		if d > 0 {
			clock.advance(d)
		}
		return nil
	}
	p := pacingStream{
		inner:    staticStream{n: 4},
		schedule: &paceSchedule{interval: interval, firstSeq: 2, clock: clock.now},
		sleep:    sleep,
	}

	got := 0
	for _, err := range p.RawLedgers(context.Background(), ledgerbackend.UnboundedRange(2)) {
		require.NoError(t, err)
		got++
	}
	require.Equal(t, 4, got)
	require.Len(t, slept, 4)
	assert.LessOrEqual(t, slept[0], time.Duration(0), "first ledger is due at the anchor")
	for i := 1; i < 4; i++ {
		assert.Equal(t, interval, slept[i], "ledger at position %d", i)
	}
}

// TestPacingStreamDrainsWhenBehind: when each ledger's ingest overruns the
// interval, every later due time is already in the past, so the pacer asks for a
// non-positive wait and drains back-to-back.
func TestPacingStreamDrainsWhenBehind(t *testing.T) {
	const interval = 100 * time.Millisecond
	const ingest = 250 * time.Millisecond // longer than the interval: always behind
	clock := &fakeClock{t: time.Unix(0, 0)}
	var slept []time.Duration
	// The consumer's ingest drives the clock here, not the sleep.
	sleep := func(_ context.Context, d time.Duration) error {
		slept = append(slept, d)
		return nil
	}
	p := pacingStream{
		inner:    staticStream{n: 4},
		schedule: &paceSchedule{interval: interval, firstSeq: 2, clock: clock.now},
		sleep:    sleep,
	}

	for _, err := range p.RawLedgers(context.Background(), ledgerbackend.UnboundedRange(2)) {
		require.NoError(t, err)
		clock.advance(ingest)
	}
	require.Len(t, slept, 4)
	assert.LessOrEqual(t, slept[0], time.Duration(0))
	for i := 1; i < 4; i++ {
		assert.Negative(t, slept[i], "ledger at position %d should not wait while draining", i)
	}
}

// TestPacingStreamContextCanceled: a wait interrupted by ctx ends the stream
// with that error and yields no further ledgers.
func TestPacingStreamContextCanceled(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	wantErr := errors.New("canceled wait")
	calls := 0
	// The first ledger's ~0 wait succeeds; the second ledger's wait is canceled.
	sleep := func(_ context.Context, _ time.Duration) error {
		calls++
		if calls == 2 {
			return wantErr
		}
		return nil
	}
	p := pacingStream{
		inner:    staticStream{n: 4},
		schedule: &paceSchedule{interval: 100 * time.Millisecond, firstSeq: 2, clock: clock.now},
		sleep:    sleep,
	}

	var gotErr error
	yielded := 0
	for _, err := range p.RawLedgers(context.Background(), ledgerbackend.UnboundedRange(2)) {
		if err != nil {
			gotErr = err
			break
		}
		yielded++
	}
	require.ErrorIs(t, gotErr, wantErr)
	assert.Equal(t, 1, yielded, "only the first ledger yields before the canceled wait")
}

// TestPacingStreamPropagatesInnerError: an error from the inner stream is passed
// through unpaced, ending the stream.
func TestPacingStreamPropagatesInnerError(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	wantErr := errors.New("source failed")
	sleep := func(_ context.Context, _ time.Duration) error { return nil }
	p := pacingStream{
		inner:    staticStream{n: 1, err: wantErr},
		schedule: &paceSchedule{interval: 100 * time.Millisecond, firstSeq: 2, clock: clock.now},
		sleep:    sleep,
	}

	var gotErr error
	for _, err := range p.RawLedgers(context.Background(), ledgerbackend.UnboundedRange(2)) {
		if err != nil {
			gotErr = err
			break
		}
	}
	require.ErrorIs(t, gotErr, wantErr)
}

// TestContextSleep covers the production sleeper: a non-positive duration
// returns at once (the drain case), a positive duration actually blocks until
// it elapses, and a canceled context returns the cancellation error rather
// than waiting out the duration.
func TestContextSleep(t *testing.T) {
	require.NoError(t, contextSleep(context.Background(), 0))
	require.NoError(t, contextSleep(context.Background(), -time.Second))

	// Go timers never fire early, so the elapsed lower bound cannot flake —
	// and it pins that the sleeper really sleeps, the property every paced
	// wall-clock measurement rests on.
	start := time.Now()
	require.NoError(t, contextSleep(context.Background(), 30*time.Millisecond))
	require.GreaterOrEqual(t, time.Since(start), 30*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, contextSleep(ctx, time.Hour), context.Canceled)
}

// TestBacklogGrew pins the drain-or-grow verdict threshold: the backlog counts
// as grown only when the final ledger committed at least one close interval
// late. An on-pace final lag (≈ that ledger's own ingest time) stays below
// the interval no matter how it compares to other ledgers'.
func TestBacklogGrew(t *testing.T) {
	const interval = 600 * time.Millisecond
	assert.False(t, backlogGrew(paceLagStatsResult{final: 30 * time.Millisecond}, interval))
	assert.False(t, backlogGrew(paceLagStatsResult{final: interval - time.Millisecond}, interval))
	assert.True(t, backlogGrew(paceLagStatsResult{final: interval}, interval))
	assert.True(t, backlogGrew(paceLagStatsResult{final: 3 * time.Second}, interval))
}
