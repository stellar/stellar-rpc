package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// runOps retries a failed (idempotent) op a bounded number of times on a fixed
// pause before giving up, so a transient sweep failure doesn't cancel ingestion
// and force a whole-daemon restart.

func TestRunOps_RetriesTransientThenSucceeds(t *testing.T) {
	cfg := Config{opRetryAttempts: 3, opRetryBackoff: time.Millisecond}
	calls := 0
	op := func() error {
		calls++
		if calls < 3 {
			return errors.New("busy file")
		}
		return nil
	}
	completed, err := runOps(context.Background(), cfg, []func() error{op})
	require.NoError(t, err)
	require.Equal(t, 1, completed, "the one op ran to success")
	require.Equal(t, 3, calls, "two transient failures retried, third try succeeds")
}

func TestRunOps_GivesUpAfterAttempts(t *testing.T) {
	cfg := Config{opRetryAttempts: 2, opRetryBackoff: time.Millisecond}
	calls := 0
	op := func() error { calls++; return errors.New("permanent") }
	completed, err := runOps(context.Background(), cfg, []func() error{op})
	require.Error(t, err)
	require.Zero(t, completed, "the failing op did not complete")
	require.Equal(t, 2, calls, "attempts total tries (1 initial + 1 retry), then gives up")
}

func TestRunOps_CtxCancelStopsBeforeOp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := Config{opRetryAttempts: 3, opRetryBackoff: time.Hour}
	calls := 0
	op := func() error { calls++; return errors.New("x") }
	completed, err := runOps(ctx, cfg, []func() error{op})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, completed, "a canceled ctx completes no op")
	require.Zero(t, calls, "a canceled ctx stops before running the op")
}

// TestRunOps_CtxCancelDuringBackoffReturnsPromptly: an op fails and the context is
// canceled DURING the (generous) backoff wait. runOps must abort the wait and return the
// ctx error promptly — well before attempts × backoff — proving the backoff is ctx-aware.
// A dropped backoff.WithContext wiring would sleep the full 30s backoff and turn this
// into a visible hang.
func TestRunOps_CtxCancelDuringBackoffReturnsPromptly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := Config{opRetryAttempts: 5, opRetryBackoff: 30 * time.Second}

	calls := 0
	op := func() error {
		calls++
		if calls == 1 {
			// Cancel shortly after the first failure so the cancel lands while runOps is
			// waiting out the 30s backoff before the retry.
			go func() { time.Sleep(20 * time.Millisecond); cancel() }()
		}
		return errors.New("transient")
	}

	start := time.Now()
	completed, err := runOps(ctx, cfg, []func() error{op})
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.Canceled, "the canceled backoff surfaces the ctx error")
	require.Zero(t, completed, "the failing op never completed")
	require.Equal(t, 1, calls, "canceled during the first backoff, the op is not retried")
	require.Less(t, elapsed, 5*time.Second, "ctx cancel aborts the backoff wait promptly")
}

// A zero-value Config (no WithLifecycleDefaults) runs each op exactly once — no
// retry, no panic on the zero backoff — so a test harness that builds Config
// directly keeps the pre-retry behavior.
func TestRunOps_ZeroConfigRunsOnce(t *testing.T) {
	calls := 0
	op := func() error { calls++; return errors.New("boom") }
	completed, err := runOps(context.Background(), Config{}, []func() error{op})
	require.Error(t, err)
	require.Zero(t, completed, "the failing op did not complete")
	require.Equal(t, 1, calls, "zero-config = single attempt")
}
