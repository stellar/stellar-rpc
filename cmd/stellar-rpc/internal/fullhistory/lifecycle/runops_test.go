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
	cfg := Config{OpRetryAttempts: 3, OpRetryBackoff: time.Millisecond}
	calls := 0
	op := func() error {
		calls++
		if calls < 3 {
			return errors.New("busy file")
		}
		return nil
	}
	require.NoError(t, runOps(context.Background(), cfg, []func() error{op}))
	require.Equal(t, 3, calls, "two transient failures retried, third try succeeds")
}

func TestRunOps_GivesUpAfterAttempts(t *testing.T) {
	cfg := Config{OpRetryAttempts: 2, OpRetryBackoff: time.Millisecond}
	calls := 0
	op := func() error { calls++; return errors.New("permanent") }
	require.Error(t, runOps(context.Background(), cfg, []func() error{op}))
	require.Equal(t, 2, calls, "attempts total tries (1 initial + 1 retry), then gives up")
}

func TestRunOps_CtxCancelStopsBeforeOp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := Config{OpRetryAttempts: 3, OpRetryBackoff: time.Hour}
	calls := 0
	op := func() error { calls++; return errors.New("x") }
	require.ErrorIs(t, runOps(ctx, cfg, []func() error{op}), context.Canceled)
	require.Zero(t, calls, "a canceled ctx stops before running the op")
}

// A zero-value Config (no WithLifecycleDefaults) runs each op exactly once — no
// retry, no panic on the zero backoff — so a test harness that builds Config
// directly keeps the pre-retry behavior.
func TestRunOps_ZeroConfigRunsOnce(t *testing.T) {
	calls := 0
	op := func() error { calls++; return errors.New("boom") }
	require.Error(t, runOps(context.Background(), Config{}, []func() error{op}))
	require.Equal(t, 1, calls, "zero-config = single attempt")
}
