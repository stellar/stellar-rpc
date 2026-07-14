package fullhistory

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

const (
	defaultTipBackoff     = time.Second
	defaultTipMaxAttempts = 5
)

// tipSource queries one backend's current network frontier — its latest ledger.
type tipSource func(ctx context.Context) (uint32, error)

// sampleTipWithRetry samples the network tip from the backend's frontier query —
// the single home for tip-retry semantics: first-start earliest_ledger resolution
// and every backfill pass both go through it (the freeze's coverage wait reads
// Backend.Tip raw; waitForCoverage has its own poll loop). A transient failure
// retries on a bounded constant backoff — built on cenkalti/backoff, the same
// retry primitive withRetries and waitForCoverage use, so ctx cancellation aborts
// the wait. A sub-genesis tip is rejected as "not ready" (permanent) so an
// unready backend never pins a garbage floor. Production call sites pass
// defaultTipBackoff/defaultTipMaxAttempts; tests pass their own.
func sampleTipWithRetry(
	ctx context.Context, tip tipSource, interval time.Duration, maxAttempts int,
) (uint32, error) {
	// Clamp so WithMaxRetries' uint64 never underflows into an unbounded loop.
	maxAttempts = max(maxAttempts, 1)
	var (
		sampled  uint32
		notReady bool
	)
	poll := func() error {
		t, err := tip(ctx)
		if err != nil {
			return err // transient — retry
		}
		if t < chunk.FirstLedgerSeq {
			// Below genesis ⇒ backend empty/not-synced; permanent (it would keep returning 0).
			notReady = true
			return backoff.Permanent(fmt.Errorf("backend tip %d is below genesis %d — backend not ready",
				t, chunk.FirstLedgerSeq))
		}
		sampled = t
		return nil
	}
	// Constant interval, count-bounded: maxAttempts tries == 1 initial + (maxAttempts-1) retries.
	retries := uint64(maxAttempts - 1) //nolint:gosec // clamped to >= 1 above
	bo := backoff.WithMaxRetries(backoff.NewConstantBackOff(interval), retries)
	switch err := backoff.Retry(poll, backoff.WithContext(bo, ctx)); {
	case err == nil:
		return sampled, nil
	case notReady, ctx.Err() != nil:
		return 0, err // permanent (not ready) or ctx-canceled: surface as-is, not "exhausted"
	default:
		return 0, fmt.Errorf("network tip unavailable after %d attempts: %w", maxAttempts, err)
	}
}
