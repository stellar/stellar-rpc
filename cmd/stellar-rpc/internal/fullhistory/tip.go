package fullhistory

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/stellar/go-stellar-sdk/historyarchive"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

const (
	defaultTipBackoff     = time.Second
	defaultTipMaxAttempts = 5
)

// tipSource queries one backend's current network frontier — its latest ledger.
type tipSource func(ctx context.Context) (uint32, error)

// tipSampler is the single home for network-tip sampling: first-start
// earliest_ledger resolution and every backfill pass both go through it. It
// queries its sources in order — the bulk lake frontier first, the history
// archives' root HAS as a fallback (and the sole source for a frontfill-only
// daemon) — and returns the first that answers, retrying a transient failure of
// them all on a bounded constant backoff. Built on cenkalti/backoff, the same
// retry primitive withRetries and waitForCoverage use, so ctx cancellation
// aborts the wait. A sub-genesis tip is rejected as "not ready" (permanent) so
// an unready backend never pins a garbage floor.
type tipSampler struct {
	sources     []tipSource // tried in order; first success wins
	interval    time.Duration
	maxAttempts int
}

// newTipSampler binds the retry defaults; sources are tried in the order given.
func newTipSampler(sources ...tipSource) *tipSampler {
	return &tipSampler{
		sources:     sources,
		interval:    defaultTipBackoff,
		maxAttempts: defaultTipMaxAttempts,
	}
}

// Sample returns the current network tip, trying each source in order and
// retrying a transient failure of them all up to maxAttempts times.
func (s *tipSampler) Sample(ctx context.Context) (uint32, error) {
	if len(s.sources) == 0 {
		return 0, errors.New("no network tip source configured: set [backfill.datastore] " +
			"or [ingestion].history_archive_urls")
	}
	// Clamp so WithMaxRetries' uint64 never underflows into an unbounded loop.
	maxAttempts := max(s.maxAttempts, 1)
	var (
		tip      uint32
		notReady bool
	)
	poll := func() error {
		t, err := s.sampleOnce(ctx)
		if err != nil {
			return err // transient — every source failed; retry
		}
		if t < chunk.FirstLedgerSeq {
			// Below genesis ⇒ backend empty/not-synced; permanent (it would keep returning 0).
			// Order-blind: this gates whichever source answered first, so a source must
			// error (not return 0) when empty or it shadows a healthy fallback in sampleOnce.
			notReady = true
			return backoff.Permanent(fmt.Errorf("backend tip %d is below genesis %d — backend not ready",
				t, chunk.FirstLedgerSeq))
		}
		tip = t
		return nil
	}
	// Constant interval, count-bounded: maxAttempts tries == 1 initial + (maxAttempts-1) retries.
	retries := uint64(maxAttempts - 1) //nolint:gosec // clamped to >= 1 above
	bo := backoff.WithMaxRetries(backoff.NewConstantBackOff(s.interval), retries)
	switch err := backoff.Retry(poll, backoff.WithContext(bo, ctx)); {
	case err == nil:
		return tip, nil
	case notReady, ctx.Err() != nil:
		return 0, err // permanent (not ready) or ctx-canceled: surface as-is, not "exhausted"
	default:
		return 0, fmt.Errorf("network tip unavailable after %d attempts: %w", maxAttempts, err)
	}
}

// sampleOnce tries each source in order, returning the first frontier that
// answers; if all fail it joins their errors (retryable).
func (s *tipSampler) sampleOnce(ctx context.Context) (uint32, error) {
	errs := make([]error, 0, len(s.sources))
	for _, src := range s.sources {
		t, err := src(ctx)
		if err == nil {
			return t, nil
		}
		errs = append(errs, err)
	}
	return 0, errors.Join(errs...)
}

// rootHASGetter is the slice of historyarchive.ArchiveInterface archiveTip needs:
// the network frontier is the CurrentLedger the root HAS publishes.
type rootHASGetter interface {
	GetRootHAS() (historyarchive.HistoryArchiveState, error)
}

// archiveTip samples the network frontier from the history archives' root HAS.
// It is the tip source for a frontfill-only daemon (no bulk lake) and the
// fallback when the lake tip is unavailable. The archives lag the lake by up to
// one checkpoint (64 ledgers); backfill's anchor = max(tip, lastCommitted) and
// the signed withinOneChunkOfTip already absorb that, so no lag adjustment here.
func archiveTip(a rootHASGetter) tipSource {
	return func(context.Context) (uint32, error) {
		has, err := a.GetRootHAS()
		if err != nil {
			return 0, fmt.Errorf("history archive root HAS: %w", err)
		}
		return has.CurrentLedger, nil
	}
}
