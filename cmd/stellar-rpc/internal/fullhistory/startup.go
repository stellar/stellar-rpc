package fullhistory

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// run is the daemon's startup: backfill to the tip, then serve reads (injected).
// Returns nil only on clean shutdown; any other return is restartable
// (ErrFirstStartNoTip on a first start with no reachable backend).
func run(ctx context.Context, cfg StartConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	cfg = cfg.withDefaults()
	cat := cfg.Exec.Catalog
	logger := cfg.Exec.Logger

	// earliest_ledger must already be pinned by validateConfig: an absent pin reads
	// as 0 and mis-classifies a first start as a restart, so refuse it loudly.
	earliest, pinned, err := cat.EarliestLedger()
	if err != nil {
		return fmt.Errorf("startup read earliest ledger: %w", err)
	}
	if !pinned {
		return errors.New("startup requires config:earliest_ledger pinned " +
			"(validateConfig pins it before run; not done here)")
	}

	// Derived, never stored: highest durably-committed ledger, clamped by earliest-1.
	lastCommitted, err := lastCommittedLedger(cat)
	if err != nil {
		return fmt.Errorf("startup derive watermark: %w", err)
	}

	metrics := observability.MetricsOrNop(cfg.Exec.Metrics)
	metrics.Watermark(lastCommitted, retentionFloorChunk(lastCommitted, cfg.RetentionChunks, earliest).FirstLedger())
	logger.WithField("last_committed", lastCommitted).
		WithField("earliest", earliest).
		WithField("pinned", pinned).
		Info("startup — watermark derived, beginning backfill")

	// Step 1: backfill to the tip.
	lastCommitted, err = backfillToTip(ctx, cfg, lastCommitted, earliest)
	if err != nil {
		return err
	}

	logger.WithField("last_committed", lastCommitted).
		Info("backfill complete — handing off to the read server")

	// Step 2: serve (injected). Its error is restartable.
	if err := cfg.ServeReads(ctx); err != nil {
		return fmt.Errorf("startup serve reads: %w", err)
	}
	// TODO(#772): production ServeReads is a no-op until the cutover, so an immediate
	// clean exit after backfill is expected, not a misconfig.
	logger.WithField("last_committed", lastCommitted).
		Info("read server returned — cold-only daemon shutting down cleanly")
	return nil
}

// backfillToTip runs the backfill loop, returning lastCommitted as backfill makes
// progress. Each pass samples networkTip, anchors on max(tip, lastCommitted),
// computes [rangeStart, rangeEnd] with the mid-chunk exclusion, and breaks on an
// empty or non-advancing range.
func backfillToTip(ctx context.Context, cfg StartConfig, lastCommitted, earliest uint32) (uint32, error) {
	retentionChunks := cfg.RetentionChunks
	metrics := observability.MetricsOrNop(cfg.Exec.Metrics)
	logger := cfg.Exec.Logger

	// runBackfill is a test seam; nil ⇒ the real backfill.RunBackfill.
	runBackfill := cfg.runBackfill
	if runBackfill == nil {
		runBackfill = backfill.RunBackfill
	}

	backfilledThrough := int64(-1)
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		tip, err := networkTip(ctx, cfg.NetworkTip, cfg.TipBackoff, cfg.TipMaxAttempts)
		if err != nil {
			if lastCommitted < earliest {
				// First start, no reachable backend: FATAL — never serve incomplete history.
				return 0, fmt.Errorf("%w: %w", ErrFirstStartNoTip, err)
			}
			// Restart with local progress: serve what's below lastCommitted, skip backfill.
			tip = lastCommitted
		}

		// max() guards a lagging bulk tip: the tip alone could regress the floor below
		// pruning or drop a complete watermark chunk.
		anchor := max(tip, lastCommitted)
		rangeStart := retentionFloorChunk(anchor, retentionChunks, earliest)

		// Same anchor for rangeEnd: a complete watermark chunk above a lagging tip
		// still folds in; chunks beyond the tip are durable and self-skip.
		rangeEndSigned := geometry.LastCompleteChunkAt(anchor)

		// Mid-chunk resume exclusion: a mid-chunk watermark within one chunk of the tip
		// leaves the partial resume chunk to ingestion. Signed so genesis reads as a boundary.
		if withinOneChunkOfTip(tip, lastCommitted) && watermarkMidChunk(lastCommitted) {
			rangeEndSigned = chunkIDOfLedger(lastCommitted) - 1 // one short of the live chunk
		}

		metrics.IngestionLag(tip, lastCommitted)
		metrics.BackfillProgress(lastCommitted, anchor)

		// Break on an empty or non-advancing range.
		if rangeEndSigned < int64(rangeStart) || rangeEndSigned <= backfilledThrough {
			break
		}
		rangeEnd := chunk.ID(rangeEndSigned) //nolint:gosec // > rangeStart >= 0

		logger.WithField("range_lo", rangeStart.String()).
			WithField("range_hi", rangeEnd.String()).
			WithField("tip", tip).
			WithField("last_committed", lastCommitted).
			Info("backfill pass starting")

		passStart := time.Now()
		if err := runBackfill(ctx, cfg.Exec, rangeStart, rangeEnd); err != nil {
			return 0, fmt.Errorf("startup backfill [%s,%s]: %w", rangeStart, rangeEnd, err)
		}
		passDuration := time.Since(passStart)

		// Advance the watermark, never regressing (a lagging tip's rangeEnd can sit below it).
		lastCommitted = max(lastCommitted, rangeEnd.LastLedger())
		backfilledThrough = rangeEndSigned

		metrics.BackfillPass(uint32(rangeStart), uint32(rangeEnd), passDuration)
		metrics.BackfillProgress(lastCommitted, anchor)
		// Refresh the derived gauges as the watermark advances and the floor rises with it.
		metrics.Watermark(lastCommitted, retentionFloorChunk(lastCommitted, retentionChunks, earliest).FirstLedger())
		// Sample the cold-tier footprint once per pass (a full tree-walk is too costly
		// per-chunk); a walk error just leaves the gauge at its last value.
		if footprint, cerr := observability.MeasureColdTierBytes(cfg.Exec.Catalog.Layout()); cerr == nil {
			metrics.ColdTierBytes(footprint)
		} else {
			logger.WithError(cerr).Debug("cold-tier footprint sample failed; skipping gauge")
		}
		logger.WithField("range_lo", rangeStart.String()).
			WithField("range_hi", rangeEnd.String()).
			WithField("last_committed", lastCommitted).
			WithField("duration", passDuration.String()).
			Info("backfill pass complete")
	}
	return lastCommitted, nil
}

// withinOneChunkOfTip reports whether the watermark sits within one chunk of the
// tip. Signed so a lagging tip below the resume point still reads true.
func withinOneChunkOfTip(tip, lastCommitted uint32) bool {
	return int64(tip)-int64(lastCommitted) < int64(chunk.LedgersPerChunk)
}

// watermarkMidChunk reports whether lastCommitted falls strictly inside a chunk.
// The only sub-genesis value it sees is the fresh-start sentinel preGenesisLedger,
// where chunkIDOfLedger yields -1 and chunk.ID(-1).LastLedger() wraps (MaxUint32+1
// overflows to 0) back to exactly preGenesisLedger — so the comparison reports a
// boundary (false) without a special case.
func watermarkMidChunk(lastCommitted uint32) bool {
	c := chunkIDOfLedger(lastCommitted)
	//nolint:gosec // c is -1 (wraps to preGenesisLedger) or a real chunk id
	return lastCommitted != chunk.ID(c).LastLedger()
}

// ErrFirstStartNoTip is the first-start FATAL: no local progress and no reachable
// tip. A sentinel so the supervisor owns the restart and tests can assert it.
var ErrFirstStartNoTip = errors.New("network tip unavailable and no local history to serve")

// ---------------------------------------------------------------------------
// Injected external boundaries (so startup is testable with fakes).
// ---------------------------------------------------------------------------

// NetworkTipBackend samples the bulk backend's current network tip during backfill.
type NetworkTipBackend interface {
	NetworkTip(ctx context.Context) (uint32, error)
}

// StartConfig is run's resolved dependency bundle.
type StartConfig struct {
	// Exec drives backfill's RunBackfill; its Catalog/Logger are the shared ones.
	Exec backfill.ExecConfig

	// RetentionChunks is the backfill floor's width; 0 ⇒ the earliest-ledger floor only.
	RetentionChunks uint32

	// NetworkTip samples the bulk backend's tip during backfill. Required.
	NetworkTip NetworkTipBackend

	// ServeReads begins serving reads; it must return promptly, not block. Required.
	ServeReads func(ctx context.Context) error

	// TipBackoff is networkTip's inter-attempt sleep; TipMaxAttempts bounds the
	// retries. Zero values fall back to defaults in withDefaults.
	TipBackoff     time.Duration
	TipMaxAttempts int

	// runBackfill is a test-only seam for one backfill pass; nil ⇒ backfill.RunBackfill.
	runBackfill func(ctx context.Context, exec backfill.ExecConfig, lo, hi chunk.ID) error
}

const (
	defaultTipBackoff     = time.Second
	defaultTipMaxAttempts = 5
)

// withDefaults fills the tip-backoff defaults and the embedded ExecConfig defaults.
func (cfg StartConfig) withDefaults() StartConfig {
	cfg.Exec = cfg.Exec.WithDefaults()
	if cfg.TipBackoff <= 0 {
		cfg.TipBackoff = defaultTipBackoff
	}
	if cfg.TipMaxAttempts <= 0 {
		cfg.TipMaxAttempts = defaultTipMaxAttempts
	}
	return cfg
}

func (cfg StartConfig) validate() error {
	if cfg.Exec.Catalog == nil {
		return errors.New("nil StartConfig.Exec.Catalog")
	}
	if cfg.Exec.Logger == nil {
		return errors.New("nil StartConfig.Exec.Logger")
	}
	if cfg.NetworkTip == nil {
		return errors.New("nil StartConfig.NetworkTip")
	}
	if cfg.ServeReads == nil {
		return errors.New("nil StartConfig.ServeReads")
	}
	return nil
}

// networkTip samples backend.NetworkTip, retrying a transient error on a bounded
// constant backoff (maxAttempts total tries) and rejecting a sub-genesis tip as
// "not ready" so an unready backend never pins a garbage floor. Built on
// cenkalti/backoff — the same retry primitive as withRetries and waitForCoverage —
// so ctx cancellation aborts the wait and the sub-genesis case is backoff.Permanent.
func networkTip(
	ctx context.Context, backend NetworkTipBackend, interval time.Duration, maxAttempts int,
) (uint32, error) {
	if maxAttempts < 1 {
		maxAttempts = 1 // never underflow WithMaxRetries' uint64 into an unbounded loop
	}
	var (
		tip      uint32
		notReady bool
	)
	poll := func() error {
		t, err := backend.NetworkTip(ctx)
		if err != nil {
			return err // transient — retry
		}
		if t < chunk.FirstLedgerSeq {
			// Below genesis ⇒ backend empty/not-synced; permanent (it would keep returning 0).
			notReady = true
			return backoff.Permanent(fmt.Errorf("backend tip %d is below genesis %d — backend not ready",
				t, chunk.FirstLedgerSeq))
		}
		tip = t
		return nil
	}
	// Constant interval, count-bounded: maxAttempts tries == 1 initial + (maxAttempts-1) retries.
	bo := backoff.WithMaxRetries(backoff.NewConstantBackOff(interval), uint64(maxAttempts-1))
	switch err := backoff.Retry(poll, backoff.WithContext(bo, ctx)); {
	case err == nil:
		return tip, nil
	case notReady, ctx.Err() != nil:
		return 0, err // permanent (not ready) or ctx-canceled: surface as-is, not "exhausted"
	default:
		return 0, fmt.Errorf("network tip unavailable after %d attempts: %w", maxAttempts, err)
	}
}
