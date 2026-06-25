package streaming

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// startStreaming is the cold-only Phase-1 backfill daemon's startup
// orchestration — the design's "Daemon flow -> Startup", reduced to:
//
//  1. CATCH UP via backfill. Bring on-disk coverage in line with the retention
//     window: each pass backfills up through the last complete chunk at the
//     network tip, re-passing while new chunks appear, with one exclusion — a
//     mid-chunk watermark within one chunk of the tip leaves the partial resume
//     chunk alone. There is no upfront producibility gate.
//
//  2. SERVE. Begin serving reads (injected) and return. The cold-only daemon has
//     no hot tier, captive core, or live ingestion loop.
//
// Everything startup cannot construct itself crosses an INJECTED interface
// (StartConfig.NetworkTip, .ServeReads), so it is unit-testable without a real
// backend or RPC server. RunDaemon calls validateConfig (which pins
// earliest_ledger) BEFORE this; startStreaming reads the pin back.
//
// It returns nil only on a clean shutdown (ctx cancelled) or a clean ServeReads
// return; any other return is restartable and surfaces to the supervisor
// (ErrFirstStartNoTip on a true first start with no reachable backend).
func startStreaming(ctx context.Context, cfg StartConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	cfg = cfg.withDefaults()
	cat := cfg.Exec.Catalog
	logger := cfg.Exec.Logger

	// earliest_ledger is pinned by validateConfig BEFORE startStreaming runs. It
	// must be present here: the loop's first-start predicate `lastCommitted <
	// earliest` only classifies correctly against the real pinned floor (genesis
	// pins earliest=2, with the sentinel preGenesisLedger=1 below it). An absent
	// pin reads as 0 and mis-classifies a first start as a degrade-and-serve
	// restart, so refuse it loudly.
	earliest, pinned, err := cat.EarliestLedger()
	if err != nil {
		return fmt.Errorf("streaming: startup read earliest ledger: %w", err)
	}
	if !pinned {
		return errors.New("streaming: startup requires config:earliest_ledger pinned " +
			"(validateConfig pins it before startStreaming; not done here)")
	}

	// Derived, never stored: the highest ledger durably committed (frozen cold
	// artifacts), clamped by earliest-1. A pure catalog read — no hot tier.
	lastCommitted, err := lastCommittedLedger(cat)
	if err != nil {
		return fmt.Errorf("streaming: startup derive watermark: %w", err)
	}

	metrics := cfg.Exec.metrics()
	metrics.Watermark(lastCommitted, effectiveRetentionFloor(lastCommitted, cfg.RetentionChunks, earliest))
	logger.WithField("last_committed", lastCommitted).
		WithField("earliest", earliest).
		WithField("pinned", pinned).
		Info("streaming: startup — watermark derived, beginning catch-up")

	// Step 1: catch up via backfill.
	lastCommitted, err = catchUp(ctx, cfg, lastCommitted, earliest)
	if err != nil {
		return err
	}

	logger.WithField("last_committed", lastCommitted).
		Info("streaming: catch-up complete — serving reads")

	// Step 2: serve. The cold-only daemon finishes after catch-up + serve.
	// ServeReads launches the read server (injected); its error is restartable.
	if err := cfg.ServeReads(ctx); err != nil {
		return fmt.Errorf("streaming: startup serve reads: %w", err)
	}
	return nil
}

// catchUp runs the design's catch-up loop, returning lastCommitted as backfill
// makes progress. Each pass samples networkTip (degrading to lastCommitted on a
// transient error, FATAL via ErrFirstStartNoTip when there is no local history
// either), anchors on max(tip, lastCommitted), computes [rangeStart, rangeEnd]
// with the mid-chunk exclusion, and breaks on an empty/already-done range.
// backfilledThrough breaks the loop once rangeEnd stops advancing.
func catchUp(ctx context.Context, cfg StartConfig, lastCommitted, earliest uint32) (uint32, error) {
	retentionChunks := cfg.RetentionChunks
	metrics := cfg.Exec.metrics()
	logger := cfg.Exec.Logger

	backfilledThrough := int64(-1)
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		tip, err := networkTip(ctx, cfg.NetworkTip, cfg.TipBackoff, cfg.TipMaxAttempts)
		if err != nil {
			if lastCommitted < earliest {
				// True first start, no reachable backend: can neither catch up nor
				// serve local history. FATAL — never serve empty/incomplete history.
				// A sentinel (not a process exit) so the supervisor owns the restart.
				return 0, fmt.Errorf("%w: %w", ErrFirstStartNoTip, err)
			}
			// Restart with local progress: the window below lastCommitted is complete,
			// so serve it and skip catch-up this pass. A later pass with a reachable
			// backend resumes extending the bottom of storage.
			tip = lastCommitted
		}

		// max() guards a lagging bulk tip in BOTH uses below: on the tip alone the
		// floor would regress below where pruning advanced and a complete watermark
		// chunk could fall outside the range. When the tip leads it is the anchor.
		anchor := maxU32(tip, lastCommitted)
		rangeStart := chunk.IDFromLedger(effectiveRetentionFloor(anchor, retentionChunks, earliest))

		// Same anchor for rangeEnd, so a complete watermark chunk above a lagging
		// bulk tip still folds into its index. The span beyond the bulk tip is only
		// durable chunks (self-skipped) — the backend is never asked for them.
		rangeEndSigned := lastCompleteChunkAt(anchor)

		// Mid-chunk resume exclusion: a mid-chunk watermark within one chunk of the
		// tip leaves the partial resume chunk to ingestion. Signed domain so the
		// genesis sentinel (chunk-aligned by construction) reads as a boundary.
		if withinOneChunkOfTip(tip, lastCommitted) && watermarkMidChunk(lastCommitted) {
			rangeEndSigned = chunkIDOfLedger(lastCommitted) - 1 // one short of the live chunk
		}

		metrics.IngestionLag(tip, lastCommitted)
		metrics.CatchupProgress(lastCommitted, anchor)

		// Break on an empty range (young network, or the exclusion left nothing) or
		// a non-advancing one (the tip stopped moving).
		if rangeEndSigned < int64(rangeStart) || rangeEndSigned <= backfilledThrough {
			break
		}
		rangeEnd := chunk.ID(rangeEndSigned) //nolint:gosec // > rangeStart >= 0

		logger.WithField("range_lo", rangeStart.String()).
			WithField("range_hi", rangeEnd.String()).
			WithField("tip", tip).
			WithField("last_committed", lastCommitted).
			Info("streaming: catch-up pass starting")

		passStart := time.Now()
		if err := runBackfill(ctx, cfg.Exec, rangeStart, rangeEnd); err != nil {
			return 0, fmt.Errorf("streaming: startup backfill [%s,%s]: %w", rangeStart, rangeEnd, err)
		}
		passDuration := time.Since(passStart)

		// Advance the watermark to the backfilled range end (never regress — a
		// lagging tip's rangeEnd can sit below lastCommitted).
		lastCommitted = maxU32(lastCommitted, rangeEnd.LastLedger())
		backfilledThrough = rangeEndSigned

		metrics.CatchupPass(uint32(rangeStart), uint32(rangeEnd), passDuration)
		metrics.CatchupProgress(lastCommitted, anchor)
		logger.WithField("range_lo", rangeStart.String()).
			WithField("range_hi", rangeEnd.String()).
			WithField("last_committed", lastCommitted).
			WithField("duration", passDuration.String()).
			Info("streaming: catch-up pass complete")
	}
	return lastCommitted, nil
}

// withinOneChunkOfTip reports whether the watermark sits within one chunk of the
// tip. SIGNED so a lagging bulk tip below the resume point yields a negative
// difference < LedgersPerChunk and reads true (the watermark is then the
// near-tip chunk's, the exclusion's intent).
func withinOneChunkOfTip(tip, lastCommitted uint32) bool {
	return int64(tip)-int64(lastCommitted) < int64(chunk.LedgersPerChunk)
}

// watermarkMidChunk reports whether lastCommitted falls strictly inside a chunk
// (not on its last ledger). The genesis sentinel maps to chunk -1 whose "last
// ledger" is preGenesisLedger, so it reads as a boundary, never mid-chunk.
func watermarkMidChunk(lastCommitted uint32) bool {
	c := chunkIDOfLedger(lastCommitted)
	return lastCommitted != completeThrough(c)
}

// maxU32 keeps the anchor/advance call sites self-documenting alongside the
// signed helpers above.
func maxU32(a, b uint32) uint32 { return max(a, b) }

// ErrFirstStartNoTip is the first-start FATAL: no committed local progress AND
// no reachable tip, so the daemon can neither catch up nor serve. A sentinel
// (not a process exit) so the supervisor owns the restart and tests can assert it.
var ErrFirstStartNoTip = errors.New("streaming: network tip unavailable and no local history to serve")

// ---------------------------------------------------------------------------
// Injected external boundaries: the network tip and the read server both cross
// an interface, so startup is exercised end to end with fakes.
// ---------------------------------------------------------------------------

// NetworkTipBackend samples the bulk backend's current network tip (the highest
// ledger it can serve), consulted only during catch-up. Production wraps the
// daemon's LedgerBackend; tests pass a reachable/unreachable/unready fake.
type NetworkTipBackend interface {
	NetworkTip(ctx context.Context) (uint32, error)
}

// StartConfig is startStreaming's resolved dependency bundle: the scheduler
// config, the catch-up floor width, the injected boundaries, and the networkTip
// backoff bounds. The full daemon Config is a superset assembled at the call
// site; only what startup reads lives here.
type StartConfig struct {
	// Exec drives catch-up's runBackfill. Its Catalog/Logger are the shared ones.
	Exec ExecConfig

	// RetentionChunks is the catch-up floor's width. 0 ⇒ the earliest-ledger floor only.
	RetentionChunks uint32

	// NetworkTip samples the bulk backend's tip during catch-up. Required.
	NetworkTip NetworkTipBackend

	// ServeReads begins serving reads. It must return promptly (it launches the
	// server, not block until shutdown). Required.
	ServeReads func(ctx context.Context) error

	// TipBackoff is networkTip's inter-attempt sleep; TipMaxAttempts bounds the
	// retries before networkTip errors (catch-up then classifies fatal vs degrade).
	// Zero values fall back to defaults in withDefaults.
	TipBackoff     time.Duration
	TipMaxAttempts int
}

const (
	defaultTipBackoff     = time.Second
	defaultTipMaxAttempts = 5
)

// withDefaults fills the tip-backoff defaults and the embedded ExecConfig
// defaults (Workers -> GOMAXPROCS).
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
		return errors.New("streaming: StartConfig.Exec.Catalog is nil")
	}
	if cfg.Exec.Logger == nil {
		return errors.New("streaming: StartConfig.Exec.Logger is nil")
	}
	if cfg.NetworkTip == nil {
		return errors.New("streaming: StartConfig.NetworkTip is nil")
	}
	if cfg.ServeReads == nil {
		return errors.New("streaming: StartConfig.ServeReads is nil")
	}
	return nil
}

// networkTip samples backend.NetworkTip, hardened against the two ways the tip
// lies: it retries a transient error on a fixed backoff (bounded by maxAttempts),
// and rejects a tip below genesis as "not ready" so an unready backend never
// pins a garbage floor. ctx cancellation aborts the wait. The catch-up loop
// degrades on the returned error except on a true first start, where it fatals.
func networkTip(
	ctx context.Context, backend NetworkTipBackend, backoff time.Duration, maxAttempts int,
) (uint32, error) {
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return 0, ctx.Err()
			case <-timer.C:
			}
		}
		tip, err := backend.NetworkTip(ctx)
		if err != nil {
			lastErr = err
			continue
		}
		if tip < chunk.FirstLedgerSeq {
			// Below genesis ⇒ the backend is empty/not-yet-synced. Not-ready (an
			// error catch-up classifies), NOT retried — it would just keep returning 0.
			return 0, fmt.Errorf("streaming: backend tip %d is below genesis %d — backend not ready",
				tip, chunk.FirstLedgerSeq)
		}
		return tip, nil
	}
	return 0, fmt.Errorf("streaming: network tip unavailable after %d attempts: %w", maxAttempts, lastErr)
}
