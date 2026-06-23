package streaming

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// validateConfig is the design's config gate (the "Configuration" /
// validateConfig pseudocode), run BEFORE startStreaming. It does three things,
// in order:
//
//  1. Stateless form validation — workers >= 1, max_retries >= 0, and
//     earliest_ledger a well-formed "genesis" | "now" | chunk-aligned numeric.
//     Validating the full static form here keeps every later parse well-formed.
//
//  2. Restart vs first start — the layout pin (config:earliest_ledger) is
//     committed on first start. Present ⟹ a prior first start completed and the
//     layout is immutable: confirm earliest_ledger is unchanged — with the
//     "now"-on-restart no-op rule (a frontfill deployment keeps "now" in its
//     config across restarts and must not abort).
//
//  3. First start — resolve earliest_ledger (genesis needs no tip; "now" and a
//     numeric floor each require a reachable, ready backend through the SAME
//     injected NetworkTipBackend startStreaming uses), then commit the pin in
//     one atomic synced batch via the Catalog.
//
// It returns the RESOLVED earliest ledger (chunk-aligned, >= genesis) the caller
// threads into StartConfig — the same value startStreaming reads back from the
// pin. Errors are plain returns (no os.Exit): the daemon's top-level loop owns
// the fatal-and-surface decision, and tests assert the errors directly.
func validateConfig(
	ctx context.Context,
	cfg Config,
	cat *Catalog,
	tip NetworkTipBackend,
	tipBackoff time.Duration,
	tipMaxAttempts int,
) (uint32, error) {
	if cat == nil {
		return 0, errors.New("streaming: validateConfig requires a non-nil Catalog")
	}

	workers := derefInt(cfg.Backfill.Workers)
	maxRetries := derefInt(cfg.Backfill.MaxRetries)

	// --- 1. Stateless form validation. ---
	if workers < 1 {
		return 0, fmt.Errorf("streaming: workers must be >= 1 (got %d) — a zero pool deadlocks executePlan", workers)
	}
	if maxRetries < 0 {
		return 0, fmt.Errorf("streaming: max_retries must be >= 0 (got %d) — 0 means run once, no retry", maxRetries)
	}
	// earliest_ledger must be "genesis", "now", or a chunk-aligned ledger >=
	// genesis. Form-validating the numeric case here keeps it out of
	// chunk.IDFromLedger's sub-genesis panic domain below.
	if err := validateEarliestForm(cfg.Streaming.EarliestLedger); err != nil {
		return 0, err
	}

	// --- 2/3. Pin inspection. ---
	earliestStored, earliestPinned, err := cat.EarliestLedger()
	if err != nil {
		return 0, fmt.Errorf("streaming: read earliest_ledger pin: %w", err)
	}

	if earliestPinned { //nolint:nestif // first-start vs restart immutability branch
		// --- 2. Restart: the layout is committed — confirm nothing changed. ---
		// earliest_ledger immutability. The backend tip is NOT re-sampled — it
		// may lag below the pinned floor and the catch-up loop's
		// max(tip, lastCommitted) handles that. A genesis/numeric value must
		// equal the stored pin or startup aborts; "now" is a deliberate no-op
		// meaning "keep the pinned floor", so a frontfill deployment leaves "now"
		// in its config across restarts without aborting.
		if cfg.Streaming.EarliestLedger != EarliestNow {
			want := uint32(chunk.FirstLedgerSeq)
			if cfg.Streaming.EarliestLedger != EarliestGenesis {
				// Already form-validated as a parseable chunk-aligned uint32.
				want = mustParseUint32(cfg.Streaming.EarliestLedger)
			}
			if want != earliestStored {
				return 0, fmt.Errorf("streaming: earliest_ledger changed: stored=%d, config=%q. "+
					"Wipe the data directory to change earliest_ledger (or use the future "+
					"set-earliest-ledger admin command)", earliestStored, cfg.Streaming.EarliestLedger)
			}
		}
		return earliestStored, nil
	}

	// --- 3. First start (or an incomplete prior start — no artifacts yet). ---
	// Resolve earliest_ledger, then commit the layout pin in one atomic batch.
	earliest, err := resolveEarliestFirstStart(ctx, cfg.Streaming.EarliestLedger, tip, tipBackoff, tipMaxAttempts)
	if err != nil {
		return 0, err
	}
	if err := cat.PinLayout(earliest); err != nil {
		return 0, fmt.Errorf("streaming: pin layout (earliest=%d): %w", earliest, err)
	}
	return earliest, nil
}

// validateEarliestForm checks the static form of earliest_ledger: "genesis",
// "now", or a chunk-aligned decimal ledger >= genesis. It does NOT resolve "now"
// or validate a numeric floor against the tip — that is first-start-only work.
func validateEarliestForm(earliest string) error {
	if earliest == EarliestGenesis || earliest == EarliestNow {
		return nil
	}
	n, err := strconv.ParseUint(earliest, 10, 32)
	if err != nil {
		return fmt.Errorf("streaming: earliest_ledger must be %q, %q, or a chunk-aligned "+
			"ledger >= %d; got %q", EarliestGenesis, EarliestNow, chunk.FirstLedgerSeq, earliest)
	}
	ledger := uint32(n)
	if ledger < chunk.FirstLedgerSeq || ledger != chunk.IDFromLedger(ledger).FirstLedger() {
		return fmt.Errorf("streaming: earliest_ledger must be %q, %q, or a chunk-aligned "+
			"ledger >= %d; got %q (not chunk-aligned or sub-genesis)",
			EarliestGenesis, EarliestNow, chunk.FirstLedgerSeq, earliest)
	}
	return nil
}

// resolveEarliestFirstStart turns the form-validated earliest_ledger string
// into the chunk-aligned ledger to pin on a first start. A genesis floor needs
// no tip (genesis is always a valid lower bound); "now" and a numeric floor each
// require a reachable, ready backend through the injected NetworkTipBackend —
// "now" has no other way to resolve, and a numeric floor is rejected if it is
// past the tip, so neither can pin a garbage or future floor.
func resolveEarliestFirstStart(
	ctx context.Context, earliest string, tip NetworkTipBackend, backoff time.Duration, maxAttempts int,
) (uint32, error) {
	switch earliest {
	case EarliestGenesis:
		return chunk.FirstLedgerSeq, nil

	case EarliestNow:
		// No local substitute for "now": resolving the floor requires a tip.
		t, err := networkTip(ctx, tip, backoff, maxAttempts)
		if err != nil {
			return 0, fmt.Errorf("streaming: earliest_ledger=%q needs a reachable, ready backend: %w",
				EarliestNow, err)
		}
		// chunkFirstLedger(chunkID(tip)) <= tip, so never past the tip.
		return chunk.IDFromLedger(t).FirstLedger(), nil

	default:
		// Numeric: already form-validated (parseable, >= genesis, chunk-aligned).
		// It is pinned immutably, so it MUST be validated against a real tip
		// first — skipping the check when the backend is down would let a floor
		// AHEAD of the network become permanent (the catch-up loop's
		// max(tip, earliest-1) anchor would then collapse the range to empty and
		// resume from a future ledger with the bad floor pinned). Like "now", a
		// numeric first-start floor therefore requires a reachable, ready backend.
		floor := mustParseUint32(earliest)
		t, err := networkTip(ctx, tip, backoff, maxAttempts)
		if err != nil {
			return 0, fmt.Errorf("streaming: first start with a numeric earliest_ledger needs a "+
				"reachable, ready backend to validate the floor against the network tip: %w", err)
		}
		if floor > t {
			return 0, fmt.Errorf("streaming: earliest_ledger (%d) is past the current network tip (%d); reject",
				floor, t)
		}
		return floor, nil
	}
}

// mustParseUint32 parses a decimal uint32 that the caller has already
// form-validated. A parse failure here is a programming error (the form check
// passed), so it panics rather than returning an error nobody can handle.
func mustParseUint32(s string) uint32 {
	n, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		panic(fmt.Sprintf("streaming: mustParseUint32(%q): %v (caller must form-validate first)", s, err))
	}
	return uint32(n)
}

func derefU32(p *uint32) uint32 {
	if p == nil {
		return 0
	}
	return *p
}

func derefInt(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}
