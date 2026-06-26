package fullhistory

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// validateConfig is the config gate run before the daemon's run loop: (1) form-validate,
// (2) on restart confirm earliest_ledger unchanged, (3) on first start resolve and
// pin it. Returns the resolved earliest ledger for StartConfig; plain error returns
// (the daemon loop owns fatal-and-surface).
func validateConfig(
	ctx context.Context,
	cfg Config,
	cat *catalog.Catalog,
	tip NetworkTipBackend,
	tipBackoff time.Duration,
	tipMaxAttempts int,
) (uint32, error) {
	if cat == nil {
		return 0, errors.New("validateConfig requires a non-nil Catalog")
	}

	workers := derefInt(cfg.Backfill.Workers)
	maxRetries := derefInt(cfg.Backfill.MaxRetries)

	// --- 1. Form validation. ---
	if workers < 1 {
		return 0, fmt.Errorf("workers must be >= 1 (got %d) — a zero pool deadlocks executePlan", workers)
	}
	if maxRetries < 0 {
		return 0, fmt.Errorf("max_retries must be >= 0 (got %d) — 0 means run once, no retry", maxRetries)
	}
	// Form-validate here so the numeric case avoids chunk.IDFromLedger's sub-genesis panic below.
	if err := validateEarliestForm(cfg.Retention.EarliestLedger); err != nil {
		return 0, err
	}

	earliestStored, earliestPinned, err := cat.EarliestLedger()
	if err != nil {
		return 0, fmt.Errorf("read earliest_ledger pin: %w", err)
	}

	if earliestPinned {
		// --- 2. Restart: confirm earliest_ledger unchanged. ---
		// genesis/numeric must equal the pin or abort; "now" is a no-op (keep the
		// pinned floor). Tip is not re-sampled (catch-up's max(tip,lastCommitted) handles lag).
		if cfg.Retention.EarliestLedger != EarliestNow {
			want := uint32(chunk.FirstLedgerSeq)
			if cfg.Retention.EarliestLedger != EarliestGenesis {
				want = mustParseUint32(cfg.Retention.EarliestLedger)
			}
			if want != earliestStored {
				return 0, fmt.Errorf("earliest_ledger changed: stored=%d, config=%q. "+
					"Wipe the data directory to change earliest_ledger (or use the future "+
					"set-earliest-ledger admin command)", earliestStored, cfg.Retention.EarliestLedger)
			}
		}
		return earliestStored, nil
	}

	// --- 3. First start: resolve, then pin earliest_ledger. ---
	earliest, err := resolveEarliestFirstStart(ctx, cfg.Retention.EarliestLedger, tip, tipBackoff, tipMaxAttempts)
	if err != nil {
		return 0, err
	}
	if err := cat.PinEarliestLedger(earliest); err != nil {
		return 0, fmt.Errorf("pin earliest ledger (earliest=%d): %w", earliest, err)
	}
	return earliest, nil
}

// validateEarliestForm checks the static form of earliest_ledger ("genesis",
// "now", or a chunk-aligned decimal ledger >= genesis); it does not resolve "now"
// or check a numeric floor against the tip (first-start-only work).
func validateEarliestForm(earliest string) error {
	if earliest == EarliestGenesis || earliest == EarliestNow {
		return nil
	}
	n, err := strconv.ParseUint(earliest, 10, 32)
	if err != nil {
		return fmt.Errorf("earliest_ledger must be %q, %q, or a chunk-aligned "+
			"ledger >= %d; got %q", EarliestGenesis, EarliestNow, chunk.FirstLedgerSeq, earliest)
	}
	ledger := uint32(n)
	if ledger < chunk.FirstLedgerSeq || ledger != chunk.IDFromLedger(ledger).FirstLedger() {
		return fmt.Errorf("earliest_ledger must be %q, %q, or a chunk-aligned "+
			"ledger >= %d; got %q (not chunk-aligned or sub-genesis)",
			EarliestGenesis, EarliestNow, chunk.FirstLedgerSeq, earliest)
	}
	return nil
}

// resolveEarliestFirstStart turns the form-validated earliest_ledger into the
// chunk-aligned ledger to pin on first start. Genesis needs no tip; "now" and a
// numeric floor each require a reachable backend so neither pins a future floor.
func resolveEarliestFirstStart(
	ctx context.Context, earliest string, tip NetworkTipBackend, backoff time.Duration, maxAttempts int,
) (uint32, error) {
	switch earliest {
	case EarliestGenesis:
		return chunk.FirstLedgerSeq, nil

	case EarliestNow:
		// Resolving "now" requires a tip.
		t, err := networkTip(ctx, tip, backoff, maxAttempts)
		if err != nil {
			return 0, fmt.Errorf("earliest_ledger=%q needs a reachable, ready backend: %w",
				EarliestNow, err)
		}
		// chunkFirstLedger(chunkID(tip)) <= tip, so never past the tip.
		return chunk.IDFromLedger(t).FirstLedger(), nil

	default:
		// Numeric: pinned immutably, so it must be checked against a real tip — a
		// floor ahead of the network would become permanent and resume from a future ledger.
		floor := mustParseUint32(earliest)
		t, err := networkTip(ctx, tip, backoff, maxAttempts)
		if err != nil {
			return 0, fmt.Errorf("first start with a numeric earliest_ledger needs a "+
				"reachable, ready backend to validate the floor against the network tip: %w", err)
		}
		if floor > t {
			return 0, fmt.Errorf("earliest_ledger (%d) is past the current network tip (%d); reject",
				floor, t)
		}
		return floor, nil
	}
}

// mustParseUint32 parses a caller-form-validated decimal uint32; a parse failure
// is a programming error, so it panics.
func mustParseUint32(s string) uint32 {
	n, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		panic(fmt.Sprintf("mustParseUint32(%q): %v (caller must form-validate first)", s, err))
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
