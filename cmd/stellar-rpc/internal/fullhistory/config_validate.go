package fullhistory

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// validateConfig is the config gate run before the daemon's run loop: (1) form-validate,
// (2) on restart confirm earliest_ledger unchanged, (3) on first start resolve and
// pin it. On success it returns the pinned, chunk-aligned earliest_ledger so the
// caller binds the retention floor from the validated path instead of re-reading it.
func validateConfig(
	ctx context.Context,
	cfg config.Config,
	cat *catalog.Catalog,
	tip tipSource,
) (uint32, error) {
	if cat == nil {
		return 0, errors.New("validateConfig requires a non-nil Catalog")
	}

	workers := deref(cfg.Backfill.Workers)
	maxRetries := deref(cfg.Backfill.MaxRetries)

	// --- 1. Form validation. ---
	if workers < 1 {
		return 0, fmt.Errorf("workers must be >= 1 (got %d) — a zero pool deadlocks executePlan", workers)
	}
	if maxRetries < 0 {
		return 0, fmt.Errorf("max_retries must be >= 0 (got %d) — 0 means run once, no retry", maxRetries)
	}
	// logging.format silently means text unless it is exactly "json", so reject any
	// other value rather than let a typo drop the operator to text logging. Empty is
	// the unset sentinel WithDefaults resolves to text, so it is not a typo.
	if f := cfg.Logging.Format; f != "" && f != config.DefaultLogFormat && f != config.LogFormatJSON {
		return 0, fmt.Errorf("logging.format must be %q or %q; got %q",
			config.DefaultLogFormat, config.LogFormatJSON, f)
	}
	// Form-validate here so the numeric case avoids chunk.IDFromLedger's sub-genesis panic below.
	if err := validateEarliestForm(cfg.Retention.EarliestLedger); err != nil {
		return 0, err
	}
	if err := validateServing(cfg.Serving); err != nil {
		return 0, err
	}

	earliestStored, earliestPinned, err := cat.EarliestLedger()
	if err != nil {
		return 0, fmt.Errorf("read earliest_ledger pin: %w", err)
	}

	if earliestPinned {
		// --- 2. Restart: confirm earliest_ledger unchanged. ---
		if err := confirmEarliestUnchanged(cfg.Retention.EarliestLedger, earliestStored); err != nil {
			return 0, err
		}
		return earliestStored, nil
	}

	// --- 3. First start: resolve, then pin earliest_ledger. ---
	earliest, err := resolveEarliestFirstStart(ctx, cfg.Retention.EarliestLedger, tip)
	if err != nil {
		return 0, err
	}
	if err := cat.PinEarliestLedger(earliest); err != nil {
		return 0, fmt.Errorf("pin earliest ledger (earliest=%d): %w", earliest, err)
	}
	return earliest, nil
}

// validateServing form-checks the [serving] knobs. It runs whether or not the
// JSON-RPC endpoint is set — a typo in a limit should fail loudly, not lie in
// wait for the day serving is switched on. A nil field means the key was never
// set (config.WithDefaults fills it), so only EXPLICIT values are judged here.
func validateServing(s config.ServingConfig) error {
	limits := []struct {
		name     string
		def, max *uint
	}{
		{"events", s.DefaultEventsLimit, s.MaxEventsLimit},
		{"transactions", s.DefaultTransactionsLimit, s.MaxTransactionsLimit},
		{"ledgers", s.DefaultLedgersLimit, s.MaxLedgersLimit},
	}
	for _, l := range limits {
		if l.max != nil && *l.max == 0 {
			return fmt.Errorf("[serving] max_%s_limit must be > 0", l.name)
		}
		if l.def != nil && *l.def == 0 {
			return fmt.Errorf("[serving] default_%s_limit must be > 0", l.name)
		}
		if l.def != nil && l.max != nil && *l.def > *l.max {
			return fmt.Errorf("[serving] default_%s_limit (%d) must not exceed max_%s_limit (%d)",
				l.name, *l.def, l.name, *l.max)
		}
	}
	if d := s.MaxRequestExecutionDuration; d != nil && *d <= 0 {
		return errors.New("[serving] max_request_execution_duration must be > 0")
	}
	if d := s.MaxHealthyLedgerLatency; d != nil && *d <= 0 {
		return errors.New("[serving] max_healthy_ledger_latency must be > 0")
	}
	if q := s.RequestBacklogGlobalQueueLimit; q != nil && *q == 0 {
		return errors.New("[serving] request_backlog_global_queue_limit must be > 0")
	}
	if c := s.LedgerReaderCache; c != nil && *c < 0 {
		return errors.New("[serving] ledger_reader_cache must be >= 0 (0 = built-in default)")
	}
	if c := s.EventReaderCache; c != nil && *c < 0 {
		return errors.New("[serving] event_reader_cache must be >= 0 (0 = built-in default)")
	}
	return nil
}

// confirmEarliestUnchanged enforces earliest_ledger's restart immutability: genesis or
// a numeric value must equal the stored pin, while "now" is a no-op that keeps the
// pinned floor. The tip is not re-sampled — backfill's max(tip,lastCommitted) handles lag.
func confirmEarliestUnchanged(configured string, stored uint32) error {
	if configured == config.EarliestNow {
		return nil
	}
	want := uint32(chunk.FirstLedgerSeq)
	if configured != config.EarliestGenesis {
		want = mustParseUint32(configured)
	}
	if want != stored {
		return fmt.Errorf("earliest_ledger changed: stored=%d, config=%q. "+
			"Wipe the data directory to change earliest_ledger (or use the future "+
			"set-earliest-ledger admin command)", stored, configured)
	}
	return nil
}

// validateEarliestForm checks the static form of earliest_ledger ("genesis",
// "now", or a chunk-aligned decimal ledger >= genesis); it does not resolve "now"
// or check a numeric floor against the tip (first-start-only work).
func validateEarliestForm(earliest string) error {
	if earliest == config.EarliestGenesis || earliest == config.EarliestNow {
		return nil
	}
	n, err := strconv.ParseUint(earliest, 10, 32)
	if err != nil {
		return fmt.Errorf("earliest_ledger must be %q, %q, or a chunk-aligned "+
			"ledger >= %d; got %q", config.EarliestGenesis, config.EarliestNow, chunk.FirstLedgerSeq, earliest)
	}
	ledger := uint32(n)
	if ledger < chunk.FirstLedgerSeq || ledger != chunk.IDFromLedger(ledger).FirstLedger() {
		return fmt.Errorf("earliest_ledger must be %q, %q, or a chunk-aligned "+
			"ledger >= %d; got %q (not chunk-aligned or sub-genesis)",
			config.EarliestGenesis, config.EarliestNow, chunk.FirstLedgerSeq, earliest)
	}
	return nil
}

// resolveEarliestFirstStart turns the form-validated earliest_ledger into the
// chunk-aligned ledger to pin on first start. Genesis needs no tip; "now" and a
// numeric floor each require a reachable backend so neither pins a future floor.
// tip is the backend's raw frontier query; the retry policy is applied here.
func resolveEarliestFirstStart(
	ctx context.Context, earliest string, tip tipSource,
) (uint32, error) {
	switch earliest {
	case config.EarliestGenesis:
		return chunk.FirstLedgerSeq, nil

	case config.EarliestNow:
		// Resolving "now" requires a tip.
		t, err := sampleTipWithRetry(ctx, tip, defaultTipBackoff, defaultTipMaxAttempts)
		if err != nil {
			return 0, fmt.Errorf("earliest_ledger=%q needs a reachable, ready backend: %w",
				config.EarliestNow, err)
		}
		// chunkFirstLedger(chunkID(tip)) <= tip, so never past the tip.
		return chunk.IDFromLedger(t).FirstLedger(), nil

	default:
		// Numeric: pinned immutably, so it must be checked against a real tip — a
		// floor ahead of the network would become permanent and resume from a future ledger.
		floor := mustParseUint32(earliest)
		t, err := sampleTipWithRetry(ctx, tip, defaultTipBackoff, defaultTipMaxAttempts)
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

// deref returns *p, or the zero value of T when p is nil.
func deref[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}
