package rpcv2

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/limits"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
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

	// --- 1. Form validation. runDaemonWith already ran this right after config
	// load (so a malformed config is rejected before the catalog, captive core,
	// or datastore are touched); re-running here keeps validateConfig a complete
	// gate for callers and tests that enter through it directly. ---
	if err := validateForm(cfg); err != nil {
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

// validateForm runs every check that needs only the parsed config — no catalog,
// no backend, no filesystem. It runs AFTER WithDefaults (pointers non-nil) and
// BEFORE the daemon opens anything, so a malformed config is rejected without
// side effects.
func validateForm(cfg config.Config) error {
	if workers := deref(cfg.Backfill.Workers); workers < 1 {
		return fmt.Errorf("workers must be >= 1 (got %d) — a zero pool deadlocks executePlan", workers)
	}
	if maxRetries := deref(cfg.Backfill.MaxRetries); maxRetries < 0 {
		return fmt.Errorf("max_retries must be >= 0 (got %d) — 0 means run once, no retry", maxRetries)
	}
	if err := validateBSB(cfg.Backfill.BSB); err != nil {
		return err
	}
	// logging.format silently means text unless it is exactly "json", so reject any
	// other value rather than let a typo drop the operator to text logging. Empty is
	// the unset sentinel WithDefaults resolves to text, so it is not a typo.
	if f := cfg.Logging.Format; f != "" && f != config.DefaultLogFormat && f != config.LogFormatJSON {
		return fmt.Errorf("logging.format must be %q or %q; got %q",
			config.DefaultLogFormat, config.LogFormatJSON, f)
	}
	// Form-validate here so validateConfig's numeric path can't hit
	// chunk.IDFromLedger's sub-genesis panic.
	if err := validateEarliestForm(cfg.Retention.EarliestLedger); err != nil {
		return err
	}
	return validateService(cfg.Service)
}

// minConfiguredDuration guards go-toml v1's nanosecond trap: a bare integer
// duration decodes as NANOSECONDS (max_execution_duration = 10 → 10ns), so any
// configured duration under 1ms is a near-certain typo for the string form
// ("10s"). Zero is rejected by the same check — a zero execution budget or
// warning threshold is never intended.
const minConfiguredDuration = time.Millisecond

// validateBSB form-validates [backfill.bsb]. It runs AFTER WithDefaults, so
// every pointer is non-nil. max_retries needs no check — any uint32 is valid,
// including 0 (no retries).
func validateBSB(bsb config.BSBConfig) error {
	if *bsb.BufferSize < 1 {
		return errors.New("[backfill.bsb].buffer_size must be >= 1")
	}
	if *bsb.NumWorkers < 1 {
		return errors.New("[backfill.bsb].num_workers must be >= 1")
	}
	if *bsb.NumWorkers > *bsb.BufferSize {
		return fmt.Errorf("[backfill.bsb].num_workers (%d) cannot exceed buffer_size (%d) — "+
			"each worker needs a buffer slot to download into", *bsb.NumWorkers, *bsb.BufferSize)
	}
	if *bsb.RetryWait < minConfiguredDuration {
		return fmt.Errorf("[backfill.bsb].retry_wait is %v — durations below 1ms are rejected; "+
			"a bare TOML integer parses as nanoseconds, write a string like \"5s\"", *bsb.RetryWait)
	}
	return nil
}

// validateService form-validates the [service] section. It runs AFTER
// WithDefaults, so every pointer is non-nil and the checks cover all three
// tiers of the methods cascade (an explicit zero at any tier lands here).
func validateService(svc config.ServiceConfig) error {
	if *svc.MaxConcurrentRequests < 1 {
		return errors.New("[service].max_concurrent_requests must be >= 1")
	}
	durations := []struct {
		name string
		d    time.Duration
	}{
		{"[service].max_request_execution_duration", *svc.MaxRequestExecutionDuration},
		{"[service].request_execution_warning_threshold", *svc.RequestExecutionWarningThreshold},
		{"[service.methods.getHealth].max_healthy_ledger_latency", *svc.Methods.GetHealth.MaxHealthyLedgerLatency},
	}
	m := svc.Methods
	if m.QueueLimit != nil && *m.QueueLimit < 1 {
		return errors.New("[service.methods].queue_limit must be >= 1")
	}
	if m.MaxExecutionDuration != nil {
		durations = append(durations, struct {
			name string
			d    time.Duration
		}{"[service.methods].max_execution_duration", *m.MaxExecutionDuration})
	}

	methods := []struct {
		name  string
		queue uint
		dur   time.Duration
	}{
		{"getHealth", *m.GetHealth.QueueLimit, *m.GetHealth.MaxExecutionDuration},
		{"getNetwork", *m.GetNetwork.QueueLimit, *m.GetNetwork.MaxExecutionDuration},
		{"getVersionInfo", *m.GetVersionInfo.QueueLimit, *m.GetVersionInfo.MaxExecutionDuration},
		{"getLatestLedger", *m.GetLatestLedger.QueueLimit, *m.GetLatestLedger.MaxExecutionDuration},
		{"getTransaction", *m.GetTransaction.QueueLimit, *m.GetTransaction.MaxExecutionDuration},
		{"getTransactions", *m.GetTransactions.QueueLimit, *m.GetTransactions.MaxExecutionDuration},
		{"getLedgers", *m.GetLedgers.QueueLimit, *m.GetLedgers.MaxExecutionDuration},
		{"getEvents", *m.GetEvents.QueueLimit, *m.GetEvents.MaxExecutionDuration},
		{"getFeeStats", *m.GetFeeStats.QueueLimit, *m.GetFeeStats.MaxExecutionDuration},
	}
	for _, mm := range methods {
		if mm.queue < 1 {
			return fmt.Errorf("[service.methods.%s].queue_limit must be >= 1", mm.name)
		}
		durations = append(durations, struct {
			name string
			d    time.Duration
		}{"[service.methods." + mm.name + "].max_execution_duration", mm.dur})
	}
	for _, dd := range durations {
		if dd.d < minConfiguredDuration {
			return fmt.Errorf("%s is %v — durations below 1ms are rejected; "+
				"a bare TOML integer parses as nanoseconds, write a string like \"10s\"", dd.name, dd.d)
		}
	}

	if err := validatePaginatedMethods(m); err != nil {
		return err
	}
	return validateFeeWindows(svc.FeeStats)
}

func validatePaginatedMethods(m config.MethodsConfig) error {
	paginated := []struct {
		name string
		p    config.PaginatedMethodConfig
	}{
		{"getTransactions", m.GetTransactions},
		{"getLedgers", m.GetLedgers},
		{"getEvents", m.GetEvents},
	}
	for _, pp := range paginated {
		if *pp.p.MaxItemsPerResponse < 1 {
			return fmt.Errorf("[service.methods.%s].max_items_per_response must be >= 1", pp.name)
		}
		if *pp.p.DefaultItemsPerResponse < 1 {
			return fmt.Errorf("[service.methods.%s].default_items_per_response must be >= 1", pp.name)
		}
		if *pp.p.DefaultItemsPerResponse > *pp.p.MaxItemsPerResponse {
			return fmt.Errorf("[service.methods.%s].default_items_per_response (%d) cannot exceed max_items_per_response (%d)",
				pp.name, *pp.p.DefaultItemsPerResponse, *pp.p.MaxItemsPerResponse)
		}
	}
	return nil
}

func validateFeeWindows(fs config.FeeStatsConfig) error {
	feeWindows := []struct {
		name string
		v    uint32
	}{
		{"[service.fee_stats].classic_fee_window_ledgers", *fs.ClassicFeeWindowLedgers},
		{"[service.fee_stats].soroban_inclusion_fee_window_ledgers", *fs.SorobanInclusionFeeWindowLedgers},
	}
	for _, fw := range feeWindows {
		if fw.v < 1 {
			return fmt.Errorf("%s must be >= 1", fw.name)
		}
		if err := limits.ValidateFeeStatsRetentionWindow(fw.name, fw.v); err != nil {
			return err
		}
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
