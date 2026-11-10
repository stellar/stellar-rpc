package limits

import "fmt"

const (
	// OneDayOfLedgers is (roughly) a 24 hour window of ledgers.
	OneDayOfLedgers   = 17280
	SevenDayOfLedgers = OneDayOfLedgers * 7

	// MaxFeeStatsRetentionWindow is the maximum allowed fee stats retention
	// window (~55 minutes). Larger windows cause slow startup due to
	// O(n^2) fee distribution recomputation and provide no meaningful
	// improvement in fee estimates.
	MaxFeeStatsRetentionWindow = 1000

	// MinSimulationMemoryLimit is the smallest memory limit, in bytes, an
	// operator may set for a preflight worker: 8 times 2,000,000, the smallest
	// per-transaction memory limit stellar-core allows
	// (MinimumSorobanNetworkConfig::MEMORY_LIMIT in its NetworkConfig.h).
	MinSimulationMemoryLimit = 16_000_000
)

// ValidateFeeStatsRetentionWindow rejects fee-stats retention windows larger
// than MaxFeeStatsRetentionWindow. name is the config option being validated,
// used in the error message.
func ValidateFeeStatsRetentionWindow(name string, v uint32) error {
	if v > MaxFeeStatsRetentionWindow {
		return fmt.Errorf(
			"%s cannot exceed %d ledgers (got %d)",
			name, MaxFeeStatsRetentionWindow, v)
	}
	return nil
}

// ValidateSimulationMemoryLimit rejects a simulation memory limit below
// MinSimulationMemoryLimit. Zero, which leaves the limit to the network, is
// valid. name is the config option being validated, used in the error message.
func ValidateSimulationMemoryLimit(name string, v uint64) error {
	if v != 0 && v < MinSimulationMemoryLimit {
		return fmt.Errorf(
			"%s must be 0 or at least %d bytes (got %d)",
			name, MinSimulationMemoryLimit, v)
	}
	return nil
}
