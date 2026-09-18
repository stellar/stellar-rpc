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
