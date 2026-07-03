package lifecycle

import (
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
)

// Test-only aliases for the single progress derivation, LastCommittedLedger.
// There is no logger-less mode: when a "ready" hot key leads the cold term the
// derivation always opens that DB read-only, so both aliases pass a real logger.
// deriveCompleteThrough names the cold/floor/positional-selection intent (its
// callers seed no ready-above-cold hot key, or seed an empty real hot DB whose
// refinement falls back to the positional term); deriveWatermark names the
// refinement-value intent. Production callers use LastCommittedLedger directly.
func deriveCompleteThrough(cat *catalog.Catalog) (uint32, error) {
	return LastCommittedLedger(cat, silentLogger())
}

func deriveWatermark(cat *catalog.Catalog, logger *supportlog.Entry) (uint32, error) {
	return LastCommittedLedger(cat, logger)
}
