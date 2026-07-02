package lifecycle

import (
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
)

// Test-only aliases for the consolidated progress derivation. The design folded
// deriveCompleteThrough + deriveWatermark into ONE LastCommittedLedger(cat[, logger]):
//
//   - deriveCompleteThrough(cat)   == LastCommittedLedger(cat, nil)    (chunk
//     granularity, pure catalog read — the positional term, no hot DB open).
//   - deriveWatermark(cat, logger) == LastCommittedLedger(cat, logger) (one
//     read-only refinement of the highest ready hot DB opened by its Layout path,
//     loss detected LAZILY on it).
//
// These shims keep the tests' intent legible; production callers use
// LastCommittedLedger directly.
func deriveCompleteThrough(cat *catalog.Catalog) (uint32, error) {
	return LastCommittedLedger(cat, nil)
}

func deriveWatermark(cat *catalog.Catalog, logger *supportlog.Entry) (uint32, error) {
	return LastCommittedLedger(cat, logger)
}
