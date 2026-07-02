package lifecycle

import (
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
)

// Test-only aliases for the consolidated progress derivation. The design folded
// deriveCompleteThrough + deriveWatermark into ONE LastCommittedLedger(cat[, probe]):
//
//   - deriveCompleteThrough(cat)  == LastCommittedLedger(cat, nil)   (chunk
//     granularity, pure catalog read — the positional term, no hot DB open).
//   - deriveWatermark(cat, probe) == LastCommittedLedger(cat, probe) (one
//     refinement read of the highest ready hot DB, loss detected LAZILY on it).
//
// These shims keep the tests' intent legible; production callers use
// LastCommittedLedger directly.
func deriveCompleteThrough(cat *catalog.Catalog) (uint32, error) {
	return LastCommittedLedger(cat, nil)
}

func deriveWatermark(cat *catalog.Catalog, probe backfill.HotProbe) (uint32, error) {
	return LastCommittedLedger(cat, probe)
}
