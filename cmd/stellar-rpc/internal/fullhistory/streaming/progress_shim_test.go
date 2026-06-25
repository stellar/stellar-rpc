package streaming

import "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/catalog"

// Test-only aliases for the consolidated progress derivation (item R2-4). The
// design folded deriveCompleteThrough + deriveWatermark into ONE
// lastCommittedLedger(cat[, probe]):
//
//   - deriveCompleteThrough(cat)      == lastCommittedLedger(cat, nil)   (chunk
//     granularity, pure catalog read — the positional term, no hot DB open).
//   - deriveWatermark(cat, probe)     == lastCommittedLedger(cat, probe) (one
//     refinement read of the highest ready hot DB, loss detected LAZILY on it).
//
// These shims keep the existing tests' intent legible against the old names; the
// production callers all use lastCommittedLedger directly.
func deriveCompleteThrough(cat *catalog.Catalog) (uint32, error) {
	return lastCommittedLedger(cat, nil)
}

func deriveWatermark(cat *catalog.Catalog, probe HotProbe) (uint32, error) {
	return lastCommittedLedger(cat, probe)
}
