package streaming

// Test-only alias for the cold progress derivation. The cold-only daemon has no
// hot tier, so the single lastCommittedLedger(cat) IS the chunk-granularity
// "complete through" bound — a pure catalog read. The shim keeps the existing
// tests' intent legible against the old name; the production callers use
// lastCommittedLedger directly.
func deriveCompleteThrough(cat *Catalog) (uint32, error) { return lastCommittedLedger(cat) }
