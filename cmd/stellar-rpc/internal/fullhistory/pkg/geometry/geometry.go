// Package geometry — ledger-seq ↔ chunk_id ↔ tx_index_id math.
// Today: LedgersPerChunk only. Future slices add the rest.
// Operator-invisible; no TOML knobs here.
package geometry

// LedgersPerChunk — fixed ledger count per chunk. Part of the
// on-disk format contract; not operator-configurable.
const LedgersPerChunk uint32 = 10_000
