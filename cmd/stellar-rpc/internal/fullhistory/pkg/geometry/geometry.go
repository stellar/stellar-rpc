// Package geometry hosts the arithmetic that relates the project's
// three ledger-grouping concepts: ledger sequence, chunk, and
// tx-index.
//
// Today the package carries only the LedgersPerChunk constant.
// Future slices — primarily #684's geometry slice and #691's DAG
// task slice — extend it with the rest of the ledger-seq ↔
// chunk_id ↔ tx_index_id math (ChunkID(ledgerSeq),
// FirstLedgerInChunk(chunkID), LastChunkInTxIndex(txIndexID), …).
//
// Operator-invisible by design: nothing here is configurable from
// TOML.
// LedgersPerChunk is a compile-time constant; ledgers-per-tx-index
// is operator-set but read from the metastore at runtime.
package geometry

// LedgersPerChunk is the fixed ledger count per chunk.
// Pinned at 10 000 — chunk 0 covers ledgers 1..10000, chunk 1
// covers 10001..20000, etc.
// Intentionally NOT operator-configurable; the chunk shape is part
// of the on-disk format contract.
const LedgersPerChunk uint32 = 10_000
