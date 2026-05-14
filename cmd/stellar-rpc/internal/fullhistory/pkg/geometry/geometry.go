// Package geometry hosts the pure-function arithmetic that relates
// the project's three ledger-grouping concepts: ledger sequence, chunk,
// and tx-index.
//
// Today the package carries only the helpers the metastore's
// MarkTxHashIndexComplete needs (chunk-range expansion from a
// tx-index ID).
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
// Pinned at 10 000 — chunk 0 covers ledgers 1..10000, chunk 1 covers
// 10001..20000, etc.
// This is intentionally NOT operator-configurable; the chunk shape
// is part of the on-disk format contract.
const LedgersPerChunk uint32 = 10_000

// ChunksInTxIndex returns the chunk IDs covered by the given
// tx-index, in ascending order.
//
// Math: the tx-index of width ledgersPerTxIndex contains
// (ledgersPerTxIndex / LedgersPerChunk) chunks, starting at chunk
// (txIndexID * chunksPerTxIndex).
//
// ledgersPerTxIndex MUST be a positive multiple of LedgersPerChunk;
// the metastore enforces this contract by storing
// ledgersPerTxIndex once on first run as an immutability marker.
// This function does not re-validate — passing an invalid value
// produces nonsense output without panicking.
//
// Example: at the design default of 100_000 ledgers per tx-index:
//
//	ChunksInTxIndex(0, 100_000) == [0 1 2 3 4 5 6 7 8 9]
//	ChunksInTxIndex(7, 100_000) == [70 71 72 73 74 75 76 77 78 79]
func ChunksInTxIndex(txIndexID, ledgersPerTxIndex uint32) []uint32 {
	chunksPerTxIndex := ledgersPerTxIndex / LedgersPerChunk
	if chunksPerTxIndex == 0 {
		return nil
	}
	out := make([]uint32, chunksPerTxIndex)
	start := txIndexID * chunksPerTxIndex
	for i := range chunksPerTxIndex {
		out[i] = start + i
	}
	return out
}
