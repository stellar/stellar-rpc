// Package backfill is the home for the run_backfill subroutine and the
// three DAG task types (process_chunk, build_txhash_index, cleanup_txhash)
// invoked by the unified stellar-rpc service's Phase 1 (catchup).
//
// This package is currently a placeholder — the orchestrator, DAG engine,
// task types, and Layer-2 metastore facade land in subsequent slices
// (#687, #688, #689, #690, #691). The Layer-1 RocksDB wrapper that every
// store builds on lives one level up at
// cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb (#685).
package backfill
