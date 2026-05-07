// Package streaming is the home for the unified stellar-rpc service's
// Phase 2-4 work — hot ledger / hot txhash stores, captive-core ingestion,
// retention/freeze transitions, the federated reader, and the JRPC server
// wiring. Owned by streaming epic #704.
//
// This package is currently a placeholder — concrete subpackages
// (ledger/, txhash/, …) land in subsequent slices (#584, #729, …). The
// Layer-1 RocksDB wrapper that every store builds on lives at
// cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb (#685).
package streaming
