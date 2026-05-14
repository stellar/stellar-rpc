// Package stores declares the typed contracts every persistent store
// in the unified stellar-rpc fullhistory service implements.
//
// One interface per store family:
//
//   - TxHashStore  — recent (txhash → ledgerSeq) lookups; concrete impl in pkg/rocksdb.
//   - MetaStore    — service-wide state key/value (backfill flags, streaming checkpoint);
//     concrete impl in pkg/rocksdb.
//   - LedgerStore  — recent ledger storage; concrete impl in pkg/rocksdb.
//
// Why this package is separate from pkg/rocksdb:
//
//   - Consumers (the DAG engine, ProcessChunkTask, federated reader, future
//     query handlers) take a stores interface as a dependency.
//     They never need to import pkg/rocksdb just to talk to a store.
//
//   - When a second implementation eventually shows up (a chaos-wrapped
//     fault-injection version, an in-memory test double, a future
//     non-RocksDB backend), it lives next to the rocksdb impl and
//     satisfies the same interface — no caller changes required.
//
//   - Today the rule "no interface for single-implementation modules" from
//     the project conventions is deliberately set aside for this slice;
//     the interfaces ship now so consumer-side code (especially DAG tests)
//     can plan around them without waiting for a follow-up extraction PR.
//
// Error sentinels (ErrNotFound, ErrStoreClosed) are also declared here so
// callers detect them via errors.Is without reaching into pkg/rocksdb.
// Concrete impls translate underlying RocksDB-wrapper errors into these
// sentinels at the interface boundary.
package stores
