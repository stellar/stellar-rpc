// Package rocksdb is the Layer-1 generic RocksDB wrapper for the
// unified stellar-rpc fullhistory codebase. CF-aware (zero / one / N
// column families per store), WAL-on default, flock-protected,
// auto-mkdir on Open, per-store Config struct hardcoded by the calling
// Layer-2 facade.
//
// Every RocksDB-backed store in the project — backfill meta store, hot
// ledger store, hot txhash store, hot events store — is a Layer-2
// typed facade that builds on this wrapper. Each facade has its own
// RocksDB directory, its own flock, and its own Config; nothing is
// shared across facades.
package rocksdb
