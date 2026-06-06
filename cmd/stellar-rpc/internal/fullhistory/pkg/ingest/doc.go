// Package ingest is the reusable full-history ingest core: it streams raw
// ledgers from a source backend and writes ledgers, transaction hashes, and
// contract events into the hot and cold full-history stores.
//
// The package exposes a small surface: a Config selecting which data types to
// ingest, a ChunkSource abstraction (with built-in NewPackSource /
// NewDataStoreSource / NewGCSSource / NewS3Source constructors) that yields a
// per-chunk ledger stream from any backend, and two drivers — RunHot
// (single-chunk fan-out into the hot stores) and RunCold (chunk-range, parallel
// chunk workers, each finalizing its own cold artifact). Both drivers take a
// ChunkSource and open one stream per chunk.
//
// ChunkSource is the extension point: GCS, S3, and Filesystem share one
// datastore-backed code path and differ only by config, and a new backend just
// implements the interface — there is no central switch to edit.
//
// The per-type ingesters (ledgers/txhash/events × hot/cold) are unexported;
// consumers drive ingest through the two driver functions. Decode is always
// parsed: when events or txhash are enabled the driver unmarshals each ledger's
// raw bytes into an xdr.LedgerCloseMeta once and shares it read-only across the
// enabled ingesters. Per-ledger fan-out is always parallel via errgroup.
//
// This package is a production refactor of the bench-fullhistory ingest
// harness, with all benchmarking-only code (collectors, samples, metrics,
// CSV/percentile reporting, profiling, flag parsing) removed.
package ingest
