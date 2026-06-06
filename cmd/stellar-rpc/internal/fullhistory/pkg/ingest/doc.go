// Package ingest is the reusable full-history ingest core: it streams raw
// ledgers from a source backend and writes ledgers, transaction hashes, and
// contract events into the hot and cold full-history stores.
//
// The package exposes a small surface: a Config selecting which data types to
// ingest, a Source/SourceOpts pair plus OpenChunkStream for opening per-chunk
// ledger streams, and two drivers — RunHot (continuous single-stream fan-out
// into the hot stores) and RunCold (chunk-range, parallel chunk workers, each
// finalizing its own cold artifact).
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
