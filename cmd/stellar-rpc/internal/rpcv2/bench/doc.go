// Package bench benchmarks full-history storage from both sides: writes
// (bench-ingest) and reads (bench-query). Each side has a cold and a hot
// subcommand, since the two tiers are different code paths with different
// numbers.
//
// An ingest run drives the daemon's production ingestion code over a
// benchmark-controlled ledger source and times it: cold calls
// backfill.RunBackfill, hot calls the production ingestion loop. Both report
// their per-stage timings through the MetricSink and observability.Metrics
// interfaces.
//
// A query run reads a dataset an ingest run left on disk. It rebuilds the
// catalog state those artifacts imply, then issues the requested query types at
// each requested arrival rate through query.ReadView — the daemon's read
// facade, so routing resolves each chunk's tier exactly as a served request
// does — and times each query itself.
//
// Either way a csvSink collects the signals and aggregates the run into
// percentile CSV reports, laid out by the schema that run declared.
package bench
