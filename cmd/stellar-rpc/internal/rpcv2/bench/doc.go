// Package bench benchmarks ingestion and reads. bench ingest
// times the cold backfill that bulk-materializes past ledgers at startup, and
// the hot loop that ingests the live stream as it advances.
//
// A bench ingest run drives the daemon's production ingestion code over a
// benchmark-controlled ledger source and times it: cold calls
// backfill.RunBackfill, hot calls the production ingestion loop. Both report
// their per-stage timings through the MetricSink and observability.Metrics
// interfaces; a csvSink implements those interfaces, collects the signals, and
// aggregates each run into percentile CSV reports.
//
// bench query measures read latency on the data that bench ingest wrote: cold
// reads frozen artifacts, hot reads one hot chunk database. Each scenario runs
// one query type at one target rate through query.ReadView. An open-loop load
// generator starts requests on a fixed schedule, so a slow read does not lower
// the arrival rate. The timings cover the storage read path only; RPC
// handlers, response serialization and network time are outside them. A run
// writes latency.csv and scenarios.csv.
package bench
