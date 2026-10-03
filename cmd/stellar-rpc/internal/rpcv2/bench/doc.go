// Package bench benchmarks full-history ingestion and reads. bench-ingest
// times the cold backfill that bulk-materializes past ledgers at startup, and
// the hot loop that ingests the live stream as it advances.
//
// A bench-ingest run drives the daemon's production ingestion code over a
// benchmark-controlled ledger source and times it: cold calls
// backfill.RunBackfill, hot calls the production ingestion loop. Both report
// their per-stage timings through the MetricSink and observability.Metrics
// interfaces; a csvSink implements those interfaces, collects the signals, and
// aggregates each run into percentile CSV reports.
//
// bench-query measures read latency on the data that bench-ingest wrote. Its
// open-loop load generator, runConstantArrivalRate, starts requests on a fixed
// schedule, and queryReport writes the results. README.md defines the
// terms and the formulas.
package bench
