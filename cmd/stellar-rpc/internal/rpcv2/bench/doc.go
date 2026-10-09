// Package bench benchmarks full-history ingestion: the cold backfill that
// bulk-materializes past ledgers at startup, and the hot loop that ingests the
// live stream as it advances.
//
// A run drives the daemon's production ingestion code over a
// benchmark-controlled ledger source and times it: cold calls
// backfill.RunBackfill, hot calls the production ingestion loop. Both report
// their per-stage timings through the MetricSink and observability.Metrics
// interfaces; a csvSink implements those interfaces, collects the signals, and
// aggregates each run into percentile CSV reports.
package bench
