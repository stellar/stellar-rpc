// Package bench benchmarks ingestion and reads. bench ingest
// times the cold backfill that bulk-materializes past ledgers at startup, and
// the hot loop that ingests the live stream as it advances.
//
// A bench ingest run drives the daemon's production ingestion code over a
// benchmark-controlled ledger source and times it: cold calls
// backfill.RunBackfill, hot calls the production ingestion loop. Both report
// their per-stage timings through the MetricSink and observability.Metrics
// interfaces; a csvSink implements those interfaces, collects the signals, and
// aggregates each run into percentile CSV reports. Each run writes a new
// dataset, with its catalog, into a missing or empty directory, and bench query
// opens that dataset.
//
// bench query measures read latency on the data that bench ingest wrote. It
// opens the dataset the way daemon startup opens its data dir, and the catalog
// tells whether the queried chunks are cold (frozen) or hot. Each scenario runs
// one query type at one target rate. Each request calls the daemon's method
// handler (rpcv2.ReadMethods) through the daemon's JSON-RPC bridge
// (jsonrpc.NewBridge), in process, which runs one handler per CPU. An
// open-loop load generator starts requests on a fixed schedule, so a slow read
// does not lower the arrival rate. latency covers the bridge's request parse,
// the wait for a handler slot, the handler and the response write;
// handler_latency covers the handler alone, read view included. HTTP and
// network time are outside both. A run writes latency.csv and scenarios.csv.
package bench
