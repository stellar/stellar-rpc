// Package ingest drives full-history ingestion: it pulls raw ledgers
// (wire-format xdr.LedgerCloseMeta bytes) from a ChunkSource and writes
// the three data types — ledgers, txhashes, contract events — into the
// full-history stores, one chunk at a time, via the zero-copy view
// extractors in the sibling views package.
//
// Two tiers share the per-ledger extraction but differ in everything
// else:
//
//   - Hot (RunHot): one chunk into the long-lived, caller-owned hot
//     stores. The stores are INJECTED and never opened or closed here;
//     each ledger is durable before the next is pulled. Per-ledger
//     fan-out across the enabled ingesters is concurrent (HotService).
//   - Cold (RunCold): N chunks into per-chunk cold artifacts
//     (ledger .pack, txhash .bin, events pack+index), up to
//     chunkWorkers chunks concurrently. Each cold ingester OPENS its
//     own per-chunk writer; Finalize publishes the artifact and Close
//     drops partials on the failure path (ColdService orchestrates).
//
// Failure semantics (cold): a chunk either fully publishes or is
// abandoned — there is no mid-chunk resume. Once any Ingest fails, the
// chunk is released via Close (Finalize must not run; see the
// ColdIngester contract), and once any Finalize fails, the remaining
// ingesters are closed unpublished and the already-finalized ones are
// rolled back via unpublish (ColdService.Finalize), so a failed chunk
// leaves no committed artifacts from that attempt. A retry re-runs the
// whole chunk: the cold constructors truncate or remove any prior
// artifact so stale state cannot leak through — which is also why
// runOneChunkCold proves the source can yield the chunk's first ledger
// (and buildColdIngesters validates every enabled type's directory)
// BEFORE the destructive constructors run.
//
// Data types are processed in canonical ledgers→txhash→events order;
// the constructor table in buildColdIngesters is the order's single
// definition site. The on-disk formats and per-chunk filenames are
// owned by the store packages (ledger.PackName, txhash.ColdBinName +
// its .bin codec, eventstore's cold-format helpers); this package only
// composes the {bucketID:05d}/ bucket directories around them.
//
// Inputs are borrowed: every Ingest receives a view over the source
// stream's buffer, valid only until the next ledger is pulled, and
// each ingester copies what it retains (see HotIngester). Sources are
// pluggable through ChunkSource, whose contract includes yielding an
// error on ctx cancellation — the drain loop relies on the stream for
// cancellation. Metrics flow through MetricSink (Prometheus in prod,
// recorders in tests); the cold tier's invariant is exactly one
// ColdChunkTotal per chunk attempt, including pre-service failures.
package ingest
