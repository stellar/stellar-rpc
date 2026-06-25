// Package ingest drives full-history ingestion: it pulls raw ledgers
// (wire-format xdr.LedgerCloseMeta bytes) from a ChunkSource and writes
// the three data types — ledgers, txhashes, contract events — into the
// full-history stores, one chunk at a time, via the zero-copy view
// extractors in the go-stellar-sdk ingest package and the RPC-side
// events.LCMViewToPayloads emitter.
//
// Two tiers share the per-ledger extraction but differ in everything
// else:
//
//   - Hot: one ledger at a time into the per-chunk shared multi-CF hot
//     DB as one atomic synced WriteBatch across all CFs (HotService over
//     hotchunk.DB). The streaming daemon's ingestion loop drives it.
//   - Cold (RunColdChunk): ONE chunk into per-chunk cold artifacts
//     (ledger .pack, txhash .bin, events pack+index). Each cold ingester
//     OPENS its own per-chunk writer; Finalize publishes the artifact and
//     Close drops partials on the failure path (ColdService orchestrates).
//
// Artifact model (cold) — the contract every layer here relies on:
//
//   - Cold artifacts are NOT authoritative on their own. The
//     orchestrator's completion record — written only after every
//     enabled ingester's Finalize returned and its data is durable
//     (writers fsync before reporting success) — is the single source
//     of truth for whether a chunk exists.
//   - Nothing may consume cold artifacts by scanning directories. A
//     consumer (the serving tier, the deferred index build) takes
//     explicit paths composed from the completion record.
//   - A chunk attempt owns its chunk's paths exclusively and
//     overwrites freely. Disk under coldDir is scratch until the
//     completion record says otherwise: stale or partial files from a
//     failed or crashed attempt are inert, and the retry's overwrite
//     is the cleanup. No writer needs tmp+rename atomicity, no
//     constructor needs to pre-clean, and no failure path needs to
//     roll committed siblings back.
//
// Failure semantics (cold) follow from the model: a chunk either fully
// finalizes — and only then may the orchestrator record completion — or
// the attempt is abandoned and re-run from scratch; there is no
// mid-chunk resume. Once any Ingest fails, the chunk is released via
// Close (Finalize must not run; see the ColdIngester contract). Once
// any Finalize fails, ColdService stops at the first error; whatever
// the earlier ingesters already wrote stays on disk as inert scratch.
//
// Data types are processed in canonical ledgers→txhash→events order;
// the constructor table in buildColdIngestersIn is the order's single
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
