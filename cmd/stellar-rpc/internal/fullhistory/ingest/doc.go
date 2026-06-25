// Package ingest drives full-history ingestion: it pulls raw ledgers from an
// injected stream and writes the three data types (ledgers, txhashes, contract
// events) into the full-history stores, one chunk at a time, via the SDK's
// zero-copy view extractors and events.LCMViewToPayloads.
//
// Two tiers share the per-ledger extraction:
//
//   - Hot (HotService): one ledger into the per-chunk shared multi-CF hot DB
//     (hotchunk.DB), committed as ONE atomic synced WriteBatch across all enabled
//     CFs — a ledger is fully present or fully absent (decision (a)), no fan-out.
//   - Cold (WriteColdChunk): one chunk into per-chunk cold artifacts (ledger
//     .pack, txhash .bin, events pack+index). SOURCE-BLIND — the caller resolves
//     the ledger source and passes the raw iterator, so the materializer never
//     learns whether the bytes came from a local .pack or the bulk backend. Each
//     cold ingester opens its own writer; Finalize publishes, Close drops
//     partials on failure (ColdService orchestrates).
//
// Artifact model (cold) — the contract every layer relies on:
//
//   - Cold artifacts are NOT authoritative alone. The orchestrator's completion
//     record — written only after every Finalize returned and its data is durable
//     — is the single source of truth for whether a chunk exists.
//   - Nothing consumes cold artifacts by scanning directories; consumers take
//     explicit paths composed from the completion record.
//   - A chunk attempt owns its paths exclusively and overwrites freely. Disk is
//     scratch until the completion record says otherwise: partial/stale files
//     from a failed attempt are inert and the retry's overwrite is the cleanup —
//     so no writer needs tmp+rename, no pre-clean, no rollback.
//
// Failure semantics (cold) follow: a chunk fully finalizes (then the orchestrator
// records completion) or is abandoned and re-run from scratch — no mid-chunk
// resume. A failed Ingest releases via Close (Finalize must not run); a failed
// Finalize stops ColdService at the first error.
//
// Types are processed in canonical ledgers→txhash→events order (buildColdIngesters
// is the single definition site). On-disk formats/filenames are owned by the store
// packages; this package only composes the {bucketID:05d}/ bucket dirs.
//
// Inputs are borrowed: every Ingest gets a view valid only until the next pull,
// and each ingester copies what it retains. The raw iterator yields an error on
// ctx cancellation, so the drain loop needn't poll ctx. Metrics flow through
// MetricSink; the cold invariant is exactly one ColdChunkTotal per chunk attempt,
// including pre-service failures.
package ingest
