// Package streaming holds the orchestration spine for the full-history
// streaming daemon: catch-up on startup, live ingestion from captive core, and
// the freeze → discard → prune lifecycle over the merged storage layer
// (fullhistory/pkg/...). It is built ON that layer — the catalog WRAPS
// metastore.Store rather than reinventing a RocksDB wrapper.
//
// This file map covers Slice 1 (the daemon skeleton) plus Slice 2 (events).
// Events is a second per-chunk artifact woven into the existing seams — it adds
// no new files here, only events column families, a processChunk segment
// writer, and the matching resolver/audit kind-loops. Slice 3 then adds the
// tx-hash data type (see "Later slices" below).
//
// # Data model (keys-first)
//
// Every durable artifact (a per-chunk file: ledger or events) and every per-chunk hot DB is named
// by exactly one catalog key, and the path on disk is a fixed bijection of that
// key. Nothing ever lists a directory to find work; every scan and sweep
// iterates keys. The authoritative spec is
// design-docs/full-history-streaming-workflow.md (Data model, One write
// protocol).
//
// # File map
//
// This is intentionally one cohesive package, not a flat dump: the crash-safety
// invariants are verified by fault-injection hooks fired from INSIDE the real
// methods (see hooks.go), so the catalog, the one-write protocol, the sweeps,
// and the I/O paths they protect must share a package to keep those hooks
// package-private and the invariant tests meaningful. The files group by
// concern:
//
//	Foundation     keys.go, paths.go
//	                 the catalog key schema, the key↔path bijection, and chunk
//	                 geometry.
//	Catalog        catalog.go, catalog_protocol.go, catalog_sweep.go
//	                 the catalog (a metastore.Store wrapper), the one-write
//	                 protocol (mark "freezing" → fsync file+dirent → flip
//	                 "frozen"), and the key-driven sweep (the only deletion body).
//	Config         config.go, config_lock.go, config_validate.go
//	                 the TOML schema/loader/defaults, the single-process flock,
//	                 and validateConfig (the network-dependent earliest-ledger
//	                 resolution + the two-pin first-start commit).
//	Cross-cutting  artifacts.go
//	                 the ArtifactSet/Kind abstraction the later layers subset.
//	Storage        process.go, hotsource.go
//	                 processChunk + backfillSource materialize a chunk's cold
//	                 artifacts from the cheapest source (ready hot DB → frozen
//	                 local .pack → bulk backend); hotsource exposes the hot tier
//	                 as a freeze source.
//	Planner        resolve.go, execute.go, eligibility.go
//	                 the postcondition resolver (catalog diff → Plan), the
//	                 bounded-worker executor, and discard/prune eligibility.
//	Ingestion      ingest.go
//	                 the live hot-DB ingestion loop (indexed GetLedger, one synced
//	                 WriteBatch per ledger) and the chunk-boundary handoff.
//	Orchestration  progress.go, lifecycle.go, retention.go
//	                 derived progress (the resume point), the lifecycle tick
//	                 (plan → discard → prune), and retention-floor arithmetic +
//	                 the reader-retention gate.
//	Daemon         startup.go, daemon.go
//	                 startStreaming (catalog → validate → catch-up → serve+ingest
//	                 handoff) and the daemon/CLI wiring.
//	Operability    recovery.go, audit.go, audit_invariants.go
//	                 surgical recovery (atomic key-demotion), the audit command,
//	                 and the INV-1..4 invariant walks.
//	Observability  observability.go
//	                 the metrics sink interface and the signals it emits.
//	Test seam      hooks.go
//	                 test-only crash-injection points fired from inside the real
//	                 protocol/sweep methods (every field nil in production).
//
// # Later slices
//
// Slice 3 adds the tx-hash data type with its per-window rolling index —
// additive on this ledgers+events skeleton.
package streaming
