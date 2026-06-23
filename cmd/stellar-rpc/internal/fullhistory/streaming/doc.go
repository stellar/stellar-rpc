// Package streaming holds the orchestration spine for the full-history
// streaming daemon: catch-up on startup, live ingestion from captive core, and
// the freeze → rebuild → discard → prune lifecycle over the merged storage
// layer (fullhistory/pkg/...). It is built ON that layer — the catalog WRAPS
// metastore.Store rather than reinventing a RocksDB wrapper.
//
// This file map covers the complete daemon — Slice 1 (the skeleton) + Slice 2
// (events) + Slice 3 (tx-hash). Tx-hash is the per-window rolling-index
// subsystem: it threads a window dimension through the catalog and adds the
// cold .bin/.idx artifacts and their rebuild.
//
// # Data model (keys-first)
//
// Every durable artifact (a per-chunk file — ledger, events, or tx-hash .bin —
// or a per-window index .idx) and every per-chunk hot DB is named by exactly
// one catalog key, and the path on disk is a fixed bijection of that key.
// Nothing ever lists a directory to find work; every scan and sweep iterates
// keys. The authoritative spec is
// design-docs/full-history-streaming-workflow.md (Data model, One write
// protocol) and gettransaction-full-history-design.md §6.3 (index keys,
// coverage, and the uniqueness invariant).
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
//	Foundation     keys.go, paths.go, window.go
//	                 the catalog key schema, the key↔path bijection, and the
//	                 chunk + window geometry.
//	Catalog        catalog.go, catalog_protocol.go, catalog_sweep.go
//	                 the catalog (a metastore.Store wrapper), the one-write
//	                 protocol (mark "freezing" → fsync file+dirent → flip
//	                 "frozen"), and the key-driven sweeps (the only deletion
//	                 bodies — per-chunk and per-window index).
//	Config         config.go, config_lock.go, config_validate.go
//	                 the TOML schema/loader/defaults, the single-process flock,
//	                 and validateConfig (the network-dependent earliest-ledger
//	                 resolution + the two-pin first-start commit).
//	Cross-cutting  artifacts.go
//	                 the ArtifactSet/Kind abstraction the data-type slices subset.
//	Storage        process.go, hotsource.go
//	                 processChunk + backfillSource materialize a chunk's cold
//	                 artifacts from the cheapest source (ready hot DB → frozen
//	                 local .pack → bulk backend); hotsource exposes the hot tier
//	                 as a freeze source.
//	Index          txindex.go
//	                 the per-window rolling cold tx-hash index: buildTxhashIndex
//	                 (k-way merge of the chunk .bin runs → a coverage-named .idx),
//	                 the atomic promote/demote commit batch, and buildThenSweep.
//	Planner        resolve.go, execute.go, eligibility.go
//	                 the postcondition resolver (catalog diff → Plan, incl. the
//	                 per-window index rule), the bounded-worker executor (the
//	                 chunk→index done-channel stratum), and discard/prune
//	                 eligibility (incl. the index-aware discard gate).
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
package streaming
