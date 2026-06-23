// Package streaming holds the orchestration spine for the full-history
// streaming daemon: catch-up on startup, live ingestion from captive core, and
// the freeze → discard → prune lifecycle over the merged storage layer
// (fullhistory/pkg/...). It is built ON that layer — the catalog WRAPS
// metastore.Store rather than reinventing a RocksDB wrapper.
//
// This file map covers Slice 1 · Layers 1–2 (foundations + storage). The
// orchestration and daemon assembly stack on top in later layers (see "Later
// layers" below).
//
// # Data model (keys-first)
//
// Every durable artifact (a per-chunk file) and every per-chunk hot DB is named
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
//	Config         config.go, config_lock.go
//	                 the TOML schema/loader/defaults and the single-process flock
//	                 over the catalog + storage roots.
//	Cross-cutting  artifacts.go
//	                 the ArtifactSet/Kind abstraction the later layers subset.
//	Storage        process.go, hotsource.go
//	                 processChunk + backfillSource materialize a chunk's cold
//	                 artifacts from the cheapest source (ready hot DB → frozen
//	                 local .pack → bulk backend); hotsource exposes the hot tier
//	                 as a freeze source.
//	Test seam      hooks.go
//	                 test-only crash-injection points fired from inside the real
//	                 protocol/sweep methods (every field nil in production).
//
// # Later layers
//
// Layer 3 adds the postcondition resolver/executor, the live ingestion loop,
// and the lifecycle tick (orchestration); Layer 4 adds startStreaming,
// validateConfig, surgical recovery, and the audit command (daemon assembly).
// Slices 2 and 3 then weave in the events and tx-hash data types.
package streaming
