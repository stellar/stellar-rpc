// Package streaming holds the orchestration spine for the full-history
// streaming daemon: catch-up on startup, live ingestion from captive core, and
// the freeze → discard → prune lifecycle over the merged storage layer
// (fullhistory/pkg/...). The catalog WRAPS metastore.Store rather than
// reinventing a RocksDB wrapper.
//
// This covers Slice 1 · Layer 1 (foundations): the durable-state substrate
// only, no daemon goroutines yet. Storage, orchestration, and daemon assembly
// stack on top in later layers.
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
// One cohesive package by design: the crash-safety invariants are verified by
// fault-injection hooks fired from INSIDE the real methods (hooks.go), so the
// catalog, protocol, sweeps, and the I/O paths they protect must share a
// package to keep those hooks package-private. This layer adds:
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
//	Test seam      hooks.go
//	                 test-only crash-injection points fired from inside the real
//	                 protocol/sweep methods (every field nil in production).
//
// Layers 2–4 stack on this foundation (storage → orchestration → daemon
// assembly); slices 2–3 add the events and tx-hash data types.
package streaming
