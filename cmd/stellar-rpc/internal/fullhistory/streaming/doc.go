// Package streaming holds the orchestration spine for the full-history
// streaming daemon: catch-up on startup, live ingestion from captive core, and
// the freeze → rebuild → discard → prune lifecycle over the merged storage
// layer (fullhistory/pkg/...). It is built ON that layer — the catalog WRAPS
// metastore.Store rather than reinventing a RocksDB wrapper.
//
// # Data model (keys-first)
//
// Every durable artifact (a per-chunk file or a per-window index coverage) and
// every per-chunk hot DB is named by exactly one meta-store key, and the path
// on disk is a fixed bijection of that key. Nothing ever lists a directory to
// find work; every scan and sweep iterates keys. The authoritative spec is
// design-docs/full-history-streaming-workflow.md (Data model, One write
// protocol) and gettransaction-full-history-design.md §6.3 (keys, coverage, the
// uniqueness invariant). See also design-docs/full-history-implementation-status.md
// for the issue-by-issue map of this package.
//
// # File map
//
// This is intentionally one cohesive package, not a flat dump: the crash-safety
// invariants are verified by fault-injection hooks fired from INSIDE the real
// methods (see hooks.go), so the catalog, the one-write protocol, the sweeps,
// and the I/O paths they protect must share a package to keep those hooks
// package-private and the invariant tests meaningful. The files group by layer:
//
//	Foundation     keys.go, paths.go, window.go
//	                 key schema, the key↔path bijection, and chunk/window geometry.
//	Catalog        catalog.go, catalog_protocol.go, catalog_sweep.go
//	                 the meta-store wrapper, the one-write protocol
//	                 (mark "freezing" → fsync file+dirent → flip "frozen"), and
//	                 the two key-driven sweeps (the only deletion bodies).
//	Config         config.go, config_validate.go, config_lock.go
//	                 the TOML schema, validateConfig, and single-process flock.
//	Freeze engine  process.go, artifacts.go, txindex.go, eligibility.go,
//	               resolve.go, execute.go
//	                 processChunk + backfillSource materialize a chunk's cold
//	                 artifacts; txindex.go builds the rolling cold tx-hash index;
//	                 resolve/execute are the postcondition planner and the
//	                 bounded-worker executor.
//	Ingestion      ingest.go, hotsource.go
//	                 the live hot-DB ingestion loop (indexed GetLedger, one
//	                 synced WriteBatch per ledger) and the hot freeze source.
//	Orchestration  progress.go, lifecycle.go, retention.go, startup.go, daemon.go
//	                 derived progress, the lifecycle tick, retention arithmetic,
//	                 startStreaming, and the daemon/CLI wiring.
//	Operability    recovery.go, audit.go, audit_invariants.go, observability.go
//	                 surgical recovery, the audit command (INV-1..4) plus its
//	                 invariant walks, and the metrics + structured-logging sink.
//	Test seam      hooks.go
//	                 test-only crash-injection points fired from inside the real
//	                 protocol/sweep/ingest methods (every field nil in production).
//
// Dependencies flow downward — foundation ← catalog ← {config, freeze engine,
// ingestion} ← orchestration — wired by a config-struct hierarchy
// (ProcessConfig/BuildConfig → ExecConfig → LifecycleConfig → StartConfig) and
// by consumer-defined interfaces (LedgerGetter, CoreOpener, NetworkTipBackend,
// Metrics, DeepDeriver, HotProbe/HotChunk/BackendWaiter), so each layer is
// wired at the edges and independently testable.
package streaming
