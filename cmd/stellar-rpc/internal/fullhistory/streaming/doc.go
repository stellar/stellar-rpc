// Package streaming holds the orchestration spine for the cold-only Phase-1
// full-history backfill daemon: startup catch-up from a bulk backend, the
// freeze → rebuild → prune cold lifecycle over the merged storage layer
// (fullhistory/pkg/...), and serving reads. The catalog WRAPS metastore.Store
// rather than reinventing a RocksDB wrapper. This cold-only subset has no hot
// tier — no per-chunk hot DB, no live ingestion loop, no captive core.
//
// # Data model (keys-first)
//
// Every durable artifact (a per-chunk file or a per-window index coverage) is
// named by exactly one meta-store key, and its on-disk path is a fixed bijection
// of that key. Nothing lists a directory to find work; every scan and sweep
// iterates keys. The authoritative spec is
// design-docs/full-history-streaming-workflow.md (Data model, One write protocol)
// and gettransaction-full-history-design.md §6.3 (keys, coverage, uniqueness).
//
// # File map
//
// This is intentionally one cohesive package, not a flat dump: the crash-safety
// invariants are verified by fault-injection hooks fired from INSIDE the real
// methods (see hooks.go), so the catalog, the one-write protocol, the sweeps, and
// the I/O paths they protect must share a package to keep those hooks
// package-private and the invariant tests meaningful. The files group by layer:
//
//	Foundation     keys.go, paths.go, window.go
//	                 key schema, the key↔path bijection, chunk/window geometry.
//	Catalog        catalog.go, catalog_protocol.go, catalog_sweep.go
//	                 the meta-store wrapper, the one-write protocol, and the two
//	                 key-driven sweeps (the only deletion bodies).
//	Config         config.go, config_validate.go, config_lock.go
//	                 the TOML schema, validateConfig, and single-process flock.
//	Freeze engine  process.go, artifacts.go, txindex.go, resolve.go, execute.go
//	                 processChunk + backfillSource materialize a chunk's cold
//	                 artifacts; txindex.go builds the rolling tx-hash index;
//	                 resolve/execute are the planner and the bounded-worker executor.
//	Orchestration  progress.go, retention.go, startup.go, daemon.go
//	                 derived progress + cold/retention arithmetic, startStreaming
//	                 (catch-up + serve), and the daemon/CLI wiring.
//	Observability  observability.go
//	                 the control-plane metrics sink (catch-up + lifecycle phases).
//	Test seam      hooks.go
//	                 test-only crash-injection points fired from inside the real
//	                 protocol/sweep methods (every field nil in production).
//
// Phase 2 adds the hot tier, the live ingestion + lifecycle loops, and the
// operability layer (surgical recovery + the INV-1..4 audit command).
//
// Dependencies flow downward — foundation ← catalog ← {config, freeze engine} ←
// orchestration — wired by a config-struct hierarchy (ProcessConfig/BuildConfig →
// ExecConfig → StartConfig) and consumer-defined interfaces (NetworkTipBackend,
// Metrics, BackendWaiter), so each layer is wired at the edges and testable.
package streaming
