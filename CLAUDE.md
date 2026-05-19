# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

The Stellar RPC server: a JSON-RPC service that lets clients read ledger/transaction/event state from the Stellar network and submit transactions. Built in Go with two Rust static libraries linked via CGo (`preflight` for transaction simulation, `xdr2json` for XDR encoding). Single binary: `stellar-rpc`.

## Build, test, lint

**Toolchain required:** Go 1.26 (see `go.mod`), Rust stable with `wasm32-unknown-unknown` target (see `rust-toolchain.toml`), `jq`, `cargo`.

```bash
make build          # builds Rust libs first (preflight + xdr2json), then go build with version stamping
make install        # build + go install
make test           # go-test + rust-test (go-test runs build-libs first — bare `go test ./...` will fail without the .a files)
make go-test        # Go tests only (also runs build-libs)
make rust-test
make check          # go-check (golangci-lint) + rust-check (cargo fmt --check + clippy)
make go-check-branch  # lint only files changed vs origin/main — useful during PR work
make fmt            # gofmt + cargo fmt --all
make bench          # go test -bench=. across all packages
```

**Single Go test:** `go test -run TestName ./cmd/stellar-rpc/internal/methods/` (run `make build-libs` once first if you've never built).

**Integration tests** (`./cmd/stellar-rpc/internal/integrationtest/...`) require a `stellar-core` binary and explicit env vars — see the README. They are skipped by default.

**Cargo.lock auto-regenerates** when `Cargo.toml` changes (Make rule). Don't hand-edit it.

## High-level architecture

Entry: `cmd/stellar-rpc/main.go` → cobra root → `daemon.MustNew(cfg).Run()` in `cmd/stellar-rpc/internal/daemon/daemon.go`. The daemon spins up two long-running components:

1. **Ingest service** (`internal/ingest`) — runs a captive `stellar-core` subprocess, streams ledger close meta from it, writes to the local SQLite DB, and feeds the in-memory `feewindow` + `ledgerbucketwindow` caches. This is the recent-history hot path.

2. **JSON-RPC HTTP handler** (`internal/jsonrpc.go`) — registers one handler per RPC method (`internal/methods/*.go`, one file per method like `get_ledgers.go`, `get_events.go`). The handler table at `jsonrpc.go:167-330` wires each method through Prometheus metrics + duration warning middleware. Use this table as the index when adding/modifying methods.

**Read-path layering** for historical queries (e.g. `getLedgers`, `getEvents`):
- First try the local DB (`internal/db`, SQLite-backed, holds the recent retention window).
- Fall back to `internal/rpcdatastore` (a GCS/S3-backed history reader) for older data. Handlers carry both readers and merge results when a request spans the boundary — see `get_ledgers.go` for the canonical pattern.

**Rust libraries** (`cmd/stellar-rpc/lib/preflight` and `lib/xdr2json`):
- `preflight` simulates transactions via `soroban-env-host`. Built against *both* `soroban-env-host-curr` and `soroban-env-host-prev` so the binary speaks the previous and current Soroban protocol simultaneously. Go wrapper is in `internal/preflight`.
- `xdr2json` converts XDR ↔ JSON. Go wrapper is in `internal/xdr2json`.
- Both produce `.a` static libs under `cmd/stellar-rpc/lib/<name>/target/<triple>/release-with-panic-unwind/`. Go links them via CGo; `go test` will fail until they exist.

**Version stamping**: the Makefile injects git commit, branch, version tag, build timestamp, and the Soroban env versions into the binary via `-ldflags -X`. `go build` without the Makefile produces an unstamped binary that the daemon will still run but won't report meaningful version info.

## Full-history work (experimental, branch-specific)

Active in-progress feature: store the *entire* Stellar history in a tiered hot/cold local store so RPC can answer deep-history queries without a separate ingest pipeline. **Not wired into the JSON-RPC handlers yet** — handlers still go through `rpcdatastore`. Production-bound work is PR #743 only.

Code layout:
- `internal/fullhistory/pkg/chunk` — ledgers grouped into 10,000-ledger chunks; 1,000 chunks per bucket directory (`{bucket:05d}/{chunk:08d}-ledgers.pack`).
- `internal/fullhistory/pkg/stores/{ledger,eventstore,txhash,metastore}` — per-tier hot (RocksDB) and cold (packfile) implementations.
- `internal/packfile` — immutable append-only file format. Single `os.Open` + `ReadAt` (pread); no mmap. Open cost is one ~256 KB speculative tail read to load trailer + offset index. See `design-docs/packfile-library.md`.
- `internal/zstd` — pooled compressor/decompressor wrappers (zstd dominates cold-read latency, not I/O).

Exploratory drivers under `cmd/stellar-rpc/scripts/` (NOT production code): `full-history-backfill` (the only one being upstreamed), `bench-fullhistory` (latency/throughput harness), plus ephemeral migration/verification scripts.

On the `chowbao/full-history-benchmarks-poc` branch, `BENCHMARK_GOAL.md` + `SESSION_LEARNINGS.md` document the design rationale, dataset locations, performance findings, and gotchas — read those before modifying anything under `internal/fullhistory/` or `internal/packfile/`.

@SESSION_LEARNINGS.md

## Things worth knowing before editing

- **Add a new JSON-RPC method**: write the handler in `internal/methods/<method>.go`, register it in the `handlers` slice in `internal/jsonrpc.go`, add tests next to the handler. The method-name constants live in the external `go-stellar-sdk/protocols/rpc` package.
- **DB migrations** live in `internal/db/sqlmigrations/`. The migration runner is `internal/db/migration.go`. Migrations are embedded via `go:embed`.
- **`ledgerbucketwindow`** and **`feewindow`** are in-memory ring buffers fed by the ingest service. They are *not* persisted; on restart they rebuild from the DB.
- **Mac builds** need a min OS version flag (see Makefile `Darwin` block) to keep Rust's and Go's linkers in agreement.
- **`xdr.LedgerCloseMeta` has multiple versions (V0/V1/V2)** — use the version-agnostic helpers like `.CountTransactions()`/`.TransactionHash(i)` rather than switching on `lcm.V`.
- **`supportlog.New()` defaults to WARN level** — call `logger.SetLevel(logrus.InfoLevel)` explicitly when writing scripts/tooling, or `Infof` calls silently disappear.
