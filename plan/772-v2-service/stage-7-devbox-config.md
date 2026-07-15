# Stage 7 — Devbox config, runbook, smoke script

## Mission

- Produce the working config + runbook Karthik runs on the aws-devbox, and a smoke script that exercises every served endpoint.
- Everything environment-specific stays a clearly-marked placeholder (decision: Karthik fills `earliest_ledger` and the datastore/BSB blocks himself).

## Read first

- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/00-DECISIONS.md`, `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/CHECKPOINT.md` (final as-built config keys from stages 1 and 6 — the TOML below must match what was ACTUALLY built, not this doc's guess).
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/design-docs/full-history-streaming-workflow.md` §Configuration (the base v2 TOML contract).
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/config/config.go` (as extended by stages 1/6).

## Deliverables

### 1. `deploy/devbox/full-history.toml` (placeholders marked `# FILL:`)

```toml
[service]
default_data_dir = "/mnt/nvme/stellar-rpc-v2"        # FILL: fast local disk

[retention]
earliest_ledger = "genesis"                           # FILL: Karthik sets this
retention_chunks = 0                                  # 0 = full history

[backfill]
workers = 0                                           # 0/unset = GOMAXPROCS

[backfill.datastore]                                  # FILL: BSB lake (Karthik)
# type = "S3" | "GCS"
# [backfill.datastore.params] ...
# [backfill.datastore.schema] ...

[backfill.bsb]                                        # FILL: tuning (optional)

[ingestion]
captive_core_config = "/etc/stellar/captive-core.toml"   # FILL
history_archive_urls = ["https://..."]                    # FILL
stellar_core_binary_path = "/usr/local/bin/stellar-core"  # FILL
captive_core_http_query_port = 11628
# + the query thread-pool/snapshot keys stage 6 added

[serving]
endpoint = "0.0.0.0:8000"
admin_endpoint = "0.0.0.0:8001"
# limits/cache keys: stage-6 defaults are fine to start

[logging]
level = "info"
format = "json"
```

- Also drop a minimal captive-core TOML TEMPLATE next to it (`NETWORK_PASSPHRASE`, validators/quorum for the chosen network) marked FILL throughout — v1's embedded configs under `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/config/` show the shape.

### 2. `deploy/devbox/RUNBOOK.md`

- Build: `go build -o stellar-rpc ./cmd/stellar-rpc` (note CGo deps: grocksdb + zstd + libpreflight — capture the exact package list/flags by DOING a clean build, including anything the repo's Makefile/README already documents; record what an Amazon Linux/Ubuntu devbox needs installed).
- Run: `./stellar-rpc full-history --config deploy/devbox/full-history.toml`; systemd unit example; SIGTERM = clean shutdown; supervised restart semantics.
- Observe: `curl :8001/metrics`, `curl :8001/latency.json`, gate behavior during backfill, disk sizing note (2× retention transient after long downtime — streaming doc §Startup).

### 3. `deploy/devbox/smoke.sh`

- Sequenced curls with expected shapes, exit non-zero on mismatch:
  - `getHealth` (backfilling → "backfill in progress"; serving → "healthy")
  - `metrics` (non-zero ingest.e2e count once ingestion runs)
  - `getLatestLedger`, `getNetwork`, `getVersionInfo`
  - `getLedgers` (start=oldest, limit=5), cursor page 2
  - `getTransactions` (start=recent ledger), `getTransaction` (hash plucked from the getTransactions response)
  - `getEvents` (recent 1000-ledger window, no filter + a contract filter)
  - `getLedgerEntries` (a well-known key, e.g. the native asset contract instance — document how to build the base64 key)
  - `getFeeStats` → expects the unsupported error (stub proof)

## Acceptance

- A clean-checkout build on the devbox following RUNBOOK.md succeeds.
- With Karthik's filled placeholders: daemon starts, backfill runs, gate opens, `smoke.sh` passes end-to-end.
- CHECKPOINT.md updated: final config file paths + any RUNBOOK gotchas discovered on the devbox; mark the plan COMPLETE.
