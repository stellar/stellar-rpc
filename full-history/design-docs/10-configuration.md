# Configuration

## Overview

All configuration is provided via a TOML file. The service is started with `--config path/to/config.toml --mode (backfill|streaming)`. Mode is a required startup flag, not a config file value.

---

## TOML Reference

### [service]

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `data_dir` | string | **Required** | — | Base directory for all data stores. Relative sub-paths are resolved from here. |
| `http_port` | int | Optional | `8080` | HTTP server port (health/status in backfill; all endpoints in streaming) |

---

### [meta_store]

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `path` | string | Optional | `{data_dir}/meta/rocksdb` | Absolute or relative path to meta store RocksDB directory. WAL is always enabled. |

---

### [active_stores]

Used in streaming mode only. Ignored during backfill.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `base_path` | string | Optional | `{data_dir}/active` | Base directory for active stores. Individual stores created as `{base_path}/ledger-store-chunk-{chunkID:06d}/` and `{base_path}/txhash-store-index-{indexID:04d}/` |

---

### [immutable_stores]

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `ledgers_base` | string | Optional | `{data_dir}/immutable/ledgers` | Base path for LFS chunk files |
| `txhash_base` | string | Optional | `{data_dir}/immutable/txhash` | Base path for raw txhash flat files and RecSplit index files |

---

### [backfill]

Required when `--mode backfill` is passed at startup. Ignored in streaming mode.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `start_ledger` | uint32 | **Required** | — | Must equal `indexFirstLedger(N)` for some N. Valid values: 2, 10000002, 20000002, … |
| `end_ledger` | uint32 | **Required** | — | Must equal `indexLastLedger(M)` for some M ≥ N. Valid values: 10000001, 20000001, 30000001, … |
| `chunks_per_txhash_index` | int | Optional | `1000` | Number of chunks that form one txhash index. Valid values: 1, 10, 100, 1000. Controls the cadence at which `build_txhash_index` fires and the minimum pruning granularity. With default 1000: `index_id = chunk_id / 1000`. |
| `workers` | int | Optional | `40` | Total concurrent task slots (process_chunk + build_txhash_index + cleanup_txhash combined). Replaces the old parallel_ranges × instances_per_range model. |

**Ledger backend (mutually exclusive)**: exactly one of `[backfill.bsb]` or `[backfill.captive_core]` must be present. Both present → startup error. Neither present → startup error.

---

### [backfill.bsb]

Use when fetching historical ledgers from GCS/S3 (recommended for backfill).

Cannot be combined with `[backfill.captive_core]`.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `bucket_path` | string | **Required** | — | GCS or S3 path, e.g. `"gs://stellar-ledgers/mainnet"` |
| `buffer_size` | int | Optional | `1000` | BSB internal ledger prefetch depth per BSB instance. |
| `num_workers` | int | Optional | `20` | BSB internal download worker count per BSB instance. |

---

### [backfill.captive_core]

Use when fetching historical ledgers by replaying via a local `stellar-core` binary.

Cannot be combined with `[backfill.bsb]`.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `binary_path` | string | **Required** | — | Path to `stellar-core` binary |
| `config_path` | string | **Required** | — | Path to `captive-core.cfg` |

**Important constraints when using captive_core for backfill**:
- Each CaptiveStellarCore instance replays ledgers sequentially — BSB-style parallelism does not apply
- Each CaptiveStellarCore process requires ~8 GB RAM; the `workers` setting controls how many run concurrently
- Prefer `[backfill.bsb]` for large-scale backfill; `captive_core` is for environments without GCS access

---

### [streaming]

Required when `--mode streaming` is passed at startup.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `start_ledger` | uint32 | Optional | auto | Override the ledger to start streaming from. Normally derived from meta store (`streaming:last_committed_ledger + 1`). |

---

### [streaming.captive_core]

Required in streaming mode.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `binary_path` | string | **Required** | — | Path to `stellar-core` binary |
| `config_path` | string | **Required** | — | Path to `captive-core.cfg` |

---

### [rocksdb]

Shared RocksDB tuning. Applied to meta store and active store (streaming).

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `block_cache_mb` | int | Optional | `8192` | Shared block cache in MB. **Streaming-only** — has no meaningful effect in backfill mode (backfill writes flat files; only the tiny meta store uses RocksDB). |
| `write_buffer_mb` | int | Optional | `64` | Write buffer per column family in MB |
| `max_write_buffer_number` | int | Optional | `2` | Max memtables per CF before stall |

---

## Validation Rules

### Index Boundary Validation (Backfill)

```go
func indexFirstLedger(indexID uint32) uint32 {
    return (indexID * 10_000_000) + 2
}
func indexLastLedger(indexID uint32) uint32 {
    return ((indexID + 1) * 10_000_000) + 1
}
```

- `start_ledger` must satisfy `(start_ledger - 2) % 10_000_000 == 0`
- `end_ledger` must satisfy `(end_ledger - 1) % 10_000_000 == 0`
- `end_ledger >= start_ledger`

**Valid `start_ledger` values**: 2, 10000002, 20000002, 30000002, …
**Valid `end_ledger` values**: 10000001, 20000001, 30000001, …

Both `start_ledger` and `end_ledger` MUST align to index boundaries. Specifically:
- `start_ledger` must equal `indexFirstLedger(N)` for some index N (i.e., `(start_ledger - 2) % 10,000,000 == 0`)
- `end_ledger` must equal `indexLastLedger(M)` for some index M (i.e., `(end_ledger - 1) % 10,000,000 == 0`)

If either value is not index-aligned, the service exits with a startup error. Partial indexes are not supported — every requested index must be complete (all chunks). Note: with the default `chunks_per_txhash_index=1000`, index boundaries coincide with the old range boundaries.

### Path Resolution

- Relative paths are resolved relative to `data_dir`
- Absolute paths are used as-is
- Parent directories must exist or be creatable by the process

### Streaming Gap Validation

Before streaming starts, the service validates that all indexes prior to the current streaming index are in a valid state in the meta store:

- Prior indexes must have completed their txhash index build (indicated by `index:{N}:txhash` key present). Indexes without this key that have all chunk flags set are in-progress and can be resumed.
- If a prior index is missing its `index:{N}:txhash` key and not all chunk flags are set, this is a fatal startup error — the index was never fully ingested. The service logs the offending index IDs and exits.

See [04-streaming-and-transition.md](./04-streaming-and-transition.md) § Startup Validation for the full flowchart.

---

## Example Configurations

### Example 1: Backfill with GCS (Recommended)

Ingest ledgers 2–30,000,001 (ranges 0, 1, 2) from GCS:

```toml
[service]
data_dir = "/data/stellar-rpc"   # required
# http_port = 8080               # optional — defaults to 8080

[backfill]
start_ledger    = 2              # required — must be a valid index start (2, 10000002, …)
end_ledger      = 30000001       # required — must be a valid index end (10000001, 20000001, …)
# chunks_per_txhash_index = 1000        # optional — defaults to 1000; valid: 1/10/100/1000
# workers          = 40          # optional — defaults to 40

[backfill.bsb]
bucket_path   = "gs://stellar-ledgers/mainnet"  # required
# buffer_size   = 1000           # optional — defaults to 1000
# num_workers   = 20             # optional — defaults to 20

# [rocksdb]
# block_cache_mb has no meaningful effect in backfill mode (backfill writes flat files,
# not RocksDB data stores). Only the tiny meta store uses RocksDB during backfill.
# write_buffer_mb and max_write_buffer_number may be left at defaults.
```

**Run**: `ingestion-workflow --config config.toml --mode backfill`

---

### Example 2: Backfill with CaptiveStellarCore

```toml
[service]
data_dir = "/data/stellar-rpc"   # required
# http_port = 8080               # optional — defaults to 8080

[backfill]
start_ledger    = 30000002       # required
end_ledger      = 50000001       # required
# workers = 40                   # optional — defaults to 40; each captive_core task needs ~8GB RAM, tune accordingly

[backfill.captive_core]
binary_path = "/usr/local/bin/stellar-core"   # required
config_path = "/etc/stellar/captive-core.cfg" # required
```

**Note**: `workers` controls total concurrency (default 40 is fine for captive core too, but keep RAM budget in mind — each CaptiveStellarCore process uses ~8 GB).

---

### Example 3: Streaming Mode

```toml
[service]
data_dir  = "/data/stellar-rpc"  # required
# http_port = 8080               # optional — defaults to 8080

# [streaming]
# start_ledger = <auto>          # optional — defaults to streaming:last_committed_ledger + 1
#                                #   (or first ledger after last COMPLETE range if no checkpoint exists)
#                                #   Override only if you need to force a specific resume point.

[streaming.captive_core]
binary_path = "/usr/local/bin/stellar-core"   # required
config_path = "/etc/stellar/captive-core.cfg" # required

[rocksdb]
# All fields optional; shown here with streaming-appropriate values
block_cache_mb          = 8192   # optional — defaults to 8192; keep high for streaming query performance
# write_buffer_mb         = 64   # optional — defaults to 64
# max_write_buffer_number = 2    # optional — defaults to 2
```

**Run**: `ingestion-workflow --config config.toml --mode streaming`

---

### Example 4: Multi-Disk Layout

Spread stores across separate SSD volumes for maximum I/O parallelism. All stores benefit from SSD — meta store requires fast random I/O, active stores require fast write throughput, and immutable stores see large sequential writes during backfill and frequent reads during query serving.

```toml
[service]
data_dir = "/data/stellar-rpc"   # required; used as base for any unset sub-paths below

[meta_store]
# optional — defaults to {data_dir}/meta/rocksdb
path = "/ssd0/stellar-rpc/meta/rocksdb"

[active_stores]
# optional — defaults to {data_dir}/active
# used in streaming mode only; ignored during backfill
base_path = "/ssd1/stellar-rpc/active"

[immutable_stores]
# both optional — default to {data_dir}/immutable/ledgers and {data_dir}/immutable/txhash
ledgers_base = "/ssd2/stellar-rpc/immutable/ledgers"
txhash_base  = "/ssd3/stellar-rpc/immutable/txhash"

[streaming.captive_core]
binary_path = "/usr/local/bin/stellar-core"   # required
config_path = "/etc/stellar/captive-core.cfg" # required
```

---

## Configuration Tips

### Memory Budget and `workers` Sizing

See [12-metrics-and-sizing.md](./12-metrics-and-sizing.md) for the full memory budget breakdown across all modes and `workers` sizing guidance.

### `[backfill.bsb]` vs `[backfill.captive_core]`

| Dimension | `[backfill.bsb]` | `[backfill.captive_core]` |
|-----------|-----------------|--------------------------|
| Data source | GCS / S3 bucket | Local stellar-core binary |
| Parallelism model | Concurrent task slots via `workers` | Concurrent task slots via `workers` |
| RAM per task | Low (network I/O bound) | ~8GB (one stellar-core process) |
| Recommended for | Large-scale backfill, cloud environments | Air-gapped / no GCS access |
| `workers` safe value | 40 (default) | Tune based on available RAM / 8GB |

---

## Related Documents

- [09-directory-structure.md](./09-directory-structure.md) — how config paths map to on-disk layout
- [03-backfill-workflow.md](./03-backfill-workflow.md) — how backfill config drives the workflow
- [04-streaming-and-transition.md](./04-streaming-and-transition.md) — how streaming config is applied
- [01-architecture-overview.md](./01-architecture-overview.md) — hardware requirements
- [12-metrics-and-sizing.md](./12-metrics-and-sizing.md) — memory budgets, workers sizing, storage estimates
