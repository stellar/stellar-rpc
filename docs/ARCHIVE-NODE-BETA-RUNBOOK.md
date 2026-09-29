# Stellar RPC Archive Node (Beta): Node Operator Runbook

## 1. Overview

The Stellar RPC Archive Node maintains full network history from genesis, whereas standard RPC nodes are optimized for short retention windows (e.g., 7 days). To support full history efficiently, the archive node uses a hybrid storage architecture combining RocksDB for recent hot data and immutable flat files for historical cold data.

The archive node is currently in beta (`rpcv2-v0.1.0-beta.1`), so details in this guide may change before the full release.

This guide covers running the node. See the API changes guide for endpoint differences.

### Storage Engine

Data moves through a two-tier storage system:

- **Hot Tier (RocksDB):** Stores live incoming ledgers and serves queries for recent data.
- **Cold Tier (Flat Files):** Every 10,000 ledgers (~15–17 hours), data is packed from RocksDB into immutable flat files and then pruned from RocksDB.
- **Unified Query Layer:** Server directs incoming requests to RocksDB or flat files based on the requested ledger range.

---

## 2. Hardware & Infrastructure Requirements

| Resource | Minimum Specification | Operational Notes |
|---|---|---|
| CPU | 8 vCPU | |
| RAM | 32 GB | |
| Storage Volume | 6 TB initial | Grows by ~1.1 TB per year at current network activity rates. |
| Storage Type | Local, direct-attached NVMe | Network storage (e.g., AWS EBS, GCP Persistent Disk) is **NOT** tested. |

---

## 3. Data Lake Access for Backfill

During initial startup, the node downloads historical ledgers from an external ledger datalake. You have two options for your datalake source:

### Option 1: Public AWS Open Data Bucket

Utilize the public AWS Open Data bucket provided by Stellar:

- **Pubnet Bucket:** `s3://aws-public-blockchain/v1.1/stellar/ledgers/pubnet`

### Option 2: Self-Hosted Data Lake

Build and host your own data lake using Galexie.

- [Galexie Admin Guide](https://developers.stellar.org/docs/data/indexers/build-your-own/galexie)

The public AWS Open Data bucket supports anonymous access and requires no AWS credentials or IAM permissions. If using a self-hosted private data lake, configure access via IAM instance roles or mounted cloud credential files.

---

## 4. Setup & Installation

Create local host directories and pull the container image:

```bash
mkdir -p /srv/rpc-archive/config /srv/rpc-archive/data
docker pull unsafe-stellar-rpc/stellar-rpc-v2:rpcv2-v0.1.0-beta.1
```

---

## 5. Configuration Guide

The archive node requires two primary configuration files:

- **`rpc-archive.toml`:** Configures the RPC server settings, storage, and backfill sources.
- **`captive-core.toml`:** Configures the embedded Stellar Core instance.

### 5.1 RPC Configuration (`/srv/rpc-archive/config/rpc-archive.toml`)

Download or copy the reference configuration file directly from GitHub:

[`rpc-v2-sample-config.toml` on GitHub](https://github.com/stellar/stellar-rpc/blob/feature/full-history/cmd/stellar-rpc/rpcv2/rpc-v2-sample-config.toml)

**Note:** The RPC server doesn't read environment variables (e.g., `SOROBAN_RPC_*`). Set its parameters in `rpc-archive.toml` or pass them as command-line flags.

**Mandatory parameters you must set:**

```toml
[storage]
# Data directory for RocksDB, flat files, and Captive Core scratch space. Must be on NVMe.
default_data_dir = "/data"

[service]
endpoint       = "0.0.0.0:8000"  # JSON-RPC endpoint (Binds 0.0.0.0 for Docker forwarding)
admin_endpoint = "0.0.0.0:6061"  # Prometheus metrics & pprof (Internal only - DO NOT EXPOSE TO INTERNET)

[backfill.datastore]
type = "S3"                     # "S3" or "GCS"

[backfill.datastore.params]
destination_bucket_path = "aws-public-blockchain/v1.1/stellar/ledgers/pubnet"
region                  = "us-east-2" # AWS region hosting the public data lake bucket

[ingestion]
captive_core_config  = "/config/captive-core.toml"
history_archive_urls = [
  "https://history.stellar.org/prd/core-live/core_live_001",
  "https://history.stellar.org/prd/core-live/core_live_002",
  "https://history.stellar.org/prd/core-live/core_live_003"
]
```

**Retention** is another important configuration block. It is configured by default to retain full history, so you do not need to modify these settings:

```toml
[retention]
earliest_ledger  = "genesis"    # Pinned on first startup; cannot be changed without wiping data
retention_chunks = 0            # 0 indicates unbounded/full history retention
```

**Splitting storage across disks (optional):** By default, the node keeps all of its data under `default_data_dir`. If a single NVMe volume is too small for full history, you can move the large stores (`ledgers`, `events`) to a second NVMe volume. The stores are:

| Store key | Default path | What it holds |
|---|---|---|
| `catalog` | `{default_data_dir}/catalog/rocksdb` | Catalog RocksDB |
| `ledgers` | `{default_data_dir}/ledgers` | Immutable ledger pack files |
| `events` | `{default_data_dir}/events/data` | Immutable event packs |
| `events_index` | `{default_data_dir}/events/index` | Immutable event indexes |
| `txhash_raw` | `{default_data_dir}/txhash/raw` | Transient transaction-hash files |
| `txhash_index` | `{default_data_dir}/txhash/index` | Frozen transaction-hash indexes |
| `hot` | `{default_data_dir}/hot` | Per-chunk hot RocksDB databases |

To put a store on a different volume, set its key inside `[storage]`.

### 5.2 Captive Core Configuration (`/srv/rpc-archive/config/captive-core.toml`)

You should create a configuration file for [Stellar Core](https://github.com/stellar/stellar-core). If you are already running standard Stellar RPC, you can copy your existing Stellar Core configuration file. Otherwise, a sample configuration file for Pubnet is available here:

- [Pubnet Sample Config](https://github.com/stellar/go-stellar-sdk/blob/main/ingest/ledgerbackend/configs/captive-core-pubnet.cfg)

### 5.3 Launch Container

Once configuration files are in place, run the container:

```bash
docker run -d --name stellar-rpc-v2 --restart unless-stopped \
  -v /srv/rpc-archive/config:/config:ro \
  -v /srv/rpc-archive/data:/data \
  -p 8000:8000 \
  -p 127.0.0.1:6061:6061 \
  unsafe-stellar-rpc/stellar-rpc-v2:rpcv2-v0.1.0-beta.1 \
  --config /config/rpc-archive.toml
```

- **Persistent Storage:** Preserve `/srv/rpc-archive/data` across restarts. Wiping it triggers a full backfill.
- **Process Restarts:** The node exits non-zero on fatal errors and relies on the orchestrator to restart it from a durable state. Backfill progress is persisted per 10,000-ledger chunk, allowing restarts to resume without losing completed work.

---

## 6. Initial Backfill Phase

On initial startup, the container downloads ledger metadata from the configured data lake and backfills history before serving queries. **Nothing is served until the backfill completes.**

- **Estimated Duration:** 24 to 48 hours depending on network bandwidth and disk IOPS.
- **Port 8000 Status & Health Checks:** Port 8000 remains closed and `getHealth` fails throughout backfill. Do **not** configure liveness probes (or ECS target group checks) on port 8000 during initial backfill, as failing health checks will trigger continuous restart loops. Use admin port 6061 for liveness checks during backfill, and enable `getHealth` probes on port 8000 only for readiness or post-backfill serving.
- **Admin Port (6061):** Open and scraping metrics. Note that `soroban_rpc_fullhistory_streaming_last_committed_ledger` updates in batch jumps (at the end of each 10,000-ledger chunk pass) rather than continuously ledger-by-ledger.

### 6.1 Monitoring Backfill Progress

Monitor backfill progress via container logs (`docker logs -f stellar-rpc-v2`), disk capacity, or Prometheus metrics.

**Log Signals:**

- `msg="backfill pass starting"` / `msg="backfill pass complete"`
- `msg="chunk build started"`
- `msg="chunk frozen"` — Emitted per chunk; reports progress (e.g., `done=X of=Y`) and throughput
- A line that starts with `msg="backfill complete` followed by `msg="read server listening"` — Ready to serve

**Disk Growth:** Capacity increases primarily inside `events/` and `ledgers/` under your data directory.

**Prometheus Gauges:** Track progress via `soroban_rpc_fullhistory_streaming_backfill_chunks_planned` and `soroban_rpc_fullhistory_streaming_backfill_chunks_completed`.

### 6.2 Verifying Node Readiness

Once `read server listening` appears in logs, verify node health:

```bash
curl -s localhost:8000 -H 'content-type: application/json' -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}'
```

> [!NOTE]
> `getHealth` will return an error until the process commits its first live ledger.

---

## 7. Monitoring & Operational Alerting

Metrics are exposed via Prometheus on `service.admin_endpoint` at `/metrics` (namespace: `soroban_rpc`).

### Key Metrics to Monitor

| Metric Name | Description / Alert Condition |
|---|---|
| `soroban_rpc_fullhistory_streaming_last_committed_ledger` | Highest ledger written to disk. Alert if flat/unmoving for > 2 minutes (active serving mode only; ignore during initial batch backfill). |
| `soroban_rpc_fullhistory_streaming_retention_floor_ledger` | Oldest ledger served by the node (should be 1 / genesis). |
| `soroban_rpc_fullhistory_streaming_live_hot_chunks` | Open RocksDB chunk databases. Expected: 1 (briefly 2 during boundary conversion). |
| `soroban_rpc_json_rpc_request_duration_seconds` | Histogram tracking latency per method and status code. |

### Critical Error Counters

Alert if `rate(...) > 0` for any of the following:

- `soroban_rpc_fullhistory_streaming_failed_destroys_total`
- `soroban_rpc_fullhistory_streaming_tx_index_inconsistencies_total`
- `soroban_rpc_fullhistory_streaming_store_ops_after_deferred_close_total`
- `soroban_rpc_fullhistory_streaming_unavailable_chunk_resolves_total`
- `soroban_rpc_fullhistory_streaming_missing_cold_pack_opens_total`
