# Stellar RPC Archive Node (Beta): Node Operator Runbook

## 1. Overview

The Stellar RPC Archive Node maintains full network history from genesis, whereas standard RPC nodes are optimized for short retention windows (e.g., 7 days). To support full history efficiently, the archive node uses a hybrid storage architecture combining RocksDB for recent hot data and immutable flat files for historical cold data.

The archive node is currently in beta ([`rpcv2-v0.1.0-beta.1`](https://github.com/stellar/stellar-rpc/tree/rpcv2-v0.1.0-beta.1)), so details in this guide may change before the full release.

This guide covers running the node. See the [API changes guide](API-CHANGES-BETA.md) for endpoint differences.

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
| Storage Volume | 7 TB initial | Grows by ~1.1 TB per year at current network activity rates. |
| Storage Type | Local, direct-attached NVMe | Network storage (e.g., AWS EBS, GCP Persistent Disk) is **NOT** tested. |

---

## 3. Data Lake Access for Backfill

During initial startup, the node downloads historical ledgers from an external ledger data lake. You have two options for your data lake source:

### Option 1: Public Data Lake

Use the public AWS Open Data bucket:

- **Pubnet Bucket:** `s3://aws-public-blockchain/v1.1/stellar/ledgers/pubnet`

See the [Galexie data lake providers](https://developers.stellar.org/docs/data/indexers/build-your-own/galexie/providers) page for the full list.

### Option 2: Self-Hosted Data Lake

Build and host your own data lake using Galexie.

- [Galexie Admin Guide](https://developers.stellar.org/docs/data/indexers/build-your-own/galexie)

The public AWS Open Data bucket supports anonymous access and requires no AWS credentials or IAM permissions. If using a self-hosted private data lake, configure access via IAM instance roles or mounted cloud credential files.

---

## 4. Setup & Installation

Create local host directories to hold the configuration files and network data, respectively. Then, pull the container image. Put `/srv/rpc-archive` on the 7 TB NVMe volume:

```bash
mkdir -p /srv/rpc-archive/config /srv/rpc-archive/data
docker pull stellar/unsafe-stellar-rpc:archive-node-beta
```

---

## 5. Configuration Guide

The archive node requires two primary configuration files:

- **`rpc-archive.toml`:** Configures the RPC server settings, storage, and backfill sources.
- **`captive-core.toml`:** Configures the embedded Stellar Core instance.

These will go in the `/srv/rpc-archive/config/` folder.
### 5.1 RPC Configuration (`/srv/rpc-archive/config/rpc-archive.toml`)

**Note:** The RPC server doesn't read environment variables (e.g., `SOROBAN_RPC_*`). Set its parameters in `rpc-archive.toml` or pass them as command-line flags.

There are two ways to create the file:

- **Option 1: Minimal configuration.** Copy the block below into `rpc-archive.toml`. It is a complete configuration. Every key not shown takes its default.
- **Option 2: Full configuration.** Download [`rpc-v2-sample-config.toml`](https://github.com/stellar/stellar-rpc/blob/rpcv2-v0.1.0-beta.1/cmd/stellar-rpc/rpcv2/rpc-v2-sample-config.toml) and set the keys shown in the block below.

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

[backfill.datastore.schema]
ledgers_per_file     = 1
files_per_partition = 64000

[ingestion]
captive_core_config  = "/config/captive-core.toml"
history_archive_urls = [
  "https://history.stellar.org/prd/core-live/core_live_001",
  "https://history.stellar.org/prd/core-live/core_live_002",
  "https://history.stellar.org/prd/core-live/core_live_003"
]
```

**Retention** defaults to full history. Leave the `[retention]` section at its defaults.

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

To put a store on a different volume, set its key inside `[storage]`. Mount that volume into the container with a second `-v` flag, and use the container path in the key.

### 5.2 Captive Core Configuration (`/srv/rpc-archive/config/captive-core.toml`)

You should create a configuration file for [Stellar Core](https://github.com/stellar/stellar-core). If you are already running standard Stellar RPC, you can copy your existing Stellar Core configuration file. Otherwise, a sample configuration file for Pubnet is available here:

- [Pubnet Sample Config](https://github.com/stellar/go-stellar-sdk/blob/main/ingest/ledgerbackend/configs/captive-core-pubnet.cfg)

The sample file is not for production use. Its quorum set is only an example. Select the quorum set yourself before you run the node.

### 5.3 Launch Container

Once configuration files are in place, run the container:

```bash
docker run -d --name stellar-rpc-v2 \
  -v /srv/rpc-archive/config:/config:ro \
  -v /srv/rpc-archive/data:/data \
  -p 8000:8000 \
  -p 127.0.0.1:6061:6061 \
  stellar/unsafe-stellar-rpc:archive-node-beta \
  --config /config/rpc-archive.toml
```

- **Persistent Storage:** Preserve `/srv/rpc-archive/data` across restarts. Wiping it triggers a full backfill.
- **Process Restarts:** The node exits non-zero on fatal errors. Check the logs, then start it again. Backfill progress is persisted per 10,000-ledger chunk, so a restart resumes without losing completed work.

---

## 6. Initial Backfill Phase

On initial startup, the container downloads ledger metadata from the configured data lake and backfills history before serving queries. **Nothing is served until the backfill completes.**

- **Estimated Duration:** 24 to 48 hours depending on network bandwidth and disk IOPS.
- **Port 8000 Status & Health Checks:** Port 8000 remains closed and `getHealth` fails throughout backfill. Do **not** configure liveness probes (or ECS target group checks) on port 8000 during initial backfill, as failing health checks will trigger continuous restart loops. Use admin port 6061 for liveness checks during backfill, and enable `getHealth` probes on port 8000 only for readiness or post-backfill serving.
- **Admin Port (6061):** Open and scraping metrics. `soroban_rpc_fullhistory_streaming_last_committed_ledger` does not move during backfill. It updates only when a backfill pass ends. Use the `backfill_chunks_planned` and `backfill_chunks_completed` gauges for per-chunk progress.

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

Metrics are exposed via Prometheus on `service.admin_endpoint` at `/metrics` (namespace: `soroban_rpc`). Set alerts in your own monitoring system on the metrics below.

### Key Metrics to Monitor

| Metric Name | Description / Alert Condition |
|---|---|
| `soroban_rpc_fullhistory_streaming_last_committed_ledger` | Highest ledger written to disk. Alert if flat/unmoving for > 2 minutes (active serving mode only; ignore during initial batch backfill). |
| `soroban_rpc_fullhistory_streaming_retention_floor_ledger` | Lowest ledger the retention policy allows. Expected: 2 for full history. Not a coverage or readiness signal. |
| `soroban_rpc_fullhistory_streaming_live_hot_chunks` | Open RocksDB chunk databases. Expected: 1 (briefly 2 during boundary conversion). |
| `soroban_rpc_json_rpc_request_duration_seconds` | Summary of request latency per method and status code. |

### Critical Error Counters

Alert if `rate(...) > 0` for any of the following. Any count means a fault in the node.

- `soroban_rpc_fullhistory_streaming_failed_destroys_total`
- `soroban_rpc_fullhistory_streaming_tx_index_inconsistencies_total`
- `soroban_rpc_fullhistory_streaming_store_ops_after_deferred_close_total`
- `soroban_rpc_fullhistory_streaming_unavailable_chunk_resolves_total`
- `soroban_rpc_fullhistory_streaming_missing_cold_pack_opens_total`

For support or to report errors, reach out to SDF through your usual channels.
