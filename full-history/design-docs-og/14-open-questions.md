# Open Questions

> **Status**: Active — tracks unresolved design decisions that will affect future implementation

---

## OQ-1: getEvents Retrofitting — All Four Workflows and Meta Store Evolution

### Context

The current design has **four distinct workflows**, each of which carries an explicit `getEvents` placeholder section acknowledging future work:

| Workflow | Document | Current getEvents Status |
|----------|----------|--------------------------|
| **Backfill ingestion** | [03-backfill-workflow.md](./03-backfill-workflow.md) | Placeholder: 3rd write step per chunk (events flat file → fsync → `events_done`) |
| **Backfill transition** | [03-backfill-workflow.md](./03-backfill-workflow.md) | Placeholder: additional `build_events_index` task after `build_txhash_index` |
| **Streaming ingestion** | [04-streaming-and-transition.md](./04-streaming-and-transition.md) | Placeholder: separate active events RocksDB store, per-ledger event writes, per-chunk flush to immutable events index |
| **Streaming transition** | [04-streaming-and-transition.md](./04-streaming-and-transition.md) | Placeholder: events index build as an independent sub-flow (likely at chunk cadence during ACTIVE), before transitioning txhash store deletion |

Additionally, [07-crash-recovery.md](./07-crash-recovery.md) and [02-meta-store-design.md](./02-meta-store-design.md) both carry getEvents placeholder sections for recovery semantics and new meta store keys respectively.

### What's TBD

The exact transition flow and cadence for storing **events** across all four workflows is still undecided. Specifically:

**Backfill ingestion** — The `process_chunk` task currently has two write steps (LFS chunk + raw txhash flat file). A third step (events flat file → fsync → `chunk:{C}:events`) needs to be added. The cadence is likely **10K ledgers** (consistent with existing chunk granularity), but the events data format and file structure are not yet defined.

**Backfill transition** — Currently: `build_txhash_index(index_id)` runs after all chunks for an index are done. A new `build_events_index(index_id)` task needs to be added. The events index build would read per-chunk event flat files (analogous to how RecSplit reads raw txhash flat files). Ordering, parallelism, and whether events index build can overlap with RecSplit build are TBD.

**Streaming ingestion** — Per-ledger event data needs to be written to a **separate active events RocksDB store** (its own RocksDB instance, independent of the ledger store and txhash store). Background per-chunk flush to immutable events index (same cadence as LFS: per 10K ledgers, while ACTIVE) is anticipated. The events store rotation cadence and architecture affect memory budget and crash recovery.

**Streaming transition** — Currently: ledger sub-flow transitions independently at each chunk boundary during ACTIVE (LFS flush + `chunk:{C}:lfs`), and at the index boundary, only the txhash sub-flow transitions (RecSplit build from transitioning txhash store → verify → `RemoveTransitioningTxHashStore`). An events sub-flow needs to be added — likely at the same cadence as the ledger sub-flow (per 10K ledgers during ACTIVE). The events store's transition cadence, whether it shares the ledger sub-flow's chunk boundary or runs independently, and its interaction with the index-boundary coordination (`waitForLedgerTransitionComplete` would need to also wait for events) are all TBD.

**Crash recovery** — Each of the above workflows has crash recovery semantics that must extend for events. The chunk skip rule generalizes: a chunk is skippable on resume only when **all** applicable flags are set (today: `chunk:{C}:lfs` AND `chunk:{C}:txhash`; future: AND `chunk:{C}:events`).

### What Will NOT Change

> **RESOLVED**: Meta store key schema simplified to 3 flat keys — `chunk:{C}:lfs`, `chunk:{C}:txhash`, `index:{N}:txhash` — replacing the previous nested `range:{N:04d}:chunk:{C:010d}:*` / `range:{N:04d}:recsplit:*` hierarchy.

**Existing meta store keys and state machines are stable.** The current key hierarchy (documented in [02-meta-store-design.md](./02-meta-store-design.md)) will not change:

| Sub-workflow | Keys | Stable? |
|-------------|------|---------|
| Chunk LFS flags (`chunk:{C:010d}:lfs`) | 1 per chunk | ✅ Unchanged |
| Chunk txhash flags (`chunk:{C:010d}:txhash`) | 1 per chunk (backfill only) | ✅ Unchanged |
| Index txhash index flag (`index:{N:010d}:txhash`) | 1 per index | ✅ Unchanged |
| Streaming checkpoint (`streaming:last_committed_ledger`) | 1 global | ✅ Unchanged |

### What Will Likely Be Added

A **new sub-flow** for events with its own state tracking across all four workflows, following the existing additive pattern:

```
# New meta store keys (additive — no modifications to existing keys):
chunk:{C:010d}:events          ← per-chunk flag, analogous to chunk:{C}:lfs / chunk:{C}:txhash
index:{N:010d}:eventsindex     ← per-index flag, analogous to index:{N}:txhash
```

The backfill task graph extends with an additional task:

```
# Backfill (new task added after build_txhash_index):
build_events_index(index_id)   ← reads per-chunk event flat files, builds events index, sets index:{N}:eventsindex

# Streaming:
(unchanged — events sub-flow transitions at chunk cadence during ACTIVE, same as ledger sub-flow)
```

### Per-Workflow Impact Summary

| Workflow | New step | Cadence | Input | Output |
|----------|----------|---------|-------|--------|
| Backfill ingestion | 3rd write per chunk | 10K ledgers (chunk) | Ledger events from BSB | Events flat file + `chunk:{C}:events` flag |
| Backfill transition | Phase 3 after RecSplit | Per index (`chunks_per_txhash_index` x 10K ledgers) | `chunks_per_txhash_index` per-chunk event flat files | Events index files |
| Streaming ingestion | Per-ledger write + per-chunk flush | 1 ledger (write) / 10K ledgers (flush) | Ledger events from CaptiveStellarCore | Active events RocksDB store + immutable events chunks |
| Streaming transition | Independent sub-flow at chunk cadence during ACTIVE | 10K ledgers (chunk boundary) | Active events RocksDB store | Events index files |
| Crash recovery | Extended skip rule | N/A | `chunk:{C}:events` flags | Skip only when ALL flags set |

### Design Principle

The meta store key hierarchy was designed to be **additive**. New sub-flows introduce new keys — they never modify or reinterpret existing keys. This is already anticipated in the placeholder sections throughout the design docs (see [02-meta-store-design.md](./02-meta-store-design.md)).

---

## OQ-2: Service Identity — `stellar-full-history-rpc` vs `stellar-rpc`

### Context

Should the full-history service ship as a **new binary** (`stellar-full-history-rpc`) or be **integrated into** the existing `stellar-rpc`?

`stellar-rpc` already has a `--backfill` flag ([PR #571](https://github.com/stellar/stellar-rpc/pull/571), merged Jan 2026) that synchronously fills SQLite with the most recent ~7 days of ledgers from CDP before starting captive-core. This is a warm-up for the existing sliding-window retention, not a full-history solution.

### Trade-offs

| | New binary | Integrated into `stellar-rpc` |
|---|---|---|
| **Config** | Clean slate | Coexists with 80+ existing fields; `--backfill` name collision |
| **Storage** | RocksDB + LFS only | Two DB engines in one process (SQLite + RocksDB); complicates `mustInitializeStorage()`, `ResetCache()`, and fee-window resets |
| **Startup** | Long-lived daemon, non-blocking | Existing `--backfill` blocks ~3h for 7 days; full-history can't block |
| **Retention** | No conflict — "everything" | `HistoryRetentionWindow` controls SQLite pruning; full-history = `MAX_UINT32` may break fee-window assumptions |

### The `--backfill` Name Collision

| | Existing `--backfill` (PR #571) | Full-history backfill (this design) |
|---|---|---|
| **Scope** | ~7 days (120K ledgers) | Genesis → tip (58M+ ledgers) |
| **Storage** | SQLite | RocksDB + LFS + RecSplit |
| **Blocking** | Yes (~3h) | No (days/weeks) |
| **Retention** | Sliding window, pruned | Permanent |

### If Integrated: Config Sketch

All full-history config under `[full_history]` to avoid collision with the existing top-level `BACKFILL` flag:

```toml
# Existing stellar-rpc config (unchanged)
BACKFILL = true                            # existing: fill SQLite with 7 days
HISTORY_RETENTION_WINDOW = 120960

# Full-history extension (new, self-contained)
[full_history]
  enabled = true
  mode = "backfill"                        # "backfill" | "streaming"
  data_dir = "/data/stellar-full-history"
```

### Existing Exploration in `stellar-rpc`

The team is exploring multiple directions with no consensus yet:

- [#583](https://github.com/stellar/stellar-rpc/issues/583) — RocksDB store for full-history ingestion + getLedgers
- [#584](https://github.com/stellar/stellar-rpc/issues/584) — RocksDB-backed LedgerStore reader
- [#531](https://github.com/stellar/stellar-rpc/issues/531) — Spike: fully embedded archive node
- [#586](https://github.com/stellar/stellar-rpc/issues/586) — Spike: file-based ledger storage as alternative to RocksDB

### Recommendation Leaning

Separate binary. The storage backends (SQLite vs RocksDB+LFS), retention models (sliding window vs permanent), startup semantics (blocking vs daemon), and crash recovery strategies are fundamentally different.

---

## OQ-3: Transaction Submission Support

### Context

The current design supports two query endpoints (`getTransactionByHash`, `getLedgerBySequence`) and two status endpoints (`getHealth`, `getStatus`). The question is whether the full-history service should also support **transaction submission** — accepting signed transactions from clients and forwarding them to the Stellar network.

**Key fact**: In streaming mode, the service already runs a CaptiveStellarCore instance for live ledger ingestion (see [04-streaming-and-transition.md](./04-streaming-and-transition.md)). This means the process already has persistent network connectivity to the Stellar network. Transaction submission through this existing connection may be low incremental cost.

### Precedent: Erigon (Ethereum Archive Node)

Erigon, the most widely deployed Ethereum archive node, **fully supports transaction submission** alongside full-history querying. It is not a read-only archive — the same process handles both historical queries and live transaction relay.

| Capability | Support | Source |
|---|---|---|
| `eth_sendRawTransaction` | ✅ Implemented | [`rpc/jsonrpc/send_transaction.go`](https://github.com/erigontech/erigon/blob/main/rpc/jsonrpc/send_transaction.go) |
| `eth_sendRawTransactionSync` | ✅ Implemented ([EIP-7966](https://eips.ethereum.org/EIPS/eip-7966)) | [same file, line 67+](https://github.com/erigontech/erigon/blob/main/rpc/jsonrpc/send_transaction.go#L72) |
| `eth_sendTransaction` (node-side signing) | ❌ Not implemented | Nodes should not manage private keys |

**Architecture**: Erigon's txpool runs inline by default (same process). For high-throughput deployments, it can be extracted as a [standalone txpool daemon](https://github.com/erigontech/erigon/blob/main/cmd/txpool/main.go) alongside a Sentry daemon (p2p gossip). The RPCDaemon connects to the txpool via gRPC using the [`--txpool.api.addr`](https://github.com/erigontech/erigon/blob/main/cmd/rpcdaemon/cli/config.go#L134) flag and forwards `eth_sendRawTransaction` calls to it. All node types (Minimal, Full, Archive) expose the same RPC API including transaction submission — see the [EthAPI interface](https://github.com/erigontech/erigon/blob/main/rpc/jsonrpc/eth_api.go#L105-L111).

### Approaches

| Approach | Description | Pros | Cons |
|----------|-------------|------|------|
| **No TX submission** | Read-only archive; clients submit via a separate RPC endpoint | Simpler; pure storage + query | Operators need two services; clients need two endpoints |
| **Proxy TX submission** | Accept `sendTransaction` and forward to a configured upstream Stellar RPC | Single endpoint for clients; no p2p complexity | Extra network hop; depends on upstream availability |
| **Native TX submission** | Submit through the CaptiveStellarCore instance already running in streaming mode | Fully self-contained (like Erigon); CaptiveStellarCore already has network connectivity | Only available in streaming mode; adds submission code path |

### Decision Criteria

1. **Is CaptiveStellarCore already running?** In streaming mode, yes — network connectivity exists. Adding TX submission through it may be low incremental cost.
2. **Does the operator already run a separate Stellar RPC?** If yes, proxy is trivial. If this is their only Stellar service, native submission is more valuable.
3. **Is the primary use case historical data retrieval or full-node functionality?** This affects how much complexity TX submission justifies.

---

## OQ-4: Streaming Backpressure and Drift Detection

### Context

In streaming mode, the daemon ingests ledgers from CaptiveStellarCore at the network's production rate (~1 ledger every 5-6 seconds). If downstream writes slow down (e.g., disk I/O bottleneck, RocksDB compaction stalls), the ingestion pipeline backpressures and the gap between the last committed ledger and the network tip grows.

Currently, the design has **no mechanism** to detect, alert on, or respond to this drift.

### What's TBD

1. **Drift detection**: Should the daemon monitor the gap between `streaming:last_committed_ledger` and the network's current ledger? If so, how is the network tip obtained — from CaptiveStellarCore metadata, or an external source?

2. **Alerting threshold**: Should there be a configurable `max_streaming_drift_ledgers` threshold that triggers a warning or error when exceeded? What's a reasonable default (e.g., 1,000 ledgers ≈ ~1.5 hours)?

3. **Response strategy**: When drift exceeds the threshold, should the daemon:
   - Log a warning and continue (operator monitors externally)?
   - Expose a health endpoint that reports unhealthy (for orchestration systems like Kubernetes)?
   - Pause ingestion and wait for writes to catch up?
   - Abort and require operator intervention?

4. **Metrics exposure**: Should drift, write latency, and ingestion rate be exposed as Prometheus metrics for external monitoring?

### Design Principle

The streaming pipeline is currently "fire and forget" — it processes ledgers as fast as CaptiveStellarCore produces them with no feedback loop. Adding drift detection and backpressure would make the system self-aware of its own health, but increases complexity.

---

## OQ-5: RecSplit Sharding — 16 Files vs Single Index

> **RESOLVED**: Replaced with all-or-nothing build tracked by a single `index:{N:010d}:txhash` key. Per-CF recovery (`recsplit:cf:XX:done` flags) is eliminated. The active txhash store in streaming mode still uses 16 RocksDB CFs for write performance, but that is orthogonal to the index file count.

### Context

The current design builds **16 RecSplit index files per index**, sharded by the first hex character of the transaction hash (the "nibble"). This is a parallelism optimization, not a fundamental architectural requirement.

### Why 16 Shards Today

| Approach | Entries | Build Time | Rationale |
|----------|---------|------------|-----------|
| Single RecSplit index | ~3 billion | ~7 hours | Building one index for all entries in a 10M-ledger index is memory and CPU intensive |
| 16 parallel RecSplit indexes | ~200M each | ~45 minutes per shard | Each shard builds independently; 16 can run in parallel |

With 16 shards, the total RecSplit build time drops from ~7 hours to ~4 hours (limited by DAG scheduling, not per-shard parallelism). Per-shard builds are embarrassingly parallel.

### Resolution: All-or-Nothing with Single Index Key

The design now tracks RecSplit completion with a single key per index (`index:{N:010d}:txhash`). If the build is interrupted, the entire `build_txhash_index` task is rerun from scratch — at most one index worth of work is redone. This eliminates the per-CF `cf:XX:done` flag complexity entirely.

### What Changed

The sharding decision is **isolated to the txhash sub-workflow and RecSplit build phase**. The simplification did not affect:

- The overall two-pipeline architecture
- The data hierarchy (Index → Chunk)
- The chunk-level meta store keys (`chunk:{C}:lfs`, `chunk:{C}:txhash`)
- The LFS ledger store design
- The streaming transition cadences (ledger sub-flow at chunk boundary, txhash sub-flow at index boundary)

The active txhash store in streaming mode still uses 16 CFs for write performance (this is a RocksDB optimization separate from the RecSplit file count).

---

## OQ-6: Pre-Created Archives as Alternative Backfill Source

### Context

The current backfill pipeline ingests ledgers from GCS (via BufferedStorageBackend) or CaptiveStellarCore, writes LFS chunks and raw txhash flat files, then builds RecSplit indexes. This is an ingestion-bound process that takes days to weeks for the full history.

A potential third backfill mode: **download pre-built immutable archives** (LFS chunks + RecSplit indexes) hosted on S3/GCS by a trusted operator. This would skip the entire ingestion + transition pipeline, making backfill a network-bound download-and-verify operation.

### Trade-offs

| Aspect | Current Backfill (BSB/CaptiveCore) | Archive-Based Backfill |
|--------|--------------------------------------|------------------------|
| Speed | Ingestion-bound (days/weeks) | Network-bound (potentially 10–100x faster) |
| Processing | Full ingestion + RecSplit build | Download + verify only |
| Meta store | Tracks chunk flags (`lfs`, `txhash`) + index completion flag (`txhash`) | Simplified: track download progress per index |
| Transition | Required (raw txhash → RecSplit) | Skipped (files already in final format) |
| Trust model | Self-generated from ledger data | Requires trust in archive provider (or verification) |

### Impact on Existing Design

**Nothing changes for existing BSB/CaptiveCore backfill.** The archive-based mode would be a third code path alongside the existing two, selected by configuration (e.g., `[backfill.archive]` section). The existing `[backfill.bsb]` and `[backfill.captive_core]` paths remain identical.

**Meta store changes would be additive only.** The existing key hierarchy (chunk flags, index completion flag) is unchanged. A new archive-mode backfill would likely introduce a simpler set of per-index download tracking keys — for example, a single `index:{N:010d}:archive_download` key — since there is no per-chunk ingestion to track. The index would transition directly from download-in-progress to verification to complete.

**Verification becomes critical.** Downloaded files must be validated before marking an index complete: checksums, RecSplit spot-check queries, and LFS chunk integrity checks. The verification step in the existing streaming transition workflow (spot-check 1,000 samples per index) provides a pattern to follow.

### What's TBD

1. **Archive format and hosting**: What file layout? Tar per index, or individual files? S3/GCS/HTTP?
2. **Verification protocol**: Checksums only, or full RecSplit spot-check verification?
3. **Partial download resume**: How to resume after a network failure mid-index?
4. **Trust model**: Is the archive provider trusted, or must files be independently verified against ledger data?

---

## Resolved Configuration and Naming Items

The following smaller questions were resolved during the design revamp and do not require their own OQ sections:

- **Range → Index rename** — **RESOLVED**: The data unit previously called a "range" is now called an "index" throughout. A range of ledgers is still the underlying concept, but the code, config, and meta store keys all use `index` terminology (e.g., `index_id`, `chunks_per_txhash_index`, `index:{N:010d}:txhash`).

- **`chunks_per_txhash_index` config param** — **RESOLVED**: Added as an explicit config parameter with default 1000. Replaces any implicit assumption of 1,000 chunks per range.

- **`parallel_ranges` vs `workers`** — **RESOLVED**: Single `workers` parameter controls the flat goroutine pool. No separate `parallel_ranges` / `parallel_indexes` concept exists; the DAG scheduler naturally limits per-index parallelism.

- **`flush_interval`** — **RESOLVED**: Treated as an internal constant (10K ledgers per chunk), not a user-facing config parameter. Operators cannot change it.

- **`cleanup_txhash` meta key deletion** — **RESOLVED**: `cleanup_txhash(index_id)` now deletes `chunk:{C}:txhash` keys for all chunks in the index as part of cleanup, in addition to deleting the raw `.bin` flat files.

- **LFS-first streaming in `process_chunk`** — **RESOLVED**: `process_chunk` uses LFS data (if the LFS chunk file is present but txhash flat file is absent) to avoid redundant re-ingestion. Specifically: if `chunk:{C}:lfs` is set but `chunk:{C}:txhash` is absent, only the txhash flat file is (re)written.

---

## Related Documents

- [01-architecture-overview.md](./01-architecture-overview.md) — two-pipeline design, getEvents placeholder, RecSplit sharding (OQ-5), pre-created archives (OQ-6)
- [02-meta-store-design.md](./02-meta-store-design.md) — key hierarchy and getEvents placeholder
- [03-backfill-workflow.md](./03-backfill-workflow.md) — backfill DAG, task types, RecSplit build, getEvents placeholder
- [04-streaming-and-transition.md](./04-streaming-and-transition.md) — streaming ingestion, transition workflow, getEvents placeholder, backpressure context (OQ-4)
- [07-crash-recovery.md](./07-crash-recovery.md) — crash recovery and getEvents placeholder
- [10-configuration.md](./10-configuration.md) — TOML reference
- [12-metrics-and-sizing.md](./12-metrics-and-sizing.md) — metrics, sizing, monitoring reference (OQ-4)
