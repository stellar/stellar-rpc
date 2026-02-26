# Open Questions

> **Status**: Active — tracks unresolved design decisions that will affect future implementation

---

## OQ-1: getEvents Retrofitting — All Four Workflows and Meta Store Evolution

### Context

The current design has **four distinct workflows**, each of which carries an explicit `getEvents` placeholder section acknowledging future work:

| Workflow | Document | Current getEvents Status |
|----------|----------|--------------------------|
| **Backfill ingestion** | [03-backfill-workflow.md](./03-backfill-workflow.md) | Placeholder: 3rd write step per chunk (events flat file → fsync → `events_done`) |
| **Backfill transition** | [05-backfill-transition-workflow.md](./05-backfill-transition-workflow.md) | Placeholder: Phase 3 events index build from per-chunk event files, after RecSplit |
| **Streaming ingestion** | [04-streaming-workflow.md](./04-streaming-workflow.md) | Placeholder: separate active events RocksDB store, per-ledger event writes, per-chunk flush to immutable events index |
| **Streaming transition** | [06-streaming-transition-workflow.md](./06-streaming-transition-workflow.md) | Placeholder: events index build as an independent sub-flow (likely at chunk cadence during ACTIVE), before transitioning txhash store deletion |

Additionally, [07-crash-recovery.md](./07-crash-recovery.md) and [02-meta-store-design.md](./02-meta-store-design.md) both carry getEvents placeholder sections for recovery semantics and new meta store keys respectively.

### What's TBD

The exact transition flow and cadence for storing **events** across all four workflows is still undecided. Specifically:

**Backfill ingestion** — The chunk sub-workflow currently has two write steps (LFS chunk + raw txhash flat file). A third step (events flat file → fsync → `events_done`) needs to be added. The cadence is likely **10K ledgers** (consistent with existing chunk granularity), but the events data format and file structure are not yet defined.

**Backfill transition** — Currently: `INGESTING → RECSPLIT_BUILDING → COMPLETE`. A new Phase 3 (`EVENTS_INDEX_BUILDING`) needs to be inserted. The events index build would run after RecSplit completes, reading per-chunk event flat files (analogous to how RecSplit reads raw txhash flat files). Ordering, parallelism, and whether events index build can overlap with RecSplit build are TBD.

**Streaming ingestion** — Per-ledger event data needs to be written to a **separate active events RocksDB store** (its own RocksDB instance, independent of the ledger store and txhash store). Background per-chunk flush to immutable events index (same cadence as LFS: per 10K ledgers, while ACTIVE) is anticipated. The events store rotation cadence and architecture affect memory budget and crash recovery.

**Streaming transition** — Currently: ledger sub-flow transitions independently at each chunk boundary during ACTIVE (LFS flush + `lfs_done`), and at the range boundary, only the txhash sub-flow transitions (RecSplit build from transitioning txhash store → verify → `RemoveTransitioningTxHashStore`). An events sub-flow needs to be added — likely at the same cadence as the ledger sub-flow (per 10K ledgers during ACTIVE). The events store's transition cadence, whether it shares the ledger sub-flow's chunk boundary or runs independently, and its interaction with the range-boundary coordination (`waitForLedgerTransitionComplete` would need to also wait for events) are all TBD.

**Crash recovery** — Each of the above workflows has crash recovery semantics that must extend for events. The chunk skip rule generalizes: a chunk is skippable on resume only when **all** applicable flags are set (today: `lfs_done` AND `txhash_done`; future: AND `events_done`).

### What Will NOT Change

**Existing meta store keys and state machines are stable.** The current key hierarchy (documented in [02-meta-store-design.md](./02-meta-store-design.md)) will not change:

| Sub-workflow | Keys | Stable? |
|-------------|------|---------|
| Range state (`range:{N:04d}:state`) | 1 per range | ✅ Unchanged |
| Chunk LFS flags (`range:{N:04d}:chunk:{C:06d}:lfs_done`) | 1,000 per range | ✅ Unchanged |
| Chunk txhash flags (`range:{N:04d}:chunk:{C:06d}:txhash_done`) | 1,000 per range (backfill only) | ✅ Unchanged |
| RecSplit state (`range:{N:04d}:recsplit:state`) | 1 per range | ✅ Unchanged |
| RecSplit CF flags (`range:{N:04d}:recsplit:cf:{XX}:done`) | 16 per range | ✅ Unchanged |
| Streaming checkpoint (`streaming:last_committed_ledger`) | 1 global | ✅ Unchanged |

### What Will Likely Be Added

A **new sub-flow** for events with its own state tracking across all four workflows, following the existing additive pattern:

```
# New meta store keys (additive — no modifications to existing keys):
range:{N:04d}:chunk:{C:06d}:events_done          ← per-chunk flag, analogous to lfs_done/txhash_done
range:{N:04d}:events_index:state                  ← PENDING / BUILDING / COMPLETE
range:{N:04d}:events_index:cf:{XX}:done           ← per-partition done flag
```

The range state machine extends differently per mode:

```
# Backfill:
INGESTING → RECSPLIT_BUILDING → EVENTS_INDEX_BUILDING → COMPLETE

# Streaming:
ACTIVE → TRANSITIONING → COMPLETE  (unchanged — events sub-flow transitions at chunk cadence during ACTIVE, same as ledger sub-flow)
```

### Per-Workflow Impact Summary

| Workflow | New step | Cadence | Input | Output |
|----------|----------|---------|-------|--------|
| Backfill ingestion | 3rd write per chunk | 10K ledgers (chunk) | Ledger events from BSB | Events flat file + `events_done` flag |
| Backfill transition | Phase 3 after RecSplit | Per range (10M ledgers) | 1,000 per-chunk event flat files | Events index files |
| Streaming ingestion | Per-ledger write + per-chunk flush | 1 ledger (write) / 10K ledgers (flush) | Ledger events from CaptiveStellarCore | Active events RocksDB store + immutable events chunks |
| Streaming transition | Independent sub-flow at chunk cadence during ACTIVE | 10K ledgers (chunk boundary) | Active events RocksDB store | Events index files |
| Crash recovery | Extended skip rule | N/A | `events_done` flags | Skip only when ALL flags set |

### Design Principle

The meta store key hierarchy was designed to be **additive**. New sub-flows introduce new keys — they never modify or reinterpret existing keys. This is already anticipated in the placeholder sections throughout the design docs (see [02-meta-store-design.md — getEvents Placeholder](./02-meta-store-design.md#getevents-immutable-store--placeholder)).

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

**Key fact**: In streaming mode, the service already runs a CaptiveStellarCore instance for live ledger ingestion (see [04-streaming-workflow.md](./04-streaming-workflow.md)). This means the process already has persistent network connectivity to the Stellar network. Transaction submission through this existing connection may be low incremental cost.

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

## Related Documents

- [02-meta-store-design.md](./02-meta-store-design.md) — current key hierarchy and getEvents placeholder
- [03-backfill-workflow.md](./03-backfill-workflow.md) — backfill ingestion and getEvents placeholder
- [04-streaming-workflow.md](./04-streaming-workflow.md) — streaming ingestion, getEvents placeholder, and backpressure/drift context (OQ-4)
- [05-backfill-transition-workflow.md](./05-backfill-transition-workflow.md) — backfill transition and getEvents placeholder
- [06-streaming-transition-workflow.md](./06-streaming-transition-workflow.md) — streaming transition and getEvents placeholder
- [07-crash-recovery.md](./07-crash-recovery.md) — crash recovery and getEvents placeholder
- [01-architecture-overview.md](./01-architecture-overview.md) — two-pipeline design and getEvents placeholder
- [10-configuration.md](./10-configuration.md) — current TOML reference
- [12-metrics-and-sizing.md](./12-metrics-and-sizing.md) — metrics, sizing, and monitoring reference (OQ-4)
