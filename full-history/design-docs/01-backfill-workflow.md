# Backfill Workflow

## Overview

Backfill is the RPC service's historical-ingestion subroutine — it pulls ledgers from a remote object store (BSB) and writes them as immutable, query-ready artifacts on local disk. It runs once per daemon start, as part of Phase 1 (catchup), to close the gap between on-disk state and the current network tip before live ingestion takes over. Interruption at any point leaves recoverable state; on restart, already-complete work is skipped.

**What it produces:**

Three immutable artifact types, one per full-history RPC query, scoped to **chunks** (10_000-ledger blocks) and **tx indexes** (consecutive chunks, default 1_000 = 10_000_000 ledgers each — see [Geometry](#geometry)).

| Immutable output | Query it enables | Scope |
|-----------------|-----------------|-------|
| Ledger [pack file](https://github.com/stellar/stellar-rpc/pull/633) | `getLedger` | Per chunk (10_000 ledgers) |
| Tx-index files | `getTransaction` | Per tx index (default 10_000_000 ledgers) |
| [Events cold segment](https://github.com/stellar/stellar-rpc/pull/635) | `getEvents` | Per chunk |

**How it does it:**

- Backfill is a subroutine invoked by the RPC service's **Phase 1 (catchup)** — see [02-streaming-workflow.md — Phase 1](./02-streaming-workflow.md#phase-1--catchup). Internal to the daemon; no `full-history-backfill` subcommand, no per-run flags.
- Ledger source is **BSB** (Buffered Storage Backend) — a remote object-store reader for `LedgerCloseMeta`, configured under `[BSB]` in the TOML config. Interface details in [02-streaming-workflow.md — Ledger Source](./02-streaming-workflow.md#ledger-source).
- Input: an integer chunk range `[range_start_chunk_id, range_end_chunk_id]` and a `make_bsb` partial function — calling `make_bsb()` returns a fresh `BSBSource` instance.
- Ingests historical ledgers via per-task BSB instances. Each `process_chunk` task (the per-chunk unit of work; full pseudocode in [Task Details](#task-details) below) calls `make_bsb()` to get its own `BSBSource`, calls `prepare_range` scoped to the chunk's 10_000 ledgers, reads in a loop, and tears down. Independent per chunk — no shared source state.
- Writes directly to immutable file formats — no RocksDB active stores (mutable RocksDB instances holding in-flight live-ingestion data; streaming's concern, see [02-streaming-workflow.md — Active Store Architecture](./02-streaming-workflow.md#active-store-architecture)).
- Tracks per-chunk and per-tx-index completion in a small **meta store** — a dedicated RocksDB with WAL always on, separate from streaming's active stores. Each flag is written after its artifact's `fsync`, and flag presence drives all resume decisions.
- Schedules work as a DAG of idempotent tasks dispatched via a flat worker pool capped at `GOMAXPROCS`.
- Returns when every chunk in the range is complete; on crash, Phase 1 (catchup) re-invokes with the same range and already-complete chunks are skipped via per-chunk idempotency.
- **BSB-only.** Backfill does not use captive core (the embedded `stellar-core` subprocess the daemon runs for live ingestion) as a ledger source. Captive core belongs to Phase 4 (live ingestion); if BSB isn't configured, backfill is not invoked at all and Phase 4 (live ingestion)'s captive core catches up from a leapfrog'd resume ledger — a start ledger chosen forward of genesis so ingestion stays within the retention window — as part of normal startup. See [02-streaming-workflow.md — Phase 4](./02-streaming-workflow.md#phase-4--live-ingestion) and [Ledger Source](./02-streaming-workflow.md#ledger-source).

For the distinction between *backfill (this subroutine)* and *Phase 1 (catchup) (the startup phase that invokes it)* — two terms that get conflated because their scopes overlap — see [02-streaming-workflow.md — Backfill vs Phase 1 (catchup)](./02-streaming-workflow.md#backfill-vs-phase-1-catchup).

---

## Geometry

Stellar's first ledger is `GENESIS_LEDGER = 2`. Mapping functions subtract it to zero-base the `ledger_seq ↔ chunk_id` axis.

```python
GENESIS_LEDGER          = 2
LEDGERS_PER_CHUNK       = 10_000                                    # hardcoded; not configurable
CHUNKS_PER_TXHASH_INDEX = 1000 # read from config, immutable after first run. Acceptable values - 1 / 10 / 100 / 1_000; default 1_000
LEDGERS_PER_INDEX       = CHUNKS_PER_TXHASH_INDEX * LEDGERS_PER_CHUNK
                          # at cpi=1_000 this is 10_000_000
```

- In pseudocode, `cpi` in inline comments is shorthand for `CHUNKS_PER_TXHASH_INDEX`.
- All IDs use uniform `%08d` zero-padding (supports up to `99_999_999`).

---

## Configuration

- Backfill reads the subset of the unified TOML config described below. Daemon-level keys unused by backfill are specified in [02-streaming-workflow.md — Configuration](./02-streaming-workflow.md#configuration).

### TOML Config

**[SERVICE]**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `DEFAULT_DATA_DIR` | string | **required** | Base directory for meta store and default storage paths. |
| `CHUNKS_PER_TXHASH_INDEX` | int | `1000` | Chunks per tx index. Defines data layout; stored in the meta store on first run and fatal if changed on any subsequent run. |

**[IMMUTABLE_STORAGE.LEDGERS]** (optional)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `PATH` | string | `{DEFAULT_DATA_DIR}/ledgers` | Base path for ledger pack files. |

**[IMMUTABLE_STORAGE.EVENTS]** (optional)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `PATH` | string | `{DEFAULT_DATA_DIR}/events` | Base path for events cold segments. |

**[IMMUTABLE_STORAGE.TXHASH_RAW]** (optional)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `PATH` | string | `{DEFAULT_DATA_DIR}/txhash/raw` | Base path for raw txhash `.bin` files (transient). |

**[IMMUTABLE_STORAGE.TXHASH_INDEX]** (optional)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `PATH` | string | `{DEFAULT_DATA_DIR}/txhash/index` | Base path for RecSplit (minimal-perfect-hash index library) `.idx` files (permanent). |

The `IMMUTABLE_STORAGE` prefix disambiguates from `ACTIVE_STORAGE` (RocksDB-backed mutable stores owned by the streaming workflow).

**[BSB]** — Buffered Storage Backend (optional at the daemon level; required when [Phase 1 (catchup)](./02-streaming-workflow.md#phase-1--catchup) selects `BSBSource`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `BUCKET_PATH` | string | **required** | Remote object store path to fetch LedgerCloseMeta (without `gs://` prefix for GCS). |
| `BUFFER_SIZE` | int | `1000` | Prefetch buffer depth per connection. |
| `NUM_WORKERS` | int | `20` | Download workers per connection. |

- `[BSB]` is effectively required when backfill runs. If absent, Phase 1 (catchup) does not invoke `run_backfill` at all — Phase 4's captive core handles initial catchup instead (see [02-streaming-workflow.md — Ledger Source](./02-streaming-workflow.md#ledger-source)).

**[LOGGING]** (optional)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `LEVEL` | string | `"info"` | Minimum log severity. Accepted values: `debug` / `info` / `warn` / `error`. Daemon CLI flag `--log-level` wins when both are set. |
| `FORMAT` | string | `"text"` | Log output format. Accepted values: `text` / `json`. Daemon CLI flag `--log-format` wins when both are set. |

**[META_STORE]** (optional)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `PATH` | string | `{DEFAULT_DATA_DIR}/meta/rocksdb` | Meta store RocksDB directory. |

### Example TOML

```toml
[SERVICE]
DEFAULT_DATA_DIR = "/data/stellar-rpc"
CHUNKS_PER_TXHASH_INDEX = 1000

[IMMUTABLE_STORAGE.LEDGERS]
PATH = "/mnt/nvme/ledgers"

[IMMUTABLE_STORAGE.EVENTS]
PATH = "/mnt/nvme/events"

[IMMUTABLE_STORAGE.TXHASH_RAW]
PATH = "/mnt/nvme/txhash/raw"

[IMMUTABLE_STORAGE.TXHASH_INDEX]
PATH = "/mnt/nvme/txhash/index"

[BSB]
BUCKET_PATH = "sdf-ledger-close-meta/v1/ledgers/pubnet"

[LOGGING]
LEVEL = "info"
FORMAT = "text"
```

The TOML above is consumed by the RPC service entry point (`stellar-rpc --config ...`); backfill is invoked internally by [Phase 1 (catchup)](./02-streaming-workflow.md#phase-1--catchup) with the chunk range and source it computed.

---

## Directory Structure

With geometry and storage paths (`IMMUTABLE_STORAGE.*`) defined above, here is how they map to the filesystem.

- Each data type has its own directory tree rooted at its `IMMUTABLE_STORAGE.*.PATH`.
- Chunk-level files (ledgers, events, raw txhash) are grouped into subdirectories (`bucket`) of 1_000 chunks:
  - `bucket_id = chunk_id // 1000` (hardcoded, not configurable), formatted as `%05d`.
  - `bucket_id` is purely a filesystem concern — it does not appear in meta store keys, DAG dependencies, or config.
- Tx-index output is the only structure that uses `tx_index_id` instead of `bucket_id`.
- Directories are created on-demand via `os.MkdirAll` (safe for concurrent writes).

```
{DEFAULT_DATA_DIR}/
├── meta/
│   └── rocksdb/                                  ← Meta store (WAL = Write-Ahead Log; always enabled)
│
├── ledgers/                                      ← IMMUTABLE_STORAGE.LEDGERS.PATH
│   ├── 00000/                                    ← chunk_ids 0–999 (1_000 .pack files)
│   │   ├── 00000000.pack                         ← ledger pack file (PR #633)
│   │   ├── 00000001.pack
│   │   └── ...
│   ├── 00001/                                    ← chunk_ids 1_000–1_999
│   │   └── ...
│   └── .../
│
├── events/                                       ← IMMUTABLE_STORAGE.EVENTS.PATH
│   ├── 00000/                                    ← chunk_ids 0–999 (3_000 files: 3 per chunk)
│   │   ├── 00000000-events.pack                  ← compressed event blocks
│   │   ├── 00000000-index.pack                   ← serialized roaring bitmaps
│   │   ├── 00000000-index.hash                   ← MPHF (Minimal Perfect Hash Function) for term → slot lookup
│   │   └── ...
│   └── .../
│
└── txhash/
    ├── raw/                                      ← IMMUTABLE_STORAGE.TXHASH_RAW.PATH
    │   ├── 00000/                                ← chunk_ids 0–999 (1_000 .bin files)
    │   │   ├── 00000000.bin                      ← TRANSIENT — deleted after RecSplit or by Phase 2 (.bin hydration)
    │   │   └── ...
    │   └── .../
    └── index/                                    ← IMMUTABLE_STORAGE.TXHASH_INDEX.PATH
        ├── 00000000/                             ← tx_index_id=0 (16 RecSplit CF files)
        │   └── cf-{0-f}.idx                      ← PERMANENT
        └── .../
```

`CHUNKS_PER_TXHASH_INDEX` only affects `txhash/index/` — all other trees use the hardcoded 1_000-chunk `bucket_id` grouping regardless.

Directory-count tradeoffs for a 2_000-chunk (20M-ledger) dataset:

| `CHUNKS_PER_TXHASH_INDEX` | Tx-index dirs | Tradeoff |
|---------------------------|---------------|----------|
| `1000` (default) | `2_000 / 1_000 = 2` | Fewer dirs, larger indexes — longer build time per index, fewer files to search at query time |
| `100` | `2_000 / 100 = 20` | More dirs, smaller indexes — faster build time per index, more files to search at query time |
| `1` | `2_000 / 1 = 2_000` | One index per chunk — fastest build, most files to search |

### Path Conventions

| File Type | Pattern | Example |
|-----------|---------|---------|
| Ledger pack | `{IMMUTABLE_STORAGE.LEDGERS.PATH}/{bucket_id:05d}/{chunk_id:08d}.pack` | `ledgers/00000/00000042.pack` |
| Raw txhash | `{IMMUTABLE_STORAGE.TXHASH_RAW.PATH}/{bucket_id:05d}/{chunk_id:08d}.bin` | `txhash/raw/00000/00000042.bin` |
| RecSplit CF | `{IMMUTABLE_STORAGE.TXHASH_INDEX.PATH}/{tx_index_id:08d}/cf-{nibble}.idx` | `txhash/index/00000000/cf-a.idx` |
| Events data | `{IMMUTABLE_STORAGE.EVENTS.PATH}/{bucket_id:05d}/{chunk_id:08d}-events.pack` | `events/00000/00000042-events.pack` |
| Events index | `{IMMUTABLE_STORAGE.EVENTS.PATH}/{bucket_id:05d}/{chunk_id:08d}-index.pack` | `events/00000/00000042-index.pack` |
| Events hash | `{IMMUTABLE_STORAGE.EVENTS.PATH}/{bucket_id:05d}/{chunk_id:08d}-index.hash` | `events/00000/00000042-index.hash` |

- **Nibble** = high 4 bits of `txhash[0]`, i.e., `txhash[0] >> 4`. Values `0`–`f`. Determines which of 16 CFs a txhash is routed to.
- **Raw txhash format**: 36 bytes per entry, no header: `[txhash: 32 bytes][ledger_seq: 4 bytes big-endian]`.
- **Events cold segment**: see [getEvents full-history design](https://github.com/stellar/stellar-rpc/pull/635) for the full format.

---

## Meta Store Keys

- Single RocksDB instance with WAL (Write-Ahead Log) always enabled.
- Authoritative source for crash recovery — all resume decisions derive from key presence.

### Key Schema

All IDs use uniform `%08d` zero-padding, matching the directory structure.

| Key Pattern | Value | Written When |
|-------------|-------|-------------|
| `chunk:{chunk_id:08d}:lfs` | `"1"` | After ledger `.pack` file is fsynced |
| `chunk:{chunk_id:08d}:txhash` | `"1"` | After raw txhash `.bin` file is fsynced |
| `chunk:{chunk_id:08d}:events` | `"1"` | After events cold segment files (`events.pack`, `index.pack`, `index.hash`) are fsynced |
| `index:{tx_index_id:08d}:txhash` | `"1"` | After all 16 RecSplit CF `.idx` files are built and fsynced |

- Values are `"1"` (retained for `ldb`/`sst_dump` readability); key presence is the signal.
- Key absence means not started or incomplete — treated identically on resume.
- Each chunk flag is written independently after its output's fsync — a crash may leave some flags set and others absent for the same chunk.
- On resume, each chunk's flags are checked independently — only missing outputs are produced.
- WAL is always enabled — disabling it would invalidate all crash recovery.
- `chunk:{chunk_id:08d}:txhash` keys are deleted after the tx index is built (the raw `.bin` files they reference are also deleted); all other flags are permanent within backfill's scope.

**Streaming's extension.** Streaming's prune path may transition `index:{tx_index_id:08d}:txhash` through an intermediate `"deleting"` value before clearing the key entirely. Backfill's `build_txhash_index` only ever writes `"1"`. See [02-streaming-workflow.md — Pruning](./02-streaming-workflow.md#pruning) for the prune mechanism.

**Examples:**
```
chunk:00000000:lfs      →  "1"     chunk_id=0 ledger pack done
chunk:00000000:txhash   →  "1"     chunk_id=0 raw txhash done
chunk:00000000:events   →  "1"     chunk_id=0 events cold segment done
chunk:00000999:events   →  "1"     last chunk of tx_index_id=0 (at cpi=1_000)
index:00000000:txhash   →  "1"     tx_index_id=0 RecSplit complete
index:00000001:txhash   →  absent  tx_index_id=1 not yet built
```

### Validation Rules

- `validate` checks argument sanity and defensively re-asserts `CHUNKS_PER_TXHASH_INDEX` against the meta store — the daemon's `validate_config` is the real enforcer; see [02-streaming-workflow.md — Validation Pseudocode](./02-streaming-workflow.md#validation-pseudocode).
- No source probe. `run_backfill` trusts the caller's range and fires the DAG. Per-chunk idempotency means already-done chunks are no-ops; source-coverage problems surface at runtime as task failures — see [Error Handling](#error-handling).
- `[BSB]` must be configured whenever `run_backfill` is invoked. Phase 1 (catchup) only calls `run_backfill` when `[BSB]` is present.
- DAG worker cap is `GOMAXPROCS`. BSB's `NUM_WORKERS` is a per-BSB internal download pool, not a cross-task concurrency knob.

### Partial Tx Index Ranges

When the caller's chunk range does not span a complete tx index, the trailing chunks have:

- Their raw `.bin` files on disk (inside `IMMUTABLE_STORAGE.TXHASH_RAW.PATH`).
- Their `chunk:{chunk_id:08d}:txhash` flags set in the meta store.
- No RecSplit `.idx` files (RecSplit is built only when every chunk of the tx index is ready).

These trailing artifacts persist on disk after `run_backfill` returns. Phase 2 (`.bin` hydration) of the RPC service loads them into the active txhash RocksDB store on startup and then deletes the `.bin` files and `chunk:{chunk_id:08d}:txhash` flags (see [02-streaming-workflow.md — Phase 2](./02-streaming-workflow.md#phase-2--hydrate-txhash-data-from-bin)).

Ledger and events data are useful per-chunk and are not blocked by tx-index alignment — `chunk:{chunk_id:08d}:lfs` and `chunk:{chunk_id:08d}:events` flags are set as soon as each chunk's outputs are durable.


### Key Lifecycle

```
chunk ingestion    → sets chunk:{chunk_id:08d}:lfs, chunk:{chunk_id:08d}:txhash, chunk:{chunk_id:08d}:events
                     (each independently, after its output's fsync)
tx index build     → sets index:{tx_index_id:08d}:txhash
txhash cleanup     → deletes chunk:{chunk_id:08d}:txhash keys + raw .bin files
```

After a completed tx index:
- `chunk:{chunk_id:08d}:lfs`, `chunk:{chunk_id:08d}:events`, `index:{tx_index_id:08d}:txhash` — permanent within backfill's scope.
- `chunk:{chunk_id:08d}:txhash` keys + raw `.bin` files — deleted after tx index is built.

---

## Tasks and Dependencies

The backfill DAG has three task types:

| Task | Cadence | Dependencies | Produces |
|------|---------|-------------|----------|
| `process_chunk(chunk_id, source)` | Per chunk (10_000 ledgers) | None | Ledger `.pack` + raw txhash `.bin` + events cold segment |
| `build_txhash_index(tx_index_id)` | Per tx index | All `process_chunk` tasks for this tx index | 16 RecSplit `.idx` files |
| `cleanup_txhash(tx_index_id)` | Per tx index | `build_txhash_index` for this tx index | Deletes raw `.bin` files + `chunk:{chunk_id:08d}:txhash` meta keys |

- Each task is a black box to the DAG scheduler — it calls `execute()` and waits for return.
- What happens inside (goroutines, I/O, parallelism) is up to the task.

### Dependency Diagram

For the chunks of one tx index (first chunk through last chunk):

```
process_chunk(chunk_id=first)   ─┐
process_chunk(chunk_id=first+1) ─┤
process_chunk(chunk_id=first+2) ─┼──→ build_txhash_index(tx_index_id) ──→ cleanup_txhash(tx_index_id)
...                              │
process_chunk(chunk_id=last)    ─┘
```

- All `process_chunk` tasks for a tx index must complete before `build_txhash_index` fires.
- `cleanup_txhash` runs after `build_txhash_index` succeeds.
- Cleanup deletes the raw `.bin` files and their `chunk:{chunk_id:08d}:txhash` meta keys.

### Main Flow

`run_backfill` is invoked by the daemon's [Phase 1 (catchup)](./02-streaming-workflow.md#phase-1--catchup) with an integer chunk range and a `make_bsb` partial:

```python
def run_backfill(config, range_start_chunk_id, range_end_chunk_id, make_bsb):
    # make_bsb is a partial (e.g. functools.partial(BSBSource, config.bsb)). Each call
    # returns a fresh BSBSource. Every process_chunk that needs to download ledgers
    # owns its own BSB for its chunk's range — no shared-source state across tasks.
    validate(config, range_start_chunk_id, range_end_chunk_id)

    dag = build_dag(config, range_start_chunk_id, range_end_chunk_id, make_bsb)
    dag.execute(max_workers=GOMAXPROCS)
```

### Validation

- Runs before DAG construction, not as a DAG task.
- If it were a task: no-dependency tasks would start concurrently; a validation failure would leave in-flight work to cancel.
- Running it first → clean abort, no partial work.

```python
def validate(config, range_start_chunk_id, range_end_chunk_id):
    # Argument sanity only. run_backfill trusts the caller's range — any source-coverage
    # issue (upper or lower bound) surfaces at runtime as a per-task get_ledger failure.
    assert range_start_chunk_id >= 0
    assert range_end_chunk_id >= range_start_chunk_id

    # Defensive re-assert; daemon's validate_config owns the enforcement.
    assert meta_store.get("config:chunks_per_txhash_index") == str(config.service.chunks_per_txhash_index)
```

### DAG Setup

```python
def build_dag(config, range_start_chunk_id, range_end_chunk_id, make_bsb):
    dag = new DAG()

    # Tx indexes whose LAST chunk is in range: schedule process_chunk for in-range chunks
    # only (prior chunks are already `:lfs`-flagged from a prior iteration) + build +
    # cleanup. build has every chunk's .bin when it runs.
    for tx_index_id in tx_indexes_ending_in_range(range_start_chunk_id, range_end_chunk_id, config):
        chunk_tasks = []
        for chunk_id in chunks_for_tx_index(tx_index_id, config):
            if not (range_start_chunk_id <= chunk_id <= range_end_chunk_id):
                continue
            t = dag.add(ProcessChunkTask(chunk_id, make_bsb=make_bsb), deps=[])
            chunk_tasks.append(t.id)
        b = dag.add(BuildTxHashIndexTask(tx_index_id), deps=chunk_tasks)
        dag.add(CleanupTxHashTask(tx_index_id), deps=[b.id])

    # Trailing partial tx index (last chunk past range_end): process_chunk only; a future
    # iteration that covers the missing trailing chunks will schedule the build.
    for chunk_id in trailing_partial_tx_index_chunks(range_start_chunk_id, range_end_chunk_id, config):
        dag.add(ProcessChunkTask(chunk_id, make_bsb=make_bsb), deps=[])

    return dag
```

- Trailing tx index whose last chunk is past `range_end_chunk_id`: `process_chunk` scheduled for in-range chunks only; no `build_txhash_index` / `cleanup_txhash`.
- `.bin` + `chunk:{chunk_id:08d}:txhash` flags persist until a future `run_backfill` covers the missing chunks OR [Phase 2 (`.bin` hydration)](./02-streaming-workflow.md#phase-2--hydrate-txhash-data-from-bin) hydrates them — see [Partial Tx Index Ranges](#partial-tx-index-ranges).

---

## Task Details

### process_chunk(chunk_id, make_bsb)

- Processes a single 10_000-ledger chunk end-to-end.
- Occupies one DAG worker slot.
- Only produces missing outputs — checks each flag independently.
- Internal concurrency is an implementation detail.

**Outputs** (all produced in a single task, only if missing):

- Ledger pack file (`{chunk_id:08d}.pack`) — compressed ledger data in [packfile format](https://github.com/stellar/stellar-rpc/pull/633).
- Raw txhash flat file (`{chunk_id:08d}.bin`) — 36-byte entries consumed by RecSplit builder.
- Events cold segment (`events.pack` + `index.pack` + `index.hash`) — per [getEvents design](https://github.com/stellar/stellar-rpc/pull/635).

**Pseudocode:**

```python
def process_chunk(chunk_id, make_bsb):
    first_ledger = first_ledger_in_chunk(chunk_id)
    last_ledger  = last_ledger_in_chunk(chunk_id)

    need_lfs    = not meta_store.has(f"chunk:{chunk_id:08d}:lfs")
    need_txhash = not meta_store.has(f"chunk:{chunk_id:08d}:txhash")
    need_events = not meta_store.has(f"chunk:{chunk_id:08d}:events")
    if not (need_lfs or need_txhash or need_events):
        return

    # If :lfs is already on disk, read from the local packfile — no BSB, no network.
    # Otherwise instantiate a per-task BSB scoped to THIS chunk's 10_000 ledgers.
    if need_lfs:
        ledger_reader = make_bsb()
        ledger_reader.prepare_range(first_ledger, last_ledger)
    else:
        ledger_reader = local_packfile(ledger_pack_path(chunk_id))

    ledger_writer = packfile.create(ledger_pack_path(chunk_id),          overwrite=True) if need_lfs    else None
    txhash_writer = open(raw_txhash_path(chunk_id),                      overwrite=True) if need_txhash else None
    events_writer = events_segment.create(events_segment_path(chunk_id), overwrite=True) if need_events else None

    try:
        for ledger_seq in range(first_ledger, last_ledger + 1):
            lcm = ledger_reader.get_ledger(ledger_seq)
            if need_lfs:    ledger_writer.append(compress(lcm))
            if need_txhash: txhash_writer.append(extract_txhashes(lcm))   # 36 bytes per tx
            if need_events: events_writer.append(extract_events(lcm))

        # Fsync + flag each output independently (flag-after-fsync).
        if need_lfs:
            ledger_writer.fsync_and_close()
            meta_store.put(f"chunk:{chunk_id:08d}:lfs", "1")
        if need_txhash:
            txhash_writer.fsync_and_close()
            meta_store.put(f"chunk:{chunk_id:08d}:txhash", "1")
        if need_events:
            events_writer.finalize()
            meta_store.put(f"chunk:{chunk_id:08d}:events", "1")
    finally:
        ledger_reader.close()   # BSB: tears down the per-task instance. Local packfile: closes file handle.
```

Key properties:

- Only missing outputs are produced — a partially-completed chunk resumes from where it left off.
- If the ledger pack file (`:lfs` flag) is already present, reads from local NVMe instead of the source (avoids redundant download).
- Each flag is written independently after its output's fsync — no atomic WriteBatch needed.
- `packfile.create()` with `overwrite=True` handles truncation of partial files from prior crashes — no explicit `delete_if_exists` check needed.
- Naturally extends to new data types (add a fourth flag).

**Source concurrency.**
- Each `process_chunk` owns its own BSB instance; DAG dispatches up to `GOMAXPROCS` tasks in parallel.
- BSB's internal `NUM_WORKERS` is the per-instance download pool — not a cross-task concurrency knob. `6_000` chunks in the run means `6_000` independent BSB instances over the run's lifetime, up to `GOMAXPROCS` alive at any moment.
- Interface: see [02-streaming-workflow.md — Ledger Source](./02-streaming-workflow.md#ledger-source).

### build_txhash_index(tx_index_id)

- Builds the RecSplit index for one completed tx index.
- Occupies one DAG worker slot, but spawns several goroutines internally.
- The DAG guarantees all chunk `.bin` files exist before this runs.

**Pseudocode:**

```python
def build_txhash_index(tx_index_id):
    if meta_store.has(f"index:{tx_index_id:08d}:txhash"):
        return

    # All-or-nothing recovery: absent flag ⇒ any .idx on disk is from a crashed attempt.
    delete_partial_idx_files(recsplit_index_path(tx_index_id))

    # Invariant: every chunk's .bin is on disk when this runs. Prior-iteration chunks
    # keep their .bin until cleanup_txhash runs; cleanup is DAG-gated on this build.
    bin_files = list_bin_files(tx_index_id)

    # Stage 1 (COUNT) — two passes total over .bin; count entries per CF.
    cf_counts = parallel_count(bin_files, workers=100)

    # Stage 2 (ADD) — route entries into 16 per-CF builders (nibble = txhash[0] >> 4).
    cf_builders = [RecSplitBuilder(cf_counts[nibble]) for nibble in range(16)]
    parallel_add(bin_files, cf_builders, workers=100)

    # Stage 3 (BUILD) — 16 parallel CF builds; each produces one .idx; all fsynced.
    parallel_build(cf_builders, workers=16)

    # Stage 4 (VERIFY) — full verification; no wall-clock pressure at backfill time.
    parallel_verify(bin_files, cf_builders, workers=100)

    # Backfill writes "1" only; streaming's prune path may transition through "deleting".
    meta_store.put(f"index:{tx_index_id:08d}:txhash", "1")
```

Key properties:

- COUNT and ADD each read all `.bin` files (two full passes over the data).
- BUILD runs 16 goroutines in parallel (one per CF) — each CF is independent.
- VERIFY always runs (there is no `--verify-recsplit=false` escape hatch — backfill trades throughput for correctness every time).
- All-or-nothing recovery: if `index:{tx_index_id:08d}:txhash` is absent on restart → delete partial `.idx` files → rerun entire build.

### cleanup_txhash(tx_index_id)

- Runs after `build_txhash_index` completes successfully.

**Pseudocode:**

```python
def cleanup_txhash(tx_index_id):
    for chunk_id in chunks_for_tx_index(tx_index_id, config):
        if not meta_store.has(f"chunk:{chunk_id:08d}:txhash"):
            continue
        delete_if_exists(raw_txhash_path(chunk_id))   # idempotent; crash-between is safe
        meta_store.delete(f"chunk:{chunk_id:08d}:txhash")
```

Key properties:

- Modeled as a separate DAG task (not inline in `build_txhash_index`) so crash recovery works naturally.
- Per-chunk idempotency: each chunk checks its own `chunk:{chunk_id:08d}:txhash` key before deleting — a crash mid-cleanup resumes from where cleanup left off.
- On restart: DAG sees the tx-index key present (build complete) but `chunk:{chunk_id:08d}:txhash` keys still exist → cleanup runs as a normal task.

---

## Execution Model

### DAG Scheduler

- The subroutine builds a single DAG per invocation and executes it with bounded concurrency.
- The DAG is the only scheduling mechanism — no per-tx-index coordinators, no secondary worker pools.
- Each task's `execute()` is wrapped with a retry loop bounded by `MAX_RETRIES` (implementation-defined constant). Any transient failure (BSB errors, temporary I/O issues) triggers a retry at the task level.

```python
def run_dag(dag, max_workers):
    worker_slots   = Semaphore(max_workers)
    runnable_tasks = ThreadSafeQueue(dag.tasks_with_no_pending_dependencies())

    def execute_task(task):
        for attempt in range(1, MAX_RETRIES + 1):
            error = task.execute()
            if error is None:
                break
            if attempt == MAX_RETRIES:
                mark_failed(task, error)   # halt dependents
                break
            log.warn("retry", task, attempt, error)
        worker_slots.release()

        for downstream_task in dag.dependents_of(task):
            downstream_task.mark_dependency_done(task)
            if downstream_task.all_dependencies_done():
                runnable_tasks.push(downstream_task)

    while runnable_tasks:
        current_task = runnable_tasks.pop()
        worker_slots.acquire()
        run_in_background(execute_task, current_task)
```

### Worker Pool

- Single flat pool of `max_workers = GOMAXPROCS` slots.
- Any mix of task types can occupy slots simultaneously.
- `process_chunk`: 1 slot per task.
- `build_txhash_index`: 1 slot per task (uses many goroutines internally).
- `cleanup_txhash`: 1 slot per task.

---

## Crash Recovery

No separate reconciliation phase — every task's `execute()` checks its own completion state:

- `build_dag()` registers ALL tasks for the chunk range on every invocation; no meta-store scanning in setup.
- `process_chunk` checks each output flag independently — missing produced, existing skipped.
- `build_txhash_index` checks `index:{tx_index_id:08d}:txhash` — present → early return; absent → delete partial `.idx` files, rerun full build.
- `cleanup_txhash` checks `chunk:{chunk_id:08d}:txhash` per-chunk — cleaned skipped, remaining cleaned.

Three invariants make this work:

1. **Key implies durable file** — a meta store flag is set only after fsync.
2. **Tasks are idempotent** — each checks its own outputs and skips or overwrites what exists.
3. **DAG registers all tasks on every invocation** — completed tasks return immediately from `execute()`.

### Concurrent Access Prevention

The daemon acquires a directory flock on the meta-store at startup. A second process against the same datadir fails immediately.

---

## Error Handling

Two layers of retry:

- **BSB-internal retries.** `BSBSource` handles transient errors (connection resets, throttling) inside a single task execution. Invisible to the DAG.
- **Task-level retries.** DAG wraps each task's `execute()` in a retry loop bounded by `MAX_RETRIES`.
  - Source retries exhausted → task retries whole.
  - `MAX_RETRIES` exhausted → task marked failed → DAG halts dependents → `run_backfill` returns fatal → [Phase 1 (catchup)](./02-streaming-workflow.md#phase-1--catchup) propagates → daemon exits non-zero.
  - Operator fixes root cause + restarts → Phase 1 (catchup) re-enters → `run_backfill` re-invoked with a fresh range → completed work skipped via per-chunk idempotency.

| Error | Handled by | Action |
|-------|-----------|--------|
| BSB transient error (throttle, connection reset) | BSB-internal retry | Retried within the task; transparent to DAG |
| BSB persistent error (BSB retries exhausted) | Task-level retry | `MAX_RETRIES` attempts; then ABORT |
| Ledger pack write / fsync failure | Task-level retry | `MAX_RETRIES` attempts; then ABORT; flag not set |
| Txhash write / fsync failure | Task-level retry | `MAX_RETRIES` attempts; then ABORT; flag not set |
| Events write / fsync failure | Task-level retry | `MAX_RETRIES` attempts; then ABORT; flag not set |
| RecSplit build failure | Task-level retry | `MAX_RETRIES` attempts; then ABORT; tx-index key absent |
| VERIFY stage mismatch | None | ABORT immediately — data corruption; operator investigates |
| Meta store write failure | None | ABORT immediately — treat as crash; operator re-runs daemon |
