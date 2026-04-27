# Backfill Workflow

## Overview

Backfill is the RPC service's historical-ingestion subroutine — it pulls ledgers from a configured remote object store (GCS or S3) and writes them as **immutable, query-ready artifacts** on local disk. 
It runs once per service start, as part of Phase 1 (catchup), to close the gap between on-disk state and the current network tip before live ingestion takes over. Interruption at any point leaves recoverable state; on restart, already-complete work is skipped.

**What it produces:**

Three immutable artifact types, one per full-history RPC query, scoped to **chunks** (10_000-ledger blocks) and **tx indexes** (consecutive chunks, default 1_000 = 10_000_000 ledgers each — see [Geometry](#geometry)).

| Immutable output | Query it enables | Scope |
|-----------------|-----------------|-------|
| Ledger [pack file](../../design-docs/packfile-library.md) | `getLedger` | Per chunk (10_000 ledgers) |
| Tx-index files | `getTransaction` | Per tx index (default 10_000_000 ledgers) |
| [Events cold segment](../../design-docs/getevents-full-history-design.md) | `getEvents` | Per chunk |

**How it does it:**

- Backfill is a subroutine invoked by the RPC service's **Phase 1 (catchup)** — see [02-streaming-workflow.md — Phase 1](./02-streaming-workflow.md#phase-1--catchup). Internal to the service; no `full-history-backfill` subcommand, no per-run flags.
- Ledger source is **BSB** (Buffered Storage Backend) — a remote object-store reader for `LedgerCloseMeta`, configured under `[BSB]` in the TOML config. Interface details in [02-streaming-workflow.md — Ledger Source](./02-streaming-workflow.md#ledger-source).
- Ingests historical ledgers one chunk at a time. Each chunk uses its own BSB reader scoped to that chunk's 10_000 ledgers; no shared source state across chunks.
- Writes directly to immutable file formats — no RocksDB active stores (mutable RocksDB instances holding in-flight live-ingestion data; streaming's concern, see [02-streaming-workflow.md — Active Store Architecture](./02-streaming-workflow.md#active-store-architecture)).
- Tracks per-chunk and per-tx-index completion in a small **meta store** — a dedicated RocksDB with WAL always on, separate from streaming's active stores. Each flag is written after its artifact's `fsync`, and flag presence drives all resume decisions.
- Schedules work as a DAG of idempotent tasks dispatched via a bounded worker pool.
- Returns when every chunk in the range is complete; on crash, Phase 1 (catchup) re-invokes with the same range and already-complete chunks are skipped via per-chunk idempotency primitives.
- **BSB-only.** 
  - Backfill does not use captive core (the embedded `stellar-core` subprocess that the service runs during live ingestion) as a ledger source.
  - Captive core belongs to Phase 4 (live ingestion)
  - if BSB isn't configured, backfill is not invoked at all and Phase 4 (live ingestion)'s captive core catches up from a leapfrog'd resume ledger — a start ledger chosen forward of genesis so ingestion stays within the retention window — as part of normal startup. See [02-streaming-workflow.md — Phase 4](./02-streaming-workflow.md#phase-4--live-ingestion) and [Ledger Source](./02-streaming-workflow.md#ledger-source).

For the distinction between **backfill (this subroutine)** and **Phase 1 (catchup) (the startup phase that invokes backfill)** — two terms that get conflated because their scopes overlap, refer [02-streaming-workflow.md — Backfill vs Phase 1 (catchup)](./02-streaming-workflow.md#backfill-vs-phase-1-catchup).

---

## Geometry

Stellar's first ledger is `GENESIS_LEDGER = 2`. Mapping functions subtract it to zero-base the `ledger_seq ↔ chunk_id` axis.

```python
GENESIS_LEDGER          = 2
LEDGERS_PER_CHUNK       = 10_000 # hardcoded; not configurable
CHUNKS_PER_TX_INDEX     = 1_000  # read from config, immutable after first run. Acceptable values - 1, 10, 100, 1_000; default 1_000
LEDGERS_PER_TX_INDEX    = CHUNKS_PER_TX_INDEX * LEDGERS_PER_CHUNK   # at cpi=1_000 this is 10_000_000
```

- In pseudocode, `cpi` in inline comments is shorthand for `CHUNKS_PER_TX_INDEX`.
- All IDs use uniform `%08d` zero-padding (supports up to `99_999_999`).

---

## Configuration
Backfill reads the subset of the unified TOML config described below.
_Service-level keys, used by the streaming flow, are specified in [02-streaming-workflow.md — Configuration](./02-streaming-workflow.md#configuration)._

### TOML Config

**[SERVICE]**

| Key                                  | Type | Default | Description |
|--------------------------------------|------|---------|-------------|
| `DEFAULT_DATA_DIR`                   | string | **required** | Base directory for meta store and default storage paths. |
| `CHUNKS_PER_TX_INDEX` (optional) | int | `1000` | Chunks per tx index. Defines data layout; stored in the meta store on first run and fatal if changed on any subsequent run. |

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

**[BSB]** — Buffered Storage Backend (optional at the service level; required when [Phase 1 (catchup)](./02-streaming-workflow.md#phase-1--catchup) selects `BSBSource`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `BUCKET_PATH` | string | **required** | Remote object store path to fetch LedgerCloseMeta (without `gs://` prefix for GCS). |
| `BUFFER_SIZE` | int | `1000` | Prefetch buffer depth per connection. |
| `NUM_WORKERS` | int | `20` | Download workers per connection. |

- `[BSB]` is effectively required when backfill runs. If absent, Phase 1 (catchup) does not invoke `run_backfill` at all — Phase 4's captive core handles initial catchup instead (see [02-streaming-workflow.md — Ledger Source](./02-streaming-workflow.md#ledger-source)).

**[LOGGING]** (optional)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `LEVEL` | string | `"info"` | Minimum log severity. Accepted values: `debug` / `info` / `warn` / `error`. Service CLI flag `--log-level` wins when both are set. |
| `FORMAT` | string | `"text"` | Log output format. Accepted values: `text` / `json`. Service CLI flag `--log-format` wins when both are set. |

**[META_STORE]** (optional)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `PATH` | string | `{DEFAULT_DATA_DIR}/meta/rocksdb` | Meta store RocksDB directory. |

### Example TOML

```toml
[SERVICE]
DEFAULT_DATA_DIR = "/data/stellar-rpc"
CHUNKS_PER_TX_INDEX = 1000

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
│   │   ├── 00000000.pack                         ← ledger pack file for chunk_id=0 (ledgers 2–10_001)
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

`CHUNKS_PER_TX_INDEX` only affects `txhash/index/` — all other trees use the hardcoded 1_000-chunk `bucket_id` grouping regardless.

Directory-count tradeoffs for a 2_000-chunk (20M-ledger) dataset:

| `CHUNKS_PER_TX_INDEX` | Tx-index dirs | Tradeoff |
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
- **Events cold segment**: see [getEvents full-history design](../../design-docs/getevents-full-history-design.md) for the full format.

---

## Meta Store Keys

*This section is a reference for the key schema and lifecycle. It reads more naturally after [How Backfill Runs](#how-backfill-runs) below, which defines the tasks that write and consume these keys.*

- Single RocksDB instance with WAL (Write-Ahead Log) always enabled.
- Authoritative for everything backfill decides: which chunks and tx indexes are done (progress tracker), which config values can't change across runs (e.g., `CHUNKS_PER_TX_INDEX`, stored on first run and fatal if changed), and where to resume after a crash (every resume decision derives from key presence).

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

**Examples:**
```
chunk:00000000:lfs      →  "1"     chunk_id=0 ledger pack done
chunk:00000000:txhash   →  "1"     chunk_id=0 raw txhash done
chunk:00000000:events   →  "1"     chunk_id=0 events cold segment done
chunk:00000999:events   →  "1"     last chunk of tx_index_id=0 (at cpi=1_000)
index:00000000:txhash   →  "1"     tx_index_id=0 RecSplit complete
index:00000001:txhash   →  absent  tx_index_id=1 not yet built
```

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

## How Backfill Runs

Backfill's work is a static DAG. The `run_backfill` orchestrator validates the caller's range, builds the DAG over that range, and dispatches it with a bounded-concurrency worker pool (`MAX_CPU_THREADS`-capped). Each task is idempotent and checks its own completion state — the scheduler is just the dispatcher.

### Task Types and Dependencies

The backfill DAG has three task types:

| Task | Cadence | Dependencies | Produces |
|------|---------|-------------|----------|
| `process_chunk(chunk_id, source)` | Per chunk (10_000 ledgers) | None | Ledger `.pack` + raw txhash `.bin` + events cold segment |
| `build_txhash_index(tx_index_id)` | Per tx index | All `process_chunk` tasks for this tx index | 16 RecSplit `.idx` files |
| `cleanup_txhash(tx_index_id)` | Per tx index | `build_txhash_index` for this tx index | Deletes raw `.bin` files + `chunk:{chunk_id:08d}:txhash` meta keys |

- Each task is a black box to the DAG scheduler — it calls `execute()` and waits for return.
- What happens inside (concurrency, I/O, parallelism) is up to the task.

For the chunks of a single tx index (first chunk through last chunk, inclusive), the dependencies look like this:

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

`run_backfill` is invoked by the service's [Phase 1 (catchup)](./02-streaming-workflow.md#phase-1--catchup) with an integer chunk range:

```python
def run_backfill(config, range_start_chunk_id, range_end_chunk_id):
    validate(range_start_chunk_id, range_end_chunk_id)

    dag = build_dag(config, range_start_chunk_id, range_end_chunk_id)
    dag.execute(max_workers=MAX_CPU_THREADS)
```

### Pre-DAG Validation

- Validation runs in two layers, both pre-DAG:
  1. **Service startup** (`validate_config`) — runs once per process start, before any backfill is invoked. Authoritative enforcer of config-immutability: `CHUNKS_PER_TX_INDEX` (and `RETENTION_LEDGERS`) cannot change across runs. Defined in [02-streaming-workflow.md — Validation Pseudocode](./02-streaming-workflow.md#validation-pseudocode).
  2. **Per `run_backfill` call** - `validate` runs before DAG construction. Argument sanity only (chunk-range bounds).
- Why pre-DAG and not a DAG task: no-dependency tasks would start concurrently; a validation failure would leave in-flight work to cancel. Pre-DAG = clean abort, no partial work.
- No source probe. `run_backfill` trusts the caller's range; source-coverage problems surface at runtime as task failures — see [Error Handling](#error-handling).
- `[BSB]` must be configured. Phase 1 (catchup) only calls `run_backfill` when `[BSB]` is present.

```python
def validate(range_start_chunk_id, range_end_chunk_id):
    # Argument sanity only. run_backfill trusts the caller's range — any source-coverage
    # issue (upper or lower bound) surfaces at runtime as a per-task get_ledger failure.
    assert range_start_chunk_id >= 0
    assert range_end_chunk_id >= range_start_chunk_id
```

### DAG Setup

```python
def build_dag(config, range_start_chunk_id, range_end_chunk_id):
    # Invariant: range_start_chunk_id is always tx-index-aligned 
    # Phase 1 (catchup) is the only caller of this function, and it aligns the start chunk ID to the nearest tx index boundary, which is why validate() doesn't check for that. 
    # This means the first tx index in the range is always fully covered by the chunk range, and thus always buildable.  
    # A partial-at-end (trailing partial) is normal: BSB-tip lands wherever network production is, mid-index is typical.

    dag = new DAG()
    first_index = tx_index_id_of_chunk(range_start_chunk_id)
    last_index  = tx_index_id_of_chunk(range_end_chunk_id)

    for tx_index_id in range(first_index, last_index + 1):
        chunk_tasks = []
        for chunk_id in chunks_for_tx_index(tx_index_id):
            if range_start_chunk_id <= chunk_id <= range_end_chunk_id:
                t = dag.add(ProcessChunkTask(chunk_id, config), deps=[])
                chunk_tasks.append(t.id)

        # If the tx_index is fully covered (i.e., last chunk ≤ range_end),
        # schedule build_txhash_index + cleanup_txhash.
        # Otherwise the tx_index is the trailing partial — its last chunk isn't available for
        # consumption yet — and only the process_chunk tasks above run; 
        # tx-build is skipped in that case.
        if last_chunk_in_tx_index(tx_index_id) <= range_end_chunk_id:
            build_task = dag.add(BuildTxHashIndexTask(tx_index_id), deps=chunk_tasks)
            dag.add(CleanupTxHashTask(tx_index_id), deps=[build_task.id])

    return dag
```

**Examples:**

- `input chunk range = [0, 5_999]`, `cpi = 1_000`, starting chunk is 0 - already tx-index aligned → tx_indexes 0..5 fully covered and created; no trailing partial.
- `input chunk range = [3_000, 6_100]`, `cpi = 1_000`, starting chunk is 3 - already tx-index aligned → tx_indexes 3..5 fully covered; tx_index 6 trailing partial with only chunk 6_000 created; `build_txhash_index` skipped for tx_index 6. 

**Trailing partial tx-index:**

- On disk: chunks have `.bin` files + `:lfs` + `:events` + `:txhash` flags; `index:{tx_index_id:08d}:txhash` absent.
- Ledger and events data are not blocked by tx-index alignment — their flags land as each chunk's outputs are durable.
- The deferred build runs later: via a subsequent `run_backfill` call when BSB covers the tail, or via streaming's live-ingestion path — see [02-streaming-workflow.md — Phase 2 (`.bin` hydration)](./02-streaming-workflow.md#phase-2--hydrate-txhash-data-from-bin) and [02-streaming-workflow.md — RecSplit Transition](./02-streaming-workflow.md#recsplit-transition).

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

- Single flat pool of `max_workers = MAX_CPU_THREADS` slots.
- Any mix of task types can occupy slots simultaneously.
- `process_chunk`: 1 slot per task.
- `build_txhash_index`: 1 slot per task (uses internal parallelism across many concurrent workers).
- `cleanup_txhash`: 1 slot per task.
- BSB's `NUM_WORKERS` is a per-BSB internal download pool, not a cross-task concurrency knob.

---

### `process_chunk`

- Processes a single 10_000-ledger chunk end-to-end.
- Idempotent at flag granularity — produces only outputs whose flag is missing; a partially-completed chunk resumes from where it left off.

**Outputs** (each only if its flag is missing):

- Ledger pack file (`{chunk_id:08d}.pack`) — see [packfile format](../../design-docs/packfile-library.md).
- Raw txhash flat file (`{chunk_id:08d}.bin`) — 36-byte entries (`txhash[32]` + `ledgerSeq[4]`) consumed by the RecSplit builder.
- Events cold segment (`events.pack` + `index.pack` + `index.hash`) — see [getEvents design](../../design-docs/getevents-full-history-design.md).

**Pseudocode:**

```python
def process_chunk(chunk_id, config):
    first_ledger = first_ledger_in_chunk(chunk_id)
    last_ledger  = last_ledger_in_chunk(chunk_id)

    need_lfs    = not meta_store.has(f"chunk:{chunk_id:08d}:lfs")
    need_txhash = not meta_store.has(f"chunk:{chunk_id:08d}:txhash")
    need_events = not meta_store.has(f"chunk:{chunk_id:08d}:events")
    if not (need_lfs or need_txhash or need_events):
        return

    # If :lfs is already on disk, read from the local packfile — no need to use BSB.
    # Otherwise instantiate a per-task BSB scoped to THIS chunk's 10_000 ledgers.
    if need_lfs:
        ledger_reader = BSBSource(config.bsb)
        ledger_reader.prepare_range(first_ledger, last_ledger)
    else:
        ledger_reader = local_packfile(ledger_pack_path(chunk_id))

    ledger_writer = packfile.create(ledger_pack_path(chunk_id), overwrite=True) if need_lfs else None
    txhash_writer = open(raw_txhash_path(chunk_id), overwrite=True) if need_txhash else None
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

**Notes:**

- If `:lfs` is set (pack file already on disk), reads from local NVMe instead of BSB — avoids redundant downloads on restart.
- Each flag is written independently after its output's `fsync`; no atomic WriteBatch needed.
- `packfile.create(..., overwrite=True)` handles truncation of partial files from prior crashes; no explicit cleanup before write.
- Each `process_chunk` owns its own BSB instance, scoped to the chunk's 10_000 ledgers and torn down at task exit. Cross-task concurrency cap is the DAG [Worker Pool](#worker-pool); the BSB interface is documented in [02-streaming-workflow.md — Ledger Source](./02-streaming-workflow.md#ledger-source).
- Adding a new data type = adding a fourth flag + writer; no other task changes.

---

### `build_txhash_index`

- Builds the RecSplit index for one completed tx index.
- Occupies one DAG worker slot but spawns multiple concurrent workers internally (per-stage worker counts are in the pseudocode).

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

**Notes:**

- VERIFY always runs — no `--verify-recsplit=false` escape hatch; backfill trades throughput for correctness every time.
- All-or-nothing recovery on restart: absent `index:{tx_index_id:08d}:txhash` ⇒ delete partial `.idx` files and re-run the full build.

---

### `cleanup_txhash`

- Runs after `build_txhash_index` completes successfully. Modeled as a separate DAG task (not inline in `build_txhash_index`) so crash recovery falls out naturally — on restart, the DAG sees the tx-index flag set but per-chunk `:txhash` flags still present, and cleanup re-runs as a normal task.

**Pseudocode:**

```python
def cleanup_txhash(tx_index_id):
    # File-before-flag-delete on every cleanup pair (see 02-streaming-workflow.md — Flag Semantics).
    # On any crash mid-pair, the flag is the recovery signal — never an orphan file with no record.
    for chunk_id in chunks_for_tx_index(tx_index_id):
        if not meta_store.has(f"chunk:{chunk_id:08d}:txhash"):
            continue
        delete_if_exists(raw_txhash_path(chunk_id))
        meta_store.delete(f"chunk:{chunk_id:08d}:txhash")
```

**Notes:**

- Per-chunk idempotency: each chunk checks its own `chunk:{chunk_id:08d}:txhash` flag before deleting; a crash mid-cleanup resumes safely.

---

## Resilience

Crash recovery and error handling share one foundation: flag-after-fsync makes the meta store authoritative, and every task checks its own flags before doing work. Transient failures retry at BSB-internal and task-level layers; persistent failures abort the run, and on restart already-complete work is skipped.

### Crash Recovery

No separate reconciliation phase — every task's `execute()` checks its own completion state:

- `build_dag()` registers ALL tasks for the chunk range on every invocation; no meta-store scanning in setup.
- `process_chunk` checks each output flag independently — missing output is produced; existing output is skipped.
- `build_txhash_index` checks `index:{tx_index_id:08d}:txhash` — present → early return; absent → delete partial `.idx` files, rerun full build.
- `cleanup_txhash` checks `chunk:{chunk_id:08d}:txhash` per-chunk — cleaned skipped, remaining cleaned.

Three invariants make this work:

1. **Key implies durable file** — a meta store flag is set only after fsync.
2. **Tasks are idempotent** — each checks its own outputs and skips or overwrites what exists.
3. **DAG registers all tasks on every invocation** — completed tasks return immediately from `execute()`.

### Concurrent Access Prevention

The service acquires a directory flock on the meta-store at startup. A second process against the same datadir fails immediately.

### Error Handling

Two layers of retry:

- **BSB-internal retries.** `BSBSource` handles transient errors (connection resets, throttling) inside a single task execution. Invisible to the DAG.
- **Task-level retries.** DAG wraps each task's `execute()` in a retry loop bounded by `MAX_RETRIES`.
  - Source retries exhausted → task retries whole.
  - `MAX_RETRIES` exhausted → task marked failed → DAG halts dependents → `run_backfill` returns fatal → [Phase 1 (catchup)](./02-streaming-workflow.md#phase-1--catchup) propagates error to the service → service exits non-zero.
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
| Meta store write failure | None | ABORT immediately — treat as crash; operator re-runs service |
