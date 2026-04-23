# Backfill Workflow

## Overview

Backfill is a subroutine invoked by Phase 1 of the streaming daemon (see [02-streaming-workflow.md](./02-streaming-workflow.md)). Given an integer chunk range `[range_start_chunk_id, range_end_chunk_id]` and a `LedgerSource`, it produces the immutable output files for those chunks via a static DAG of idempotent per-chunk tasks.

**Not an operator CLI.** The daemon is the single operator entry point (`stellar-rpc --config path/to/config.toml`); backfill has no `full-history-backfill` subcommand and no per-run CLI flags.

**What it does:**
- Ingests historical ledgers via the `LedgerSource` passed in by the caller — BSB or captive core (see [02-streaming-workflow.md — Ledger Source](./02-streaming-workflow.md#ledger-source)).
- Writes directly to immutable file formats — no RocksDB active stores.
- Schedules work as a DAG of idempotent tasks dispatched via a flat worker pool.
- Returns when every chunk in the range is complete; on crash, Phase 1 re-invokes with the same range and already-complete chunks are skipped via per-chunk idempotency.

**What it produces:**

| Query it enables | Immutable output | Scope |
|-----------------|-----------------|-------|
| `getLedger` | Ledger [pack file](https://github.com/stellar/stellar-rpc/pull/633) | Per chunk (10_000 ledgers) |
| `getTransaction` | Txhash index files | Per tx index (default 10_000_000 ledgers) |
| `getEvents` | [Events cold segment](https://github.com/stellar/stellar-rpc/pull/635) | Per chunk |

---

## Geometry

Stellar's first ledger is `GENESIS_LEDGER = 2` (not 0 or 1). Every formula that maps `ledger_seq ↔ chunk_id` subtracts `GENESIS_LEDGER` to zero-base the axis. In the pseudocode below, `cpi` in inline comments is shorthand for `CHUNKS_PER_TXHASH_INDEX`.

```python
GENESIS_LEDGER          = 2
LEDGERS_PER_CHUNK       = 10_000                                    # hardcoded; not configurable
CHUNKS_PER_TXHASH_INDEX = <from config, immutable after first run>  # 1 / 10 / 100 / 1_000; default 1_000
LEDGERS_PER_INDEX       = CHUNKS_PER_TXHASH_INDEX * LEDGERS_PER_CHUNK
                          # at cpi=1_000 this is 10_000_000

chunk_id_of_ledger(ledger_seq)  = (ledger_seq - GENESIS_LEDGER) // LEDGERS_PER_CHUNK
                          # 56_342_637 → (56_342_637 - 2) // 10_000 = 5_634

first_ledger_in_chunk(chunk_id) = (chunk_id * LEDGERS_PER_CHUNK) + GENESIS_LEDGER
                          # chunk_id=5_634 → (5_634 * 10_000) + 2 = 56_340_002 — ends in ..._02

last_ledger_in_chunk(chunk_id)  = ((chunk_id + 1) * LEDGERS_PER_CHUNK) + (GENESIS_LEDGER - 1)
                          # chunk_id=5_634 → ((5_635) * 10_000) + 1 = 56_350_001 — ends in ..._01

tx_index_id_of_chunk(chunk_id)  = chunk_id // CHUNKS_PER_TXHASH_INDEX
                          # chunk_id=5_634 → tx_index_id=5 (at cpi=1_000)

first_ledger_in_tx_index(tx_index_id) = (tx_index_id * LEDGERS_PER_INDEX) + GENESIS_LEDGER
                          # tx_index_id=5 → (5 * 10_000_000) + 2 = 50_000_002

last_ledger_in_tx_index(tx_index_id)  = ((tx_index_id + 1) * LEDGERS_PER_INDEX) + (GENESIS_LEDGER - 1)
                          # tx_index_id=5 → ((6) * 10_000_000) + 1 = 60_000_001
```

Example rows at `CHUNKS_PER_TXHASH_INDEX = 1000` (default):

| `tx_index_id` | First Ledger | Last Ledger | Chunks |
|---|---|---|---|
| `0` | `2` | `10_000_001` | `0 – 999` |
| `1` | `10_000_002` | `20_000_001` | `1_000 – 1_999` |
| `2` | `20_000_002` | `30_000_001` | `2_000 – 2_999` |

All IDs use uniform `%08d` zero-padding (supports up to `99_999_999`).

---

## Configuration

The streaming daemon loads a single TOML file; backfill reads the subset documented here. Streaming-only sections (`[STREAMING]`, `[HISTORY_ARCHIVES]`) are in [02-streaming-workflow.md — Configuration](./02-streaming-workflow.md#configuration).

### TOML Config

**[SERVICE]**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `DEFAULT_DATA_DIR` | string | **required** | Base directory for meta store and default storage paths. |

**[BACKFILL]**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `CHUNKS_PER_TXHASH_INDEX` | int | `1000` | Chunks per tx index. Defines data layout; stored in the meta store on first run and fatal if changed on any subsequent run. |

**[IMMUTABLE_STORAGE.LEDGERS]**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `PATH` | string | `{DEFAULT_DATA_DIR}/ledgers` | Base path for ledger pack files. |

**[IMMUTABLE_STORAGE.EVENTS]**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `PATH` | string | `{DEFAULT_DATA_DIR}/events` | Base path for events cold segments. |

**[IMMUTABLE_STORAGE.TXHASH_RAW]**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `PATH` | string | `{DEFAULT_DATA_DIR}/txhash/raw` | Base path for raw txhash `.bin` files (transient). |

**[IMMUTABLE_STORAGE.TXHASH_INDEX]**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `PATH` | string | `{DEFAULT_DATA_DIR}/txhash/index` | Base path for RecSplit index files (permanent). |

The `IMMUTABLE_STORAGE` prefix disambiguates from `ACTIVE_STORAGE` (RocksDB-backed mutable stores owned by the streaming workflow).

**[BACKFILL.BSB]** — BSB / Buffered Storage Backend (optional at the daemon level; required when Phase 1 selects `BSBSource`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `BUCKET_PATH` | string | **required** | Remote object store path to fetch LedgerCloseMeta (without `gs://` prefix for GCS). |
| `BUFFER_SIZE` | int | `1000` | Prefetch buffer depth per connection. |
| `NUM_WORKERS` | int | `20` | Download workers per connection. |

Source selection at the daemon level (BSB vs captive core, based on `[BACKFILL.BSB]` presence) is described in [02-streaming-workflow.md — Ledger Source](./02-streaming-workflow.md#ledger-source). When the caller invokes `run_backfill(..., source=CaptiveCoreSource(...))`, this section is not used.

**[LOGGING]**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `LEVEL` | string | `"info"` | Minimum log severity. Accepted values: `debug` / `info` / `warn` / `error`. Daemon CLI flag `--log-level` wins when both are set. |
| `FORMAT` | string | `"text"` | Log output format. Accepted values: `text` / `json`. Daemon CLI flag `--log-format` wins when both are set. |

**[META_STORE]**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `PATH` | string | `{DEFAULT_DATA_DIR}/meta/rocksdb` | Meta store RocksDB directory. |

### Validation Rules

- `CHUNKS_PER_TXHASH_INDEX` must not change after the first run — the daemon's `validate_config` enforces this at startup (see [02-streaming-workflow.md — Validation Pseudocode](./02-streaming-workflow.md#validation-pseudocode)).
- When the caller invokes backfill with `source=BSBSource(...)`, `[BACKFILL.BSB]` must be present AND the source must cover the requested chunk range. `run_backfill`'s `validate` asserts `source.tip() >= last_ledger_in_chunk(range_end_chunk_id)` at the start; `run_backfill` then calls `source.prepare_range(first_ledger, last_ledger)` once, after which lower-bound coverage (bucket retention floor, captive core history start) is verified per-ledger via `source.get_ledger(seq)` during execution.

### Partial Tx Index Ranges

When the caller's chunk range does not span a complete tx index, the trailing chunks have:

- Their raw `.bin` files on disk (inside `IMMUTABLE_STORAGE.TXHASH_RAW.PATH`).
- Their `chunk:{chunk_id:08d}:txhash` flags set in the meta store.
- No RecSplit `.idx` files (RecSplit is built only when every chunk of the tx index is ready).

These trailing artifacts persist on disk after `run_backfill` returns. Phase 2 of the streaming daemon loads them into the active txhash RocksDB store on startup and then deletes the `.bin` files and `chunk:{chunk_id:08d}:txhash` flags (see [02-streaming-workflow.md — Phase 2](./02-streaming-workflow.md#phase-2--hydrate-txhash-data-from-bin)).

Ledger and events data are useful per-chunk and are not blocked by tx-index alignment — `chunk:{chunk_id:08d}:lfs` and `chunk:{chunk_id:08d}:events` flags are set as soon as each chunk's outputs are durable.

### Example TOML

```toml
[SERVICE]
DEFAULT_DATA_DIR = "/data/stellar-rpc"

[BACKFILL]
CHUNKS_PER_TXHASH_INDEX = 1000

[IMMUTABLE_STORAGE.LEDGERS]
PATH = "/mnt/nvme/ledgers"

[IMMUTABLE_STORAGE.EVENTS]
PATH = "/mnt/nvme/events"

[IMMUTABLE_STORAGE.TXHASH_RAW]
PATH = "/mnt/nvme/txhash/raw"

[IMMUTABLE_STORAGE.TXHASH_INDEX]
PATH = "/mnt/nvme/txhash/index"

[BACKFILL.BSB]
BUCKET_PATH = "sdf-ledger-close-meta/v1/ledgers/pubnet"

[LOGGING]
LEVEL = "info"
FORMAT = "text"
```

The TOML above is consumed by the streaming daemon entry point (`stellar-rpc --config ...`); backfill is invoked internally by Phase 1 with the chunk range and source it computed.

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
│   └── rocksdb/                                  ← Meta store (WAL always enabled)
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
│   │   ├── 00000000-index.hash                   ← MPHF for term → slot lookup
│   │   └── ...
│   └── .../
│
└── txhash/
    ├── raw/                                      ← IMMUTABLE_STORAGE.TXHASH_RAW.PATH
    │   ├── 00000/                                ← chunk_ids 0–999 (1_000 .bin files)
    │   │   ├── 00000000.bin                      ← TRANSIENT (deleted after RecSplit + Phase 2)
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

`run_backfill` is invoked by Phase 1 of the streaming daemon with an integer chunk range and a `LedgerSource`:

```python
def run_backfill(config, range_start_chunk_id, range_end_chunk_id, source):
    """
    Ingest chunks [range_start_chunk_id, range_end_chunk_id] inclusive via the given source.

    Called by Phase 1 of the streaming daemon. Idempotent per chunk — already-completed
    chunks in the range return early from their task's execute(). On crash, Phase 1
    re-invokes with the same range; previously-completed work is skipped automatically.
    """
    # 1. Validate — abort before any work if inputs are inconsistent with existing state.
    validate(config, range_start_chunk_id, range_end_chunk_id, source)

    # 2. Prime the source for random-access reads across the entire run range. Done once
    # per run_backfill invocation; process_chunk tasks then call source.get_ledger(seq)
    # concurrently for seqs inside this range. Mirrors the stellar Go SDK's LedgerBackend
    # pattern (PrepareRange → GetLedger).
    source.prepare_range(
        first_ledger_in_chunk(range_start_chunk_id),
        last_ledger_in_chunk(range_end_chunk_id),
    )

    # 3. Build DAG — register all tasks; each task's execute() handles its own no-op check.
    dag = build_dag(config, range_start_chunk_id, range_end_chunk_id, source)

    # 4. Execute — dispatch all tasks concurrently, bounded by the source's parallelism.
    dag.execute(max_workers=source.max_parallelism())
```

### Validation

Validation runs before DAG construction, not as a DAG task — if it were a task, other tasks with no dependencies would start executing concurrently before validation completes, and a failure would leave in-flight work to cancel. Running it first means a clean abort with no partial work.

```python
def validate(config, range_start_chunk_id, range_end_chunk_id, source):
    # Range sanity.
    assert range_start_chunk_id >= 0
    assert range_end_chunk_id >= range_start_chunk_id

    # Source tip coverage. source.tip() is the highest ledger the source can serve;
    # lower-bound availability (BSB bucket retention floor, captive core history start)
    # is source-specific and surfaces as a per-task failure during execution — retried at
    # the DAG level and ultimately fatal if unrecoverable.
    last_ledger = last_ledger_in_chunk(range_end_chunk_id)
    assert source.tip() >= last_ledger

    # CHUNKS_PER_TXHASH_INDEX immutability. The daemon's validate_config (see
    # 02-streaming-workflow.md) stores this on first run and enforces it on every
    # subsequent start; backfill re-asserts the match defensively.
    assert meta_store.get("config:chunks_per_txhash_index") == str(config.backfill.chunks_per_txhash_index)
```

### DAG Setup

```python
def build_dag(config, range_start_chunk_id, range_end_chunk_id, source):
    # Wires up tasks and dependency edges — no completion checks or skip logic.
    # Each task's execute() handles its own no-op check.
    dag = new DAG()

    # For each tx index whose LAST chunk falls in [range_start_chunk_id, range_end_chunk_id],
    # schedule process_chunk (for in-range chunks only) + build + cleanup. Prior chunks of
    # such a tx index are either also in the current range (first-ever invocation) or
    # already flagged :lfs by a prior Phase 1 iteration — either way, build has every
    # chunk's .bin file available when it runs.
    for tx_index_id in tx_indexes_ending_in_range(range_start_chunk_id, range_end_chunk_id, config):
        chunk_tasks = []
        for chunk_id in chunks_for_tx_index(tx_index_id, config):
            if not (range_start_chunk_id <= chunk_id <= range_end_chunk_id):
                continue                                                   # prior iteration processed this chunk
            t = dag.add(ProcessChunkTask(chunk_id, source=source), deps=[])
            chunk_tasks.append(t.id)
        b = dag.add(BuildTxHashIndexTask(tx_index_id), deps=chunk_tasks)
        dag.add(CleanupTxHashTask(tx_index_id), deps=[b.id])

    # Trailing partial tx index: its last chunk is past range_end_chunk_id, so no build /
    # cleanup this run. Schedule process_chunk for its in-range chunks only; a future
    # Phase 1 iteration covering the missing trailing chunks will trigger the build.
    for chunk_id in trailing_partial_tx_index_chunks(range_start_chunk_id, range_end_chunk_id, config):
        dag.add(ProcessChunkTask(chunk_id, source=source), deps=[])

    return dag
```

A trailing tx index whose last chunks fall past `range_end_chunk_id` has its `process_chunk` tasks scheduled but no `build_txhash_index` / `cleanup_txhash`. Its `.bin` files + `chunk:{chunk_id:08d}:txhash` flags persist until a future `run_backfill` covers the missing chunks OR Phase 2 hydrates them — see [Partial Tx Index Ranges](#partial-tx-index-ranges).

---

## Task Details

### process_chunk(chunk_id, source)

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
def process_chunk(chunk_id, source):
    bucket_id    = chunk_id // 1000   # hardcoded subdirectory grouping (see Directory Structure)
    first_ledger = first_ledger_in_chunk(chunk_id)
    last_ledger  = last_ledger_in_chunk(chunk_id)

    # 1. Check which outputs are missing.
    need_lfs    = not meta_store.has(f"chunk:{chunk_id:08d}:lfs")
    need_txhash = not meta_store.has(f"chunk:{chunk_id:08d}:txhash")
    need_events = not meta_store.has(f"chunk:{chunk_id:08d}:events")
    if not (need_lfs or need_txhash or need_events):
        return                                                             # all outputs already present

    # 2. Choose data source for the in-loop ledger read. Both options expose get_ledger(seq)
    # for random-access reads; source was already prepared by run_backfill's prepare_range call.
    if not need_lfs:
        ledger_reader = local_packfile(ledger_pack_path(bucket_id, chunk_id))   # NVMe; no source call
    else:
        ledger_reader = source                                                   # BSB or captive core; pre-prepared

    # 3. Open writers only for missing outputs.
    ledger_writer = packfile.create(ledger_pack_path(bucket_id, chunk_id),
                                    overwrite=True) if need_lfs    else None
    txhash_writer = open(raw_txhash_path(bucket_id, chunk_id),
                         overwrite=True)                           if need_txhash else None
    events_writer = events_segment.create(events_path(bucket_id, chunk_id),
                                          overwrite=True)          if need_events else None

    # 4. Process each ledger.
    for ledger_seq in range(first_ledger, last_ledger + 1):
        lcm = ledger_reader.get_ledger(ledger_seq)
        if need_lfs:    ledger_writer.append(compress(lcm))
        if need_txhash: txhash_writer.append(extract_txhashes(lcm))    # 36 bytes per tx
        if need_events: events_writer.append(extract_events(lcm))

    # 5. Fsync + flag each output independently.
    if need_lfs:
        ledger_writer.fsync_and_close()
        meta_store.put(f"chunk:{chunk_id:08d}:lfs", "1")
    if need_txhash:
        txhash_writer.fsync_and_close()
        meta_store.put(f"chunk:{chunk_id:08d}:txhash", "1")
    if need_events:
        events_writer.finalize()                                           # flush, build MPHF + bitmap index, fsync
        meta_store.put(f"chunk:{chunk_id:08d}:events", "1")

    # Close the local packfile handle when we used one. The source case is shared across
    # tasks and stays open until run_backfill returns — no per-task close.
    if not need_lfs:
        ledger_reader.close()
```

Key properties:

- Only missing outputs are produced — a partially-completed chunk resumes from where it left off.
- If the ledger pack file (`:lfs` flag) is already present, reads from local NVMe instead of the source (avoids redundant download).
- Each flag is written independently after its output's fsync — no atomic WriteBatch needed.
- `packfile.create()` with `overwrite=True` handles truncation of partial files from prior crashes — no explicit `delete_if_exists` check needed.
- Naturally extends to new data types (add a fourth flag).

**Source concurrency.** With `source=BSBSource(...)`, many `process_chunk` tasks run in parallel (bounded by `source.max_parallelism() = GOMAXPROCS`). With `source=CaptiveCoreSource(...)`, `source.max_parallelism() = 1` — a single captive core subprocess cannot serve multiple chunk ranges in parallel, so the DAG dispatches chunks sequentially. See [02-streaming-workflow.md — Ledger Source](./02-streaming-workflow.md#ledger-source) for the interface.

### build_txhash_index(tx_index_id)

- Builds the RecSplit index for one completed tx index.
- Occupies one DAG worker slot, but spawns several goroutines internally.
- The DAG guarantees all chunk `.bin` files exist before this runs.

**Pseudocode:**

```python
def build_txhash_index(tx_index_id):
    if meta_store.has(f"index:{tx_index_id:08d}:txhash"):
        return                                                             # already built — no-op

    # Invariant: every chunk of tx_index_id has its .bin file on disk when this runs.
    # Prior-iteration chunks keep their .bin until cleanup_txhash runs, and cleanup only
    # runs AFTER this build succeeds (DAG dep). The list below therefore includes every
    # chunk's .bin, regardless of which Phase 1 iteration wrote it.
    bin_files = list_bin_files(tx_index_id)                                # all .bin files for chunks in this tx index

    # Stage 1: COUNT — scan all .bin files, count entries per CF.
    cf_counts = parallel_count(bin_files, workers=100)
    # cf_counts[nibble] = number of (txhash, ledger_seq) entries routed to that CF

    # Stage 2: ADD — re-read .bin files, route entries to CF builders.
    cf_builders = [RecSplitBuilder(cf_counts[nibble]) for nibble in range(16)]
    parallel_add(bin_files, cf_builders, workers=100)
    # each entry routed to cf_builders[txhash[0] >> 4] (mutex per CF)

    # Stage 3: BUILD — build MPH index per CF, one .idx file each.
    parallel_build(cf_builders, workers=16)
    # each CF produces one .idx file; all fsynced

    # Stage 4: VERIFY — look up every key in the built indexes (full verification,
    # since backfill has no wall-clock pressure).
    parallel_verify(bin_files, cf_builders, workers=100)

    # Flag. Backfill writes "1" only; streaming's prune path may later transition
    # this key through "deleting" before clearing it — see 02-streaming-workflow.md.
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
            continue                                                       # already cleaned up — skip
        bucket_id = chunk_id // 1000
        delete_if_exists(raw_txhash_path(bucket_id, chunk_id))             # remove .bin (idempotent — crash
                                                                           # between .bin delete and flag delete
                                                                           # is safe to retry on restart)
        meta_store.delete(f"chunk:{chunk_id:08d}:txhash")                  # remove meta key
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
        """Runs in a background thread — one per dispatched task."""
        for attempt in range(1, MAX_RETRIES + 1):
            error = task.execute()
            if error is None:
                break
            if attempt == MAX_RETRIES:
                mark_failed(task, error)                                   # halt all dependents
                break
            log.warn("retry", task, attempt, error)

        worker_slots.release()                                             # free worker slot

        # Check if completing this task unblocks any downstream tasks.
        for downstream_task in dag.dependents_of(task):
            downstream_task.mark_dependency_done(task)
            if downstream_task.all_dependencies_done():
                runnable_tasks.push(downstream_task)                       # now eligible to run

    # Main loop — dispatches tasks as they become runnable.
    while runnable_tasks:
        current_task = runnable_tasks.pop()
        worker_slots.acquire()                                             # block until a worker slot is free
        run_in_background(execute_task, current_task)                      # launch — returns immediately
```

### Worker Pool

- Single flat pool of `max_workers` slots, set by `source.max_parallelism()`:
  - `BSBSource.max_parallelism() = GOMAXPROCS`.
  - `CaptiveCoreSource.max_parallelism() = 1`.
- Any mix of task types can occupy slots simultaneously.
- `process_chunk`: 1 slot per task.
- `build_txhash_index`: 1 slot per task (uses many goroutines internally).
- `cleanup_txhash`: 1 slot per task.

---

## Crash Recovery

There is no separate crash recovery, reconciliation, or startup triage phase. Recovery happens organically because every task's `execute()` checks its own completion state:

- On every invocation, `build_dag()` registers ALL tasks for the chunk range — no meta store scanning in DAG setup.
- `process_chunk` checks each output flag independently — missing outputs are produced, existing outputs are skipped.
- `build_txhash_index` checks `index:{tx_index_id:08d}:txhash` — if present, returns immediately; if absent, deletes partial `.idx` files and reruns the full build.
- `cleanup_txhash` checks `chunk:{chunk_id:08d}:txhash` per-chunk — already-cleaned chunks are skipped, remaining chunks are cleaned up.

Three invariants make this work:

1. **Key implies durable file** — a meta store flag is set only after fsync.
2. **Tasks are idempotent** — each checks its own outputs and skips or overwrites what exists.
3. **DAG registers all tasks on every invocation** — completed tasks return immediately from `execute()`.

### Concurrent Access Prevention

The daemon acquires a directory flock on the meta-store at startup. A second process against the same datadir fails immediately.

---

## Error Handling

Two layers of retry:

- **Source-internal retries** — the `LedgerSource` handles transient errors internally (BSB connection resets, throttling, captive core subprocess hiccups). These retries happen inside a single task execution and are invisible to the DAG scheduler.
- **Task-level retries** — the DAG scheduler wraps each task's `execute()` with a retry loop bounded by `MAX_RETRIES`. After the source has exhausted its own retries, the scheduler retries the entire task. After `MAX_RETRIES` exhausted → task marked failed → DAG halts all dependents → `run_backfill` returns a fatal error → Phase 1 propagates → daemon exits non-zero. Operator investigates, fixes the root cause, and restarts the daemon; restart re-enters Phase 1, which re-invokes `run_backfill` with a fresh chunk range, and already-complete work is skipped via per-chunk idempotency.

| Error | Handled by | Action |
|-------|-----------|--------|
| Source transient error (throttle, connection reset) | Source-internal retry | Retried within the task; transparent to DAG |
| Source persistent error (source retries exhausted) | Task-level retry | `MAX_RETRIES` attempts; then ABORT |
| Ledger pack write / fsync failure | Task-level retry | `MAX_RETRIES` attempts; then ABORT; flag not set |
| Txhash write / fsync failure | Task-level retry | `MAX_RETRIES` attempts; then ABORT; flag not set |
| Events write / fsync failure | Task-level retry | `MAX_RETRIES` attempts; then ABORT; flag not set |
| RecSplit build failure | Task-level retry | `MAX_RETRIES` attempts; then ABORT; tx-index key absent |
| VERIFY stage mismatch | None | ABORT immediately — data corruption; operator investigates |
| Meta store write failure | None | ABORT immediately — treat as crash; operator re-runs daemon |
