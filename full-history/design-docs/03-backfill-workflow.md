# Backfill Workflow

## Overview

Backfill populates the immutable stores for a configured ledger range `[start_ledger, end_ledger]`.

**What it does:**
- Ingests historical ledgers offline — no live queries served (only `getHealth` / `getStatus`). `getHealth` is the existing lightweight liveness check; `getStatus` is the new backfill-specific progress endpoint (see [getStatus API Response](#getstatus-api-response) below).
- Writes directly to immutable file formats — no RocksDB active stores
- Schedules work as a DAG of idempotent tasks, dispatched via a flat worker pool (default GOMAXPROCS slots)
- Exits when done; on failure, re-run the same command — completed work is never repeated

**What it produces:**

| Query it enables | Immutable output | Scope |
|-----------------|-----------------|-------|
| `getLedger` | Ledger [pack file](https://github.com/stellar/stellar-rpc/pull/633) | Per chunk (10K ledgers) |
| `getTransaction` | Txhash index files | Per txhash index (default 10M ledgers) |
| `getEvents` | [Events cold segment](https://github.com/stellar/stellar-rpc/pull/635) | Per chunk |

---

## Geometry

The Stellar blockchain starts at ledger 2. Backfill organizes data using two concepts:

- **Chunk** — 10_000 ledgers (hardcoded, not configurable)
  - Atomic unit of ingestion and crash recovery
  - Produces: one ledger `.pack` file, one raw txhash `.bin` file, one events cold segment (`events.pack`, `index.pack`, `index.hash`)
  - `chunk_id = (ledger_seq - 2) / 10_000`
- **Txhash Index** — `chunks_per_txhash_index` chunks (default 1000 = 10M ledgers)
  - One RecSplit index covers all transactions across `chunks_per_txhash_index` chunks (default: 10M ledgers worth of transactions)
  - Produces 16 CF (column family) `.idx` files per txhash index
  - `index_id = chunk_id / chunks_per_txhash_index`
  - Configurable via TOML, but must not change across runs — once set, it is fixed

### ID Formulas

```
chunk_id   = (ledger_seq - 2) / 10_000
index_id   = chunk_id / chunks_per_txhash_index
```

Example with `chunks_per_txhash_index = 1000` (default):

| Txhash Index ID | First Ledger | Last Ledger | Chunks |
|-----------------|-------------|------------|--------|
| 0 | 2 | 10_000_001 | 0–999 |
| 1 | 10_000_002 | 20_000_001 | 1000–1999 |
| 2 | 20_000_002 | 30_000_001 | 2000–2999 |
| N | (N × 10M) + 2 | ((N+1) × 10M) + 1 | N×1000 – (N+1)×1000 - 1 |

All IDs use uniform `%08d` zero-padding (supports up to 99_999_999).

---

## Configuration

TOML file, passed via `backfill-workflow --config path/to/config.toml`.

- **TOML** defines data layout and storage paths — must be stable across runs
- **CLI flags** define per-run parameters (range, workers, retries)

### TOML Config

**[service]**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `default_data_dir` | string | **required** | Base directory for meta store and default storage paths. |

**[backfill]**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `chunks_per_txhash_index` | int | `1000` | Chunks per txhash index. Defines data layout — must be stable across runs. | 

**[immutable_storage.ledgers]**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `path` | string | `{default_data_dir}/ledgers` | Base path for ledger pack files. |

**[immutable_storage.events]**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `path` | string | `{default_data_dir}/events` | Base path for events cold segments. |

**[immutable_storage.txhash_raw]**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `path` | string | `{default_data_dir}/txhash/raw` | Base path for raw txhash `.bin` files (transient). |

**[immutable_storage.txhash_index]**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `path` | string | `{default_data_dir}/txhash/index` | Base path for RecSplit index files (permanent). |

The `immutable_storage` prefix disambiguates from `active_storage` (RocksDB-backed mutable stores used by the streaming workflow).

**[backfill.bsb]** — ledger backend (required)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `bucket_path` | string | **required** | Cloud storage path (without `gs://` prefix for GCS). |
| `buffer_size` | int | `1000` | Prefetch buffer depth per connection. |
| `num_workers` | int | `20` | Download workers per connection. |

### CLI Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--start-ledger` | uint32 | **required** | First ledger (inclusive). Must be ≥ 2. |
| `--end-ledger` | uint32 | **required** | Last ledger (inclusive). Must be > `start_ledger`. |
| `--workers` | int | `GOMAXPROCS` | Total concurrent DAG task slots. |
| `--verify-recsplit` | bool | `true` | Run RecSplit verify phase after build. |
| `--max-retries` | int | `3` | Max retries per task before marking it failed. |

### Optional TOML Sections

| Section | Key | Default | Description |
|---------|-----|---------|-------------|
| `[meta_store]` | `path` | `{default_data_dir}/meta/rocksdb` | Meta store RocksDB directory |
| `[logging]` | `log_file` | `{default_data_dir}/logs/backfill.log` | Main log file |
| `[logging]` | `max_scope_depth` | `0` | Max log scope nesting depth. 0=unlimited (all logs). 1=pipeline-level only. 2=+per-txhash-index. 3=+per-chunk/RecSplit. |

### Validation Rules

The only hard constraints are:

- `start_ledger >= 2`
- `end_ledger > start_ledger`
- `[backfill.bsb]` must be present
- `chunks_per_txhash_index` must not change after the first run — changing it invalidates existing txhash index boundaries
- Changing `--start-ledger` / `--end-ledger` between runs is fine as long as the new range **includes** all txhash indexes already in the meta store. Widening or extending the range works; narrowing to exclude existing txhash indexes does not.

**Valid:** Run 1 does `--start-ledger 2 --end-ledger 30_000_001` (txhash indexes 0–2). Run 2 widens to `--start-ledger 2 --end-ledger 60_000_001` (txhash indexes 0–5). Txhash indexes 0–2 are already complete → skipped. Txhash indexes 3–5 get new tasks.

**Invalid:** Run 1 does `--start-ledger 2 --end-ledger 60_000_001` (txhash indexes 0–5). Run 2 narrows to `--start-ledger 20_000_002 --end-ledger 60_000_001`. The system finds `index:00000000` and `index:00000001` in the meta store but outside the new range → abort. The operator must either widen the range to include them or use a fresh meta store.

No txhash-index-alignment required. The operator can pass any arbitrary ledger range.

#### Chunk Boundary Expansion

- System expands the requested range **outward** to the nearest chunk boundaries
- Start expands DOWN to the first ledger of its chunk
- End expands UP to the last ledger of its chunk
- Never clamps inward — the effective range is always ≥ the requested range
- Operator doesn't need to manually calculate chunk-aligned values

```
Operator requests:     --start-ledger 5_000_000  --end-ledger 56_337_842
Chunk boundary expand: start=5_000_000 falls within chunk 499 (starts at 4_990_002)
                       → expand start to 4_990_002
                       end=56_337_842 falls within chunk 5633 (ends at 56_340_001)
                       → expand end to 56_340_001
Effective range:       ledgers 4_990_002–56_340_001 = 5_135 chunks
```

#### BSB Availability Validation

After expansion, the system validates that BSB (the cloud storage ledger backend) contains all ledgers in the expanded range:

- Expanded end exceeds BSB availability → error at startup (no silent truncation)
- Operator must either reduce `--end-ledger` or wait for more ledgers to land in cloud storage

#### Partial Txhash Index Ranges

If the expanded range does not complete a full txhash index:

- Chunks are still backfilled and immediately serve `getLedger`/`getEvents` when the service is started in streaming mode
- Txhash index creation only happens once **all** input chunks for the txhash index are ready
- If txhash index creation does not happen in the current backfill run, the remaining chunks are completed either by a subsequent backfill run (should the operator run backfill again) or when streaming mode starts for the first time (see [Implications for Streaming Workflow](#implications-for-streaming-workflow) below)

Ledger and events data are useful per-chunk and should not be blocked by txhash index alignment. Without relaxed validation:

- A node at ledger 56_340_000 cannot backfill the latest ~6.3M ledgers because `50_000_002–56_340_001` doesn't align to a 10M txhash index boundary — the operator would have to wait until ledger 60_000_001
- Incremental backfill (extending coverage from a completed txhash index to recent history) would be blocked unless the chain happens to sit on a txhash index boundary

#### Implications for Streaming Workflow

When backfill completes at a non-txhash-index-aligned boundary, a partially-filled txhash index remains. The streaming workflow completes the remaining chunks:

- Streaming continues chunk ingestion from where backfill left off, writing the same per-chunk outputs (LFS, txhash, events) using the same flag-based idempotency
- When streaming completes the last chunk needed for a pending txhash index, txhash index creation becomes eligible and runs
- The meta store is the shared coordination point — streaming checks the same chunk flags as backfill, so there is no gap or overlap between backfill and streaming coverage

See [PR #617 discussion](https://github.com/stellar/stellar-rpc/pull/617#discussion_r2969796337) for the original rationale.

### Example: GCS Backfill Config

```toml
[service]
default_data_dir = "/data/stellar-rpc"

[backfill]
chunks_per_txhash_index = 1000

[immutable_storage.ledgers]
path = "/mnt/nvme/ledgers"

[immutable_storage.events]
path = "/mnt/nvme/events"

[immutable_storage.txhash_raw]
path = "/mnt/nvme/txhash/raw"

[immutable_storage.txhash_index]
path = "/mnt/nvme/txhash/index"

[backfill.bsb]
bucket_path = "sdf-ledger-close-meta/v1/ledgers/pubnet"
```

```bash
backfill-workflow --config config.toml \
  --start-ledger 2 \
  --end-ledger 30_000_001 \
  --workers 40
```

---

## Directory Structure

With geometry (chunk, txhash index) and storage paths (`immutable_storage.*`) defined above, here is how they map to the filesystem.

**Data is organized by type at the top level:**
- Each data type (ledgers, events, txhash) has its own directory tree
- Each type's root comes from its `immutable_storage.*.path` config
- Chunks are grouped into subdirectories of 1_000 (`bucket_id = chunk_id / 1000`, formatted as `%05d`)
- Txhash index output is the only structure that uses `index_id`

```
{default_data_dir}/
├── meta/
│   └── rocksdb/                                  ← Meta store (WAL always enabled)
│
├── ledgers/                                      ← immutable_storage.ledgers.path
│   ├── 00000/                                    ← chunks 0–999
│   │   ├── 00000000.pack                         ← ledger pack file (PR #633)
│   │   ├── 00000001.pack
│   │   └── ...
│   ├── 00001/                                    ← chunks 1000–1999
│   │   └── ...
│   └── .../
│
├── events/                                       ← immutable_storage.events.path
│   ├── 00000/
│   │   ├── 00000000-events.pack                  ← compressed event blocks
│   │   ├── 00000000-index.pack                   ← serialized roaring bitmaps
│   │   ├── 00000000-index.hash                   ← MPHF for term → slot lookup
│   │   └── ...
│   └── .../
│
└── txhash/
    ├── raw/                                      ← immutable_storage.txhash_raw.path
    │   ├── 00000/
    │   │   ├── 00000000.bin                      ← TRANSIENT (deleted after RecSplit)
    │   │   └── ...
    │   └── .../
    └── index/                                    ← immutable_storage.txhash_index.path
        ├── 00000000/                             ← txhash index 0
        │   └── cf-{0-f}.idx                      ← PERMANENT (16 RecSplit CF files)
        └── .../
```

### Concrete Example

20M ledgers (2_000 chunks), `chunks_per_txhash_index = 1000`:

```
ledgers/
├── 00000/                                         ← chunks 0–999
│   ├── 00000000.pack ... 00000999.pack              (1_000 .pack files)
└── 00001/                                         ← chunks 1000–1999
    ├── 00001000.pack ... 00001999.pack

events/
├── 00000/
│   ├── 00000000-events.pack ... 00000999-events.pack
│   ├── 00000000-index.pack  ... 00000999-index.pack
│   └── 00000000-index.hash  ... 00000999-index.hash  (3_000 files)
└── 00001/
    └── ...                                            (3_000 files)

txhash/
├── raw/
│   ├── 00000/00000000.bin ... 00000999.bin            (1_000 .bin files)
│   └── 00001/00001000.bin ... 00001999.bin
└── index/
    ├── 00000000/cf-0.idx ... cf-f.idx               ← txhash index 0 (chunks 0–999, 16 files)
    └── 00000001/cf-0.idx ... cf-f.idx               ← txhash index 1 (chunks 1000–1999)
```

**`chunks_per_txhash_index` only affects `txhash/index/`.**
- `ledgers/`, `events/`, `txhash/raw/` — grouped by the hardcoded 1_000-chunk divisor, identical regardless of `chunks_per_txhash_index`
- Smaller txhash index = more RecSplit builds, each covering fewer chunks
  - `chunks_per_txhash_index = 1000` → 2 txhash index dirs
  - `chunks_per_txhash_index = 100` → 20 txhash index dirs
  - `chunks_per_txhash_index = 1` → 2_000 txhash index dirs

### Path Conventions

- Chunk-level files (ledgers, events, raw txhash) use only `chunk_id` and `bucket_id` — no `index_id` in any path
- Only the RecSplit txhash index output uses `index_id`: `txhash/index/{indexID:08d}/`

| File Type | Pattern | Example |
|-----------|---------|---------|
| Ledger pack | `{immutable_storage.ledgers.path}/{bucketID:05d}/{chunkID:08d}.pack` | `ledgers/00000/00000042.pack` |
| Raw txhash | `{immutable_storage.txhash_raw.path}/{bucketID:05d}/{chunkID:08d}.bin` | `txhash/raw/00000/00000042.bin` |
| RecSplit CF | `{immutable_storage.txhash_index.path}/{indexID:08d}/cf-{nibble}.idx` | `txhash/index/00000000/cf-a.idx` |
| Events data | `{immutable_storage.events.path}/{bucketID:05d}/{chunkID:08d}-events.pack` | `events/00000/00000042-events.pack` |
| Events index | `{immutable_storage.events.path}/{bucketID:05d}/{chunkID:08d}-index.pack` | `events/00000/00000042-index.pack` |
| Events hash | `{immutable_storage.events.path}/{bucketID:05d}/{chunkID:08d}-index.hash` | `events/00000/00000042-index.hash` |

- **Nibble** = high 4 bits of `txhash[0]`, i.e., `txhash[0] >> 4`. Values `0`–`f`. Determines which of 16 CFs a txhash is routed to.
- **Raw txhash format**: 36 bytes per entry, no header: `[txhash: 32 bytes][ledgerSeq: 4 bytes big-endian]`
- **Events cold segment**: See [getEvents full-history design](https://github.com/stellar/stellar-rpc/pull/635) for the full format specification.
- Directories are created on-demand via `os.MkdirAll`. Safe for concurrent writes.

---

## Meta Store Keys

- Single RocksDB instance with WAL (Write-Ahead Log) always enabled
- Authoritative source for crash recovery — all resume decisions derive from key presence in this store

### Key Schema

All IDs use uniform `%08d` zero-padding, matching the directory structure.

| Key Pattern | Value | Written When |
|-------------|-------|-------------|
| `chunk:{C:08d}:lfs` | `"1"` | After ledger `.pack` file is fsynced |
| `chunk:{C:08d}:txhash` | `"1"` | After raw txhash `.bin` file is fsynced |
| `chunk:{C:08d}:events` | `"1"` | After events cold segment files (`events.pack`, `index.pack`, `index.hash`) are fsynced |
| `index:{N:08d}:txhash` | `"1"` | After all 16 RecSplit CF `.idx` files are built and fsynced |

- Values are `"1"` (retained for `ldb`/`sst_dump` readability); key presence is the signal
- Key absence means not started or incomplete — treated identically on resume
- Each chunk flag is written independently after its output's fsync — a crash may leave some flags set and others absent for the same chunk
- On resume, each chunk's flags are checked independently — only missing outputs are produced
- WAL is always enabled — disabling it would invalidate all crash recovery
- `chunk:{C}:txhash` keys are deleted after the txhash index is built (the raw `.bin` files they reference are also deleted); all other flags are permanent

**Examples:**
```
chunk:00000000:lfs     →  "1"     chunk 0 ledger pack done
chunk:00000000:txhash  →  "1"     chunk 0 raw txhash done
chunk:00000000:events  →  "1"     chunk 0 events cold segment done
chunk:00000999:events  →  "1"     last chunk of txhash index 0
index:00000000:txhash  →  "1"     txhash index 0 RecSplit complete
index:00000001:txhash  →  absent  txhash index 1 not yet built
```

### Key Lifecycle

```
chunk ingestion        → sets chunk:{C}:lfs, chunk:{C}:txhash, chunk:{C}:events
                         (each independently, after its output's fsync)
txhash index build     → sets index:{N}:txhash
txhash cleanup         → deletes chunk:{C}:txhash keys + raw .bin files
```

After a completed txhash index:
- `chunk:{C}:lfs`, `chunk:{C}:events`, `index:{N}:txhash` — permanent
- `chunk:{C}:txhash` keys + raw `.bin` files — deleted after txhash index is built

---

## Tasks and Dependencies

The backfill DAG has three task types:

| Task | Cadence | Dependencies | Produces |
|------|---------|-------------|----------|
| `process_chunk(chunk_id)` | Per chunk (10K ledgers) | None | Ledger `.pack` + raw txhash `.bin` + events cold segment |
| `build_txhash_index(index_id)` | Per txhash index | All `process_chunk` tasks for this txhash index | 16 RecSplit `.idx` files |
| `cleanup_txhash(index_id)` | Per txhash index | `build_txhash_index` for this txhash index | Deletes raw `.bin` files + `chunk:{C}:txhash` meta keys |

- Each task is a black box to the DAG scheduler — it calls `Execute()` and waits for return
- What happens inside (goroutines, I/O, parallelism) is up to the task

### Dependency Diagram

For a single txhash index with N chunks:

```
process_chunk(chunk 0) ─┐
process_chunk(chunk 1) ─┤
process_chunk(chunk 2) ─┼──→ build_txhash_index(index_id) ──→ cleanup_txhash(index_id)
...                     │
process_chunk(chunk N) ─┘
```

- All `process_chunk` tasks for a txhash index must complete before `build_txhash_index` fires
- `cleanup_txhash` runs after `build_txhash_index` succeeds
- Cleanup deletes the raw `.bin` files and their `chunk:{C}:txhash` meta keys

### Main Flow

```python
def run_backfill(config, flags):

    # 1. Validate — abort before any work if config is incompatible with existing state
    validate(config, flags)

    # 2. Build DAG — register all tasks; each task's execute() handles its own no-op check
    dag = build_dag(config, flags)

    # 3. Execute — dispatch all tasks concurrently, bounded by worker count
    dag.execute(max_workers=flags.workers)    # default GOMAXPROCS
```

### Validation

Validation runs before DAG construction, not as a DAG task. If it were a DAG task, other tasks with no dependencies would start executing concurrently before validation completes — and if validation fails, in-flight work that should never have started would need to be cancelled. Running it first means a clean abort with no partial work.

```python
def validate(config, flags):
    # See Validation Rules for the full list of checks.
    # Key check: no txhash index keys in meta store outside the configured range.
    for index_key in meta_store.scan_prefix("index:"):
        if index_key.index_id not in configured_indexes(config, flags):
            abort("meta store has txhash index outside configured range")
```

### DAG Setup

```python
def build_dag(config, flags):
    # Wires up tasks and dependency edges — no completion checks or skip logic.
    # Each task's execute() handles its own no-op check (early return if already complete).

    dag = new DAG()

    for index_id in configured_indexes(config, flags):
        chunk_tasks = []
        for chunk_id in chunks_for_index(index_id):
            t = dag.add(ProcessChunkTask(chunk_id), deps=[])
            chunk_tasks.append(t.id)
        b = dag.add(BuildTxHashIndexTask(index_id),
                     deps=chunk_tasks)
        dag.add(CleanupTxHashTask(index_id), deps=[b.id])

    return dag
```

---

## Task Details

### process_chunk(chunk_id)

- Processes a single 10K-ledger chunk end-to-end
- Occupies one DAG worker slot
- Only produces missing outputs — checks each flag independently
- Internal concurrency is an implementation detail

**Outputs** (all produced in a single task, only if missing):
- Ledger pack file (`{chunkID:08d}.pack`) — compressed ledger data in [packfile format](https://github.com/stellar/stellar-rpc/pull/633)
- Raw txhash flat file (`{chunkID:08d}.bin`) — 36-byte entries consumed by RecSplit builder
- Events cold segment (`events.pack` + `index.pack` + `index.hash`) — per [getEvents design](https://github.com/stellar/stellar-rpc/pull/635)

**Pseudocode:**

```python
process_chunk(chunk_id):
    bucket_id    = chunk_id / 1000 # hardcoded subdirectory grouping (see Directory Structure)
    first_ledger = chunk_first_ledger(chunk_id)
    last_ledger  = chunk_last_ledger(chunk_id)

    # 1. Check which outputs are missing
    need_lfs    = not meta_store.has(f"chunk:{chunk_id:08d}:lfs")
    need_txhash = not meta_store.has(f"chunk:{chunk_id:08d}:txhash")
    need_events = not meta_store.has(f"chunk:{chunk_id:08d}:events")

    if not (need_lfs or need_txhash or need_events):
        return    # all outputs already present

    # 2. Choose data source
    if not need_lfs:
        source = local_packfile(ledger_pack_path(bucket_id, chunk_id))   # NVMe, no remote fetch
    else:
        source = BSBFactory.create(first_ledger, last_ledger)            # cloud storage connection

    # 3. Open writers only for missing outputs
    ledger_writer = packfile.create(ledger_pack_path(bucket_id, chunk_id),
                                    overwrite=True) if need_lfs else None
    txhash_writer = open(raw_txhash_path(bucket_id, chunk_id),
                         overwrite=True) if need_txhash else None
    events_writer = events_segment.create(events_path(bucket_id, chunk_id),
                                          overwrite=True) if need_events else None

    # 4. Process each ledger
    for seq in range(first_ledger, last_ledger + 1):
        lcm = source.get_ledger(seq)

        if need_lfs:    ledger_writer.append(compress(lcm))
        if need_txhash: txhash_writer.append(extract_txhashes(lcm))   # 36 bytes per tx
        if need_events: events_writer.append(extract_events(lcm))

    # 5. Fsync + flag each output independently
    if need_lfs:
        ledger_writer.fsync_and_close()
        meta_store.put(f"chunk:{chunk_id:08d}:lfs", "1")

    if need_txhash:
        txhash_writer.fsync_and_close()
        meta_store.put(f"chunk:{chunk_id:08d}:txhash", "1")

    if need_events:
        events_writer.finalize()          # flush, build MPHF + bitmap index, fsync
        meta_store.put(f"chunk:{chunk_id:08d}:events", "1")

    source.close()
```

Key properties:
- Only missing outputs are produced — a partially-completed chunk resumes from where it left off
- If LFS is already present, reads from local NVMe instead of cloud storage (avoids redundant download)
- Each flag is written independently after its output's fsync — no atomic WriteBatch needed
- `packfile.Create()` with `overwrite=True` handles truncation of partial files from prior crashes — no explicit `delete_if_exists` check needed
- Naturally extends to new data types (add a fourth flag)

**BSB** (BufferedStorageBackend):
- Cloud storage-backed ledger source
- Each `process_chunk` task creates its own cloud storage connection
- Internal prefetch workers: `buffer_size` ledgers ahead, `num_workers` download goroutines
- GCS is the primary deployment backend; BSB supports other object stores

### build_txhash_index(index_id)

- Builds the RecSplit txhash index for one completed txhash index
- Occupies one DAG worker slot, but spawns several goroutines internally
- The DAG guarantees all chunk `.bin` files exist before this runs

**Pseudocode:**

```python
build_txhash_index(index_id):
    if meta_store.has(f"index:{index_id:08d}:txhash"):
        return                                            # already built — no-op

    bin_files = list_bin_files(index_id)      # all .bin files for chunks in this txhash index

    # Phase 1: COUNT — scan all .bin files, count entries per CF
    cf_counts = parallel_count(bin_files, workers=100)
    # cf_counts[nibble] = number of (txhash, ledgerSeq) entries routed to that CF

    # Phase 2: ADD — re-read .bin files, route entries to CF builders
    cf_builders = [RecSplitBuilder(cf_counts[n]) for n in range(16)]
    parallel_add(bin_files, cf_builders, workers=100)
    # each entry routed to cf_builders[txhash[0] >> 4] (mutex per CF)

    # Phase 3: BUILD — build MPH index per CF, one .idx file each
    parallel_build(cf_builders, workers=16)
    # each CF produces one .idx file; all fsynced

    # Phase 4: VERIFY (optional) — look up every key in the built indexes
    if verify_recsplit:
        parallel_verify(bin_files, cf_builders, workers=100)

    # Mark index complete
    meta_store.put(f"index:{index_id:08d}:txhash", "1")
```

Key properties:
- COUNT and ADD each read all `.bin` files (two full passes over the data)
- BUILD runs 16 goroutines in parallel (one per CF) — each CF is independent
- VERIFY is skippable via `--verify-recsplit=false` cli flag
- All-or-nothing recovery: if `index:{N}:txhash` is absent on restart → delete partial `.idx` files → rerun entire build

### cleanup_txhash(index_id)

- Runs after `build_txhash_index` completes successfully

**Pseudocode:**

```python
cleanup_txhash(index_id):
    for chunk_id in chunks_for_index(index_id):
        if not meta_store.has(f"chunk:{chunk_id:08d}:txhash"):
            continue                                      # already cleaned up — skip
        delete(raw_txhash_path(bucket_id, chunk_id))      # remove .bin file
        meta_store.delete(f"chunk:{chunk_id:08d}:txhash")  # remove meta key
```

Key properties:
- Modeled as a separate DAG task (not inline in `build_txhash_index`) so crash recovery works naturally
- Per-chunk idempotency: each chunk checks its own `chunk:{C}:txhash` key before deleting — a crash mid-cleanup resumes from where cleanup left off
- On restart: DAG sees txhash index key present (build complete) but `chunk:{C}:txhash` keys still exist → cleanup runs as a normal task

---

## Execution Model

### DAG Scheduler

- Pipeline builds a single DAG at startup, executes it with bounded concurrency
- The DAG is the only scheduling mechanism — no per-txhash-index coordinators, no secondary worker pools
- Each task's `Execute()` is wrapped with a retry loop bounded by `--max-retries` (default 3). Any transient failure (cloud storage errors, temporary I/O issues) triggers a retry at the task level.

```python
run_dag(dag, max_workers):
    worker_slots   = Semaphore(max_workers)
    runnable_tasks = ThreadSafeQueue(dag.tasks_with_no_pending_dependencies())

    def execute_task(task):
        """Runs in a background thread — one per dispatched task."""
        for attempt in range(1, max_retries + 1):
            error = task.execute()
            if error is None:
                break
            if attempt == max_retries:
                mark_failed(task, error)              # halt all dependents
                break
            log.warn("retry", task, attempt, error)

        worker_slots.release()                        # free worker slot

        # Check if completing this task unblocks any downstream tasks
        for downstream in dag.dependents_of(task):
            downstream.mark_dependency_done(task)
            if downstream.all_dependencies_done():
                runnable_tasks.push(downstream)       # now eligible to run

    # Main loop — dispatches tasks as they become runnable
    while runnable_tasks:
        current_task = runnable_tasks.pop()
        worker_slots.acquire()                        # block until a worker slot is free
        run_in_background(execute_task, current_task) # launch — returns immediately
```

### Worker Pool

- Single flat pool of `workers` slots (default `GOMAXPROCS`)
- Any mix of task types can occupy slots simultaneously
- `process_chunk`: 1 slot per task
- `build_txhash_index`: 1 slot per task (uses many goroutines internally)
- `cleanup_txhash`: 1 slot per task

### How Work Flows Through the Pipeline

- All `process_chunk` tasks have no dependencies → DAG dispatches up to `workers` slots immediately at startup
- Chunks from different txhash indexes run side by side — the scheduler does not process txhash indexes sequentially
- When the last chunk of a txhash index completes → `build_txhash_index` becomes eligible, claims a slot
- After build completes → `cleanup_txhash` becomes eligible
- Remaining slots continue processing chunks for other txhash indexes throughout — no special coordination needed

---

## Crash Recovery

There is no separate crash recovery, reconciliation, or startup triage phase. Recovery happens organically because every task's `execute()` checks its own completion state:

- On every startup, `build_dag()` registers ALL tasks for the configured range — no meta store scanning in DAG setup
- `process_chunk` checks each output flag independently — missing outputs are produced, existing outputs are skipped
- `build_txhash_index` checks `index:{N}:txhash` — if present, returns immediately; if absent, deletes partial `.idx` files and reruns the full build
- `cleanup_txhash` checks `chunk:{C}:txhash` per-chunk — already-cleaned chunks are skipped, remaining chunks are cleaned up

This works because of three invariants:

1. **Key implies durable file** — a meta store flag is set only after fsync
2. **Tasks are idempotent** — each checks its own outputs and skips or overwrites what exists
3. **DAG registers all tasks on every startup** — completed tasks return immediately from `execute()`

### Concurrent Access Prevention

- Meta store RocksDB uses kernel-level `flock()` on a `LOCK` file
- A second process attempting to open the same meta store fails immediately
- Released automatically on process exit (including `kill -9`)


---

## getStatus API Response

During backfill, `getStatus` returns progress as task-type summaries:
- No per-txhash-index breakdown — just completed/pending/in_progress counts per task type

```json
{
  "mode": "BACKFILL",
  "tasks": {
    "process_chunk":        {"completed": 288, "pending": 5712, "in_progress": 40},
    "build_txhash_index":   {"completed": 0, "pending": 6, "in_progress": 0},
    "cleanup_txhash":       {"completed": 0, "pending": 6, "in_progress": 0}
  },
  "eta_seconds": 1820
}
```

---

## Error Handling

Two layers of retry:

- **BSB (cloud storage) retries** — BSB handles transient cloud storage errors internally (connection resets, throttling, etc). These retries happen within a single task execution and are not visible to the DAG scheduler.
- **Task-level retries** — the DAG scheduler wraps each task's `execute()` with a retry loop bounded by `--max-retries` (default 3). If a task returns an error after BSB has exhausted its own retries, the scheduler retries the entire task. After `--max-retries` exhausted → task marked failed → DAG halts all dependent tasks → process exits non-zero.

Operator re-runs the same command; completed work is never repeated.

| Error | Handled by | Action |
|-------|-----------|--------|
| Cloud storage transient error (throttle, connection reset) | BSB internal retry | Retried within the task; transparent to DAG |
| Cloud storage persistent error (BSB retries exhausted) | Task-level retry | `--max-retries` attempts; then ABORT |
| Ledger pack write / fsync failure | Task-level retry | `--max-retries` attempts; then ABORT; flag not set |
| TxHash write / fsync failure | Task-level retry | `--max-retries` attempts; then ABORT; flag not set |
| Events write / fsync failure | Task-level retry | `--max-retries` attempts; then ABORT; flag not set |
| RecSplit build failure | Task-level retry | `--max-retries` attempts; then ABORT; txhash index key absent |
| Verify phase mismatch | None | ABORT immediately — data corruption, operator investigates |
| Meta store write failure | None | ABORT immediately — treat as crash, operator re-runs |
