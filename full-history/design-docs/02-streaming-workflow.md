# Streaming Workflow

## Overview

- Streaming mode ingests live Stellar ledgers from CaptiveStellarCore, one ledger at a time, while serving queries
- Default mode — when `--mode` is not specified, the service runs in streaming mode

**What streaming does:**
- Ingests ledgers from CaptiveStellarCore in real-time (~1 ledger every 6 seconds)
- Writes each ledger to three active stores: ledger RocksDB, txhash RocksDB (16 CFs), and events hot segment
- Serves `getLedger`, `getTransaction`, and `getEvents` queries concurrently with ingestion
- At chunk boundaries (every 10_000 ledgers): transitions ledger store to LFS pack file and freezes events hot segment to cold segment — both in background
- At index boundaries (every 10_000_000 ledgers): builds RecSplit txhash index from transitioning txhash store — in background
- Long-running daemon that exits only on fatal error

**How streaming differs from backfill:**

| Dimension | Backfill | Streaming |
|---|---|---|
| Data source | BSB (GCS) | CaptiveStellarCore |
| RocksDB for ingestion | No — writes directly to files | Yes — three active stores |
| Txhash write format | `.bin` flat files (36 bytes/entry) | RocksDB txhash store (16 CFs) |
| RecSplit input | `.bin` flat files | RocksDB txhash store |
| Checkpoint granularity | Per-chunk (10_000 ledgers) | Per-ledger |
| Concurrency | Flat worker pool (DAG scheduler, default GOMAXPROCS slots) | Single ingestion goroutine, background transition goroutines |
| Queries | Not served (`getHealth`/`getStatus` only) | All endpoints available |
| Crash recovery | Re-run from first incomplete chunk | Resume from `streaming:last_committed_ledger + 1` |
| Process lifecycle | Exits when done | Long-running daemon |
| Transition workflow | Direct-write to immutable files (no active store to tear down) | Active RocksDB → immutable (LFS + RecSplit) via background goroutines |
| Dependency tracking | Static DAG — all tasks known upfront, dispatched as dependencies resolve | Dynamic DAG — tasks created at chunk/index boundaries, dependencies enforced by per-sub-flow sync points |

**Why streaming uses a dynamic DAG:**
- Backfill knows the full ledger range at startup — all `process_chunk`, `build_txhash_index`, and `cleanup_txhash` tasks can be registered upfront
- Streaming ingests indefinitely — chunk/index boundaries are discovered as ledgers arrive, so tasks cannot be registered upfront
- The dependency structure is the same (flush/freeze must complete before next boundary, all chunk transitions must complete before RecSplit) — only the registration timing differs
- See [Sub-flow Transitions](#sub-flow-transitions) for the full transition DAG

### Main Flow

```python
def run_streaming(config):

    # 1. Startup — validate, reconcile, recover from any prior crash
    meta_store, resume_ledger = startup(config)

    # 2. Start CaptiveStellarCore — takes ~4-5 min to reach resume_ledger
    core = start_captive_core(config, resume_ledger)

    # 3. Ingest — one ledger at a time, forever
    for lcm in core.stream_ledgers():
        process_ledger(lcm, meta_store)
```

Each step is detailed in the sections that follow: [Startup Sequence](#startup-sequence), [Ingestion Loop](#ingestion-loop), and [Sub-flow Transitions](#sub-flow-transitions).

### Boundary Math Reference

Boundary formulas are defined in [01-backfill-workflow.md](./01-backfill-workflow.md#geometry). Quick reference:

```python
# Stellar ledgers start at 2 (not 0 or 1), so all formulas subtract 2 to zero-base
chunk_id              = (ledger_seq - 2) // 10_000           # e.g. ledger 56_340_001 → chunk 5633
chunk_first_ledger(C) = (C * 10_000) + 2                    # e.g. chunk 5634 → ledger 56_340_002
chunk_last_ledger(C)  = ((C + 1) * 10_000) + 1              # e.g. chunk 5634 → ledger 56_350_001
index_id              = chunk_id // chunks_per_txhash_index  # e.g. chunk 5634 → index 5
index_last_ledger(N)  = ((N + 1) * chunks_per_txhash_index * 10_000) + 1  # e.g. index 5 → ledger 60_000_001
```

---

## Configuration

### Immutable Config (shared with backfill)

- Streaming reads the same TOML config as backfill
- The following keys are immutable — set on first backfill, fatal error if changed:

| Key | Stored in Meta Store | Set By | Description |
|---|---|---|---|
| `chunks_per_txhash_index` | `config:chunks_per_txhash_index` | First backfill run | Chunks per txhash index. Must never change. |

All `[immutable_storage.*]` paths from the backfill config apply unchanged.

### Streaming-Specific TOML

**[streaming]**

| Key | Type | Default | Description |
|---|---|---|---|
| `captive_core_config` | string | **required** | Path to CaptiveStellarCore config file. |
| `drift_warning_ledgers` | int | `10` | Log warning when ingestion lags network tip by this many ledgers (~60 seconds at 10 ledgers). |

**[streaming.active_storage]**

| Key | Type | Default | Description |
|---|---|---|---|
| `path` | string | `{default_data_dir}/active` | Base path for active RocksDB stores (ledger, txhash, events). |

### CLI Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--mode` | string | `streaming` | `streaming` or `full-history-backfill`. |
| `--config` | string | **required** | Path to TOML config file. |

---

## Active Store Architecture

Streaming maintains three active stores for the current ingestion position:

| Store | Path | Key | Value | Transition cadence |
|---|---|---|---|---|
| Ledger | `{active_storage.path}/ledger-store-chunk-{chunkID:08d}/` | `uint32BE(ledgerSeq)` | `zstd(LCM bytes)` | Every 10_000 ledgers (chunk) |
| TxHash | `{active_storage.path}/txhash-store-index-{indexID:08d}/` | `txhash[32]` | `uint32BE(ledgerSeq)` | Every 10_000_000 ledgers (index) |
| Events | In-memory hot segment + persisted index deltas | Sequential event ID | Event XDR + metadata | Every 10_000 ledgers (chunk) |

- **Ledger store**: default CF only. One RocksDB instance per chunk. WAL required.
- **TxHash store**: 16 column families (`cf-0` through `cf-f`), routed by `txhash[0] >> 4`. One RocksDB instance per index (spans 1_000 chunks). WAL required.
- **Events hot segment**: in-memory roaring bitmaps + persisted per-ledger index deltas (for crash recovery — replayed on startup to rebuild bitmaps). ~370 MB per segment (measured). See [getEvents full-history design](../../design-docs/getevents-full-history-design.md) for format details.

### Store Pre-creation

- Stores for the next chunk/index are pre-created before the boundary is reached
- At transition time, only internal pointers change (active → transitioning, pre-created → active)
- Pre-created stores eliminate creation failures at boundary time
- On restart, pre-created stores are expected to exist — not treated as orphans

### Max Concurrent Stores

| Sub-flow | Max active | Max transitioning | Max total |
|---|---|---|---|
| Ledger | 1 | 1 | 2 |
| Events | 1 (hot) | 1 (freezing) | 2 |
| TxHash | 1 | 1 | 2 |

---

## Startup Sequence

- The startup sequence is the same code path for first start in streaming mode (after backfill) and regular restarts
- The presence or absence of `streaming:last_committed_ledger` in the meta store distinguishes the two cases

### Startup Flow

```python
def start_streaming(config):
    meta_store = open_meta_store(config)

    # ── 1. Validate immutable config ──
    validate_config(config, meta_store)

    # ── 2. Validate backfill completeness ──
    head_chunk, durable_tail = validate_chunk_coverage(config, meta_store)

    # ── 3. Load backfill txhash data into RocksDB ──
    #    If backfill left a partial txhash index, .bin files exist for the backfill chunks.
    #    Load .bin → RocksDB, then delete .bin files + txhash flags.
    #    No-op if backfill ended on an index boundary or on restarts.
    load_backfill_bins(config, meta_store)

    # ── 4. Reconcile orphaned transitions ──
    #    Complete any in-flight transitions from a previous crash.
    reconcile_orphaned_transitions(config, meta_store)

    # ── 5. Replay missed boundary handling ──
    #    If checkpoint is at a chunk/index boundary but transitions never fired.
    replay_missed_boundaries(config, meta_store)

    # ── 6. Detect BUILD_READY indexes, spawn background RecSplit ──
    spawn_pending_recsplit_builds(config, meta_store)

    # ── 7. Determine resume ledger ──
    last_committed = meta_store.get("streaming:last_committed_ledger")
    if last_committed is None:
        # First start in streaming mode: set checkpoint to backfill's last ledger
        last_committed = chunk_last_ledger(durable_tail)
        meta_store.put("streaming:last_committed_ledger", last_committed)
    resume_ledger = last_committed + 1

    # ── 8. Open/create active stores for the resume position ──
    active_stores = open_active_stores(config, meta_store, resume_ledger)

    # ── 9. Start CaptiveStellarCore ──
    #    CaptiveStellarCore takes ~4-5 minutes to spin up to the target ledger.
    #    Steps 1-8 run sequentially before this point.
    #    If backfill left a partial index with .bin files, step 3 takes ~4-5 minutes
    #    (one-time cost, does not recur). Otherwise, steps 1-8 complete in seconds.
    core = start_captive_core(config, resume_ledger)

    # ── 10. Begin ingestion loop ──
    run_ingestion_loop(core, active_stores, meta_store)
```

### Step 1: Validate Immutable Config

```python
def validate_config(config, meta_store):
    stored_cpi = meta_store.get("config:chunks_per_txhash_index")
    if stored_cpi is not None and stored_cpi != config.chunks_per_txhash_index:
        fatal(f"chunks_per_txhash_index changed: {stored_cpi} -> {config.chunks_per_txhash_index}")
    if stored_cpi is None:
        # First ever run writes the value. Backfill writes this on first run;
        # if streaming runs first (no prior backfill), streaming writes it.
        meta_store.put("config:chunks_per_txhash_index", config.chunks_per_txhash_index)
```

### Step 2: Validate Chunk Coverage

```python
def validate_chunk_coverage(config, meta_store):
    cpi = config.chunks_per_txhash_index

    lfs_set    = meta_store.scan_keys_with_suffix(":lfs")       # set of chunk IDs
    events_set = meta_store.scan_keys_with_suffix(":events")
    txhash_set = meta_store.scan_keys_with_suffix(":txhash")    # backfill-only flags

    if not lfs_set:
        fatal("no chunk data found — run backfill first")

    head_chunk = min(lfs_set | events_set)             # lowest chunk with any flag
    head_index = head_chunk // cpi                      # which index that chunk belongs to

    # Head must be index-aligned — the first chunk must be the first chunk of its index.
    # If not, the head index can never be completed (RecSplit needs all cpi chunks).
    if head_chunk != head_index * cpi:                  # e.g. head_chunk=2300, expected=2000
        fatal(f"partial index at head: chunk {head_chunk} is not first chunk "
              f"of index {head_index} (expected {head_index * cpi}). "
              f"Run backfill to complete index {head_index}.")

    # Find contiguous tails — walk forward from head_chunk, stop at first gap
    durable_tail_lfs    = contiguous_tail(lfs_set, head_chunk)     # last contiguous lfs chunk
    durable_tail_events = contiguous_tail(events_set, head_chunk)  # last contiguous events chunk
    durable_tail        = min(durable_tail_lfs, durable_tail_events)  # both must be present

    # Verify complete indexes have index:{N}:txhash keys.
    # durable_tail // cpi gives the index containing the tail chunk.
    for index_id in range(head_index, durable_tail // cpi + 1):
        idx_last = (index_id + 1) * cpi - 1    # last chunk of this index
        if idx_last > durable_tail:
            break    # partial index at tail — streaming will complete
        if not meta_store.has(f"index:{index_id:08d}:txhash"):
            # All chunks present but RecSplit not built — BUILD_READY
            # (handled in step 6, not an error)
            pass

    # First start in streaming mode only: verify backfill chunks have all three flags
    if not meta_store.has("streaming:last_committed_ledger"):
        for chunk_id in range(head_chunk, durable_tail + 1):
            index_id = chunk_id // cpi
            if meta_store.has(f"index:{index_id:08d}:txhash"):
                continue    # complete index — txhash flags already cleaned up
            if not meta_store.has(f"chunk:{chunk_id:08d}:txhash"):
                fatal(f"first start in streaming mode: chunk {chunk_id} missing txhash flag "
                      f"in incomplete index {index_id} — run backfill to complete")

    return head_chunk, durable_tail
```

**Example — first start in streaming mode after backfill:**
```
Backfill ran: --start-ledger 20_000_002 --end-ledger 56_340_001
Expanded to chunks 2000-5633. chunks_per_txhash_index = 1000.

Meta store:
  chunk:00002000:lfs through chunk:00005633:lfs = "1"         (3634 chunks)
  chunk:00002000:txhash through chunk:00005633:txhash = "1"
  chunk:00002000:events through chunk:00005633:events = "1"
  index:00000002:txhash = "1"  (chunks 2000-2999)
  index:00000003:txhash = "1"  (chunks 3000-3999)
  index:00000004:txhash = "1"  (chunks 4000-4999)
  index:00000005:txhash = absent (chunks 5000-5633 done, 5634-5999 missing)

Validation:
  head_chunk = 2000 → 2000 % 1000 == 0 → index-aligned
  durable_tail = 5633
  Indexes 2,3,4 → COMPLETE
  Index 5: partial (634 of 1000 chunks)
  First start in streaming mode: all backfill chunks 2000-5633 have txhash flags → valid

Result: head_chunk=2000, durable_tail=5633
```

### Step 3: Load Backfill TxHash Data into RocksDB

- If backfill left a partial txhash index, `.bin` files exist for the backfill chunks. Streaming loads these into the RocksDB txhash store — needed for query serving and as the single input source for RecSplit.
- If backfill ended on an index boundary, no `.bin` files exist — this step is a no-op
- Runs on every startup for robustness. On restarts, the loop finds no `txhash` flags and skips.

```python
def load_backfill_bins(config, meta_store):
    cpi = config.chunks_per_txhash_index

    # 1. Clean up complete indexes with leftover .bin files
    #    (backfill crashed after RecSplit but before cleanup_txhash)
    for index_id in all_indexes_with_txhash_key(meta_store):
        for chunk_id in range(index_id * cpi, (index_id + 1) * cpi):
            if meta_store.has(f"chunk:{chunk_id:08d}:txhash"):
                meta_store.delete(f"chunk:{chunk_id:08d}:txhash")
                delete_if_exists(raw_txhash_path(chunk_id))

    # 2. Load .bin files for current incomplete index into RocksDB txhash store
    #    IMPORTANT: open existing store (WAL recovery), do NOT delete-and-recreate —
    #    previously loaded chunks' .bin files are already deleted
    txhash_store = open_or_create_txhash_store(config, current_incomplete_index(meta_store))
    for chunk_id in chunks_for_current_incomplete_index(meta_store, cpi):
        if not meta_store.has(f"chunk:{chunk_id:08d}:txhash"):
            continue    # already loaded (flag deleted), or streaming chunk (no .bin)
        bin_path = raw_txhash_path(chunk_id)
        if os.path.exists(bin_path):
            load_bin_into_rocksdb(bin_path, txhash_store)   # idempotent writes
        meta_store.delete(f"chunk:{chunk_id:08d}:txhash")   # delete flag first
        delete_if_exists(bin_path)                            # delete .bin second

    # 3. Sweep orphaned .bin files (flag gone, file lingering from prior crash)
    for bin_file in scan_bin_files_for_index(current_incomplete_index(meta_store)):
        chunk_id = parse_chunk_id(bin_file)
        if not meta_store.has(f"chunk:{chunk_id:08d}:txhash"):
            os.remove(bin_file)
```

**Crash safety for .bin loading (step 3):**
- If crash during loading: `streaming:last_committed_ledger` is still absent → next startup redoes the sequence
- Already-loaded chunks: `.bin` deleted, flag deleted, data in RocksDB via WAL recovery
- Not-yet-loaded chunks: `.bin` and flag still present → loop picks up where it left off
- The txhash store MUST be opened (WAL recovery), never deleted-and-recreated — already-loaded chunks' `.bin` files are gone and cannot be re-read

### Step 4: Reconcile Orphaned Transitions

```python
def reconcile_orphaned_transitions(config, meta_store):
    last_committed = meta_store.get("streaming:last_committed_ledger")
    if last_committed is None:
        return    # first start in streaming mode — no prior streaming state to reconcile

    # Derive which chunk and index the checkpoint falls in.
    # Subtract 2 because Stellar ledgers start at 2, not 0.
    current_chunk = (last_committed - 2) // 10_000    # chunk containing the last committed ledger
    current_index = current_chunk // config.chunks_per_txhash_index  # index containing that chunk

    # Orphaned transitioning ledger stores
    for store_dir in scan_ledger_store_dirs(config):
        chunk_id = parse_chunk_id_from_dir(store_dir)
        if chunk_id == current_chunk:
            continue    # active store — keep (WAL recovery)
        if chunk_id == current_chunk + 1:
            continue    # pre-created store — keep
        if meta_store.has(f"chunk:{chunk_id:08d}:lfs"):
            delete_dir(store_dir)    # flag set, cleanup didn't finish → delete
        elif chunk_id < current_chunk:
            # transitioning store, flush didn't complete → complete it
            complete_lfs_flush(store_dir, chunk_id, meta_store)
        else:
            delete_dir(store_dir)    # orphaned future store → delete

    # Orphaned transitioning txhash stores
    for store_dir in scan_txhash_store_dirs(config):
        index_id = parse_index_id_from_dir(store_dir)
        if index_id == current_index:
            continue    # active store — keep
        if meta_store.has(f"index:{index_id:08d}:txhash"):
            delete_dir(store_dir)    # complete, cleanup didn't finish → delete
        # BUILD_READY handled in step 6
```

### Step 5: Replay Missed Boundary Handling

- Handles the case where `streaming:last_committed_ledger` is exactly at a boundary but transitions never fired (crash between checkpoint and transitions)

```python
def replay_missed_boundaries(config, meta_store):
    last_committed = meta_store.get("streaming:last_committed_ledger")
    if last_committed is None:
        return

    current_chunk = (last_committed - 2) // 10_000    # subtract 2: ledgers start at 2
    cpi = config.chunks_per_txhash_index

    # Check if checkpoint is exactly at a chunk boundary.
    # chunk_last_ledger(C) = ((C + 1) * 10_000) + 1, the last ledger that belongs to chunk C.
    # If checkpoint == this value, the chunk is fully ingested but transitions may not have fired.
    if last_committed == chunk_last_ledger(current_chunk):
        if not meta_store.has(f"chunk:{current_chunk:08d}:lfs"):
            trigger_lfs_flush(current_chunk, config, meta_store)
        if not meta_store.has(f"chunk:{current_chunk:08d}:events"):
            trigger_events_freeze(current_chunk, config, meta_store)

    # Check if checkpoint is also at an index boundary.
    # The last chunk of index N is ((N + 1) * cpi) - 1.
    # If current_chunk equals this, the index is fully ingested.
    current_index = current_chunk // cpi
    if current_chunk == (current_index + 1) * cpi - 1:
        # Index boundary — handled by step 6 (BUILD_READY detection)
        pass
```

**Example — crash between checkpoint and chunk boundary transitions:**
```
streaming:last_committed_ledger = 56_370_001 (= chunk_last_ledger(5636))
chunk:00005636:lfs = absent (swap never happened, flush never spawned)
chunk:00005636:events = absent (freeze never started)
Active ledger store: ledger-store-chunk-005636/ (has all 10_000 ledgers via WAL)

Detection: 56_370_001 == chunk_last_ledger(5636) AND flags absent
Action: trigger LFS flush for chunk 5636, trigger events freeze for chunk 5636
```

---

## Ingestion Loop

### Per-Ledger Processing

```python
def run_ingestion_loop(core, active_stores, meta_store):
    for lcm in core.stream_ledgers():
        ledger_seq = lcm.ledger_sequence
        process_ledger(ledger_seq, lcm, active_stores, meta_store)

def process_ledger(ledger_seq, lcm, active_stores, meta_store):
    # 1. Write to all three stores in parallel goroutines.
    #    Each store's write is atomic (WriteBatch + WAL for RocksDB,
    #    atomic commit for events hot segment).
    run_in_background(write_ledger_store, active_stores.ledger, ledger_seq, lcm)
    run_in_background(write_txhash_store, active_stores.txhash, ledger_seq, lcm)
    run_in_background(write_events_hot_segment, active_stores.events, ledger_seq, lcm)
    wait_for_all()    # all three must succeed

    # 2. Set per-ledger checkpoint AFTER all writes succeed.
    #    INVARIANT: checkpoint is written ONLY after all three stores
    #    have durably committed the ledger data (WAL flush).
    meta_store.put("streaming:last_committed_ledger", ledger_seq)

    # 3. If chunk boundary: trigger sub-flow transitions.
    #    Transitions happen AFTER checkpoint — a crash between checkpoint
    #    and transitions is detected on startup (see replay_missed_boundaries).
    #    Subtract 2 because Stellar ledgers start at 2.
    current_chunk = (ledger_seq - 2) // 10_000
    #    chunk_last_ledger(C) = ((C+1) * 10_000) + 1 — the last ledger in chunk C.
    #    Equality means this ledger completes the chunk.
    if ledger_seq == chunk_last_ledger(current_chunk):
        on_chunk_boundary(current_chunk, active_stores, meta_store)

    # 4. If index boundary: trigger index-level transitions.
    #    The index boundary ledger is always also a chunk boundary ledger
    #    (chunk boundaries align exactly with index boundaries).
    current_index = current_chunk // chunks_per_txhash_index
    #    index_last_ledger(N) = ((N+1) * cpi * 10_000) + 1 — the last ledger in index N.
    if ledger_seq == index_last_ledger(current_index):
        on_index_boundary(current_index, active_stores, meta_store)
```

### Per-Store Write Details

**Ledger store write:**
```python
def write_ledger_store(store, ledger_seq, lcm):
    key = uint32_big_endian(ledger_seq)
    value = zstd_compress(lcm.to_bytes())
    store.put(key, value)    # WriteBatch + WAL
```

**TxHash store write:**
```python
def write_txhash_store(store, ledger_seq, lcm):
    batch = WriteBatch()
    for tx in lcm.transactions:
        cf_name = f"cf-{tx.hash[0] >> 4:x}"           # route by first nibble
        batch.put_cf(cf_name, tx.hash, uint32_big_endian(ledger_seq))
    store.write(batch)    # single WriteBatch across all CFs + WAL
```

**Events hot segment write:**
```python
def write_events_hot_segment(hot_segment, ledger_seq, lcm):
    events = extract_contract_and_system_events(lcm)   # excludes diagnostic events
    for event in events:
        event_id = hot_segment.next_event_id()
        hot_segment.store_event(event_id, event)       # persist event data
        for term_key in index_terms(event):             # contractId + topic0-3
            hot_segment.add_to_bitmap(term_key, event_id)    # in-memory bitmap update
    hot_segment.persist_deltas(ledger_seq)             # (term_key, event_id) pairs → DB for crash recovery
    hot_segment.update_offset_array(ledger_seq)        # cumulative event count
    hot_segment.commit(ledger_seq)                     # atomic commit
```

---

## Sub-flow Transitions

- Three independent sub-flows, each with its own goroutine, flag, and cleanup step
- No combined transitions — each sub-flow waits for its own predecessor only

### Chunk Boundary (every 10_000 ledgers)

Triggered when `ledger_seq == chunk_last_ledger(current_chunk)`:

```python
def on_chunk_boundary(chunk_id, active_stores, meta_store):
    # ── LFS sub-flow ──
    # Wait for OWN predecessor only (max 1 transitioning ledger store)
    wait_for_lfs_complete()
    transitioning_ledger = active_stores.ledger
    active_stores.ledger = open_precreated_ledger_store(chunk_id + 1)
    run_in_background(lfs_transition, chunk_id, transitioning_ledger, meta_store)

    # ── Events sub-flow ──
    # Wait for OWN predecessor only (max 1 freezing events segment)
    wait_for_events_complete()
    freezing_segment = active_stores.events
    active_stores.events = create_events_hot_segment(chunk_id + 1)
    run_in_background(events_transition, chunk_id, freezing_segment, meta_store)
```

### LFS Transition (background goroutine)

```python
def lfs_transition(chunk_id, transitioning_ledger_store, meta_store):
    # ── Transition: read from RocksDB, write pack file ──
    pack_path = ledger_pack_path(chunk_id)
    first_ledger = chunk_first_ledger(chunk_id)
    last_ledger  = chunk_last_ledger(chunk_id)

    writer = packfile.create(pack_path, overwrite=True)    # handles partial files
    for seq in range(first_ledger, last_ledger + 1):
        lcm_bytes = transitioning_ledger_store.get(uint32_big_endian(seq))
        writer.append(lcm_bytes)
    writer.fsync_and_close()
    meta_store.put(f"chunk:{chunk_id:08d}:lfs", "1")      # flag after fsync

    # ── Cleanup (separate step — if crash here, flag is set, retry cleanup) ──
    transitioning_ledger_store.close()
    delete_dir(ledger_store_path(chunk_id))
    signal_lfs_complete()
```

### Events Transition (background goroutine)

```python
def events_transition(chunk_id, freezing_segment, meta_store):
    # ── Transition: freeze hot segment to cold segment ──
    events_path = events_segment_path(chunk_id)

    # Write three cold segment files:
    #   {chunkID:08d}-events.pack  — zstd-compressed event blocks
    #   {chunkID:08d}-index.pack   — serialized roaring bitmaps
    #   {chunkID:08d}-index.hash   — MPHF for term lookup
    write_cold_segment(freezing_segment, events_path)
    fsync_all(events_path)
    meta_store.put(f"chunk:{chunk_id:08d}:events", "1")    # flag after fsync

    # ── Cleanup (separate step) ──
    freezing_segment.discard()    # delete persisted deltas + in-memory bitmaps
    signal_events_complete()
```

### Index Boundary (every 10_000_000 ledgers)

- Triggered when `ledger_seq == index_last_ledger(current_index)`
- The index boundary ledger is always also a chunk boundary ledger — `on_chunk_boundary` fires first, then `on_index_boundary`

```python
def on_index_boundary(index_id, active_stores, meta_store):
    # Wait for ALL chunk-level sub-flows to complete
    # (the last chunk's LFS flush and events freeze must finish
    #  before the txhash store can be promoted)
    wait_for_lfs_complete()
    wait_for_events_complete()

    # Defense-in-depth: verify all chunk flags for the index
    verify_all_chunk_flags(index_id, meta_store)

    # ── TxHash sub-flow ──
    transitioning_txhash = active_stores.txhash
    active_stores.txhash = open_precreated_txhash_store(index_id + 1)

    run_in_background(recsplit_transition, index_id, transitioning_txhash, meta_store)
```

### RecSplit Transition (background goroutine)

```python
def recsplit_transition(index_id, transitioning_txhash_store, meta_store):
    # ── Transition: build RecSplit from RocksDB ──
    # RecSplit builder reads from the transitioning txhash store (RocksDB, 16 CFs).
    # This is the ONLY input source — both backfill .bin data (loaded at startup)
    # and streaming txhash data are in the same RocksDB store.
    #
    # Contrast with backfill: backfill builds RecSplit from .bin flat files.
    # Streaming builds RecSplit from RocksDB.

    idx_path = recsplit_index_path(index_id)
    delete_partial_idx_files(idx_path)    # clean up any partial files from prior crash

    # Build all 16 CF index files — what happens inside (goroutines,
    # parallelism, memory) is up to the task. All-or-nothing.
    build_recsplit(transitioning_txhash_store, idx_path)
    fsync_all_idx_files(idx_path)

    # Verify: spot-check random ledgers and txhashes against immutable stores
    verify_spot_check(index_id, idx_path, meta_store)

    meta_store.put(f"index:{index_id:08d}:txhash", "1")    # flag after fsync + verify

    # ── Cleanup (separate step) ──
    transitioning_txhash_store.close()
    delete_dir(txhash_store_path(index_id))
```

### Transition Dependencies

```python
# Per chunk boundary:
#   lfs_transition(C)    → cleanup: delete ledger store   → unblocks lfs_transition(C+1)
#   events_transition(C) → cleanup: delete persisted deltas → unblocks events_transition(C+1)
#
# Per index boundary (= last chunk boundary of that index):
#   ALL lfs_transition + events_transition for the index must complete
#       → recsplit_transition(N) → cleanup: delete txhash store
```

- Each sub-flow waits for its own predecessor at chunk boundaries
- At index boundary: ALL sub-flows must complete before RecSplit starts
- Cleanup is a separate step after the flag — crash after flag = retry just cleanup on restart

---

## Crash Recovery

### Invariants

Six invariants handle all crash recovery. No special-case logic outside these.

1. **Flag-after-fsync** — meta store flags set only after corresponding files are fsynced
   - Flag absent = output treated as missing → transition retried from scratch
2. **Idempotent writes** — same input ledger always produces same key-value pairs in all stores
   - Re-processing after crash is always safe
3. **Per-ledger checkpoint** — `streaming:last_committed_ledger` written only after all three stores durably commit
   - On crash: resume from `last_committed_ledger + 1`
   - Events system truncates hot segment data beyond checkpoint on startup (prevents duplicate event IDs)
4. **No separate recovery phase** — startup derives state from meta store keys + on-disk artifacts, completes or discards incomplete work
   - Same code path for first start, restarts, and crash recovery
5. **Max-1-transitioning per sub-flow** — previous transition must complete before next starts
   - Applies to both steady-state and crash recovery
6. **DAG-structured cleanup** — cleanup runs as a separate step after flag is set
   - Crash between flag and cleanup: flag is durable, startup retries just the cleanup
   - The transition itself is never re-run

### Startup Validation Guards

Four validation rules prevent starting in an invalid state:

- **`chunks_per_txhash_index` immutable** — fatal if changed after first run
- **Head index-aligned** — fatal if the lowest chunk is not the first chunk of its index
- **Contiguous flags** — fatal if any gap exists in `lfs` or `events` flags within backfill's range
- **Backfill completeness (first start in streaming mode)** — fatal if any backfill chunk is missing a `txhash` flag in an incomplete index

### How Invariants Resolve the Hardest Scenarios

**Compound recovery — orphaned transition from chunk C-1 + missed boundary for chunk C:**
```
State:  streaming:last_committed_ledger = 56_380_001 (= chunk_last_ledger(5637))
        chunk:00005636:events = absent (freeze crashed)
        chunk:00005637:lfs = absent (transitions never started)
        chunk:00005637:events = absent
        Events DB has persisted deltas for chunks 5636 + 5637

Invariants applied:
  - Flag-after-fsync (#1): chunk 5636 events flag absent → freeze is retried
  - No separate recovery (#4): startup detects absent flags, triggers transitions
  - Max-1-transitioning (#5): 5636 freeze completes BEFORE 5637 freeze starts
  - Per-ledger checkpoint (#3): resume from 56_380_002 after transitions complete
```

**Checkpoint-boundary gap — crash between checkpoint and boundary transitions:**
```
State:  streaming:last_committed_ledger = 56_370_001 (= chunk_last_ledger(5636))
        chunk:00005636:lfs = absent (swap never happened)
        chunk:00005636:events = absent (freeze never started)

Invariants applied:
  - Per-ledger checkpoint (#3): checkpoint is at the boundary, data is in WAL
  - No separate recovery (#4): startup detects checkpoint at boundary + absent flags →
    triggers LFS flush and events freeze before resuming
  - Idempotent writes (#2): if any partial data exists from a half-started transition,
    re-writing with overwrite=True produces identical results
```

**Crash during .bin loading (step 3) — partial load, some .bin files already deleted:**
```
State:  chunks 5000-5399: .bin deleted, txhash flag deleted, data in RocksDB (WAL)
        chunks 5400-5633: .bin present, txhash flag present, not yet loaded
        streaming:last_committed_ledger = absent

Invariants applied:
  - Per-ledger checkpoint (#3): absence of checkpoint signals "first start in streaming mode" → redo sequence
  - Idempotent writes (#2): WAL-recovered data for 5000-5399 survives; loop loads 5400-5633
  - Flag-after-fsync (#1): txhash flags track which .bin files are pending (flag deleted = loaded)
  - CRITICAL: must open existing txhash store (WAL recovery), not delete-and-recreate —
    chunks 5000-5399 data would be lost since .bin files are gone
```

### Recovery Decision Tree

```python
def recover_on_startup(config, meta_store):
    last_committed = meta_store.get("streaming:last_committed_ledger")

    if last_committed is None:
        # First start in streaming mode — no prior streaming checkpoint exists.
        # Validate that backfill left complete data, then load .bin files into RocksDB.
        validate_backfill_flags(config, meta_store)
        load_backfill_bins(config, meta_store)
        last_committed = chunk_last_ledger(durable_tail)
        meta_store.put("streaming:last_committed_ledger", last_committed)

    # Reconcile: complete orphaned transitions, delete orphaned artifacts.
    # Handles crashes during LFS flush, events freeze, or RecSplit build.
    reconcile_orphaned_transitions(config, meta_store)

    # Replay missed boundary transitions.
    # If the checkpoint landed exactly on a chunk boundary but the process crashed
    # before the boundary transitions (swap store, spawn flush/freeze) fired,
    # the flags for that chunk are absent. Detect and trigger them now.
    current_chunk = (last_committed - 2) // 10_000    # subtract 2: Stellar ledgers start at 2
    if last_committed == chunk_last_ledger(current_chunk):
        if not meta_store.has(f"chunk:{current_chunk:08d}:lfs"):
            trigger_lfs_flush(current_chunk)
        if not meta_store.has(f"chunk:{current_chunk:08d}:events"):
            trigger_events_freeze(current_chunk)

    # Spawn background RecSplit for any BUILD_READY indexes.
    # An index is BUILD_READY when all chunk lfs+events flags are set but
    # index:{N:08d}:txhash is absent (RecSplit not yet built or crashed mid-build).
    for index_id in indexes_with_all_chunk_flags_but_no_index_flag(meta_store):
        run_in_background(recsplit_transition, index_id)

    resume_ledger = last_committed + 1
    return meta_store, resume_ledger
```

---

## Meta Store Keys

### Keys Introduced by Streaming

| Key | Value | Written When |
|---|---|---|
| `streaming:last_committed_ledger` | `uint32` | After every successfully committed ledger |
| `config:chunks_per_txhash_index` | `uint32` | On first run (backfill or streaming) — immutable |

### Keys Shared with Backfill

| Key | Written By | Notes |
|---|---|---|
| `chunk:{C:08d}:lfs` | Both | Backfill: after pack file fsync in `process_chunk`. Streaming: after LFS flush goroutine fsync. |
| `chunk:{C:08d}:events` | Both | Backfill: after cold segment fsync in `process_chunk`. Streaming: after events freeze goroutine fsync. |
| `chunk:{C:08d}:txhash` | Backfill only | After `.bin` file fsync. Streaming does NOT write `.bin` files — txhash data goes to RocksDB. Flags deleted during startup step 3 (.bin loading). |
| `index:{N:08d}:txhash` | Both | After all 16 RecSplit CF `.idx` files built + fsynced. Backfill: from `.bin` files. Streaming: from RocksDB txhash store. |

### Key Lifecycle in Streaming

```
Per ledger:
  streaming:last_committed_ledger = ledger_seq    (after all 3 stores commit)

Per chunk (background, after chunk boundary):
  chunk:{C:08d}:lfs    = "1"                      (after pack file fsync)
  chunk:{C:08d}:events = "1"                      (after cold segment fsync)

Per index (background, after index boundary):
  index:{N:08d}:txhash = "1"                      (after all 16 .idx files fsync + verify)

Startup step 3 (first start in streaming mode only):
  chunk:{C:08d}:txhash → DELETED                  (after .bin loaded into RocksDB)
  .bin files → DELETED                             (after flag deleted)
```

---

## Backpressure and Drift Detection

- Streaming ingests ledgers at the Stellar network's production rate (~1 ledger every 6 seconds)
- All transitions (LFS flush, events freeze, RecSplit build) run in background goroutines — the ingestion loop should never stall
- If ingestion falls behind, the cause is typically disk I/O saturation or RocksDB compaction stalls

### Drift Metric

```python
drift_ledgers = network_tip_ledger - last_committed_ledger
```

- `network_tip_ledger` obtained from CaptiveStellarCore metadata
- Exposed as a Prometheus gauge: `streaming_drift_ledgers`
- Threshold: **10 ledgers** (~60 seconds). More than 10 ledgers behind means something is wrong — transitions are background and should not cause drift.

### Health Endpoint

- `getHealth` returns unhealthy when `drift_ledgers > drift_warning_ledgers` (default 10)
- Kubernetes readiness probes use `getHealth` to remove the node from the service pool
- No automatic response (no pause, no abort) — the operator investigates and acts

---

## Error Handling

| Error | Action |
|---|---|
| CaptiveStellarCore unavailable | RETRY with backoff; ABORT after N retries |
| Ledger store write failure | ABORT — disk full or storage corruption |
| TxHash store write failure | ABORT — disk full or storage corruption |
| Events hot segment write failure | ABORT — disk full or storage corruption |
| Meta store write failure | ABORT — cannot maintain checkpoint |
| LFS flush failure (pack file write/fsync) | Do NOT set `chunk:{C:08d}:lfs`; ABORT transition goroutine; restart re-triggers flush |
| Events freeze failure (cold segment write/fsync) | Do NOT set `chunk:{C:08d}:events`; ABORT transition goroutine; restart re-triggers freeze |
| RecSplit build failure | Do NOT set `index:{N:08d}:txhash`; ABORT transition goroutine; restart deletes partials and rebuilds |
| RecSplit verification mismatch | ABORT; do NOT delete transitioning txhash store; log error; operator investigates |
| Startup: `chunks_per_txhash_index` changed | FATAL — cannot change after first run |
| Startup: head not index-aligned | FATAL — run backfill to complete the head index |
| Startup: gap in chunk flags | FATAL — run backfill to fill the gap |
| Startup: backfill chunk missing txhash flag (first start in streaming mode) | FATAL — run backfill to complete |

---

## Query Routing

- Covered in a separate design document
- Summary routing table for reference:

| Query | Active phase | Transitioning phase | Complete phase |
|---|---|---|---|
| `getLedger` | Active ledger store (or LFS for already-flushed chunks) | LFS (all ledger stores already flushed) | LFS |
| `getTransaction` | Active txhash store (RocksDB CF lookup) | Transitioning txhash store (still open for reads) | RecSplit index |
| `getEvents` | Events hot segment (in-memory bitmap lookup) | Freezing segment (still serves reads) | Cold segment (MPHF + packfile) |

---

## Related Documents

- [01-backfill-workflow.md](./01-backfill-workflow.md) — backfill pipeline, DAG tasks, partial index handling
- [getEvents full-history design](../../design-docs/getevents-full-history-design.md) — events hot/cold segments, bitmap indexes, freeze process
- Query routing — separate design document (TBD)
