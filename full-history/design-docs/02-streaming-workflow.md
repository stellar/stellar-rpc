# Streaming Workflow

## Overview

stellar-rpc is the **unified full-history RPC service** — historical backfill and live streaming under one binary, one invocation, one long-running process.

- Operator runs `stellar-rpc --config path/to/config.toml`. No subcommand. No `--mode` flag. No behavior-switching flags.
- On every start, the service runs four sequential startup phases, then enters a live ingestion loop it stays in until killed.
- Behavior across the three operator profiles — **archive** (full history), **pruning-history** (retention-windowed history with bulk catchup from a remote object store), **tip-tracker** (retention-windowed history, no object store; captive-core-only) — is determined entirely by TOML config; no profile flag. Full matrix: [Operator Profiles](#operator-profiles).
- Backfill (specified in [01-backfill-workflow.md](./01-backfill-workflow.md)) is used as an internal subroutine by Phase 1 (catchup). Operators never invoke backfill directly.

**What the service does end-to-end:**
- Validates config against immutable meta-store state (`CHUNKS_PER_TX_INDEX`, `RETENTION_LEDGERS`).
- Catches up to the current network tip using **BSB** (Buffered Storage Backend — remote object-store reader for `LedgerCloseMeta`) or captive core (embedded `stellar-core` subprocess), whichever is configured. See [Ledger Source](#ledger-source).
- Hydrates any in-flight state left by a prior run.
- Ingests live ledgers from captive core into three **active RocksDB stores** (per-chunk ledger + events, per-index txhash). See [Active Store Architecture](#active-store-architecture).
- Freezes active stores to immutable files at chunk and index boundaries in background.
- Prunes past-retention indexes atomically when retention is configured.
- Serves `getLedger` / `getTransaction` / `getEvents` only after startup phases complete; returns HTTP 4xx during startup.

---

## Geometry

See [01-backfill-workflow.md — Geometry](./01-backfill-workflow.md#geometry). Streaming uses the same constants (`GENESIS_LEDGER`, `LEDGERS_PER_CHUNK`, `LEDGERS_PER_TX_INDEX`, `CHUNKS_PER_TX_INDEX`), mapping functions, and derived helpers.

---

## Configuration

Streaming reads the same TOML file as backfill, plus additional keys described below.

### Shared Config (from backfill)

These sections come from backfill — see [01-backfill-workflow.md — Configuration](./01-backfill-workflow.md#configuration) for the full schemas:

- `[SERVICE]` — service-wide settings (`DEFAULT_DATA_DIR`, `CHUNKS_PER_TX_INDEX`).
- `[BSB]` — Buffered Storage Backend source settings.
- `[IMMUTABLE_STORAGE.*]` — on-disk paths for immutable artifacts (ledger packs, events, raw txhash, txhash index).
- `[META_STORE]` — meta-store RocksDB path.
- `[LOGGING]` — log level + format.

Streaming extends `[SERVICE]` with extra keys and introduces `[CAPTIVE_CORE]` (embedded `stellar-core` subprocess settings), `[ACTIVE_STORAGE]` (active RocksDB paths), and `[HISTORY_ARCHIVES]` (Stellar history-archive URLs for tip sampling).

### Immutable Keys (stored in meta store, fatal if changed)

Stored on first start; fatal on any subsequent start where the config value differs. Changing either requires wiping the datadir.

| Key | Stored under | Set by | Rule |
|---|---|---|---|
| `CHUNKS_PER_TX_INDEX` | `config:chunks_per_tx_index` | first run | Fatal if changed. |
| `RETENTION_LEDGERS` | `config:retention_ledgers` | first run | Fatal if changed. |

Source selection (BSB vs captive core) is determined per-startup by `[BSB]` presence; operators _may add or remove_ `[BSB]` between runs. `RETENTION_LEDGERS` already pins the retained window, so locking the source choice would add nothing.

### Streaming TOML Config

**[SERVICE] — streaming additions**

Extends the `[SERVICE]` table in [01-backfill-workflow.md — Configuration](./01-backfill-workflow.md#configuration)

| Key | Type | Default | Description                                                                                                                                                  |
|---|---|---|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `RETENTION_LEDGERS` | uint32 | `0` | `0` = full history; otherwise must be a positive multiple of `LEDGERS_PER_TX_INDEX`. See [Validation Rules](#validation-rules).                                 |
| `NETWORK_PASSPHRASE` | string | **required** | Stellar network passphrase. Must match the `NETWORK_PASSPHRASE` in the captive-core config file. |

**[CAPTIVE_CORE]**

| Key | Type | Default | Description |
|---|---|---|---|
| `CONFIG_PATH` | string | **required** | Path to the captive-core TOML config file (consumed by the embedded `stellar-core` subprocess). |
| `STELLAR_CORE_BINARY_PATH` | string | **required** | Path to the `stellar-core` binary that captive core spawns as a subprocess. |

**[ACTIVE_STORAGE]** (optional)

| Key | Type | Default | Description |
|---|---|---|---|
| `PATH` | string | `{DEFAULT_DATA_DIR}/active` | Base path for active RocksDB stores (ledger, txhash, events). |

**[HISTORY_ARCHIVES]**

| Key | Type | Default | Description                                                                                                                                                                                                                |
|---|---|---|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `URLS` | []string | **required** | List of Stellar history archive URLs. Used to sample network tip for the no-BSB resume-from-ledger calculation  |

**[BSB]** (optional) — same schema as in the backfill doc; presence determines Phase 1 (catchup) behavior. See [Operator Profiles](#operator-profiles).

### CLI Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--config` | string | **required** | Path to TOML config file. |

**No other flags.** No `--mode`; no `--start-ledger`, `--end-ledger`; no subcommand for backfill or streaming.
Per-run behavior is driven by config or derived at runtime from meta store + tip.

### Validation Rules

- `CHUNKS_PER_TX_INDEX` and `RETENTION_LEDGERS` are immutable across runs (see [Immutable Keys](#immutable-keys-stored-in-meta-store-fatal-if-changed)).
- `RETENTION_LEDGERS` must be `0` OR a positive integer multiple of `LEDGERS_PER_TX_INDEX`. Valid at cpi=1_000: `0`, `10_000_000`, `20_000_000`, ...; invalid: `5_000_000`, `15_000_000`. Pruning is whole-index — non-aligned windows would leave partial indexes perpetually on disk.
- **`[BSB]` absent AND `RETENTION_LEDGERS = 0` is fatal.** Full history requires BSB — captive-core archive-catchup from genesis would take weeks-to-months.
- `[HISTORY_ARCHIVES].URLS`, `[CAPTIVE_CORE].CONFIG_PATH`, `[CAPTIVE_CORE].STELLAR_CORE_BINARY_PATH`, `[SERVICE].NETWORK_PASSPHRASE` are required in all profiles.

### Validation Pseudocode

```python
def validate_config(config, meta_store):
    apply_static_rules(config)   # see "Validation Rules" above
    _enforce_immutable(meta_store, "config:chunks_per_tx_index", str(config.service.chunks_per_tx_index))
    _enforce_immutable(meta_store, "config:retention_ledgers",   str(config.service.retention_ledgers))


def _enforce_immutable(meta_store, key, current_value):
    stored = meta_store.get(key)
    if stored is None:
        meta_store.put(key, current_value)
    elif stored != current_value:
        fatal(f"{key} changed: stored={stored}, config={current_value}. Wipe datadir.")
```

### Operator Profiles

Three profiles emerge from config combinations. No separate profile flag.

| Profile | `RETENTION_LEDGERS` | `[BSB]` | Use case | Backfill behavior |
|---|---|---|---|---|
| Archive | `0` | present | Public archive node; full history. | Backfill over full history (chunks `[0, bsb_tip_chunk]`) |
| Pruning-history | `N × LEDGERS_PER_TX_INDEX`, N ≥ 1 | present | Windowed history with bulk initial catchup. | Backfill over retention window (start aligned to first chunk of the tx index containing the retention floor) |
| Tip-tracker | `N × LEDGERS_PER_TX_INDEX`, N ≥ 1 | absent | App developer; short retention; no object-store dep. | **No-op.** Phase 4 (live ingestion)'s captive core archive-catches-up from a `resume_ledger` aligned to the retention-aligned tx-index boundary |
| (invalid) | `0` | absent | Rejected by `validate_config`: full history requires BSB. | — |

---

## Meta Store Keys

Single RocksDB instance, WAL always enabled. Authoritative source for every startup decision. Reference for the schema and lifecycle below; reads more naturally after [Startup Sequence](#startup-sequence) defines the phases that write and consume these keys.

### Keys Introduced by Streaming

| Key | Value | Written when |
|--|-|--|
| `streaming:last_committed_ledger` | uint32 | Monotonic progress marker; written via `advance_progress_marker`. Two writers: Phase 1 (post-catchup) and the live ingestion loop (per ledger). |
| `config:retention_ledgers` | uint32 | First run (stored); enforced on subsequent starts. |
| `hot:chunk:{chunk_id:08d}:lfs` | `"1"` | Set BEFORE active ledger store dir is created; cleared AFTER dir is removed by freeze. Presence ⇒ dir exists or its lifecycle is incomplete. |
| `hot:chunk:{chunk_id:08d}:events` | `"1"` | Same pattern, active events store dir. |
| `hot:index:{tx_index_id:08d}:txhash` | `"1"` | Same pattern, active txhash store dir. Per-index cadence (one per tx index). |
| `pruning:index:{tx_index_id:08d}` | `"1"` | Set by `prune_tx_index` BEFORE any file delete; cleared AFTER everything else (artifacts + `index:{N}:txhash`). QueryRouter returns 4xx while present; `prunable_tx_index_ids` re-enqueues N if the marker survives a crash. |

### Keys Shared with Backfill

Defined in [01-backfill-workflow.md — Meta Store Keys](./01-backfill-workflow.md#meta-store-keys); streaming uses the same contract:

- `config:chunks_per_tx_index`
- `chunk:{chunk_id:08d}:lfs`
- `chunk:{chunk_id:08d}:events`
- `chunk:{chunk_id:08d}:txhash`
- `index:{tx_index_id:08d}:txhash`

All values are binary (`"1"` or absent); prune-in-progress is tracked via the separate `pruning:index:*` key family rather than overloading the value space.

### Flag Semantics

- **Flag-after-fsync** — a freeze flag is set only AFTER its artifact is fsynced. Present ⇒ artifact durable.
- **File-before-flag-delete** — cleanup paths delete the file/dir FIRST, clear the key LAST. Reverse order would orphan a file with no meta-store record on a crash mid-pair, recoverable only by filesystem scan.
- **Hot keys before mkdir, cleared after dir-delete** — every active store dir has a `hot:*` key set BEFORE `mkdir` and cleared AFTER `delete_dir_if_exists`.

_**The meta-store flag is the always-correct signal of artifact state on disk, both for immutable files and for active-store directories. A crash anywhere leaves a state the next start recovers from by flag presence alone — no filesystem scan anywhere.**_

---

## Active Store Architecture

Three RocksDB-backed active stores; WAL always enabled.

- **Ledger** — one per chunk at `{ACTIVE_STORAGE.PATH}/ledger-store-chunk-{chunk_id:08d}/`. Key `uint32BE(ledgerSeq)`, value `zstd(LCM bytes)`.
- **TxHash** — one per tx index at `{ACTIVE_STORAGE.PATH}/txhash-store-index-{tx_index_id:08d}/`. Key `txhash[32]`, value `uint32BE(ledgerSeq)`. 16 column families (`cf-0`..`cf-f`) routed by `txhash[0] >> 4`; each CF pairs 1:1 with one of the 16 RecSplit `.idx` files.
- **Events** — one per chunk at `{ACTIVE_STORAGE.PATH}/events-store-chunk-{chunk_id:08d}/`. Schema per [getEvents full-history design](../../design-docs/getevents-full-history-design.md). Per-ledger writes are idempotent.

### Store Lifecycle

- At every chunk boundary, the next chunk's ledger + events stores open synchronously (~100 ms) while the just-finished ones are handed to background freeze tasks; tx-index boundaries do the same for txhash. Each kind holds at most one active + one transitioning. Ingestion never blocks on the freeze.
- The freeze task deletes the active dir only after the immutable artifact is fsynced and its freeze flag is set.
- Active-store dirs surviving a crash are reconciled by [Phase 3](#phase-3--reconcile).

---

## Ledger Source

Two ledger sources, scoped to different phases:

- **Backfill (Phase 1) uses `BSBSource`** — backfill-only reader (`PrepareRange` + `GetLedger`). Each `process_chunk` constructs its own instance scoped to its chunk's 10_000 ledgers. Captive core cannot be a backfill source — see [Backfill vs Phase 1 (catchup)](#backfill-vs-phase-1-catchup).
- **Live streaming (Phase 4 (live ingestion)) uses captive core directly** — `PrepareRange(UnboundedRange(resume_ledger))` + per-ledger `GetLedger(seq)` against the captive-core subprocess.

---

## Startup Sequence

Four sequential phases on every start. The first three are bounded bootstrap; Phase 4 is the long-running ingestion state.

- **Phase 1 — catchup.** When `[BSB]` is configured, invokes the backfill subroutine in a loop to close the gap between on-disk artifacts and current network tip. Without `[BSB]`, Phase 1 is a no-op and Phase 4's captive core handles initial catchup via `PrepareRange(UnboundedRange(resume_ledger))`.
- **Phase 2 — hydrate txhash.** Loads any `.bin` files Phase 1 left (trailing partial index) into the active txhash store, then deletes them.
- **Phase 3 — reconcile.** Two passes: drop past-retention state, then recover in-flight freezes left by a prior crash.
- **Phase 4 — live ingestion.** Opens active stores, starts captive core, spawns the lifecycle task, enters the ingestion loop. Runs until process exit.

<a id="backfill-vs-phase-1-catchup"></a>**Backfill vs Phase 1 (catchup):** `run_backfill` (subroutine, [01-backfill-workflow.md](./01-backfill-workflow.md)) is BSB-only — captive core's serial subprocess can't be sharded per-chunk like BSB. `phase1_catchup` (startup phase) invokes backfill when `[BSB]` is configured, no-ops otherwise.

### Service Entry Point

```python
def main():
    args = parse_cli_flags()
    config = load_config_toml(args.config)
    run_rpc_service(config)

def run_rpc_service(config):
    meta_store = open_meta_store(config)
    validate_config(config, meta_store)
    start_http_server(config)              # /getHealth servable; getLedger/Tx/Events 4xx until set_service_ready

    # Phases 1-3 + compute_resume_ledger: bring on-disk state into consistency.
    # No query traffic during this window.
    last_phase1_chunk_id = phase1_catchup(config, meta_store)
    phase2_hydrate_txhash(config, meta_store, last_phase1_chunk_id)
    phase3_reconcile(config, meta_store)
    resume_ledger = compute_resume_ledger(config, meta_store)

    # Frozen-artifact queries don't need captive core; flip service_ready here
    # (not after spinup) to avoid an unnecessary 4-5 min outage per restart.
    set_service_ready()
    phase4_live_ingest(config, meta_store, resume_ledger)
```

See [Query Contract](#query-contract) for the query-gating contract.

### Phase 1 — Catchup

```python
def phase1_catchup(config, meta_store) -> Optional[int]:
    """
    Catch up history via BSB; no-op when [BSB] absent.

    Loop samples BSB tip, computes retention-aligned start, runs backfill, and
    repeats until BSB stops advancing. The loop exists because the remote
    object store lags the live network tip — each iteration may surface new
    chunks that landed while the previous backfill was running. After the
    loop, advances the progress marker so Phase 3 / compute_resume_ledger
    see the post-catchup position rather than a stale prior-run value
    (long-downtime correctness).

    Idempotent across restarts — backfill skips already-flagged chunks.
    last_scheduled_end_chunk is local-only and resets every start.

    Returns: highest chunk_id completed (input to Phase 2), or None on no-op.
    """
    if config.bsb is None:
        # Tip-tracker profile. Nothing to backfill.
        return None

    retention_ledgers = config.service.retention_ledgers
    last_scheduled_end_chunk = -1

    while True:
        end_chunk = bsb_latest_complete_chunk_id(config.bsb)
        if end_chunk <= last_scheduled_end_chunk:
            break                                              # BSB has no new complete chunks since last iter
        start_chunk = retention_aligned_start_chunk(last_ledger_in_chunk(end_chunk), retention_ledgers)
        if end_chunk < start_chunk:
            # Defensive only — unreachable under current geometry. retention is 0
            # or N×LEDGERS_PER_TX_INDEX and floor is clamped to GENESIS, so
            # first_chunk_of_tx_index_containing(floor) <= end_chunk always.
            break
        log.info(f"phase1_catchup bsb_tip_chunk={end_chunk} range=[{start_chunk}, {end_chunk}]")
        run_backfill(config, start_chunk, end_chunk)
        last_scheduled_end_chunk = end_chunk

    if last_scheduled_end_chunk < 0:
        # Reached only when iter 1 broke on `end_chunk <= last_scheduled_end_chunk`
        # with last_scheduled_end_chunk still at -1, i.e., bsb_latest_complete_chunk_id
        # returned -1 (BSB has zero complete chunks — fresh bucket or transient
        # empty state). No end_chunk to advance the marker to; Phase 4's captive
        # core handles resume from whatever state exists.
        # The other loop break (end_chunk < start_chunk) cannot land here under
        # the current retention geometry — start_chunk <= end_chunk always.
        return None

    # Advance marker past any stale prior-run value so Phase 3's floor calc and
    # compute_resume_ledger don't replay chunks that are about to be pruned.
    advance_progress_marker(meta_store, last_ledger_in_chunk(last_scheduled_end_chunk))
    return last_scheduled_end_chunk

def retention_floor_ledger(tip_ledger, retention_ledgers) -> int:
    # Bottom edge of the retention window — oldest ledger to keep. Clamped to
    # GENESIS for the early-bootstrap case where tip < retention_ledgers
    # (tip - retention would otherwise be a non-existent negative ledger).
    # Shared by Phase 1 (start-chunk computation) and Phase 3 Pass 1 (past-retention floor).
    return max(tip_ledger - retention_ledgers, GENESIS_LEDGER)


def retention_aligned_start_chunk(tip_ledger, retention_ledgers):
    # Aligns DOWN to a tx-index boundary (no-gaps); up to LEDGERS_PER_TX_INDEX - 1 ledgers below strict retention.
    if retention_ledgers == 0:
        return 0
    return first_chunk_id_of_tx_index_containing(retention_floor_ledger(tip_ledger, retention_ledgers))

def advance_progress_marker(meta_store, candidate_ledger):
    """
    Monotonic write to streaming:last_committed_ledger. Two callers — Phase 1
    (post-catchup) and the live ingestion loop (per ledger, after all three
    active stores commit). Regression would cause re-ingest waste and a stale
    retention floor.
    """
    prior = meta_store.get("streaming:last_committed_ledger")
    if prior is None or candidate_ledger > prior:
        meta_store.put("streaming:last_committed_ledger", candidate_ledger)
```

**Worker concurrency:** `run_backfill` caps DAG concurrency at `MAX_CPU_THREADS` — see [01-backfill-workflow.md — process_chunk](./01-backfill-workflow.md#process_chunk). Catchup time ≈ `retention_window / (BSB throughput)`.

### Phase 2 — Hydrate TxHash Data from `.bin`

Phase 1's backfill range almost always ends mid-tx-index — BSB tip lands wherever the live network is, rarely on an index boundary. 
For that trailing partial tx index, per-chunk `.bin` files are on disk but `index:N:txhash` is not yet written (RecSplit waits until every chunk of N is complete).
Phase 2 loads each surviving `.bin` into the active txhash store, then deletes the `.bin` and `chunk:{chunk_id:08d}:txhash` flag.
After Phase 2: no `.bin` files and no `:txhash` chunk flags remain.

```python
def phase2_hydrate_txhash(config, meta_store, last_phase1_chunk_id):
    # Both sweeps: file-before-flag-delete (see Flag Semantics).

    # Sweep 1: clean leftover .bin from completed indexes (cleanup_txhash crashed mid-pair).
    for tx_index_id in tx_index_ids_with_txhash_flag(meta_store):
        for chunk_id in chunks_for_tx_index(tx_index_id):
            if meta_store.has(f"chunk:{chunk_id:08d}:txhash"):
                delete_if_exists(raw_txhash_path(chunk_id))
                meta_store.delete(f"chunk:{chunk_id:08d}:txhash")

    # Sweep 2: hydrate the trailing incomplete tx index into the active txhash store.
    # Phase 1 returns the highest chunk it completed; the trailing tx index is
    # the one containing that chunk — no separate scan needed.
    if last_phase1_chunk_id is None:
        return                                       # no Phase 1 work → no .bin files
    tx_index_id = tx_index_id_of_chunk(last_phase1_chunk_id)
    if meta_store.has(f"index:{tx_index_id:08d}:txhash"):
        return                                       # last touched index already complete (RecSplit done)

    txhash_store = open_active_txhash_store(config, meta_store, tx_index_id)
    try:
        for chunk_id in chunks_for_tx_index(tx_index_id):
            if not meta_store.has(f"chunk:{chunk_id:08d}:txhash"):
                # Phase 1 didn't reach this chunk in the trailing tx_index
                # (chunk_id > last_phase1_chunk_id) — no .bin file to hydrate.
                continue
            bin_path = raw_txhash_path(chunk_id)
            # .bin absent + :txhash flag set ⇒ a prior Phase 2 deleted the file
            # but crashed before clearing the flag (file-before-flag-delete).
            # The data is already durable in the active txhash store from that
            # prior load; skip the re-load and just finish flag cleanup below.
            if os.path.exists(bin_path):
                load_bin_into_rocksdb(bin_path, txhash_store)
            delete_if_exists(bin_path)
            meta_store.delete(f"chunk:{chunk_id:08d}:txhash")
    finally:
        txhash_store.close()

def tx_index_ids_with_txhash_flag(meta_store) -> Set[int]:
    # Scans chunk:*:txhash and returns the unique tx_index_ids those chunks
    # belong to. Used by Sweep 1 to find indexes whose cleanup_txhash crashed
    # mid-pair (some chunks of N still carry :txhash flags + .bin files).
    result = set()
    for key in meta_store.scan_prefix("chunk:"):
        if not key.endswith(":txhash"):
            continue
        chunk_id = parse_chunk_id_from_chunk_key(key)
        result.add(tx_index_id_of_chunk(chunk_id))
    return result

def open_active_txhash_store(config, meta_store, tx_index_id) -> RocksDBStore:
    """
    Idempotent. Safe to call repeatedly on the same tx_index_id:
    
    Hot key is written BEFORE mkdir (See Flag Semantics).
    A crash in between leaves hot:* set + dir absent,
    which Phase 3 Pass 2 reconciles. The reverse order would orphan the
    dir with no meta-store record — only recoverable via filesystem scan, which
    violates the meta-store-driven-recovery invariant.
    """
    meta_store.put(f"hot:index:{tx_index_id:08d}:txhash", "1")
    path = active_store_path_for("index:txhash", tx_index_id)
    os.makedirs(path, exist_ok=True)
    column_families = [f"cf-{nibble:x}" for nibble in range(16)]
    # open_rocksdb_store recovers from any partial WAL on an existing DB.
    return open_rocksdb_store(path, txhash_rocksdb_settings(column_families))


def open_active_ledger_store(config, meta_store, chunk_id) -> RocksDBStore:
    ...  # same shape: put hot:chunk:{C}:lfs (idempotent), mkdir, open RocksDB.

def open_active_events_store(config, meta_store, chunk_id) -> RocksDBStore:
    ...  # same shape: put hot:chunk:{C}:events (idempotent), mkdir, open RocksDB.
```

Pure-streaming restarts (no recent Phase 1 output) never see `.bin` files; the live path writes txhash directly to the active store. Phase 2 is a no-op.

### Phase 3 — Reconcile

Two passes, both meta-store-driven. Pass 1 drops past-retention state first so Pass 2 only has to handle hot:* keys that are still within retention.

```python
def phase3_reconcile(config, meta_store):
    pass1_drop_past_retention_state(config, meta_store)
    pass2_recover_in_flight_transitions(config, meta_store)


def pass1_drop_past_retention_state(config, meta_store):
    """
    Drop every meta-store key and on-disk artifact whose ledger range falls
    below the retention floor. Three distinct categories of state get cleaned:

    1. Lifecycle-unreachable orphans.
       - hot:* keys + active-store dirs (LFS / events / txhash) for chunks
         and indexes below the floor. The freeze can't run on past-retention
         data, so these dirs have no path forward.
       - chunk:{C}:lfs / :events / :txhash flags + their immutable files for
         chunks of an INCOMPLETE prior-run tx index. Because index:N:txhash
         was never written for that tx index, the pruning lifecycle's
         past-retention scan (which keys off index:N:txhash = "1") cannot
         find them — Pass 1 is their only cleanup path. The chunk loop also
         applies a sibling blanket-delete: for every chunk K below floor that
         carries any chunk:K:* key, delete_if_exists is called on ALL three
         artifact paths (.pack, .bin, events cold segment), not just the one
         this key represents. Catches partial files left by a prior crashed
         process_chunk where one kind completed (flag set) but the next kind
         hadn't — its partial file on disk has no key tracking it.

    2. Lifecycle-reachable complete state, drained eagerly.
       - index:N:txhash flags + the 16 RecSplit .idx files for fully-built
         indexes below the floor. The pruning lifecycle would pick these up
         on its next sweep; Pass 1 drops them at startup so the first sweep
         starts with no backlog.

    3. Mid-prune markers from a prior-run crash, below floor.
       - pruning:index:N markers below floor. The chunk/index loops above
         have already deleted N's files + flags, so this loop just clears
         the marker. Above-floor markers are left alone — the pruning
         lifecycle's crash-recovery scan picks them up on its next sweep.

    Floor reference: sample the live network tip from the history archive.
    The progress marker is NOT a reliable tip proxy — `[BSB]` may be configured
    but no longer advancing (operator stopped updating the remote store; BSB
    outage; etc.), in which case the marker reflects BSB's last seen ledger,
    not the live network tip. Falling back to the marker on archive-unreachable
    is degraded best-effort cleanup; the pruning lifecycle catches up later as
    captive core advances the marker through the gap.
    """
    retention_ledgers = config.service.retention_ledgers
    if retention_ledgers == 0:
        return                                                    # archive profile — no floor

    current_tip = try_sample_network_tip(config)
    if current_tip is None:
        # Archive unreachable. Degraded fallback to the local marker — Pass 1
        # under-cleans for now; pruning lifecycle catches up after captive core
        # advances the marker.
        current_tip = meta_store.get("streaming:last_committed_ledger")
    if current_tip is None:
        log.warn("phase3 pass1: no tip reference available (archive unreachable + marker absent); skipping past-retention cleanup")
        return

    floor_ledger = retention_floor_ledger(current_tip, retention_ledgers)
    floor_chunk  = chunk_id_of_ledger(floor_ledger)

    # Category 1 — lifecycle-unreachable orphans (hot keys + chunks of incomplete prior-run tx indexes).
    for hot_key in meta_store.scan_prefix("hot:chunk:"):
        store_kind, chunk_id = parse_hot_key(hot_key)
        if chunk_id < floor_chunk:
            delete_dir_if_exists(active_store_path_for(store_kind, chunk_id))
            meta_store.delete(hot_key)
            log.info(f"phase3 pass1: discarded past-retention {hot_key}")

    for hot_key in meta_store.scan_prefix("hot:index:"):
        _, tx_index_id = parse_hot_key(hot_key)
        if last_ledger_in_tx_index(tx_index_id) < floor_ledger:
            delete_dir_if_exists(active_store_path_for("index:txhash", tx_index_id))
            meta_store.delete(hot_key)
            log.info(f"phase3 pass1: discarded past-retention {hot_key}")

    # Sibling blanket-delete: the first time we see any chunk:K:* key for K
    # below floor, delete_if_exists ALL three artifact paths for K (.pack, .bin,
    # events cold segment) — not just the one this key represents. Catches
    # partial files from a prior crashed process_chunk where some kinds
    # completed (flag set) but others didn't (flag absent + partial file on
    # disk with no key tracking it). Idempotent — extra delete_if_exists calls
    # on already-clean paths are no-ops.
    blanket_cleaned = set()
    for chunk_key in meta_store.scan_prefix("chunk:"):
        chunk_id, _ = parse_chunk_key(chunk_key)
        if chunk_id >= floor_chunk:
            continue
        if chunk_id not in blanket_cleaned:
            delete_if_exists(ledger_pack_path(chunk_id))
            delete_events_segment(chunk_id)
            delete_if_exists(raw_txhash_path(chunk_id))
            blanket_cleaned.add(chunk_id)
        meta_store.delete(chunk_key)

    # Category 2 — past-retention complete indexes; lifecycle could reach but Pass 1 drains eagerly.
    for index_key in meta_store.scan_prefix("index:"):
        _, tx_index_id = parse_index_key(index_key)
        if last_ledger_in_tx_index(tx_index_id) < floor_ledger:
            delete_recsplit_idx_files(tx_index_id)
            meta_store.delete(index_key)

    # Category 3 — clear below-floor pruning:index:* markers; files + flags already removed above.
    for pruning_key in meta_store.scan_prefix("pruning:index:"):
        tx_index_id = parse_pruning_index_id(pruning_key)
        if last_ledger_in_tx_index(tx_index_id) < floor_ledger:
            meta_store.delete(pruning_key)


def pass2_recover_in_flight_transitions(config, meta_store):
    # Pass 1 has already removed past-retention hot keys; every entry here is
    # at or above the retention floor.
    last_committed = meta_store.get("streaming:last_committed_ledger")
    if last_committed is None:
        # No progress marker means neither Phase 1 nor the live loop has ever
        # advanced it. Without a resume position, hot:* keys can't be classified
        # into scenarios A/B/C/D described below — bail. Any stranded hot:* keys from a prior abnormal
        # exit get classified on a later restart once the marker is set.
        return

    resume_chunk_id    = chunk_id_of_ledger(last_committed + 1)
    resume_tx_index_id = tx_index_id_of_chunk(resume_chunk_id)

    for hot_key in meta_store.scan_prefix("hot:"):
        store_kind, scope_id = parse_hot_key(hot_key)
        resume_id = resume_chunk_id if store_kind.startswith("chunk:") else resume_tx_index_id
        store_path = active_store_path_for(store_kind, scope_id)
        freeze_flag_key = freeze_flag_key_for(store_kind, scope_id)

        if scope_id == resume_id:
            continue                                              # A: resume target — Phase 4 reopens.
        elif meta_store.has(freeze_flag_key):
            # B: flag-is-truth. Frozen, but cleanup didn't finish.
            delete_dir_if_exists(store_path)
            meta_store.delete(hot_key)
        elif scope_id < resume_id:
            # C: freeze interrupted; restart to completion.
            finish_interrupted_freeze(store_kind, scope_id, meta_store)
        else:
            # D: future-orphan — shouldn't occur in normal flow. Log + cleanup.
            log.warn(f"phase3 pass2: future-orphan {store_kind}/{scope_id:08d} > resume {resume_id:08d}")
            delete_dir_if_exists(store_path)
            meta_store.delete(hot_key)


```

`finish_interrupted_freeze` reopens the active store (idempotent on existing or partial dirs) and runs the corresponding live-path freeze ([LFS](#lfs-transition), [events](#events-transition), or [RecSplit](#recsplit-transition)) to produce the artifact.

### Compute Resume Ledger

Runs once per service start, after Phase 3, before Phase 4. Returns the ledger sequence Phase 4's captive core resumes from via `PrepareRange(UnboundedRange(resume_ledger))`.

```python
def compute_resume_ledger(config, meta_store) -> int:
    """
    Where should captive core resume ingestion?
    Shorthand: `marker` = `streaming:last_committed_ledger`.

    Why not just "if marker exists, return marker + 1"? The marker is our
    local progress checkpoint, not the live network's tip. In long-downtime
    or BSB-stale scenarios it can be tens of millions of ledgers behind the
    live tip; resuming at marker + 1 there would archive-catchup from the
    stale point and re-ingest weeks of ledgers that the pruning lifecycle
    would delete moments later. Branch 2 sidesteps that by sampling the
    live tip and skipping forward when the marker has fallen behind.

    Three branches:

      Branch 1 — marker present and current (or staleness unconfirmed):
        return marker + 1. Covers archive profile (no floor), normal
        restarts, and the degraded path where the history archive is
        unreachable.

      Branch 2 — marker present, retention > 0, marker below the retention
        floor: stale. Return retention_aligned_resume_ledger_with_tip. Two
        cases land here: (1) no-BSB long-downtime — prior live loop hasn't
        committed in a while; (2) BSB-stale-but-configured — operator
        stopped advancing BSB while the network kept producing, so Phase 1
        only advanced the marker to BSB's stale tip. The first live commit
        on resume monotonically overwrites the stale marker via
        advance_progress_marker.

      Branch 3 — marker absent (fresh operator start):
        return retention_aligned_resume_ledger.

    No consistency validation. Phase 1 self-heals incomplete chunks; Phase 3
    recovers in-flight freezes; pruning lifecycle handles past-retention state.
    """
    marker = meta_store.get("streaming:last_committed_ledger")

    # Branch 3 — fresh operator start.
    if marker is None:
        return retention_aligned_resume_ledger(config)

    # Branch 2 — staleness check. Skipped when retention=0 (no floor) or
    # when the archive is unreachable (best-effort fall-through to Branch 1).
    if config.service.retention_ledgers > 0:
        current_tip = try_sample_network_tip(config)
        if current_tip is not None:
            floor = retention_floor_ledger(current_tip, config.service.retention_ledgers)
            if marker < floor:
                log.info(f"compute_resume_ledger: marker ({marker}) below retention floor ({floor}); skipping forward")
                return retention_aligned_resume_ledger_with_tip(config, current_tip)

    # Branch 1 — marker is current (or staleness unconfirmed).
    return marker + 1


def retention_aligned_resume_ledger(config) -> int:
    # No-BSB resume cursor: align to the first ledger of the tx index containing
    # the retention floor; captive core archive-catches-up from that point.
    network_tip = get_latest_network_tip(config.history_archives.urls)
    return retention_aligned_resume_ledger_with_tip(config, network_tip)


def retention_aligned_resume_ledger_with_tip(config, network_tip_ledger) -> int:
    retention_ledgers = config.service.retention_ledgers
    target_ledger = max(network_tip_ledger - retention_ledgers, GENESIS_LEDGER)
    return first_ledger_of_tx_index_containing(target_ledger)


def try_sample_network_tip(config) -> Optional[int]:
    # Returns None on archive failure instead of raising. Used where a missing
    # tip is recoverable (compute_resume_ledger's stale-marker check).
    try:
        return get_latest_network_tip(config.history_archives.urls)
    except NetworkTipUnreachable:
        return None
```

### Phase 4 — Live Ingestion

Opens active stores for the resume position, spawns the lifecycle task, starts captive core, enters the ingestion loop. Query serving was already enabled by `run_rpc_service`; Phase 4 is purely about ingestion. Captive core takes 4–5 minutes to spin up — historical queries continue to be served against frozen artifacts during that window. See [Query Contract](#query-contract).

```python
def phase4_live_ingest(config, meta_store, resume_ledger):
    # open_active_*_store writes the hot:* key BEFORE mkdir; Phase 3 has cleaned
    # stale hot keys, so mkdir lands on an empty path or (SCENARIO A) on the
    # prior-run active dir, idempotently re-opened.
    active_stores = open_active_stores_for_resume(config, meta_store, resume_ledger)

    # Initial sweep handles any pruning:index:* markers left by a prior crash.
    run_in_background(run_prune_lifecycle_loop, config, meta_store)

    # PrepareRange blocks ~4-5 min during captive-core spinup.
    ledger_backend = make_ledger_backend(config.captive_core.config_path)
    ledger_backend.PrepareRange(UnboundedRange(resume_ledger))

    run_live_ingestion_loop(config, ledger_backend, active_stores, meta_store, resume_ledger)


def open_active_stores_for_resume(config, meta_store, resume_ledger):
    # Per-kind stores share no state; opening order is free. Idempotent on
    # existing dirs (mkdir no-ops; RocksDB recovers from partial WAL).
    resume_chunk_id    = chunk_id_of_ledger(resume_ledger)
    resume_tx_index_id = tx_index_id_of_chunk(resume_chunk_id)

    return ActiveStores(
        ledger = open_active_ledger_store(config, meta_store, resume_chunk_id),
        events = open_active_events_store(config, meta_store, resume_chunk_id),
        txhash = open_active_txhash_store(config, meta_store, resume_tx_index_id),
    )
```

---

## Ingestion Loop

Single background task. Pull-based: the service drives sequential `GetLedger(seq)` calls. Same code path drains captive core's internal buffer during catchup and switches cadence to live closes (~5 s per ledger) once caught up.

```python
def run_live_ingestion_loop(config, ledger_backend, active_stores, meta_store, resume_ledger):
    ledger_seq = resume_ledger
    while True:
        lcm = ledger_backend.GetLedger(ledger_seq)   # blocks until available
        wait_all(    # all three writes durably commit before advancing the checkpoint
            run_in_background(write_ledger_store, active_stores.ledger, ledger_seq, lcm),
            run_in_background(write_txhash_store, active_stores.txhash, ledger_seq, lcm),
            run_in_background(write_events_store, active_stores.events, ledger_seq, lcm),
        )
        meta_store.put("streaming:last_committed_ledger", ledger_seq)

        chunk_id = chunk_id_of_ledger(ledger_seq)
        if ledger_seq == last_ledger_in_chunk(chunk_id):
            on_chunk_boundary(chunk_id, active_stores, meta_store)

        # Every tx-index boundary is also a chunk boundary; index handler runs after chunk handler.
        tx_index_id = tx_index_id_of_chunk(chunk_id)
        if ledger_seq == last_ledger_in_tx_index(tx_index_id):
            on_tx_index_boundary(tx_index_id, active_stores, meta_store)

        ledger_seq += 1
```

Per-store writes are atomic via RocksDB WriteBatch + WAL.

---

## Freeze Transitions

Three independent background transitions per boundary; each has its own task, flag, and cleanup. Live ingestion never blocks on them.

- **LFS transition** (per chunk) — retired ledger RocksDB → `.pack` file.
- **Events transition** (per chunk) — retired events RocksDB → cold segment (3 files).
- **RecSplit transition** (per index) — retired txhash RocksDB → 16 `.idx` files.

Streaming's freezes never produce `.bin` files; those are transient backfill output (Phase 1 only).

### Concurrency Model

- **`active_stores` is owned by the ingestion loop.** Fields `ledger` / `events` / `txhash` are mutated only inside the boundary handlers. Freeze tasks receive a handle by value at spawn and never read back through `active_stores`.
- **Meta-store is single-writer.** Serialized across the ingestion loop (per-ledger checkpoint), freeze tasks (flags), and lifecycle loop (prune marker + key deletes).
- **Per-kind single-flight gates.** One outstanding LFS / events / RecSplit transition each; the next starts only after the previous releases. Not a global barrier — kinds remain independent.
- **Query routing.** Per-data-type storage managers own state-transition synchronization; query handlers never touch `active_stores` directly. A query sees either pre-transition or post-transition data, never a mix; never routes to an immutable artifact whose freeze flag is unset. Concrete lock primitives + routing logic are deferred to a separate query-routing doc.

### Chunk Boundary (every 10_000 ledgers)

Triggered when the ingestion loop commits `last_ledger_in_chunk(chunk_id)`. Handoffs to two freeze transitions (LFS + events) that run in background.

```python
def on_chunk_boundary(chunk_id, active_stores, meta_store):
    # LFS + events run independently — events doesn't wait on LFS.
    wait_for_lfs_complete()
    transitioning_ledger_store = active_stores.ledger
    active_stores.ledger = open_active_ledger_store(config, meta_store, chunk_id + 1)
    run_in_background(freeze_ledger_chunk_to_pack_file, chunk_id, transitioning_ledger_store, meta_store)

    wait_for_events_complete()
    transitioning_events_store = active_stores.events
    active_stores.events = open_active_events_store(config, meta_store, chunk_id + 1)
    run_in_background(freeze_events_chunk_to_cold_segment, chunk_id, transitioning_events_store, meta_store)

    notify_lifecycle()   # wake prune loop
```

### LFS Transition

Converts the retired ledger RocksDB store to an immutable `.pack` file, then discards the store.

```python
def freeze_ledger_chunk_to_pack_file(chunk_id, transitioning_ledger_store, meta_store):
    # overwrite=True discards any prior partial. Crash after the freeze flag but
    # before delete_dir leaves an orphan; Phase 3 (reconcile) picks it up.
    pack_path = ledger_pack_path(chunk_id)
    writer = packfile.create(pack_path, overwrite=True)
    for ledger_seq in range(first_ledger_in_chunk(chunk_id), last_ledger_in_chunk(chunk_id) + 1):
        writer.append(transitioning_ledger_store.get(uint32_big_endian(ledger_seq)))
    writer.fsync_and_close()
    meta_store.put(f"chunk:{chunk_id:08d}:lfs", "1")
    transitioning_ledger_store.close()
    delete_dir_if_exists(ledger_store_path(chunk_id))
    meta_store.delete(f"hot:chunk:{chunk_id:08d}:lfs")       # cleared AFTER dir removed
    signal_lfs_complete()
```

### Events Transition

Converts the retired events RocksDB store to three immutable files (events cold segment).

```python
def freeze_events_chunk_to_cold_segment(chunk_id, transitioning_events_store, meta_store):
    events_path = events_segment_path(chunk_id)
    write_cold_segment(transitioning_events_store, events_path)   # 3 files: events.pack, index.pack, index.hash
    fsync_all(events_path)
    meta_store.put(f"chunk:{chunk_id:08d}:events", "1")
    transitioning_events_store.close()
    delete_dir_if_exists(events_store_path(chunk_id))
    meta_store.delete(f"hot:chunk:{chunk_id:08d}:events")
    signal_events_complete()
```

### Tx-Index Boundary (every `LEDGERS_PER_TX_INDEX` ledgers)

The last chunk of a tx index has just rolled over. Before RecSplit can start, every chunk in the tx index must have its `:lfs` and `:events` flags set.

```python
def on_tx_index_boundary(tx_index_id, active_stores, meta_store):
    # Drain all in-flight chunk-level freezes for this tx index before RecSplit.
    wait_for_lfs_complete()
    wait_for_events_complete()
    verify_all_chunk_flags(tx_index_id, meta_store)
    transitioning_txhash_store = active_stores.txhash
    active_stores.txhash       = open_active_txhash_store(config, meta_store, tx_index_id + 1)
    run_in_background(build_tx_index_recsplit_files, tx_index_id, transitioning_txhash_store, meta_store)
```

### RecSplit Transition

Builds the 16 RecSplit `.idx` files for tx_index_id from the retired txhash active store.

```python
def build_tx_index_recsplit_files(tx_index_id, transitioning_txhash_store, meta_store):
    # Verify before flag; flag-after-fsync as in LFS / events.
    idx_path = recsplit_index_path(tx_index_id)
    delete_partial_idx_files(idx_path)
    build_recsplit(transitioning_txhash_store, idx_path)           # 16 .idx files
    fsync_all_idx_files(idx_path)
    verify_spot_check(tx_index_id, idx_path, meta_store)
    meta_store.put(f"index:{tx_index_id:08d}:txhash", "1")
    transitioning_txhash_store.close()
    delete_dir_if_exists(txhash_store_path(tx_index_id))
    meta_store.delete(f"hot:index:{tx_index_id:08d}:txhash")
```

---

## Pruning

Retention is enforced by a single background task, woken at chunk boundaries. Prune granularity is the whole txhash index — never per chunk.

```python
def run_prune_lifecycle_loop(config, meta_store):
    # Sole caller of prune_tx_index. Initial sweep on entry catches any
    # in-progress prunes from a prior-run crash; subsequent sweeps fire on
    # chunk-boundary notifications.
    retention_ledgers = config.service.retention_ledgers
    _run_prune_sweep(meta_store, retention_ledgers, config)
    while True:
        wait_for_chunk_boundary_notification()                # set by on_chunk_boundary's notify_lifecycle()
        _run_prune_sweep(meta_store, retention_ledgers, config)


def _run_prune_sweep(meta_store, retention_ledgers, config):
    for tx_index_id in prunable_tx_index_ids(meta_store, retention_ledgers):
        prune_tx_index(tx_index_id, meta_store, config)


def prunable_tx_index_ids(meta_store, retention_ledgers):
    """
    Returns tx_index_ids that need pruning, from two sources unioned together:

    - Crash-recovery scan — any pruning:index:N key set means a prior prune
      was interrupted (marker survives crashes by design); re-run regardless
      of retention status.
    - Past-retention scan — indexes with index:N:txhash = "1" whose last
      ledger is below the retention floor.
    """
    if retention_ledgers == 0:
        return []

    result = set()

    # Crash-recovery scan: in-progress prunes from a prior crash.
    for key in meta_store.scan_prefix("pruning:index:"):
        result.add(parse_pruning_index_id(key))

    # Past-retention scan: newly-eligible indexes since the last sweep.
    last_committed_ledger = meta_store.get("streaming:last_committed_ledger")
    max_eligible_tx_index_id = max_prunable_tx_index_id(last_committed_ledger, retention_ledgers)
    for tx_index_id in range(0, max_eligible_tx_index_id + 1):
        if tx_index_id in result:
            continue
        if meta_store.get(f"index:{tx_index_id:08d}:txhash") == "1":
            result.add(tx_index_id)

    return sorted(result)


def max_prunable_tx_index_id(last_committed_ledger, retention_ledgers) -> int:
    # Highest tx_index_id whose last_ledger is strictly below the retention floor.
    # Returns -1 when no index is past-retention (range(0, 0) below is empty).
    if last_committed_ledger is None:
        return -1
    floor_ledger = last_committed_ledger - retention_ledgers
    if floor_ledger <= GENESIS_LEDGER:
        return -1
    # last_ledger_in_tx_index(N) < floor_ledger  ⇔  N < tx_index_id_of_ledger(floor_ledger)
    return tx_index_id_of_ledger(floor_ledger) - 1


def prune_tx_index(tx_index_id, meta_store, config):
    """
    Tear down all artifacts for tx_index_id. Marker set FIRST (atomic 4xx gate
    for any tx in N) and cleared LAST (survives crashes so the next sweep
    picks N back up). Everything between is individually idempotent.
    """
    # OP 1: gate queries off; mark "prune in progress" for crash recovery.
    meta_store.put(f"pruning:index:{tx_index_id:08d}", "1")

    # WORK: per-chunk file + key deletion (file-before-flag-delete).
    for chunk_id in chunks_for_tx_index(tx_index_id):
        delete_if_exists(ledger_pack_path(chunk_id))
        delete_events_segment(chunk_id)
        delete_if_exists(raw_txhash_path(chunk_id))           # defence-in-depth; normally already gone via cleanup_txhash
        meta_store.delete(f"chunk:{chunk_id:08d}:lfs")
        meta_store.delete(f"chunk:{chunk_id:08d}:events")
        meta_store.delete(f"chunk:{chunk_id:08d}:txhash")
    delete_recsplit_idx_files(tx_index_id)

    # OP 2: queries still 4xx via the marker check.
    meta_store.delete(f"index:{tx_index_id:08d}:txhash")

    # OP 3: tx index N is now fully gone.
    meta_store.delete(f"pruning:index:{tx_index_id:08d}")
```

- **Why index-atomic.** Per-chunk pruning would open a window where `getTransaction` resolves but its `getLedger` 4xxs (pack deleted); whole-index gating closes it.
- **Extra data on disk.** Up to `LEDGERS_PER_TX_INDEX - 1` ledgers past strict retention. `RETENTION_LEDGERS` is always a multiple of `LEDGERS_PER_TX_INDEX`, so the next-eligible index is exactly `LEDGERS_PER_TX_INDEX` further.
- **Separate marker family vs overloading `index:N:txhash` with a `"deleting"` value.** Keeps every meta-store key binary (present-or-absent) so reader code never decodes special values; cost is one extra sub-microsecond write per prune.
- **Marker is steady-state-only.** Phase 3 Pass 1 deletes past-retention state directly (no marker needed — `service_ready = false`, no queries to gate) and clears any below-floor `pruning:index:*` it finds.

---

## Query Contract

`getLedger` / `getTransaction` / `getEvents` are gated on `service_ready`; during startup phases they return **HTTP 4xx**.

### Readiness Signal

- In-memory boolean. Flipped `true` by `set_service_ready()` after Phases 1–3 complete and `compute_resume_ledger` returns, BEFORE Phase 4's captive-core spinup. Frozen-artifact queries don't need captive core; flipping after spinup would add an unnecessary 4–5 minute outage per restart.
- Not persisted; every startup begins `false`. Clients see 4xx on every startup until Phase 4 is reached, regardless of prior runs.
- HTTP server binds before Phase 1, so `/getHealth` is always servable.

### Behavior During Phases 1–3

- `/getLedger`, `/getTransaction`, `/getEvents` → HTTP 4xx, no payload.
- `/getHealth` → served. Response shape matches existing stellar-rpc: `status` (`catching_up` during Phases 1–3, `healthy` during Phase 4), `latestLedger` (= `streaming:last_committed_ledger`, or `0` if absent), `oldestLedger`, `ledgerRetentionWindow`.
- No partial / incremental serving while Phases 1–3 run.

### Behavior When an Index Is Being Pruned

- QueryRouter checks `pruning:index:N` first; if set, returns 4xx as if the index were past retention. Queries flip to 4xx the instant the marker is set, not when files actually disappear — no window where queries route into a half-deleted index.

---

## Resilience

Streaming extends backfill's resilience model ([01-backfill-workflow.md — Crash Recovery](./01-backfill-workflow.md#crash-recovery)) with per-ledger checkpoint discipline, per-kind single-flight freeze gates, and the `pruning:index:*` marker family. No separate recovery phase — every startup runs Phases 1–4 and skips already-complete work via meta-store flags.

### Streaming-Specific Invariants

1. **No permanently-partial tx index.** Every persisted tx index reaches a terminal state — either *complete* (`index:N:txhash = "1"`, RecSplit built) or *fully discarded* (all chunks + `index:N:txhash` deleted). The intermediate "trailing partial" state (chunks with `:lfs+:events` but no `index:N:txhash`) persists only transiently, and is completed by either (a) a future Phase 1 invocation extending the backfill range past `last_chunk_in_tx_index(N)`, (b) Phase 4 ingestion reaching `last_chunk_in_tx_index(N)` (which fires `on_tx_index_boundary` → `build_tx_index_recsplit_files`), or (c) Phase 3 Pass 1 discarding it as past-retention.
2. **No permanent orphans.** Every meta-store flag has a corresponding artifact (or is mid-cleanup, recoverable via file-before-flag-delete). Every active-store dir has a `hot:*` key. Every immutable file has a freeze flag.
3. **Pruning intent marker.** `pruning:index:{N}` is set BEFORE any file delete and cleared AFTER everything else; the lifecycle loop's `prunable_tx_index_ids` picks up surviving markers via `scan_prefix("pruning:index:")` and re-runs the prune idempotently. See [Pruning](#pruning).
