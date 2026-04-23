# Streaming Workflow

## Overview

The stellar-rpc daemon is the full-history RPC service. One binary, one invocation, one long-running process.

- Operator runs `stellar-rpc --config path/to/config.toml`. No subcommand. No `--mode` flag. No behavior-switching flags.
- On every start the daemon runs four sequential startup phases, then enters a live ingestion loop it stays in until killed.
- Behavior across the three operator profiles (archive, pruning-history, tip-tracker) is determined entirely by TOML config — no profile flag.
- Backfill (`01-backfill-workflow.md`) is used as an internal subroutine by Startup Phase 1 (catchup). Operators never invoke backfill directly.

**What the daemon does end-to-end:**
- Validates config against immutable meta-store state (`CHUNKS_PER_TXHASH_INDEX` and `RETENTION_LEDGERS`).
- Catches up to the current network tip using BSB or captive core, whichever is configured.
- Hydrates any in-flight state left by a prior run.
- Ingests live ledgers from `CaptiveStellarCore` at ~1 per 6 seconds.
- Writes each live ledger to three active stores (ledger, txhash, events).
- Freezes active stores to immutable files at chunk and index boundaries in background.
- Prunes past-retention indexes atomically when retention is configured.
- Serves `getLedger`, `getTransaction`, `getEvents` only after startup phases complete. Returns HTTP 4xx during startup.

---

## Terminology

Terms used repeatedly throughout this doc. Skim on first read, refer back when a term surfaces later.

- **Daemon** — the stellar-rpc binary running as one long-lived process. The only operator-facing entry point.
- **Startup phases 1–4** — sequential bootstrap work the daemon runs once per process start, before serving queries. Not a lifecycle concept — once Phase 4 (live ingestion) is reached, it stays there until the process exits. [Details](#startup-sequence).
- **Phase 1 (catchup)** — the startup phase that closes the gap between the last-committed ledger and the current network tip. Invokes the backfill subroutine internally.
- **Backfill (subroutine)** — a self-contained mechanism that ingests a known `[range_start, range_end]` chunk range via a static DAG of per-chunk tasks (`process_chunk`, `build_txhash_index`, `cleanup_txhash`). Specified in `01-backfill-workflow.md`. In the unified design, backfill is an internal callable only — no CLI entry point exists.
- **Leapfrog** (colloquial) — when retention is configured (`RETENTION_LEDGERS > 0`), Phase 1 (catchup) skips past ledgers older than `tip - RETENTION_LEDGERS` by starting ingestion at the first ledger of the txhash index that contains `tip - RETENTION_LEDGERS`. Always lands on an index boundary — upholds the invariant that every persisted chunk is the first chunk of its index or a forward-contiguous extension of one. Implemented by the `retention_aligned_start_chunk` helper (Phase 1 (catchup) callsite) and the `retention_aligned_resume_ledger` helper (`compute_resume_ledger`'s no-BSB fresh-start branch).
- **`compute_resume_ledger`** — shared helper called once per daemon start, AFTER Phase 3 (reconcile) and BEFORE Phase 4 (live ingestion). Scans meta-store state end-to-end, validates on-disk consistency, and returns `resume_ledger` for Phase 4 (live ingestion). Runs post-Phase-3 so any in-flight freezes Phase 3 finished (and their newly-set `:lfs` flags) are visible to the scan. See [Compute Resume Ledger](#compute-resume-ledger).
- **`streaming:last_committed_ledger` (per-ledger checkpoint)** — meta-store key written once per live ledger inside Phase 4 (live ingestion)'s ingestion loop. Tracks live-streaming progress. Never touched during Phases 1–3. Bound locally as `last_committed_ledger` in pseudocode.
- **`network_tip_ledger`** — the most recent ledger the Stellar network has produced. Sampled from the history archive via HTTP GET on `/.well-known/stellar-history.json` against `HISTORY_ARCHIVES.URLS` whenever captive core is NOT yet running (Phase 1 (catchup) loop, Phase 4 (live ingestion) leapfrog-from-tip on fresh start without BSB). Once captive core is running (inside Phase 4 (live ingestion)), the tip comes from `ledger_backend.latest_tip()` against the running subprocess — authoritative and cheaper than another HTTP round-trip. Different from `last_committed_ledger` (the daemon's own progress).
- **Active store** — a mutable store holding in-flight ledger data for the chunk or index currently being ingested. Three kinds:
  - Ledger active store — a per-chunk RocksDB (one instance per chunk).
  - TxHash active store — a per-index RocksDB with 16 column families (one instance per index).
  - Events hot segment — in-memory roaring bitmaps plus persisted per-ledger index deltas (not a RocksDB; see [getEvents design](../../design-docs/getevents-full-history-design.md)).
- **Immutable store** — on-disk files produced by freezing an active store. Three kinds:
  - Ledger pack file (one per chunk).
  - RecSplit index `.idx` files (16 per index).
  - Events cold segment (three files per chunk: `events.pack`, `index.pack`, `index.hash`).
- **Freeze transition** — a background goroutine that converts an active store's contents to immutable files and deletes the active store. Three transitions total per chunk (LFS, events) and one per index (RecSplit).
- **Chunk** — a block of 10_000 consecutive ledgers. Atomic unit of ingestion and freeze. `first_ledger_in_chunk(chunk_id)` always ends in `..._02`; `last_ledger_in_chunk(chunk_id)` always ends in `..._01`. No partial chunks — every chunk on disk is a full 10_000-ledger chunk.
- **Txhash index** (a.k.a. "tx index", "index") — `CHUNKS_PER_TXHASH_INDEX` consecutive chunks. Atomic unit of retention pruning. Formulas in [Geometry](#geometry). Both docs use "tx index" as the dominant narrative form; "txhash index" appears where the output's role as a txhash lookup is the emphasis.
- **Chunk boundary** — the moment ingestion commits the last ledger of a chunk. Triggers background LFS + events freeze for that chunk.
- **Index boundary** — the moment ingestion commits the last ledger of an index. Triggers background RecSplit build for that index. Every index boundary is also a chunk boundary.
- **Catchup** — synonym for "close the gap between last-committed ledger and current tip". Performed inside Phase 1 (catchup).
- **`.bin` file** — a backfill-produced raw txhash flat file (transient). Exists only for chunks the backfill subroutine has flagged `:txhash` but whose containing index has not yet had its RecSplit built. Deleted by Phase 2 (`.bin` hydration) once loaded into the active txhash RocksDB. Streaming's live path never produces `.bin` files.

---

## Geometry

See [01-backfill-workflow.md — Geometry](./01-backfill-workflow.md#geometry). Streaming uses the same constants (`GENESIS_LEDGER`, `LEDGERS_PER_CHUNK`, `LEDGERS_PER_INDEX`, `CHUNKS_PER_TXHASH_INDEX`) and the same mapping functions (`chunk_id_of_ledger`, `first_ledger_in_chunk`, `last_ledger_in_chunk`, `tx_index_id_of_chunk`, `first_ledger_in_tx_index`, `last_ledger_in_tx_index`).

---

## Configuration

Streaming reads the same TOML file as backfill, plus additional keys described below.

### Shared Config (from backfill)

`[SERVICE]` (for `DEFAULT_DATA_DIR` + `CHUNKS_PER_TXHASH_INDEX`), `[BSB]`, `[IMMUTABLE_STORAGE.*]`, `[META_STORE]`, `[LOGGING]` are detailed in [01-backfill-workflow.md — Configuration](./01-backfill-workflow.md#configuration). Streaming adds extra keys to `[SERVICE]` and introduces `[CAPTIVE_CORE]`, `[ACTIVE_STORAGE]`, `[HISTORY_ARCHIVES]` (below).

### Immutable Keys (stored in meta store, fatal if changed)

Stored on first start; fatal on any subsequent start where the config value differs. Changing either requires wiping the datadir.

| Key | Stored under | Set by | Rule |
|---|---|---|---|
| `CHUNKS_PER_TXHASH_INDEX` | `config:chunks_per_txhash_index` | first run | Fatal if changed. |
| `RETENTION_LEDGERS` | `config:retention_ledgers` | first run | Fatal if changed. |

- Source selection (BSB vs captive core) is determined per-startup by `[BSB]` presence; not stored as immutable.
- Operators may add or remove BSB between runs; on each start, Phase 1 (catchup) either re-runs backfill from the retention-aligned start (BSB present) or no-ops (BSB absent). `compute_resume_ledger` then derives resume from whatever chunks are on disk.
- Retention immutability alone constrains the data envelope — source choice doesn't need its own gate.

### TOML Sections Documented Here

**[SERVICE] — streaming additions**

Extends the `[SERVICE]` table in [01-backfill-workflow.md — Configuration](./01-backfill-workflow.md#configuration) (which covers `DEFAULT_DATA_DIR` and `CHUNKS_PER_TXHASH_INDEX`).

| Key | Type | Default | Description |
|---|---|---|---|
| `RETENTION_LEDGERS` | uint32 | `0` | `0` = full history; otherwise must be a positive multiple of `LEDGERS_PER_INDEX`. See [Validation Rules](#validation-rules). |
| `DRIFT_WARNING_LEDGERS` | uint32 | `10` | `getHealth` reports unhealthy when ingestion drift exceeds this. ~60 seconds at 10 ledgers. |

**[CAPTIVE_CORE]**

| Key | Type | Default | Description |
|---|---|---|---|
| `CONFIG_PATH` | string | **required** | Path to CaptiveStellarCore config file. |

**[ACTIVE_STORAGE]** (optional)

| Key | Type | Default | Description |
|---|---|---|---|
| `PATH` | string | `{DEFAULT_DATA_DIR}/active` | Base path for active RocksDB stores (ledger, txhash, events). |

**[HISTORY_ARCHIVES]**

| Key | Type | Default | Description |
|---|---|---|---|
| `URLS` | []string | **required** | List of Stellar history archive URLs. Used to sample tip via `/.well-known/stellar-history.json` for Phase 4 (live ingestion)'s leapfrog-from-tip computation (when `[BSB]` is absent on first-ever start). Same key the existing ingest service reads. |

**[BSB]** (optional)

- Same schema as in the backfill doc. Presence in the config file determines Phase 1 (catchup) behavior:
  - Present: Phase 1 (catchup) invokes backfill over the BSB (fast, parallel per-chunk catchup).
  - Absent: Phase 1 (catchup) is a no-op; Phase 4 (live ingestion)'s captive core archive-catches-up from a leapfrog'd `resume_ledger` (slower, but no object-store dep).
- See [Ledger Source](#ledger-source) for the BSB-source details and [01-backfill-workflow.md — Backfill vs Phase 1](./01-backfill-workflow.md#backfill-vs-phase-1-catchup) for the full split.

### CLI Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--config` | string | **required** | Path to TOML config file. |
| `--log-level` | string | from `[LOGGING].LEVEL` | Override log level. |
| `--log-format` | string | from `[LOGGING].FORMAT` | Override log format. |

**No other flags.** No `--mode`, no `--start-ledger`, no `--end-ledger`, no subcommand. Any per-run behavior is either driven by config or derived at runtime from meta store + tip.

### Validation Rules

- `CHUNKS_PER_TXHASH_INDEX` immutable across runs (see [Immutable Keys](#immutable-keys-stored-in-meta-store-fatal-if-changed)).
- `RETENTION_LEDGERS` immutable across runs.
- `RETENTION_LEDGERS` must be `0` OR a positive integer multiple of `LEDGERS_PER_INDEX`. Valid at `cpi=1_000`: `0`, `10_000_000`, `20_000_000`, `30_000_000`, etc. Invalid: `15_000_000` (not a multiple), `5_000_000` (below minimum). Rationale: pruning runs at whole-index granularity; retention windows that don't align to index boundaries would leave partial indexes perpetually on disk.
- `[BSB]` optional. When present → Phase 1 (catchup) invokes backfill over the BSB; when absent → Phase 1 (catchup) is a no-op and Phase 4 (live ingestion)'s captive core handles initial catchup. May be added or removed between runs.
- **`[BSB]` absent AND `RETENTION_LEDGERS = 0` is fatal.** Full history requires BSB — captive-core archive-catchup from genesis would take weeks-to-months. Not a supported operating mode.
- `[HISTORY_ARCHIVES].URLS` required in all profiles.
- `[CAPTIVE_CORE].CONFIG_PATH` required in all profiles.

### Validation Pseudocode

```python
def validate_config(config, meta_store):
    cpi = config.service.chunks_per_txhash_index
    retention_ledgers = config.service.retention_ledgers
    ledgers_per_index = cpi * LEDGERS_PER_CHUNK

    if retention_ledgers != 0 and (retention_ledgers <= 0 or (retention_ledgers % ledgers_per_index) != 0):
        fatal(f"RETENTION_LEDGERS={retention_ledgers} must be 0 or a positive multiple of "
              f"LEDGERS_PER_INDEX={ledgers_per_index}.")

    if config.bsb is None and retention_ledgers == 0:
        fatal("[BSB] is absent AND RETENTION_LEDGERS=0 (full history). Full history requires "
              "BSB — captive-core-from-genesis is not supported. Either add [BSB] or set "
              "RETENTION_LEDGERS > 0.")

    if not config.captive_core.config_path:
        fatal("CAPTIVE_CORE.CONFIG_PATH is required.")
    if not config.history_archives.urls:
        fatal("HISTORY_ARCHIVES.URLS is required.")

    _enforce_immutable(meta_store, "config:chunks_per_txhash_index", str(cpi))
    _enforce_immutable(meta_store, "config:retention_ledgers",       str(retention_ledgers))


def _enforce_immutable(meta_store, key, current_value):
    stored = meta_store.get(key)
    if stored is None:
        meta_store.put(key, current_value)
    elif stored != current_value:
        fatal(f"{key} changed: stored={stored}, config={current_value}. Wipe datadir.")
```

### Operator Profiles

Three profiles emerge from config combinations. No profile flag.

| Profile | `RETENTION_LEDGERS` | `[BSB]` | Phase 1 behavior | Use case |
|---|---|---|---|---|
| Archive | `0` | present | Backfill over full history (chunks `[0, current_chunk − 1]`) | Public archive node; full history. |
| Pruning-history | `N × LEDGERS_PER_INDEX`, N ≥ 1 | present | Backfill over retention window (leapfrog-aligned start) | Windowed history with bulk initial catchup. |
| Tip-tracker | `N × LEDGERS_PER_INDEX`, N ≥ 1 | absent | **No-op.** Phase 4 (live ingestion)'s captive core archive-catches-up from a leapfrog'd `resume_ledger` | App developer; short retention; no object-store dep. |
| (invalid) | `0` | absent | — | Rejected by `validate_config`: full history requires BSB. |

---

## Meta Store Keys

Single RocksDB instance, WAL always enabled. Authoritative source for every startup decision.

### Keys Introduced by Streaming

| Key | Value | Written when |
|---|---|---|
| `streaming:last_committed_ledger` | uint32 (big-endian) | Written only by the live ingestion loop after all three active stores durably commit a ledger. **Never written at bootstrap.** When absent, [`compute_resume_ledger`](#compute-resume-ledger) derives resume from the contiguous `:lfs` prefix (first-ever post-Phase-1) or by leapfrogging down from the current network tip to an index boundary (tip-tracker fresh start). Phase 1 (catchup) progress is tracked by `chunk:{chunk_id}:lfs` flags alone. |
| `config:retention_ledgers` | decimal string | First run (stored); enforced on subsequent starts. |

### Keys Shared with Backfill

| Key | Semantics |
|---|---|
| `config:chunks_per_txhash_index` | Set on first run by whichever invocation runs first — here, first daemon start. |
| `chunk:{chunk_id:08d}:lfs` | Set after ledger pack file fsync. |
| `chunk:{chunk_id:08d}:events` | Set after events cold segment fsync. |
| `chunk:{chunk_id:08d}:txhash` | Set by backfill subroutine after `.bin` fsync; deleted during Phase 2 (`.bin` hydration) after `.bin` is loaded into RocksDB. Streaming live path does not write this key — streaming writes txhash directly to the active RocksDB txhash store. |
| `index:{tx_index_id:08d}:txhash` | `"1"` after all 16 RecSplit CF `.idx` files built and fsynced. Transitions to `"deleting"` at the start of `prune_tx_index`, deleted entirely when prune completes. Query routing treats `"deleting"` the same as absent. |

### Key Lifecycle in Streaming

```
Phase 1 (catchup):
  chunk:{chunk_id}:lfs      = "1"   (after pack fsync)
  chunk:{chunk_id}:txhash   = "1"   (after .bin fsync)    # only present for chunks that still have .bin on disk
  chunk:{chunk_id}:events   = "1"   (after cold segment fsync)
  index:{tx_index_id}:txhash   = "1"   (after RecSplit, when all chunks of tx_index_id are done in Phase 1 (catchup))

Phase 2 (.bin hydration — see Startup Sequence):
  For every chunk with :txhash flag and a .bin file:
    load .bin into RocksDB txhash store
    delete chunk:{chunk_id}:txhash flag
    delete .bin file
  After Phase 2 (.bin hydration), no chunk:{chunk_id}:txhash flags and no .bin files remain.

Live path (per ledger):
  streaming:last_committed_ledger = ledger_seq    (after all 3 active stores commit)

Live path (per chunk, background):
  chunk:{chunk_id}:lfs      = "1"   (after pack fsync)
  chunk:{chunk_id}:events   = "1"   (after cold segment fsync)

Live path (per index, background):
  index:{tx_index_id}:txhash   = "1"   (after RecSplit + verify)

Pruning (background, when tx_index_id is past retention):
  index:{tx_index_id}:txhash   = "deleting"   (FIRST; queries now return 4xx for this index)
  [delete all files + per-chunk :lfs + :events keys for tx_index_id]
  index:{tx_index_id}:txhash   → deleted (LAST)
```

### Flag Semantics

- **Flag-after-fsync.** A flag is set only after the artifact it represents has been fsynced. Flag absent = artifact missing (or incomplete).
- **Flag-driven recovery.** Every startup decision — hydration, transition replay, RecSplit spawn, prune eligibility — derives from meta store key presence. No filesystem-scan-and-infer.

---

## Active Store Architecture

The daemon maintains three active stores for the current ingestion position. All per-chunk and per-index lifecycle is driven by the [freeze transitions](#freeze-transitions).

| Store | Path | Key | Value | Transition cadence |
|---|---|---|---|---|
| Ledger | `{ACTIVE_STORAGE.PATH}/ledger-store-chunk-{chunk_id:08d}/` | `uint32BE(ledgerSeq)` | `zstd(LCM bytes)` | Every 10_000 ledgers (chunk) |
| TxHash | `{ACTIVE_STORAGE.PATH}/txhash-store-index-{tx_index_id:08d}/` | `txhash[32]` | `uint32BE(ledgerSeq)` | Every `LEDGERS_PER_INDEX` ledgers (index) |
| Events | In-memory hot segment + persisted index deltas | Sequential event ID | Event XDR + metadata | Every 10_000 ledgers (chunk) |

- Ledger and txhash stores are RocksDB. WAL required.
- TxHash store uses 16 column families (`cf-0`..`cf-f`) routed by `txhash[0] >> 4`.
- Events hot segment is in-memory roaring bitmaps plus persisted per-ledger index deltas for crash recovery. See [getEvents full-history design](../../design-docs/getevents-full-history-design.md).

### Store Pre-creation

- The store for the next chunk / index is pre-created before the boundary is reached, so boundary-time work is a pointer swap only.
- Creation timing: when the ingestion loop commits a ledger within a configurable window before the boundary (e.g., `last_ledger_in_chunk(chunk_id) - 1_000`). The window must be large enough that store initialization (directory mkdir + RocksDB open + column family setup) completes before the boundary ledger arrives, and small enough that pre-creation doesn't run prematurely for chunks the daemon may never reach.
- On restart, a pre-created store is expected to exist — Phase 3 (reconcile) treats `resume_chunk + 1` (and `resume_index + 1`) as active, not an orphan.

### Max Concurrent Stores

| Store | Max active | Max transitioning | Max total |
|---|---|---|---|
| Ledger | 1 | 1 | 2 |
| Events | 1 (hot segment) | 1 (freezing cold segment) | 2 |
| TxHash | 1 | 1 | 2 |

---

## Ledger Source

- **Backfill (Phase 1 (catchup)) uses `BSBSource` only.** Each `process_chunk` instantiates its own per-chunk BSB via the `make_bsb` partial, prepares range for its 10_000 ledgers, reads, tears down. Captive core cannot be a backfill source — see [01-backfill-workflow.md — Backfill vs Phase 1](./01-backfill-workflow.md#backfill-vs-phase-1-catchup).
- **Live streaming (Phase 4 (live ingestion)) uses captive core directly** — no `LedgerSource` wrapper. Phase 4 (live ingestion) calls the stellar Go SDK's `ledgerBackend.PrepareRange(UnboundedRange(resume_ledger)) + GetLedger(seq)` against the captive-core subprocess.

```python
class BSBSource:
    # Used by backfill only. One instance per process_chunk task, torn down at end.
    # Interface mirrors the stellar Go SDK's LedgerBackend (PrepareRange + GetLedger).
    # prepare_range: sets the BSB-backed LedgerBackend's range; BSB prefetch workers
    #   (BUFFER_SIZE, NUM_WORKERS) fill buffers ahead of get_ledger.
    # get_ledger: SDK GetLedger(seq) reads from the prefetch buffer.
    # close: tears down the prefetch workers + connection.
    ...
```

### Make BSB Partial

```python
def make_bsb_partial(config):
    # Returns a partial that each process_chunk calls to get a fresh BSBSource.
    # None means Phase 1 (catchup) is a no-op; Phase 4 (live ingestion) captive core handles catchup.
    if config.bsb is None:
        return None
    return functools.partial(BSBSource, config.bsb)
```

---

## Startup Sequence

Four sequential phases, same code path for first start and every restart. The first three are bounded bootstrap work; Phase 4 (live ingestion) is the long-running state the daemon stays in until process exit.

- **Phase 1 — catchup.** Closes the gap between on-disk `:lfs` flags and current network tip **when `[BSB]` is configured**, by invoking the backfill subroutine in a loop. Without `[BSB]`, Phase 1 is a no-op and Phase 4's captive core handles initial catchup naturally via its own `PrepareRange(UnboundedRange(resume_ledger))`.
- **Phase 2 — hydrate txhash.** Loads any `.bin` files Phase 1 left (for the trailing partial index) into the active txhash store, then deletes them.
- **Phase 3 — reconcile orphans.** Completes any in-flight freeze transitions left by a prior crash. Truncates events hot segment beyond the last committed ledger.
- **Phase 4 — live ingestion.** Opens active stores, starts captive core, spawns the lifecycle goroutine, flips the `daemon_ready` flag, enters the ingestion loop. Runs until process exit.

"Phase" here refers to the startup ordering only. Once Phase 4 (live ingestion) is entered, there's no Phase 5 — the daemon is in live-streaming steady state.

### Backfill vs Phase 1 (catchup)

- **Backfill** is the subroutine (`run_backfill` in [01-backfill-workflow.md](./01-backfill-workflow.md)). BSB-only, runs parallel per-chunk BSB instances. Captive core cannot be a backfill source — its subprocess is serial and expensive to spin up per instantiation.
- **Phase 1 (catchup)** is a startup phase that runs on every daemon start. Its job: close the gap between on-disk state and current network tip before Phase 4 takes over.
- Phase 1 (catchup) invokes backfill as its mechanism — but only when `[BSB]` is configured. Without `[BSB]`, Phase 1 (catchup) is a no-op and Phase 4 (live ingestion)'s captive core handles catchup via `PrepareRange(UnboundedRange(resume_ledger))` as part of its own startup.
- So: "backfill" and "Phase 1 (catchup)" overlap because Phase 1 (catchup)'s whole purpose is "invoke backfill when BSB is configured".

```python
def run_rpc_service(config):
    meta_store = open_meta_store(config)
    validate_config(config, meta_store)
    make_bsb = make_bsb_partial(config)                     # None if [BSB] absent
    phase1_catchup(config, meta_store, make_bsb)
    phase2_hydrate_txhash(config, meta_store)
    phase3_reconcile_orphans(config, meta_store)
    resume_ledger = compute_resume_ledger(config, meta_store)
    phase4_live_ingest(config, meta_store, resume_ledger)
```

Query serving is gated on Phase 4 (live ingestion) being reached — see [Query Contract](#query-contract).

### Phase 1 — Catchup

- **No-op path:** if `make_bsb is None` (no `[BSB]` configured), Phase 1 (catchup) returns immediately. Phase 4 (live ingestion)'s captive core will catch up from a leapfrog'd resume ledger.
- **BSB path:** runs the backfill subroutine (`run_backfill` from [01-backfill-workflow.md](./01-backfill-workflow.md)) once per source-tip sample, until the gap closes to less than one chunk.
- Unit of work = one whole chunk, never partial. DAG dispatches chunk IDs; `process_chunk(chunk_id)` ingests `first_ledger_in_chunk..last_ledger_in_chunk` inclusive. Every chunk Phase 1 (catchup) persists starts at `..._02`, ends at `..._01` — the chunk-alignment invariant the no-gaps guarantee rests on.

```python
def phase1_catchup(config, meta_store, make_bsb):
    # Pure side-effect. Re-runs the full retention-aligned range on every start;
    # DAG idempotency inside run_backfill handles already-done chunks.
    if make_bsb is None:
        return                                                 # no [BSB] → no-op

    cpi               = config.service.chunks_per_txhash_index
    retention_ledgers = config.service.retention_ledgers
    last_scheduled_end_chunk = -1

    # Loop because tip advances during catchup; each iteration closes whatever's
    # accumulated since the previous sample.
    while True:
        network_tip_ledger = sample_network_tip(config.history_archives.urls)
        end_chunk = ((network_tip_ledger - (GENESIS_LEDGER - 1)) // LEDGERS_PER_CHUNK) - 1
        if end_chunk <= last_scheduled_end_chunk:
            break                                              # no new complete chunks since last iteration

        start_chunk = retention_aligned_start_chunk(network_tip_ledger, retention_ledgers, cpi)
        if end_chunk < start_chunk:
            break                                              # leapfrog landed past tip — pre-first-complete-chunk

        run_backfill(config, start_chunk, end_chunk, make_bsb)
        last_scheduled_end_chunk = end_chunk


def retention_aligned_start_chunk(network_tip_ledger, retention_ledgers, cpi):
    # Called by: phase1_catchup (per loop iteration) to compute range_start_chunk_id.
    # Returns the first chunk Phase 1 (catchup) should backfill:
    #   - Archive profile (retention=0): chunk 0 (full history from genesis).
    #   - Pruning-history (retention>0): first chunk of the tx index containing
    #     (tip - retention_ledgers). Aligned DOWN to a tx-index boundary so the first
    #     persisted chunk starts a complete index (upholds the no-gaps invariant).
    # Worst case: up to LEDGERS_PER_INDEX - 1 extra ledgers below strict retention.
    if retention_ledgers == 0:
        return 0
    target_ledger      = max(network_tip_ledger - retention_ledgers, GENESIS_LEDGER)
    target_chunk_id    = (target_ledger - GENESIS_LEDGER) // LEDGERS_PER_CHUNK
    target_tx_index_id = target_chunk_id // cpi
    return target_tx_index_id * cpi
```

**Worker concurrency:** `run_backfill` caps DAG concurrency at `GOMAXPROCS`. Each `process_chunk` owns its own BSB instance (`make_bsb()`), prepares range for its 10_000 ledgers, reads, and tears down — see [01-backfill-workflow.md — process_chunk](./01-backfill-workflow.md#process_chunkchunk_id-make_bsb).

**Retention effect:** retention determines Phase 1 (catchup)'s chunk range. Catchup time ≈ `retention_window / (BSB throughput)`.

### Phase 2 — Hydrate TxHash Data from `.bin`

- Phase 1 (catchup) may leave `.bin` files for chunks in the last (incomplete) tx index.
- Phase 2 (`.bin` hydration) loads each into the active txhash store, then deletes the `.bin` + `chunk:{chunk_id:08d}:txhash` flag.
- After Phase 2 (`.bin` hydration): no `.bin` files and no `:txhash` chunk flags remain.

```python
def phase2_hydrate_txhash(config, meta_store):
    cpi = config.service.chunks_per_txhash_index

    # Step 1: sweep leftover .bin for tx indexes already flagged complete — backfill
    # may have set index:N:txhash before cleanup_txhash finished on crash.
    for tx_index_id in tx_index_ids_with_txhash_flag(meta_store):
        for chunk_id in range(tx_index_id * cpi, (tx_index_id + 1) * cpi):
            if meta_store.has(f"chunk:{chunk_id:08d}:txhash"):
                meta_store.delete(f"chunk:{chunk_id:08d}:txhash")
                delete_if_exists(raw_txhash_path(chunk_id))

    # Step 2: load .bin for the trailing incomplete tx index into the active RocksDB.
    incomplete_tx_index_id = current_incomplete_tx_index_id(meta_store)
    if incomplete_tx_index_id is None:
        return

    txhash_store = open_active_txhash_store(config, incomplete_tx_index_id)
    try:
        for chunk_id in range(incomplete_tx_index_id * cpi, (incomplete_tx_index_id + 1) * cpi):
            if not meta_store.has(f"chunk:{chunk_id:08d}:txhash"):
                continue
            bin_path = raw_txhash_path(chunk_id)
            if os.path.exists(bin_path):
                load_bin_into_rocksdb(bin_path, txhash_store)
            meta_store.delete(f"chunk:{chunk_id:08d}:txhash")   # flag first
            delete_if_exists(bin_path)                           # then .bin

        # Step 3: sweep orphan .bin (flag gone, .bin lingering from a prior crash
        # between the two deletes above).
        for bin_file in scan_bin_files_for_tx_index(incomplete_tx_index_id):
            if not meta_store.has(f"chunk:{parse_chunk_id(bin_file):08d}:txhash"):
                os.remove(bin_file)
    finally:
        # Close before returning: Phase 4 (live ingestion) re-opens by directory path and the RocksDB
        # flock would collide if this handle stayed open.
        txhash_store.close()
```

**Why "load then delete" matters.**
- Without immediate deletion, every restart during the incomplete-index lifetime would re-load the same `.bin` files into RocksDB.
- At `cpi=1_000` with frequent restarts over a day: thousands of redundant loads.
- Load-then-delete makes Phase 2 (`.bin` hydration) a no-op on every subsequent restart until the next Phase 1 (catchup) deposits new `.bin` files.

**Pure-streaming restarts** (no recent Phase 1 (catchup) output) never see `.bin` files; streaming's live path writes txhash directly to the active RocksDB txhash store. Phase 2 (`.bin` hydration) is a no-op.

### Phase 3 — Reconcile Orphaned Transitions

Completes any in-flight transitions left by a prior crash. All decisions derive from meta store state + on-disk store directories.

```python
def phase3_reconcile_orphans(config, meta_store):
    # If no prior live ingestion (streaming:last_committed_ledger absent), no in-flight
    # freezes exist — fresh datadir or first-ever start. Short-circuit.
    last_committed_ledger = meta_store.get("streaming:last_committed_ledger")
    if last_committed_ledger is None:
        return

    cpi             = config.service.chunks_per_txhash_index
    resume_chunk_id = (last_committed_ledger + 1 - GENESIS_LEDGER) // LEDGERS_PER_CHUNK

    for store_dir in scan_ledger_store_dirs(config):
        chunk_id = parse_chunk_id_from_dir(store_dir)
        if chunk_id == resume_chunk_id or chunk_id == resume_chunk_id + 1:
            continue                                              # active / pre-created; keep
        if meta_store.has(f"chunk:{chunk_id:08d}:lfs"):
            delete_dir(store_dir)                                 # orphaned post-flush cleanup
        elif chunk_id < resume_chunk_id:
            finish_interrupted_ledger_freeze(store_dir, chunk_id, meta_store)
        else:
            delete_dir(store_dir)                                 # orphan future store

    resume_tx_index_id = resume_chunk_id // cpi
    for store_dir in scan_txhash_store_dirs(config):
        tx_index_id = parse_tx_index_id_from_dir(store_dir)
        if tx_index_id == resume_tx_index_id or tx_index_id == resume_tx_index_id + 1:
            continue
        if meta_store.has(f"index:{tx_index_id:08d}:txhash"):
            delete_dir(store_dir)                                 # RecSplit done; cleanup lingered
        elif all_chunks_in_tx_index_have_lfs_flag(meta_store, tx_index_id, cpi):
            # Re-spawn build. Pass the handle, not the dir path — build_tx_index_recsplit_files
            # reads from the store and closes it.
            transitioning_txhash = open_active_txhash_store(config, tx_index_id)
            run_in_background(build_tx_index_recsplit_files, tx_index_id, transitioning_txhash, meta_store)

    # Prevents duplicate event IDs when Phase 4 (live ingestion) replays the first live ledger.
    truncate_events_hot_segment(config, last_committed_ledger)
```

### Compute Resume Ledger

- `compute_resume_ledger` is a shared helper called once per daemon start, AFTER Phase 3 (reconcile) and BEFORE Phase 4 (live ingestion). Scans meta-store state end-to-end, validates on-disk consistency, and returns `resume_ledger` — the ledger sequence captive core is told to start emitting at via `PrepareRange(UnboundedRange(resume_ledger))`.
- **Runs AFTER Phase 3 (reconcile).** Phase 3's `finish_interrupted_ledger_freeze` writes `:lfs` for chunks whose freeze was in flight at a prior crash; running `compute_resume_ledger` before Phase 3 would see those mid-freeze chunks as internal `:lfs` gaps and false-positive-fatal at startup.
- **Scans every startup, even when `streaming:last_committed_ledger` is already set.** The scan's primary output in the mid-life-restart case is validation, not derivation; catching broken on-disk state before opening active stores is strictly safer than silently resuming on top.
- **Validation failures are fatal.** Any inconsistency aborts startup with "migration to streaming failed" + an operator-readable error naming what's wrong. The daemon exits non-zero; no active stores are opened.

**Derivation** — first match wins:

| `streaming:last_committed_ledger` | Scan result | Situation | `resume_ledger` |
|---|---|---|---|
| present | (validated consistent) | Mid-life restart (possibly after Phase 3 (reconcile) just finished in-flight freezes) | `value + 1` |
| absent | contiguous `:lfs` chunks `[start..end]` | First-ever post-Phase-1 (catchup), or crash between Phase 1 (catchup) end and first live commit | `last_ledger_in_chunk(end) + 1` |
| absent | no `:lfs` chunks | Tip-tracker fresh start (no `[BSB]`) | `retention_aligned_resume_ledger(config)` |

**Validation rules** (any violation → fatal):

- **No internal gap in `:lfs` coverage.** Example FAIL: chunks `[0..90] ∪ [92..N]` with `91` missing. A trailing "no chunks beyond N" is normal end-of-prefix, not a gap.
- **Start aligns to a tx-index boundary.** `start_chunk == 0` (archive) OR `start_chunk % cpi == 0` (pruning-history — first chunk of a tx index). Example FAIL at `cpi=100`: scan yields `[3456..N]`; `3456 % 100 ≠ 0`. Correct start would have been `3500`.
- **Chunk flags consistent.** Every chunk in the contiguous range has both `:lfs` AND `:events`. A chunk with one but not the other means `process_chunk` crashed mid-task and was never re-run.
- **Index flags consistent.** Every complete tx index fully inside `[start, end]` has `index:{tx_index_id:08d}:txhash`. Trailing partial indexes do NOT — those wait for Phase 2 (`.bin` hydration) on first start, or become Phase 3 (reconcile) build-respawn candidates on restart.
- **Live checkpoint consistent with scan.** When `streaming:last_committed_ledger = L` is present, chunks through `chunk_id_of_ledger(L) - 1` must all have `:lfs`. Example FAIL: `L = 56_345_672` (chunk 5_634 ingesting), but scan's highest contiguous chunk is 5_632 — chunk 5_633 must have been frozen before chunk 5_634 could be active; its absence means a recent immutable artifact went missing out of band. (Mid-freeze state at a prior crash does NOT false-positive this rule because Phase 3 (reconcile) has already finished any in-flight freeze before `compute_resume_ledger` runs.)

```python
def compute_resume_ledger(config, meta_store):
    # Called by: run_rpc_service orchestrator, after Phase 3 (reconcile), before Phase 4 (live ingestion).
    cpi  = config.service.chunks_per_txhash_index
    scan = scan_all_chunk_and_index_keys(meta_store)
    validate_scan(scan, cpi)

    last_committed_ledger = meta_store.get("streaming:last_committed_ledger")
    if last_committed_ledger is not None:
        validate_last_committed_consistency(scan, last_committed_ledger)
        return last_committed_ledger + 1

    if scan.lfs_chunks:
        end_chunk = scan.lfs_chunks[-1]                        # already validated contiguous
        return last_ledger_in_chunk(end_chunk) + 1

    return retention_aligned_resume_ledger(config)             # no on-disk chunks: Alice fresh start


def validate_scan(scan, cpi):
    # Called by: compute_resume_ledger (as part of the pre-derivation validation pass).
    # Fatal on any violation — "migration to streaming failed".
    if not scan.lfs_chunks:
        return
    start, end = scan.lfs_chunks[0], scan.lfs_chunks[-1]

    expected = set(range(start, end + 1))
    actual   = set(scan.lfs_chunks)
    if actual != expected:
        fatal(f"internal :lfs gap: missing chunks {sorted(expected - actual)}")

    if start != 0 and start % cpi != 0:
        fatal(f"start chunk {start} not tx-index aligned (expected multiple of cpi={cpi})")

    if actual != set(scan.events_chunks):
        fatal(":lfs / :events mismatch — a process_chunk task crashed mid-run and was never recovered")

    # Complete tx indexes = those whose ALL cpi chunks fall inside [start, end].
    first_complete_tx_index_id = (start + cpi - 1) // cpi
    last_complete_tx_index_id  = (end + 1) // cpi - 1
    complete  = set(range(first_complete_tx_index_id, last_complete_tx_index_id + 1))
    missing   = complete - set(scan.txhash_indexes)
    if missing:
        fatal(f"complete tx indexes {sorted(missing)} missing index:txhash flag")


def validate_last_committed_consistency(scan, last_committed_ledger):
    # Called by: compute_resume_ledger (when streaming:last_committed_ledger is present).
    # streaming:last_committed_ledger=L implies every chunk up to chunk_id_of_ledger(L)-1
    # must have :lfs. Chunk containing L itself is the currently-ingesting chunk and may
    # or may not have :lfs depending on whether L fell on a chunk boundary.
    active_chunk_id = (last_committed_ledger - GENESIS_LEDGER) // LEDGERS_PER_CHUNK
    required_last   = active_chunk_id - 1
    if required_last < 0:
        return
    actual_last = scan.lfs_chunks[-1] if scan.lfs_chunks else -1
    if actual_last < required_last:
        fatal(f"streaming:last_committed_ledger={last_committed_ledger} requires :lfs "
              f"through chunk {required_last}; scan's highest is {actual_last} — "
              f"a recent immutable artifact is missing")


def retention_aligned_resume_ledger(config):
    # Called by: compute_resume_ledger (tip-tracker fresh-start branch; no BSB, no on-disk chunks).
    # First chunk captive core ingests will be the first chunk of the tx index containing
    # (tip - retention). validate_config already rejected the [BSB]-absent + retention=0
    # combination, so this helper is never called in archive-from-genesis shape.
    network_tip_ledger = sample_network_tip(config.history_archives.urls)
    retention_ledgers  = config.service.retention_ledgers
    cpi                = config.service.chunks_per_txhash_index

    target_ledger      = max(network_tip_ledger - retention_ledgers, GENESIS_LEDGER)
    target_chunk_id    = (target_ledger - GENESIS_LEDGER) // LEDGERS_PER_CHUNK
    target_tx_index_id = target_chunk_id // cpi
    return (target_tx_index_id * cpi * LEDGERS_PER_CHUNK) + GENESIS_LEDGER
```

### Phase 4 — Live Ingestion

Opens active stores for the resume position, spawns the lifecycle goroutine, starts captive core, and enters the ingestion loop. Query serving starts here (see [Query Contract](#query-contract)).

```python
def phase4_live_ingest(config, meta_store, resume_ledger):
    # resume_ledger is already computed by the orchestrator (see Compute Resume Ledger).
    # Phase 4 (live ingestion) does NOT write streaming:last_committed_ledger at bootstrap — the first
    # write happens inside the live ingestion loop after the first durable commit
    # (invariant #9).
    active_stores = open_active_stores_for_resume(config, meta_store, resume_ledger)
    run_in_background(run_prune_lifecycle_loop, config, meta_store)

    ledger_backend = make_ledger_backend(config.captive_core.config_path)
    ledger_backend.PrepareRange(UnboundedRange(resume_ledger))

    set_daemon_ready()   # in-memory; unblocks queries
    run_live_ingestion_loop(config, ledger_backend, active_stores, meta_store, resume_ledger)


def open_active_stores_for_resume(config, meta_store, resume_ledger):
    # Open/WAL-recover the current store for each of ledger/events/txhash AND pre-create
    # the "next" stores so the first boundary rollover is a pointer swap.
    # Events hot segment replays persisted deltas from disk; safe because Phase 3 (reconcile) already
    # truncated anything past last_committed_ledger.
    resume_chunk_id    = (resume_ledger - GENESIS_LEDGER) // LEDGERS_PER_CHUNK
    resume_tx_index_id = resume_chunk_id // config.service.chunks_per_txhash_index

    return ActiveStores(
        ledger      = open_or_create_ledger_store(config, resume_chunk_id),
        ledger_next = open_or_create_ledger_store(config, resume_chunk_id + 1),
        events      = open_or_create_events_hot_segment(config, meta_store, resume_chunk_id, resume_ledger),
        events_next = open_or_create_events_hot_segment(config, meta_store, resume_chunk_id + 1, None),
        txhash      = open_or_create_txhash_store(config, resume_tx_index_id),
        txhash_next = open_or_create_txhash_store(config, resume_tx_index_id + 1),
    )
```

Captive core takes 4–5 minutes to spin up and start emitting at `resume_ledger`. During that window `getHealth` remains in `catching_up` state (see [Query Contract](#query-contract)).

---

## Ingestion Loop

Single goroutine. Pull-based: the daemon drives sequential `GetLedger(seq)` calls. Same code path drains captive core's internal buffer during catchup and switches cadence to live closes (~5 s per ledger) once caught up.

```python
def run_live_ingestion_loop(config, ledger_backend, active_stores, meta_store, resume_ledger):
    cpi = config.service.chunks_per_txhash_index   # immutable; read once
    ledger_seq = resume_ledger
    while True:
        lcm = ledger_backend.GetLedger(ledger_seq)   # blocks until available

        # Fan out to all three active stores; wait for all to durably commit before
        # advancing the checkpoint. Each write is idempotent on retry.
        wait_all(
            run_in_background(write_ledger_store,       active_stores.ledger, ledger_seq, lcm),
            run_in_background(write_txhash_store,       active_stores.txhash, ledger_seq, lcm),
            run_in_background(write_events_hot_segment, active_stores.events, ledger_seq, lcm),
        )

        # Atomic "daemon owns everything up to ledger_seq" signal — written only after
        # all three stores have durably committed. Distinct from Phase 1 (catchup)'s :lfs-derived
        # coverage end.
        meta_store.put("streaming:last_committed_ledger", ledger_seq)

        chunk_id = (ledger_seq - GENESIS_LEDGER) // LEDGERS_PER_CHUNK
        if ledger_seq == last_ledger_in_chunk(chunk_id):
            on_chunk_boundary(chunk_id, active_stores, meta_store)

        # Tx-index boundary runs AFTER on_chunk_boundary (every tx-index boundary is
        # also a chunk boundary).
        tx_index_id = chunk_id // cpi
        if ledger_seq == last_ledger_in_tx_index(tx_index_id):
            on_tx_index_boundary(tx_index_id, active_stores, meta_store)

        ledger_seq += 1
```

- Each per-store write is atomic: RocksDB WriteBatch + WAL for ledger and txhash stores; atomic commit of events hot-segment + persisted deltas.
- Key/value schemas are in [Active Store Architecture](#active-store-architecture).

---

## Freeze Transitions

Three independent background transitions per chunk/index boundary; each has its own goroutine, flag, and cleanup. Live ingestion never blocks on them.

- **LFS transition** — per chunk. Retired ledger RocksDB → `.pack` file.
- **Events transition** — per chunk. Retired events hot segment → cold segment (3 files).
- **RecSplit transition** — per index. Retired txhash RocksDB → 16 `.idx` files.
- Streaming's freeze transitions never produce `.bin` files; those are transient backfill output only (Phase 1).

### Concurrency Model

- **`active_stores` is the ingestion loop's owned state.** Fields (`ledger`, `ledger_next`, `events`, `events_next`, `txhash`, `txhash_next`) are mutated only by the ingestion loop thread — specifically inside `on_chunk_boundary` and `on_tx_index_boundary`. Freeze transitions receive a handle by value at spawn time and never read back through `active_stores`.
- **Meta-store is single-writer.** Meta-store flag writes come from: the ingestion loop (per-ledger checkpoint), freeze transitions (artifact `:lfs` / `:events` / `:txhash` flags after fsync), and the lifecycle loop (`"deleting"` marker + key delete during prune). Go's `sync.Mutex` inside the meta-store wrapper + RocksDB's own single-writer semantics keep these serialized.
- **`wait_for_lfs_complete()` / `wait_for_events_complete()` are per-kind single-flight gates.** One outstanding transition per kind (LFS / events / RecSplit). Implementation: an unbuffered `chan struct{}` per kind, or equivalently a `sync.Mutex`. `wait_for_lfs_complete()` acquires; `signal_lfs_complete()` at the end of `freeze_ledger_chunk_to_pack_file` releases. Second transition starts only after the first releases. Not a `sync.WaitGroup` — that would wait for ALL transitions globally, wrong semantics.
- **Query handlers read from storage-manager layer** (see [01-backfill-workflow.md](./01-backfill-workflow.md)'s sibling docs and the pending query-routing design). Each per-data-type storage manager owns its own state-transition synchronization; the query handler never touches `active_stores` directly.
- **Pre-creation happens at store-open time, not at a mid-chunk tripwire.** `open_active_stores_for_resume` (Phase 4 entry) opens BOTH `resume_chunk_id`'s store AND `resume_chunk_id + 1`'s store up front. Subsequent pre-creation happens inside `on_chunk_boundary` after the rollover — it opens `chunk_id + 2` so the NEXT rollover has the pre-created store already waiting. Amortizes creation cost; keeps the ingestion loop's hot path free of store-open latency.

### Chunk Boundary (every 10_000 ledgers)

Triggered when the ingestion loop commits `last_ledger_in_chunk(chunk_id)`. Handoffs to two freeze transitions (LFS + events) that run in background.

```python
def on_chunk_boundary(chunk_id, active_stores, meta_store):
    # LFS: drain the last in-flight LFS freeze (max-1-transitioning), swap pointers,
    # spawn the freeze.
    wait_for_lfs_complete()
    transitioning_ledger_store = active_stores.ledger
    active_stores.ledger = active_stores.ledger_next
    run_in_background(freeze_ledger_chunk_to_pack_file, chunk_id, transitioning_ledger_store, meta_store)

    # Events: same shape, independent goroutine (does NOT wait on LFS).
    wait_for_events_complete()
    freezing_events_segment = active_stores.events
    active_stores.events = active_stores.events_next
    run_in_background(freeze_events_chunk_to_cold_segment, chunk_id, freezing_events_segment, meta_store)

    # Pre-create "next-next" so the NEXT boundary is also a pointer swap. Background;
    # not on the hot path.
    run_in_background(precreate_next_boundary_stores, active_stores, meta_store, chunk_id + 2)

    notify_lifecycle()   # wake prune loop (this notification is ONLY for prune eligibility)


def precreate_next_boundary_stores(active_stores, meta_store, target_chunk_id):
    # Idempotent — safe to re-run when target stores already exist.
    active_stores.ledger_next = open_or_create_ledger_store(config, target_chunk_id)
    active_stores.events_next = open_or_create_events_hot_segment(config, meta_store, target_chunk_id, None)
    cpi = config.service.chunks_per_txhash_index
    target_tx_index_id = target_chunk_id // cpi
    if target_tx_index_id != tx_index_id_of_chunk(target_chunk_id - 1):
        active_stores.txhash_next = open_or_create_txhash_store(config, target_tx_index_id)
```

### LFS Transition

Converts the retired ledger RocksDB store to an immutable `.pack` file, then discards the store.

```python
def freeze_ledger_chunk_to_pack_file(chunk_id, transitioning_ledger_store, meta_store):
    # Order: overwrite=True (discard any prior partial) → write → fsync → flag → cleanup.
    # Flag-after-fsync. Crash between flag and store-delete leaves an orphan dir; Phase 3 (reconcile)
    # reconciles via `:lfs` present + store present → delete store.
    pack_path = ledger_pack_path(chunk_id)
    writer = packfile.create(pack_path, overwrite=True)
    for ledger_seq in range(first_ledger_in_chunk(chunk_id), last_ledger_in_chunk(chunk_id) + 1):
        writer.append(transitioning_ledger_store.get(uint32_big_endian(ledger_seq)))
    writer.fsync_and_close()
    meta_store.put(f"chunk:{chunk_id:08d}:lfs", "1")
    transitioning_ledger_store.close()
    delete_dir(ledger_store_path(chunk_id))
    signal_lfs_complete()


def finish_interrupted_ledger_freeze(store_dir, chunk_id, meta_store):
    # Phase 3 (reconcile) helper. Same as freeze_ledger_chunk_to_pack_file but opens the existing
    # store (WAL-recovered) and skips signal_lfs_complete (Phase 3 is synchronous).
    transitioning_ledger_store = open_or_create_ledger_store(config, chunk_id)
    pack_path = ledger_pack_path(chunk_id)
    writer = packfile.create(pack_path, overwrite=True)
    for ledger_seq in range(first_ledger_in_chunk(chunk_id), last_ledger_in_chunk(chunk_id) + 1):
        writer.append(transitioning_ledger_store.get(uint32_big_endian(ledger_seq)))
    writer.fsync_and_close()
    meta_store.put(f"chunk:{chunk_id:08d}:lfs", "1")
    transitioning_ledger_store.close()
    delete_dir(store_dir)
```

### Events Transition

Converts the retired events hot segment to three immutable files (events cold segment).

```python
def freeze_events_chunk_to_cold_segment(chunk_id, freezing_events_segment, meta_store):
    # Same flag-after-fsync order as freeze_ledger_chunk_to_pack_file.
    events_path = events_segment_path(chunk_id)
    write_cold_segment(freezing_events_segment, events_path)   # 3 files: events.pack, index.pack, index.hash
    fsync_all(events_path)
    meta_store.put(f"chunk:{chunk_id:08d}:events", "1")
    freezing_events_segment.discard()                          # drops in-memory bitmaps + persisted deltas
    signal_events_complete()
```

### Tx-Index Boundary (every `LEDGERS_PER_INDEX` ledgers)

The last chunk of a tx index has just rolled over. Before RecSplit can start, every chunk in the tx index must have its `:lfs` and `:events` flags set.

```python
def on_tx_index_boundary(tx_index_id, active_stores, meta_store):
    # Drain ALL in-flight LFS + events (the final chunk's freeze may still be running)
    # — RecSplit cannot race its input.
    wait_for_lfs_complete()
    wait_for_events_complete()
    verify_all_chunk_flags(tx_index_id, meta_store)   # defense-in-depth

    transitioning_txhash_store = active_stores.txhash
    active_stores.txhash       = active_stores.txhash_next
    run_in_background(build_tx_index_recsplit_files, tx_index_id, transitioning_txhash_store, meta_store)
```

### RecSplit Transition

Builds the 16 RecSplit `.idx` files for tx_index_id from the retired txhash active store.

```python
def build_tx_index_recsplit_files(tx_index_id, transitioning_txhash_store, meta_store):
    # Same flag-after-fsync pattern as LFS / events freeze; verify before flag.
    idx_path = recsplit_index_path(tx_index_id)
    delete_partial_idx_files(idx_path)
    build_recsplit(transitioning_txhash_store, idx_path)   # 16 .idx files
    fsync_all_idx_files(idx_path)
    verify_spot_check(tx_index_id, idx_path, meta_store)
    meta_store.put(f"index:{tx_index_id:08d}:txhash", "1")
    transitioning_txhash_store.close()
    delete_dir(txhash_store_path(tx_index_id))
```

---

## Pruning

Retention is enforced by a single background goroutine, woken at chunk boundaries. Prune granularity is the whole txhash index — never per chunk.

```python
def run_prune_lifecycle_loop(config, meta_store):
    # Initial scan at entry catches any `"deleting"` state left by a prior crashed prune;
    # without it, a crashed prune could sit unserviced until the next chunk boundary
    # (up to ~16 h at cpi=1). Subsequent sweeps fire on chunk-boundary notifications.
    cpi = config.service.chunks_per_txhash_index
    retention_ledgers = config.service.retention_ledgers

    _run_prune_sweep(meta_store, retention_ledgers, cpi, config)
    while True:
        wait_for_chunk_boundary_notification()
        _run_prune_sweep(meta_store, retention_ledgers, cpi, config)


def _run_prune_sweep(meta_store, retention_ledgers, cpi, config):
    for tx_index_id in prunable_tx_index_ids(meta_store, retention_ledgers, cpi):
        prune_tx_index(tx_index_id, meta_store, config)


def prunable_tx_index_ids(meta_store, retention_ledgers, cpi):
    # Returns tx_index_ids fully past retention and still prune-eligible (`:txhash` is
    # `"1"` or `"deleting"`). Uses streaming:last_committed_ledger (daemon's own progress),
    # not the source-reported tip.
    #
    # Derivation: tx_index_id eligible iff
    #   last_committed_ledger > last_ledger_in_tx_index(tx_index_id) + retention_ledgers
    # → max_eligible_tx_index_id = ((last_committed_ledger - GENESIS_LEDGER - retention_ledgers)
    #                               // LEDGERS_PER_INDEX) - 1
    # Check at last_committed_ledger=70_000_002, retention_ledgers=10_000_000, cpi=1_000:
    #   max_eligible = (70_000_002 - 2 - 10_000_000) // 10_000_000 - 1 = 5.
    #   tx_index 5 ends at 60_000_001 + 10M = 70_000_001; 70_000_002 > that → eligible. ✓
    #   tx_index 6 ends at 70_000_001 + 10M = 80_000_001; NOT eligible. ✓
    if retention_ledgers == 0:
        return []
    last_committed_ledger = meta_store.get("streaming:last_committed_ledger")
    ledgers_per_index = cpi * LEDGERS_PER_CHUNK
    max_eligible_tx_index_id = ((last_committed_ledger - GENESIS_LEDGER - retention_ledgers) // ledgers_per_index) - 1
    if max_eligible_tx_index_id < 0:
        return []
    result = []
    for tx_index_id in range(0, max_eligible_tx_index_id + 1):
        val = meta_store.get(f"index:{tx_index_id:08d}:txhash")
        if val in ("1", "deleting"):
            result.append(tx_index_id)
    return result


def prune_tx_index(tx_index_id, meta_store, config):
    # Two-phase marker for query-routing safety: set "deleting" BEFORE any file delete;
    # clear the key AFTER. Queries short-circuit on "deleting" (treated as absent).
    # Idempotent on crash-between-stages retry.
    cpi = config.service.chunks_per_txhash_index

    meta_store.put(f"index:{tx_index_id:08d}:txhash", "deleting")

    for chunk_id in range(tx_index_id * cpi, (tx_index_id + 1) * cpi):
        delete_if_exists(ledger_pack_path(chunk_id))
        delete_events_segment(chunk_id)
        meta_store.delete(f"chunk:{chunk_id:08d}:lfs")
        meta_store.delete(f"chunk:{chunk_id:08d}:events")
    delete_recsplit_idx_files(tx_index_id)

    meta_store.delete(f"index:{tx_index_id:08d}:txhash")
```

**Why index-atomic.**
- Per-chunk pruning would open a window where `getTransaction` resolves to a ledger seq whose pack has already been deleted.
- Whole-index gating closes that window.

**How much extra data sits on disk.**
- At most `LEDGERS_PER_INDEX - 1` ledgers past the strict retention line.
- `RETENTION_LEDGERS` is a multiple of `LEDGERS_PER_INDEX`, so the line never bisects an index; the next-eligible index is exactly `LEDGERS_PER_INDEX` further.

---

## Query Contract

Query serving is gated on Phase 4 (live ingestion) being reached. `getLedger`, `getTransaction`, `getEvents` all return **HTTP 4xx** during Phases 1–3.

### Readiness Signal

- An in-memory boolean `daemon_ready` is set by `set_daemon_ready()` at the top of Phase 4 (live ingestion), after Phases 1–3 complete and active stores are opened.
- Not persisted. On every startup the flag starts `false`; on every Phase 4 (live ingestion) entry it flips to `true`. Clean shutdown discards it implicitly (process exits).
- This means: clients see `HTTP 4xx` from `getLedger`/`getTransaction`/`getEvents` on every startup until Phase 4 (live ingestion) is reached, regardless of whether prior runs have served queries. Intentional: catchup and recovery phases must complete before the daemon serves, every time.
- Query handlers check the flag on each request. `false` → HTTP 4xx. `true` → route normally.

### Behavior During Phases 1–3

- `/getLedger`, `/getTransaction`, `/getEvents` → `HTTP 4xx` with no payload detail.
- `/getHealth` → always served; returns `catching_up` + drift when daemon is pre-Phase-4, otherwise `streaming` + drift.
- No partial / incremental serving. The daemon does not serve "whatever is ingested so far" while Phases 1–3 are running.

### Behavior When an Index Is Being Pruned

- `prune_tx_index` sets `index:{tx_index_id:08d}:txhash = "deleting"` before touching any files, and deletes the key after all files are gone. Query routing treats `"deleting"` identically to `"absent"` (key-not-present).
- Queries for a ledger in a pruning index return HTTP 4xx (past retention) starting the instant the `"deleting"` marker is set, not when the files actually disappear. No window where queries route into a half-deleted index.

### Rationale

- Without an explicit gate, implementations drift toward "best-effort serve whatever is ingested" — inconsistent across operators, breaks client assumptions.
- Explicit `daemon_ready` + HTTP 4xx gives clients an unambiguous signal.
- `catching_up` health status gives operators visibility into progress.

---

## Crash Recovery

No separate recovery phase. Every startup runs Phases 1–4 regardless — already-complete work is detected and skipped via meta store flags.

### Invariants

In addition to the backfill subroutine's invariants in [01-backfill-workflow.md — Crash Recovery](./01-backfill-workflow.md#crash-recovery), streaming adds the following:

1. **Per-ledger checkpoint.** `streaming:last_committed_ledger` is written only after all three active stores durably commit. Resume is `last_committed_ledger + 1`.
2. **No separate recovery phase.** Startup is Phases 1–4. Nothing else.
3. **Max-1-transitioning per freeze.** A freeze transition must complete before the next one starts, per kind (LFS, events, RecSplit). Applies in steady state and crash recovery.
4. **Retention immutable.** `config:retention_ledgers` is stored on first run and compared thereafter. No mid-run retention change. Past-retention orphans can only arise from leapfrog — and leapfrog is deterministic, so Phase 1 (catchup) itself avoids producing them.
5. **Two-phase prune marker.** `prune_tx_index` writes `index:{tx_index_id}:txhash = "deleting"` before any file delete and clears the key after. Queries treat `"deleting"` as absent. Crash mid-prune resumes idempotently on restart because `"deleting"` is still picked up by `prunable_tx_index_ids`.

### Compound Recovery Scenarios

Backfill's crash-recovery model in [01-backfill-workflow.md](./01-backfill-workflow.md#crash-recovery) handles every Phase 1 (catchup) crash. Streaming adds:

- **Crash during Phase 2 (`.bin` hydration).**
  - Chunks loaded pre-crash: no `:txhash` flag, no `.bin` → loop skips via flag check.
  - Chunks not yet loaded: `:txhash` + `.bin` present → loop picks them up.

- **Crash between per-ledger checkpoint and LFS freeze completion.**
  - State: `streaming:last_committed_ledger = last_ledger_in_chunk(chunk_id)`; `chunk:{chunk_id}:lfs` absent.
  - Phase 1 (catchup) on restart (assumes `[BSB]` configured): `:lfs` missing → re-runs `process_chunk(chunk_id)` with a fresh per-task BSB (idempotent per artifact).
  - Phase 3 (reconcile) then: active ledger store present + `:lfs` now set → deletes the orphaned store.
  - Cost: ~10_000 ledgers of redundant ingestion per affected chunk. Correctness preserved.

- **Crash mid-RecSplit.**
  - State: `index:{tx_index_id}:txhash` absent; all `:lfs` chunks of the tx index present.
  - Phase 3 (reconcile): re-spawns the RecSplit build after deleting partial `.idx` files.

- **Crash mid-prune.**
  - State: some files deleted, some chunk keys cleared, `index:{tx_index_id}:txhash = "deleting"` still present.
  - `prunable_tx_index_ids` picks up `"deleting"` alongside `"1"` → `prune_tx_index(tx_index_id)` re-runs, idempotent (file deletes `rm -f`, key deletes `delete_if_exists`).

---

## Backpressure and Drift Detection

- Live ingestion runs at the network's production rate (~1 ledger / 6 s). Freeze transitions run in background and must not stall the ingestion loop.
- If ingestion drifts, the cause is typically disk I/O saturation or RocksDB compaction stalls.

### Drift Metric

```python
drift_ledgers = ledger_backend.latest_tip() - meta_store.get("streaming:last_committed_ledger")
```

- Exposed as a Prometheus gauge `streaming_drift_ledgers`.
- `getHealth` returns `unhealthy` when `drift_ledgers > DRIFT_WARNING_LEDGERS` (default 10).
- No automatic response (no pause, no abort). Operator investigates.

---

## Error Handling

Three distinct policies — runtime ABORT, transition retry-via-flag-absence, startup FATAL.

### Runtime (Phase 4 ingestion)

- **CaptiveStellarCore unavailable.** RETRY with backoff; ABORT after `CAPTIVE_CORE_RETRY_MAX` attempts (implementation-defined).
- **Per-ledger store write failure (ledger / txhash / events).** ABORT — disk full or storage corruption.
- **Meta-store write failure.** ABORT — cannot maintain checkpoint.

### Freeze transitions (LFS / events / RecSplit)

All three follow the flag-after-fsync invariant: on failure, don't set the completion flag; abort the transition; restart retries the whole transition from scratch (partial `.idx` files get cleaned by the build's own preamble).

- **RecSplit verification mismatch.** ABORT; do NOT delete the transitioning txhash store; operator investigates.

### Startup (FATAL — datadir / config issues)

- `CHUNKS_PER_TXHASH_INDEX` or `RETENTION_LEDGERS` changed: wipe datadir to change.
- `RETENTION_LEDGERS` not a multiple of `LEDGERS_PER_INDEX`: fix config.
- Head not index-aligned / gap in chunk flags: datadir corruption; wipe.
