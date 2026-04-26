# Streaming Workflow

## Overview

stellar-rpc is the **unified full-history RPC service** — historical backfill and live streaming under one binary, one invocation, one long-running process.

- Operator runs `stellar-rpc --config path/to/config.toml`. No subcommand. No `--mode` flag. No behavior-switching flags.
- On every start, the service runs four sequential startup phases, then enters a live ingestion loop it stays in until killed.
- Behavior across the three operator profiles — **archive** (full history), **pruning-history** (retention-windowed history with bulk catchup from a remote object store), **tip-tracker** (retention-windowed history, no object store; captive-core-only) — is determined entirely by TOML config; no profile flag. Full matrix: [Operator Profiles](#operator-profiles).
- Backfill (specified in [01-backfill-workflow.md](./01-backfill-workflow.md)) is used as an internal subroutine by Phase 1 (catchup). Operators never invoke backfill directly.

**What the service does end-to-end:**
- Validates config against immutable meta-store state: `CHUNKS_PER_TX_INDEX` (chunks-per-tx-index constant; defines on-disk layout) and `RETENTION_LEDGERS` (history window in ledgers, or `0` for full history). Both detailed in [Configuration](#configuration).
- Catches up to the current **network tip** (most recent ledger the Stellar network has produced, sampled from the history archive — defined in [Terminology](#terminology)) using **BSB** (Buffered Storage Backend — remote object-store reader for `LedgerCloseMeta`; see [Ledger Source](#ledger-source)) or captive core (embedded `stellar-core` subprocess; see [Ledger Source](#ledger-source)), whichever is configured.
- Hydrates any in-flight state left by a prior run.
- Ingests live ledgers from captive core.
- Writes each live ledger to three **active Rocksdb stores** — mutable per-chunk or per-index RocksDB instances for ledger, txhash, events — detailed in [Active Store Architecture](#active-store-architecture).
- Freezes active stores to immutable files at chunk and index boundaries in background.
- Prunes past-retention indexes atomically when retention is configured.
- Serves `getLedger`, `getTransaction`, `getEvents` only after startup phases complete. Returns HTTP 4xx during startup.

---

## Terminology

Vocabulary used throughout this doc. Skim on first read; refer back as terms come up.

- **Service** — the stellar-rpc binary running as one long-lived process. The only thing an operator starts.

- **Startup phases 1–4** — the four steps the service runs at every start before it begins serving queries. Phase 1 catches up history, Phase 2 hydrates leftover state, Phase 3 reconciles anything left mid-flight by a prior crash, Phase 4 takes over for live streaming. Once Phase 4 is reached, the service stays there until it exits — there is no Phase 5.

- **Phase 1 (catchup)** — the startup step that closes the gap between what's already on disk and what the Stellar network has produced so far. Uses backfill as its mechanism.

- **Backfill** — the process of pulling historical ledgers from a remote object store and writing them to disk as immutable artifacts. Backfill is internal to the service — operators never invoke it directly. Specified in [01-backfill-workflow.md](./01-backfill-workflow.md).

- **Retention-aligned start** — how the service picks the starting chunk when retention is configured: the start always lands on a tx-index boundary, never mid-index, so the first tx index ingested is complete. Without this rounding, the chunks before the start would fall below the retention floor and never be ingested, leaving the tx index broken and the ingest-work on its later chunks wasted. Used in two places: Phase 1 (catchup) range-start computation when BSB is configured, and `compute_resume_ledger`'s no-BSB path (fresh start or stale-marker recovery).

- **Network tip** — the most recent ledger the Stellar network has produced. The service learns this from a public Stellar history archive over HTTP, not from its own state.

- **Resume ledger** — at every start, the service decides which ledger it should resume live ingestion at, based on what's already on disk plus anything a prior crash left mid-flight. The first ledger ingested in the new run is the resume ledger.

- **`streaming:last_committed_ledger`** — the local state-store key that records the last ledger the service successfully wrote during live streaming. Updated once per live ledger; never written during the startup phases.

- **Active store** — a writable store that holds in-flight data for whatever chunk or txhash index is currently being ingested. Three kinds, one per data type:
  - **Ledger active store** — one instance per chunk.
  - **TxHash active store** — one instance per txhash index.
  - **Events active store** — one instance per chunk.

- **Immutable store** — on-disk files produced when an active store is frozen. Three kinds, paired with the active stores above:
  - **Ledger pack file** — one per chunk.
  - **TxHash lookup files** — multiple per txhash index, for fast `txhash → ledger` lookup.
  - **Events cold segment** — three files per chunk.

- **Freeze transition** — the background work of converting an active store into its immutable counterpart, then deleting the active store. Three kinds: **ledger freeze (LFS)** and **events freeze** happen at every chunk boundary; **txhash freeze** happens at every index boundary.

- **Chunk** — a block of 10_000 consecutive ledgers. Atomic unit of ingestion and freeze: every chunk on disk is a complete 10_000-ledger chunk, never partial.

- **Txhash index** (a.k.a. "tx index" or just "index") — a group of consecutive chunks (default: 1_000 chunks = 10_000_000 ledgers). Atomic unit of retention pruning: a tx index is pruned as a whole, never per chunk. Formulas in [Geometry](#geometry).

- **Chunk boundary** — the moment ingestion finishes a chunk. Triggers the chunk's ledger and events freezes in the background.

- **Index boundary** — the moment ingestion finishes a tx index. Triggers the tx index's txhash freeze in the background. Every index boundary is also a chunk boundary.

- **`.bin` file** — a transient on-disk file produced by backfill while a tx index is still being filled in. Holds the raw txhashes for one chunk. Deleted once the tx index is complete (or once its contents are loaded into the active txhash store at startup).

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

Streaming extends `[SERVICE]` with extra keys and introduces `[CAPTIVE_CORE]` (embedded `stellar-core` subprocess settings), `[ACTIVE_STORAGE]` (active RocksDB paths), and `[HISTORY_ARCHIVES]` (Stellar history-archive URLs for tip sampling) — all defined in [TOML Sections Documented Here](#toml-sections-documented-here) below.

### Immutable Keys (stored in meta store, fatal if changed)

Stored on first start; fatal on any subsequent start where the config value differs. Changing either requires wiping the datadir.

| Key | Stored under | Set by | Rule |
|---|---|---|---|
| `CHUNKS_PER_TX_INDEX` | `config:chunks_per_tx_index` | first run | Fatal if changed. |
| `RETENTION_LEDGERS` | `config:retention_ledgers` | first run | Fatal if changed. |

- Source selection (BSB vs captive core) is determined per-startup by `[BSB]` presence; not stored as immutable.
- Operators may add or remove `[BSB]` between runs; `compute_resume_ledger` derives resume from on-disk chunks regardless of which source produced them. `RETENTION_LEDGERS` already pins the retained ledger window — locking the source choice would add nothing.

### TOML Sections Documented Here

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

| Key | Type | Default | Description                                                                                                                                                                                                             |
|---|---|---|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `URLS` | []string | **required** | List of Stellar history archive URLs. Used to sample network tip for the no-BSB resume-cursor calculation (when `[BSB]` is absent and the service needs a tip reference for retention floor / fresh-start alignment). |

**[BSB]** (optional)

- Same schema as in the backfill doc. Presence in the config file determines Phase 1 (catchup) behavior:
  - Present: Phase 1 (catchup) invokes backfill over the BSB (fast, parallel per-chunk catchup).
  - Absent: Phase 1 (catchup) is a no-op; Phase 4 (live ingestion)'s captive core archive-catches-up from a `resume_ledger` aligned to the retention-aligned tx-index boundary (slower, but no object-store dependency).
- See [Ledger Source](#ledger-source) for the BSB-source details and [Backfill vs Phase 1 (catchup)](#backfill-vs-phase-1-catchup) for the full split.

### CLI Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--config` | string | **required** | Path to TOML config file. |
| `--log-level` | string | from `[LOGGING].LEVEL` | Override log level. |
| `--log-format` | string | from `[LOGGING].FORMAT` | Override log format. |

**No other flags.** - No `--mode`; no `--start-ledger`, `--end-ledger`; no separate subcommand for backfill or streaming. Any per-run behavior is either driven by config or derived at runtime from meta store + tip.

### Validation Rules

- `CHUNKS_PER_TX_INDEX` - immutable across runs (see [Immutable Keys](#immutable-keys-stored-in-meta-store-fatal-if-changed)).
- [`RETENTION_LEDGERS` - immutable across runs. Must be `0` OR a positive integer multiple of `LEDGERS_PER_TX_INDEX` (defined in [01-backfill-workflow.md — Geometry](./01-backfill-workflow.md#geometry)).
  - Valid values of `RETENTION_LEDGERS` at `cpi=1_000`: `0`, `10_000_000`, `20_000_000`, `30_000_000` etc.
  - Invalid: `15_000_000` (not a multiple), `5_000_000` (below minimum/not a multiple).
  - Rationale: pruning runs at whole-index granularity; retention windows that don't align to index boundaries would leave partial indexes perpetually on disk.
- `[BSB]` optional. When present → Phase 1 (catchup) invokes backfill over the BSB; when absent → Phase 1 (catchup) is a no-op and Phase 4 (live ingestion)'s captive core handles initial catchup. May be added or removed between runs.
- **`[BSB]` absent AND `RETENTION_LEDGERS = 0` is fatal.** Full history requires BSB — captive-core archive-catchup from genesis would take weeks-to-months. Not a supported operating mode.
- `[HISTORY_ARCHIVES].URLS` required in all profiles.
- `[CAPTIVE_CORE].CONFIG_PATH` required in all profiles.
- `[CAPTIVE_CORE].STELLAR_CORE_BINARY_PATH` required in all profiles.
- `[SERVICE].NETWORK_PASSPHRASE` required in all profiles.

### Validation Pseudocode

`validate_config` applies the rules above and then enforces immutability for the two immutable keys. The non-obvious mechanism is the immutable-key check itself — store on first run, compare on every subsequent run:

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

Three profiles emerge from config combinations. No profile flag.

| Profile | `RETENTION_LEDGERS` | `[BSB]` | Phase 1 behavior | Use case |
|---|---|---|---|---|
| Archive | `0` | present | Backfill over full history (chunks `[0, current_chunk − 1]`) | Public archive node; full history. |
| Pruning-history | `N × LEDGERS_PER_TX_INDEX`, N ≥ 1 | present | Backfill over retention window (start aligned to first chunk of the tx index containing the retention floor) | Windowed history with bulk initial catchup. |
| Tip-tracker | `N × LEDGERS_PER_TX_INDEX`, N ≥ 1 | absent | **No-op.** Phase 4 (live ingestion)'s captive core archive-catches-up from a `resume_ledger` aligned to the retention-aligned tx-index boundary | App developer; short retention; no object-store dep. |
| (invalid) | `0` | absent | — | Rejected by `validate_config`: full history requires BSB. |

---

## Meta Store Keys

*This section is a reference for the key schema and lifecycle. It reads more naturally after [Startup Sequence](#startup-sequence) below, which defines the phases that write and consume these keys.*

Single RocksDB instance, WAL (Write-Ahead Log) always enabled. Authoritative source for every startup decision.

### Keys Introduced by Streaming

| Key | Value | Written when |
|---|---|---|
| `streaming:last_committed_ledger` | uint32 (big-endian) | Monotonic progress marker — highest ledger that has been persisted. Two writers (both via `advance_progress_marker`, never regressing): (1) Phase 1 (catchup) at end of catchup, advancing to `last_ledger_in_chunk(highest completed chunk)`; (2) live ingestion loop, per ledger, after all three active stores durably commit. When absent, [`compute_resume_ledger`](#compute-resume-ledger) falls back to the highest `:lfs` chunk (Phase 1 crashed before the post-catchup write) or to `retention_aligned_resume_ledger` (tip-tracker fresh start, no BSB, never ingested). |
| `config:retention_ledgers` | decimal string | First run (stored); enforced on subsequent starts. |
| `hot:chunk:{chunk_id:08d}:lfs` | `"1"` | Written **before** the active ledger store directory is created; deleted **after** that directory is removed by the freeze task. Presence indicates the directory exists or its lifecycle is incomplete (creation in flight, or freeze cleanup not yet finished). |
| `hot:chunk:{chunk_id:08d}:events` | `"1"` | Same pattern as `hot:chunk:lfs`, scoped to the active events store directory. |
| `hot:index:{tx_index_id:08d}:txhash` | `"1"` | Same pattern, scoped to the active txhash store directory. Per-index cadence (one per tx index, not per chunk). |
| `pruning:index:{tx_index_id:08d}` | `"1"` | Set by `prune_tx_index` BEFORE any file delete; cleared AFTER all of tx_index_id's artifacts and the `index:{tx_index_id}:txhash` key are gone. Presence means a prune is in progress (or was interrupted by a crash and needs to be re-attempted). QueryRouter treats presence as "this tx index is unservable" and returns 4xx. Lifecycle loop's `prunable_tx_index_ids` includes any index with this key set, so a crashed prune resumes idempotently on restart. |

### Keys Shared with Backfill

Defined in [01-backfill-workflow.md — Meta Store Keys](./01-backfill-workflow.md#meta-store-keys); streaming uses the same contract:

- `config:chunks_per_tx_index`
- `chunk:{chunk_id:08d}:lfs`
- `chunk:{chunk_id:08d}:events`
- `chunk:{chunk_id:08d}:txhash`
- `index:{tx_index_id:08d}:txhash`

Streaming-specific use of these keys (which paths write them when) is shown in [Key Lifecycle in Streaming](#key-lifecycle-in-streaming) below. All values are binary (`"1"` or absent); prune-in-progress is tracked via the separate `pruning:index:*` key family rather than overloading the value space.

### Key Lifecycle in Streaming

```
Phase 1 (catchup) — every freeze flag set AFTER its artifact's fsync:
  chunk:{chunk_id}:lfs       = "1"
  chunk:{chunk_id}:txhash    = "1"     # only while .bin is still on disk
  chunk:{chunk_id}:events    = "1"
  index:{tx_index_id}:txhash = "1"     # set when all chunks of tx_index_id are done

Phase 2 (.bin hydration — see Startup Sequence) — file-before-flag-delete:
  for each chunk with :txhash flag:
    if .bin exists: load into active txhash RocksDB
    delete .bin file
    delete chunk:{chunk_id}:txhash flag
  After Phase 2, no :txhash chunk flags and no .bin files remain.

Active store open (Phase 2 / Phase 4 entry / boundary handlers) —
hot:* keys set BEFORE mkdir, one per active store kind:
  hot:chunk:{chunk_id}:lfs       = "1"
  hot:chunk:{chunk_id}:events    = "1"
  hot:index:{tx_index_id}:txhash = "1"

Live path (per ledger, after all 3 active stores commit):
  streaming:last_committed_ledger = ledger_seq

Live path (per chunk, background freeze) — flag AFTER fsync, hot key AFTER dir delete:
  chunk:{chunk_id}:lfs    = "1"  → delete ledger store dir → clear hot:chunk:{chunk_id}:lfs
  chunk:{chunk_id}:events = "1"  → delete events store dir → clear hot:chunk:{chunk_id}:events

Live path (per index, background freeze) — same pattern:
  index:{tx_index_id}:txhash = "1" → delete txhash store dir → clear hot:index:{tx_index_id}:txhash

Pruning (background, when tx_index_id is past retention) — separate pruning marker:
  pruning:index:{tx_index_id} = "1"         (queries return 4xx from here on)
  delete all files + per-chunk :lfs + :events keys for tx_index_id
  index:{tx_index_id}:txhash → deleted
  pruning:index:{tx_index_id} → deleted     (cleared LAST; survives crashes for recovery)
```

### Flag Semantics

- **Flag-after-fsync (creation order).** A flag is set only AFTER the artifact it represents has been fsynced. Flag absent ⇒ artifact missing or incomplete; flag present ⇒ artifact is durable.
- **File-before-flag-delete (cleanup order).** The file is removed FIRST; the flag is cleared LAST. Flag present ⇒ cleanup may be incomplete; flag absent ⇒ cleanup done, no file exists. Reverse order would orphan a file with no meta-store record on a crash mid-pair, recoverable only by filesystem scan.
- **Flag-driven recovery.** Every startup decision — hydration, transition replay, RecSplit spawn, prune eligibility, active-store directory reconciliation — derives from meta-store key presence. No filesystem-scan-and-infer anywhere.

_**The meta-store flag is the always-correct signal of artifact state on disk, both for immutable files and for active-store directories. A crash anywhere leaves a state the next start recovers from by flag presence alone.**_

---

## Active Store Architecture

Three RocksDB-backed active stores; WAL always enabled. Each directory has a `hot:*` key (set before mkdir, cleared after dir removal); Phase 3 (reconcile) finds directories needing recovery via meta-store scan, never filesystem scan. Lifecycle driven by [freeze transitions](#freeze-transitions).

- **Ledger** — one per chunk at `{ACTIVE_STORAGE.PATH}/ledger-store-chunk-{chunk_id:08d}/`. Key `uint32BE(ledgerSeq)`, value `zstd(LCM bytes)`.
- **TxHash** — one per tx index at `{ACTIVE_STORAGE.PATH}/txhash-store-index-{tx_index_id:08d}/`. Key `txhash[32]`, value `uint32BE(ledgerSeq)`. 16 column families (`cf-0`..`cf-f`) routed by `txhash[0] >> 4`; each CF pairs 1:1 with one of the 16 RecSplit `.idx` files at the index boundary.
- **Events** — one per chunk at `{ACTIVE_STORAGE.PATH}/events-store-chunk-{chunk_id:08d}/`. Schema per [getEvents full-history design](../../design-docs/getevents-full-history-design.md). Per-ledger writes are idempotent — re-write of the same ledger overwrites cleanly, so crash-replay is corruption-free.

### Store Lifecycle

- **Boundary swap.** At every chunk boundary the next chunk's ledger + events stores open synchronously while the just-finished ones are handed to background freeze tasks; tx-index boundaries do the same for txhash. Each kind therefore holds at most one active + one transitioning at a time. Ingestion never blocks on the freeze.
- **Synchronous open cost.** ~100 ms maximum — small enough to ignore.
- **Deletion.** The freeze task deletes the active store's directory only after the immutable artifact is fsynced and its freeze flag is set.
- **Crash recovery.** Active-store directories surviving a crash are reconciled on the next start — see [Phase 3 — Reconcile](#phase-3--reconcile).

---

## Ledger Source

Two ledger sources, scoped to different phases:

- **Backfill (Phase 1 (catchup)) uses `BSBSource`** — backfill-only reader (`PrepareRange` + `GetLedger`). Each `process_chunk` constructs its own scoped to its chunk's 10_000 ledgers. Captive core cannot be a backfill source — see [Backfill vs Phase 1 (catchup)](#backfill-vs-phase-1-catchup).
- **Live streaming (Phase 4 (live ingestion)) uses captive core directly** — `PrepareRange(UnboundedRange(resume_ledger))` + per-ledger `GetLedger(seq)` against the captive-core subprocess.

---

## Startup Sequence

Four sequential phases, same code path for first start and every restart. The first three are bounded bootstrap work; Phase 4 (live ingestion) is the long-running state the service stays in until process exit.

- **Phase 1 — catchup.** Closes the gap between on-disk `:lfs` flags and current network tip **when `[BSB]` is configured**, by invoking the backfill subroutine in a loop. Without `[BSB]`, Phase 1 (catchup) is a no-op and Phase 4 (live ingestion)'s captive core handles initial catchup naturally via its own `PrepareRange(UnboundedRange(resume_ledger))`.
- **Phase 2 — hydrate txhash.** Loads any `.bin` files Phase 1 (catchup) left (for the trailing partial index) into the active txhash store, then deletes them.
- **Phase 3 — reconcile orphans.** Completes any in-flight freeze transitions left by a prior crash.
- **Phase 4 — live ingestion.** Opens active stores, starts captive core, spawns the lifecycle task, enters the ingestion loop. Runs until process exit. Note: `service_ready` is flipped by `run_rpc_service` BEFORE Phase 4 entry — historical queries are served during captive core's 4-5 minute spinup.

### Backfill vs Phase 1 (catchup)

- **Backfill** is the subroutine (`run_backfill` in [01-backfill-workflow.md](./01-backfill-workflow.md)). BSB-only; parallel per-chunk BSB instances. Captive core cannot be a backfill source — its subprocess is serial and expensive to spin up per instantiation.
- **Phase 1 (catchup)** is the startup phase that runs on every service start. Its job: close the gap between on-disk state and current network tip before Phase 4 (live ingestion) takes over. Invokes backfill as its mechanism when `[BSB]` is configured; otherwise no-op and Phase 4 (live ingestion)'s captive core handles catchup via `PrepareRange(UnboundedRange(resume_ledger))`.

```python
def main():
    args = parse_cli_flags()                              # --config, --log-level, --log-format
    config = load_config_toml(args.config)
    init_logging(config.logging, cli_overrides=args)
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

    # On-disk state is now consistent. Queries against frozen artifacts can be
    # served immediately — they don't depend on captive core having started.
    # Flipping service_ready here (rather than after captive-core spinup) cuts
    # the 4xx window by the captive-core startup time (~4-5 min).
    set_service_ready()

    # Phase 4 opens active stores and starts captive core. Live ingestion begins
    # asynchronously. Queries for ledgers > streaming:last_committed_ledger return
    # "not yet available" until ingestion catches up; that's the same client-visible
    # behavior whether captive core has spun up or not.
    phase4_live_ingest(config, meta_store, resume_ledger)
```

Query serving is gated on Phase 4 (live ingestion) being reached — see [Query Contract](#query-contract).

### Phase 1 — Catchup

- **No-op path:** if `config.bsb is None` (tip-tracker profile), Phase 1 returns `None` immediately. Phase 4's captive core does archive-catchup from `retention_aligned_resume_ledger`.
- **BSB path:** runs `run_backfill` from [01-backfill-workflow.md](./01-backfill-workflow.md) in a loop. Each iteration samples BSB's latest complete chunk and backfills `[retention_aligned_start_chunk, end_chunk]` inclusive. Loop exits when BSB has no new complete chunks. Phase 1 reads from BSB, so its horizon is BSB's chunk-aligned tip; the residual gap to network tip is closed by Phase 4's captive core.
- **Side effects on the meta store:**
  1. Backfill writes `:lfs` / `:events` / `chunk:*:txhash` / `index:*:txhash` flags as it materializes artifacts (per backfill design).
  2. After the loop, **Phase 1 advances `streaming:last_committed_ledger`** to `last_ledger_in_chunk(highest completed chunk)`. This is the durable record that Phase 1 catchup actually progressed past the prior value — used by Phase 3 to compute the retention floor and by `compute_resume_ledger` to derive the resume cursor.
- **Return value:** the highest chunk_id Phase 1 completed, or `None` for no-op. Phase 2 consumes this to find the trailing partial tx index without re-scanning meta-store.

```python
def phase1_catchup(config, meta_store) -> Optional[int]:
    """
    Catch up history via BSB (no-op when [BSB] absent).

    Scenarios handled:
      - First-ever start (archive / pruning-history) — backfill from
        retention_aligned_start_chunk to BSB tip.
      - First-ever start (tip-tracker, no BSB) — early return None.
      - Quick restart, BSB unchanged — loop runs once, run_backfill is a
        full no-op (every chunk's flags already set), loop exits on iter 2.
      - Mid-life restart, BSB advanced — backfill the new chunks; idempotent
        skip on already-frozen chunks.
      - Long-downtime restart with retention — backfill range starts at the new
        retention-aligned position (computed from current BSB tip), skipping
        past chunks now below the retention floor. Those stale chunks are left
        for Phase 3 to clean up.
      - Crash during Phase 1 — backfill is per-chunk-idempotent. On restart,
        completed chunks skip; in-flight ones re-run. last_scheduled_end_chunk
        is local-only and resets on every start; Phase 1 always re-runs from
        scratch (cheap because of idempotent skips).

    Returns: highest chunk_id Phase 1 completed (Phase 2's input),
             or None for no-op.
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
            break                                              # retention-aligned start landed past BSB's tip; Phase 4 picks up
        log.info(f"phase1_catchup bsb_tip_chunk={end_chunk} range=[{start_chunk}, {end_chunk}]")
        run_backfill(config, start_chunk, end_chunk)
        last_scheduled_end_chunk = end_chunk

    if last_scheduled_end_chunk < 0:
        return None                                            # no chunks completed (e.g., BSB hasn't published any yet)

    # Bump the streaming:last_committed_ledger key to reflect Phase 1's catchup.
    # This pushes the key past any stale value left by a prior run that's now
    # below the retention floor. Without this advance, Phase 3 would compute the
    # retention floor from the stale prior value, and compute_resume_ledger would
    # tell captive core to resume at a stale ledger — re-ingesting chunks pruning
    # is about to delete.
    advance_progress_marker(meta_store, last_ledger_in_chunk(last_scheduled_end_chunk))
    return last_scheduled_end_chunk


def retention_aligned_start_chunk(tip_ledger, retention_ledgers):
    # Aligns DOWN to a tx-index boundary (no-gaps); up to LEDGERS_PER_TX_INDEX - 1 ledgers below strict retention.
    if retention_ledgers == 0:
        return 0
    target_ledger = max(tip_ledger - retention_ledgers, GENESIS_LEDGER)
    return first_chunk_id_of_tx_index_containing(target_ledger)


def advance_progress_marker(meta_store, candidate_ledger):
    """
    Move streaming:last_committed_ledger forward to candidate_ledger, but only
    if that's an advance — never regress.

    Two callers:
      - phase1_catchup, once at end of catchup, with last_ledger_in_chunk(highest
        completed chunk).
      - run_live_ingestion_loop, once per ledger, after all three active stores
        durably commit that ledger.

    Monotonicity matters: a regression would cause compute_resume_ledger to
    point captive core at already-durable ledgers (re-ingest waste), and Phase 3
    to compute a stale retention floor.
    """
    prior = meta_store.get("streaming:last_committed_ledger")
    if prior is None or candidate_ledger > prior:
        meta_store.put("streaming:last_committed_ledger", candidate_ledger)
```

**Worker concurrency:** `run_backfill` caps DAG concurrency at `MAX_CPU_THREADS` — see [01-backfill-workflow.md — process_chunk](./01-backfill-workflow.md#process_chunk). Catchup time ≈ `retention_window / (BSB throughput)`.

### Phase 2 — Hydrate TxHash Data from `.bin`

- Phase 1 (catchup) may leave `.bin` files for chunks in the last (incomplete) tx index.
- Phase 2 (`.bin` hydration) loads each into the active txhash store, then deletes the `.bin` + `chunk:{chunk_id:08d}:txhash` flag.
- After Phase 2 (`.bin` hydration): no `.bin` files and no `:txhash` chunk flags remain.

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
                continue
            bin_path = raw_txhash_path(chunk_id)
            if os.path.exists(bin_path):
                load_bin_into_rocksdb(bin_path, txhash_store)
            delete_if_exists(bin_path)
            meta_store.delete(f"chunk:{chunk_id:08d}:txhash")
    finally:
        txhash_store.close()
```

- **Why "load then delete".** Without it, every restart during the incomplete-index lifetime would re-load the same `.bin` files. Load-then-delete makes Phase 2 a no-op on every subsequent restart until Phase 1 (catchup) deposits new `.bin` files.
- **Pure-streaming restarts** (no recent Phase 1 output) never see `.bin` files; the live path writes txhash directly to the active store. Phase 2 is a no-op.

### Phase 3 — Reconcile

Two passes, both strictly meta-store-driven (no filesystem scan):

- **Pass 1 — discard past-retention orphans.** After a long downtime, some active-store dirs and immutable artifacts from prior runs may now be below the new retention floor (because Phase 1's retention-aligned start chunk has moved forward to track the network tip). They have no meaningful next transition — can't be frozen if their hot DB is partial, shouldn't be kept because they're past retention. Discarded outright.
- **Pass 2 — recover in-flight transitions.** For active stores ABOVE the retention floor whose freeze was interrupted by the prior crash, complete the freeze (or clean up if the freeze flag was already set but cleanup didn't finish).

Order matters: Pass 1 first so Pass 2's resume-relative classification (A/B/C/D) only sees in-range entries.

```python
def phase3_reconcile(config, meta_store):
    """
    Reconciles state left by the prior process exit.

    Scenarios handled:
      - Fresh first-ever start — both passes early-return (streaming:last_committed_ledger
        absent, no hot:* keys yet).
      - Quick restart, no crash mid-freeze — Pass 1 finds nothing past retention;
        Pass 2 keeps the resume position's hot:* keys via SCENARIO A.
      - Long-downtime restart with retention (BSB present) — Phase 1's catchup
        already advanced streaming:last_committed_ledger past stale chunks; Pass 1
        discards prior-run hot:* keys + flags now below the floor; Pass 2 sees
        nothing left to do.
      - Long-downtime restart with retention (no BSB) — Phase 1 was a no-op;
        the streaming:last_committed_ledger key is stale; Pass 1 samples network
        tip from history archive and uses that as the floor reference.
      - Crash mid-LFS / mid-events / mid-RecSplit freeze — Pass 1 unaffected
        (the in-flight chunk is at the resume position, well above retention);
        Pass 2 SCENARIO C re-runs the freeze.
      - Crash between freeze flag set and active-dir delete — Pass 2 SCENARIO B
        cleans up the orphan dir.
      - Future-orphan (defensive) — Pass 2 SCENARIO D logs + cleans up.
    """
    pass1_discard_past_retention_orphans(config, meta_store)
    pass2_recover_in_flight_transitions(config, meta_store)


def pass1_discard_past_retention_orphans(config, meta_store):
    """
    Find every artifact (hot DB dir, immutable file, freeze flag) below the
    retention floor and discard it.

    Why this is needed: when a long downtime advances the network tip past
    where the prior run left off, the retention floor moves forward. Active-store
    dirs and freeze flags from the prior run can end up below the new floor.
    The pruning lifecycle handles COMPLETE tx indexes (`prunable_tx_index_ids`
    requires `index:N:txhash` set), but an INCOMPLETE tx index from a prior run
    (its `index:N:txhash` was never written, since RecSplit never ran) is
    invisible to that path. Pass 1 catches those plus a few related cases.

    Determining the floor:
      - retention_ledgers == 0 (archive) — no floor, nothing past retention.
      - BSB present — Phase 1 just advanced streaming:last_committed_ledger to
        BSB-tip's last ledger; that's the authoritative tip reference.
      - No BSB — streaming:last_committed_ledger reflects only the prior run
        (potentially weeks stale); sample current tip from history archive.
        If unreachable, skip Pass 1 (the prune lifecycle will catch up later
        as boundaries fire).
    """
    retention_ledgers = config.service.retention_ledgers
    if retention_ledgers == 0:
        return                                                    # archive profile — no floor

    current_tip = estimate_current_tip(config, meta_store)
    if current_tip is None:
        log.warn("phase3 pass1: no tip reference available; skipping past-retention cleanup")
        return

    floor_ledger = max(current_tip - retention_ledgers, GENESIS_LEDGER)
    floor_chunk  = chunk_id_of_ledger(floor_ledger)

    # Discard hot:chunk:* below floor (active LFS / events store dirs).
    for hot_key in meta_store.scan_prefix("hot:chunk:"):
        store_kind, chunk_id = parse_hot_key(hot_key)
        if chunk_id < floor_chunk:
            delete_dir_if_exists(active_store_path_for(store_kind, chunk_id))
            meta_store.delete(hot_key)
            log.info(f"phase3 pass1: discarded past-retention {hot_key}")

    # Discard hot:index:* below floor (active txhash store dirs).
    for hot_key in meta_store.scan_prefix("hot:index:"):
        _, tx_index_id = parse_hot_key(hot_key)
        if last_ledger_in_tx_index(tx_index_id) < floor_ledger:
            delete_dir_if_exists(active_store_path_for("index:txhash", tx_index_id))
            meta_store.delete(hot_key)
            log.info(f"phase3 pass1: discarded past-retention {hot_key}")

    # Discard chunk:*:lfs / :events / :txhash freeze flags + their files for
    # chunks below floor. Covers chunks of an INCOMPLETE prior-run tx index
    # that pruning lifecycle can't reach (because index:N:txhash was never
    # written). Per-chunk delete is idempotent (delete_if_exists).
    for chunk_key in meta_store.scan_prefix("chunk:"):
        chunk_id, kind = parse_chunk_key(chunk_key)
        if chunk_id < floor_chunk:
            delete_immutable_artifact(kind, chunk_id)             # .pack / events cold segment / .bin
            meta_store.delete(chunk_key)

    # Discard index:*:txhash freeze flags + RecSplit files for past-retention
    # complete indexes. (Covered by prune lifecycle in steady state, but at
    # startup we run this for completeness so the first prune sweep has no
    # backlog.)
    for index_key in meta_store.scan_prefix("index:"):
        _, tx_index_id = parse_index_key(index_key)
        if last_ledger_in_tx_index(tx_index_id) < floor_ledger:
            delete_recsplit_idx_files(tx_index_id)
            meta_store.delete(index_key)

    # Discard pruning:index:* markers for past-retention indexes that the
    # prior run was already mid-prune on. The above loops have already
    # taken care of files + index keys; this loop just clears the marker.
    # (Markers above the floor are left alone — the lifecycle loop's initial
    # sweep will pick them up and finish the prune.)
    for pruning_key in meta_store.scan_prefix("pruning:index:"):
        tx_index_id = parse_pruning_index_id(pruning_key)
        if last_ledger_in_tx_index(tx_index_id) < floor_ledger:
            meta_store.delete(pruning_key)


def pass2_recover_in_flight_transitions(config, meta_store):
    """
    For every hot:* key still set after Pass 1, classify against the resume
    position and dispatch the right recovery action.

    Why "still set after Pass 1": Pass 1 already removed past-retention hot
    keys, so every entry seen here is at or above the retention floor — i.e.,
    legitimately a candidate for either Phase 4 reopen (SCENARIO A), freeze
    cleanup (B), freeze re-run (C), or defensive cleanup (D).
    """
    last_committed = meta_store.get("streaming:last_committed_ledger")
    if last_committed is None:
        return                                                    # no prior live commits → no in-flight work

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


def estimate_current_tip(config, meta_store) -> Optional[int]:
    """
    Best estimate of the current network tip at startup. Used as the reference
    for retention floor calculations.

    BSB present: Phase 1 just advanced streaming:last_committed_ledger to
    BSB-tip's last ledger, which is within minutes of network tip (BSB upload
    lag). Use it directly.

    No BSB: streaming:last_committed_ledger is from the prior run (potentially
    weeks stale). Sample the network tip from history archive (same helper
    retention_aligned_resume_ledger uses). None if archive unreachable, in
    which case Pass 1 skips and pruning lifecycle handles cleanup later.
    """
    if config.bsb is not None:
        return meta_store.get("streaming:last_committed_ledger")

    try:
        return get_latest_network_tip(config.history_archives.urls)
    except NetworkTipUnreachable:
        return None
```

`finish_interrupted_freeze` reopens the active store (idempotent on existing or partial dirs) and runs the corresponding live-path freeze ([LFS](#lfs-transition), [events](#events-transition), or [RecSplit](#recsplit-transition)) to produce the artifact.

### Compute Resume Ledger

`compute_resume_ledger(config, meta_store) -> ledger_seq`. Runs once per service start, after Phase 3 (reconcile), before Phase 4 (live ingestion). Returns the ledger sequence Phase 4's captive core resumes from via `PrepareRange(UnboundedRange(resume_ledger))`.

**Three cases, first match wins:**

| State | Situation | `resume_ledger` |
|---|---|---|
| `streaming:last_committed_ledger` present + within retention floor | Mid-life restart (Phase 1 advanced the key, or live loop committed ledgers in this run / a recent prior run) | `value + 1` |
| `streaming:last_committed_ledger` present but stale (no-BSB tip-tracker after long downtime, value below retention floor) | Re-ingesting via captive core from the stale value would replay days of past-retention chunks for nothing | Delete the stale key, return `retention_aligned_resume_ledger(config)` (skips forward to the new retention-aligned tx-index boundary) |
| `streaming:last_committed_ledger` absent + `:lfs` chunks present | Edge case — Phase 1 wrote `:lfs` flags but crashed before `advance_progress_marker` | `last_ledger_in_chunk(highest_lfs_chunk) + 1` |
| `streaming:last_committed_ledger` absent + no `:lfs` chunks | Tip-tracker fresh start (no BSB, never ingested) | `retention_aligned_resume_ledger(config)` |

```python
def compute_resume_ledger(config, meta_store) -> int:
    """
    Decide the ledger sequence captive core's PrepareRange resumes at.

    Scenarios handled:
      - Fresh first-ever start (BSB present) — Phase 1 already advanced the
        progress marker; trivially resumes at progress_marker + 1.
      - Fresh first-ever start (no BSB) — no progress marker, no :lfs; falls
        through to retention_aligned_resume_ledger (samples tip from history
        archive, aligns down to a tx-index boundary).
      - Quick restart, BSB unchanged — progress marker is fresh; resume at + 1.
      - Long-downtime restart, BSB present — Phase 1's catchup already advanced
        the progress marker past any stale prior value; resume at the new + 1.
      - Long-downtime restart, no BSB (tip-tracker) — Phase 1 was a no-op;
        the progress marker is from the prior run (potentially weeks stale).
        Detect by comparing against the current network tip; if the marker is
        below the retention floor, delete it and skip forward to the
        retention-aligned start.
      - Phase 1 crashed after writing :lfs but before advance_progress_marker
        — fall back to deriving resume from the highest :lfs chunk.

    No consistency validation is performed here. Phase 1 backfill self-heals
    incomplete chunks within its range; Phase 3 recovers in-flight freezes;
    pruning lifecycle handles past-retention state.
    """
    progress_marker = meta_store.get("streaming:last_committed_ledger")

    if progress_marker is not None:
        # Stale-marker check — only matters when there's no BSB (tip-tracker
        # profile after long downtime). In BSB-present paths, Phase 1 has
        # already advanced the marker past any stale value before we got here.
        if config.bsb is None and config.service.retention_ledgers > 0:
            current_tip = try_sample_network_tip(config)
            if current_tip is not None:
                floor = max(current_tip - config.service.retention_ledgers, GENESIS_LEDGER)
                if progress_marker < floor:
                    log.info(f"compute_resume_ledger: marker {progress_marker} below retention floor {floor}; skipping forward")
                    meta_store.delete("streaming:last_committed_ledger")
                    return retention_aligned_resume_ledger_with_tip(config, current_tip)
        return progress_marker + 1

    # Marker absent. Phase 1 may have crashed after writing :lfs but before
    # advance_progress_marker. Recover via the highest :lfs chunk.
    highest_lfs = scan_max_lfs_chunk(meta_store)
    if highest_lfs is not None:
        return last_ledger_in_chunk(highest_lfs) + 1

    # No marker, no :lfs — tip-tracker fresh start.
    return retention_aligned_resume_ledger(config)


def scan_max_lfs_chunk(meta_store) -> Optional[int]:
    """
    Returns the highest chunk_id with `:lfs` set, or None if no :lfs key exists.

    Reverse-iterates the chunk: prefix and stops at the first :lfs key found.
    O(suffix variants per chunk) — typically 1–2 reads, regardless of total
    chunks on disk. Sub-millisecond at any cpi / archive size.
    """
    for key in meta_store.iter_prefix_reverse("chunk:"):
        if key.endswith(":lfs"):
            return parse_chunk_id_from_chunk_key(key)
    return None


def retention_aligned_resume_ledger(config) -> int:
    """
    No-BSB resume cursor: align to the first ledger of the tx index containing
    the retention floor. Captive core archive-catches-up from that point.

    Two callers:
      - compute_resume_ledger fresh-start branch (no BSB, no prior commits).
      - compute_resume_ledger stale-marker branch (no BSB long downtime —
        prior progress marker is below the new retention floor).

    Helper retention_aligned_resume_ledger_with_tip is the same with an
    explicit tip parameter, for callers that already sampled.
    """
    network_tip = get_latest_network_tip(config.history_archives.urls)
    return retention_aligned_resume_ledger_with_tip(config, network_tip)


def retention_aligned_resume_ledger_with_tip(config, network_tip_ledger) -> int:
    retention_ledgers = config.service.retention_ledgers
    target_ledger = max(network_tip_ledger - retention_ledgers, GENESIS_LEDGER)
    return first_ledger_of_tx_index_containing(target_ledger)


def try_sample_network_tip(config) -> Optional[int]:
    """
    Wrapper around get_latest_network_tip that returns None on failure
    instead of raising. Used where a missing tip is recoverable (e.g.,
    compute_resume_ledger's stale-marker check — if we can't confirm the
    marker is stale, fall through to using it).
    """
    try:
        return get_latest_network_tip(config.history_archives.urls)
    except NetworkTipUnreachable:
        return None
```

### Phase 4 — Live Ingestion

Opens active stores for the resume position, spawns the lifecycle task, starts captive core, and enters the ingestion loop. **Query serving has already been enabled by `run_rpc_service`** — Phase 4 is purely about ingestion.

```python
def phase4_live_ingest(config, meta_store, resume_ledger):
    # service_ready was set by run_rpc_service before this call. Queries against
    # frozen artifacts (chunks <= streaming:last_committed_ledger) are already
    # being served. Phase 4 starts the live ingestion path so new ledgers begin
    # to flow.

    # Open active stores at the resume position. open_active_*_store writes the
    # hot:* key BEFORE mkdir (see Flag Semantics) — Phase 3 has already cleaned
    # any stale hot keys for this chunk/index, so these mkdir calls land on an
    # empty filesystem path (or, in the SCENARIO A "keep" case, on the prior-run
    # active dir that's still on disk + idempotently re-opened).
    active_stores = open_active_stores_for_resume(config, meta_store, resume_ledger)

    # Background pruning loop. Runs an initial sweep on entry to handle any
    # pruning:index:* markers left by a crash mid-prune in the prior run.
    run_in_background(run_prune_lifecycle_loop, config, meta_store)

    # Captive core spinup. PrepareRange tells the SDK what range we want; actual
    # spinup takes 4-5 minutes during which GetLedger blocks. /getHealth shows
    # status="catching_up" during this window because streaming:last_committed_ledger
    # hasn't moved yet, but historical queries still work against frozen artifacts.
    ledger_backend = make_ledger_backend(config.captive_core.config_path)
    ledger_backend.PrepareRange(UnboundedRange(resume_ledger))

    run_live_ingestion_loop(config, ledger_backend, active_stores, meta_store, resume_ledger)


def open_active_stores_for_resume(config, meta_store, resume_ledger):
    """
    Open the three active stores for the chunk/tx-index that ingestion will
    resume into. Idempotent on existing dirs (mkdir no-ops; RocksDB recovers
    from any partial state).

    Called once from phase4_live_ingest. Per-kind stores share no state and
    can be opened in any order.
    """
    resume_chunk_id    = chunk_id_of_ledger(resume_ledger)
    resume_tx_index_id = tx_index_id_of_chunk(resume_chunk_id)

    return ActiveStores(
        ledger = open_active_ledger_store(config, meta_store, resume_chunk_id),
        events = open_active_events_store(config, meta_store, resume_chunk_id),
        txhash = open_active_txhash_store(config, meta_store, resume_tx_index_id),
    )
```

Captive core takes 4–5 minutes to spin up. During that window the service is already serving historical queries (everything up through `streaming:last_committed_ledger`). `/getHealth` reports `status = "catching_up"` until the live loop commits its first ledger; queries for ledgers above the marker return "not yet available" via the QueryRouter.

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

- Each per-store write is atomic: RocksDB WriteBatch + WAL across all three active stores (ledger / txhash / events).
- Key/value schemas are in [Active Store Architecture](#active-store-architecture).

---

## Freeze Transitions

Three independent background transitions per chunk/index boundary; each has its own task, flag, and cleanup. Live ingestion never blocks on them.

- **LFS transition** — per chunk. Retired ledger RocksDB → `.pack` file.
- **Events transition** — per chunk. Retired events RocksDB store → cold segment (3 files).
- **RecSplit transition** — per index. Retired txhash RocksDB → 16 `.idx` files.
- Streaming's freeze transitions never produce `.bin` files; those are transient backfill output only, produced during Phase 1 (catchup).

### Concurrency Model

- **`active_stores` is the ingestion loop's owned state.** Fields `ledger` / `events` / `txhash` (one handle per data type, no `*_next`) are mutated only inside `on_chunk_boundary` and `on_tx_index_boundary`. Freeze tasks receive a handle by value at spawn and never read back through `active_stores`.
- **Meta-store is single-writer.** Writers are the ingestion loop (per-ledger checkpoint), freeze tasks (`:lfs` / `:events` / `:txhash` flags), and the lifecycle loop (`pruning:index:*` marker + prune key deletes). The meta-store wrapper serializes them on top of RocksDB's single-writer semantics.
- **Per-kind single-flight gates.** One outstanding transition per kind (LFS / events / RecSplit); the next starts only after the previous releases. `wait_for_lfs_complete()` acquires the LFS gate; `signal_lfs_complete()` at the end of `freeze_ledger_chunk_to_pack_file` releases it (events / RecSplit follow the same shape). Not a global barrier — kinds remain independent.
- **Query handlers read from a storage-manager layer.** Per-data-type managers (ledger / events / txhash) own their own state-transition synchronization; query handlers never touch `active_stores` directly.
  - **Read-view invariant:** a query sees either pre-transition data (routed to the transitioning store) or post-transition data (new active store + newly-flagged immutable artifact) — never a half-state mix.
  - **Flag-is-truth applies to reads:** a query never routes to an immutable artifact whose `:lfs` / `:events` / `:txhash` flag is unset.
  - Concrete lock primitives + routing logic belong in a separate query-routing design doc.
- **Stores are opened on-demand at boundary** — see [Store Lifecycle](#store-lifecycle) for the open + transition sequence and synchronous-open cost.

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
    """
    Background goroutine spawned at the top of phase4_live_ingest. Runs an
    initial sweep on entry (catches in-progress prunes from a prior-run crash),
    then loops on chunk-boundary notifications.

    This is the ONLY caller of prune_tx_index. Phase 1, 2, 3 don't call it.
    Phase 3 Pass 1 cleans up past-retention artifacts at startup but does so
    directly (no pruning marker needed — service_ready = false during startup,
    no queries to gate).
    """
    retention_ledgers = config.service.retention_ledgers
    _run_prune_sweep(meta_store, retention_ledgers, config)   # initial: handles crash-recovered in-progress prunes
    while True:
        wait_for_chunk_boundary_notification()                # woken by on_chunk_boundary's notify_lifecycle()
        _run_prune_sweep(meta_store, retention_ledgers, config)


def _run_prune_sweep(meta_store, retention_ledgers, config):
    for tx_index_id in prunable_tx_index_ids(meta_store, retention_ledgers):
        prune_tx_index(tx_index_id, meta_store, config)


def prunable_tx_index_ids(meta_store, retention_ledgers):
    """
    Two sources of work, unioned:
      1. Crash recovery — any pruning:index:N key set means a prior prune was
         interrupted; re-attempt regardless of retention status.
      2. Steady-state — past-retention indexes whose index:N:txhash = "1".
    """
    if retention_ledgers == 0:
        return []

    result = set()

    # Source 1: in-progress prunes from a prior crash.
    # pruning:index:N is set BEFORE any file delete and cleared AFTER all files
    # + the index key are gone, so its presence at startup unambiguously means
    # "this prune was interrupted, finish it."
    for key in meta_store.scan_prefix("pruning:index:"):
        result.add(parse_pruning_index_id(key))

    # Source 2: newly past-retention indexes that need a fresh prune.
    last_committed_ledger = meta_store.get("streaming:last_committed_ledger")
    max_eligible_tx_index_id = max_prunable_tx_index_id(last_committed_ledger, retention_ledgers)
    for tx_index_id in range(0, max_eligible_tx_index_id + 1):
        if tx_index_id in result:
            continue                                          # already added from Source 1
        if meta_store.get(f"index:{tx_index_id:08d}:txhash") == "1":
            result.add(tx_index_id)

    return sorted(result)


def prune_tx_index(tx_index_id, meta_store, config):
    """
    Tear down all artifacts for tx_index_id. Called only from _run_prune_sweep.

    Two ordering constraints bookend the work:
      - pruning:index:N is set FIRST (before any file delete) — atomic gate
        that flips queries to 4xx for any tx in index N.
      - pruning:index:N is cleared LAST (after every other op) — survives
        crashes so the next sweep's prunable_tx_index_ids picks N back up.

    Everything between is individually idempotent: file deletes use
    delete_if_exists, key deletes are no-ops on absent keys. Crash anywhere
    in between leaves a state the re-run cleans up.
    """
    # OP 1: gate queries off + mark "prune in progress" for crash recovery.
    meta_store.put(f"pruning:index:{tx_index_id:08d}", "1")

    # WORK: per-chunk file + key deletion. File-before-flag-delete preserved
    # for each chunk's keys.
    for chunk_id in chunks_for_tx_index(tx_index_id):
        delete_if_exists(ledger_pack_path(chunk_id))
        delete_events_segment(chunk_id)
        delete_if_exists(raw_txhash_path(chunk_id))           # defence-in-depth; normally already gone via cleanup_txhash
        meta_store.delete(f"chunk:{chunk_id:08d}:lfs")
        meta_store.delete(f"chunk:{chunk_id:08d}:events")
        meta_store.delete(f"chunk:{chunk_id:08d}:txhash")
    delete_recsplit_idx_files(tx_index_id)

    # OP 2: index:N:txhash transitions from "1" to absent. With the pruning
    # marker still set, queries continue to see 4xx via the marker check.
    meta_store.delete(f"index:{tx_index_id:08d}:txhash")

    # OP 3: clear the prune marker. Tx index N is now fully gone.
    # Past this point, prunable_tx_index_ids no longer sees N.
    meta_store.delete(f"pruning:index:{tx_index_id:08d}")
```

- **Why index-atomic.** Per-chunk pruning would open a window where `getTransaction` resolves to a ledger seq whose pack has been deleted; whole-index gating closes it.
- **Extra data on disk.** Up to `LEDGERS_PER_TX_INDEX - 1` ledgers past strict retention. `RETENTION_LEDGERS` is a multiple of `LEDGERS_PER_TX_INDEX`, so the next-eligible index is exactly `LEDGERS_PER_TX_INDEX` further.
- **Why a separate `pruning:index:*` key family** (instead of overloading `index:N:txhash` with a `"deleting"` value). Keeps every meta-store key binary (present-or-absent), so reader code never has to decode special values. The marker's "set first, cleared last" pattern is the same in either encoding; using a separate key family makes the prune-intent explicit and self-documenting in meta-store dumps. Cost: one extra meta-store op per prune (a sub-microsecond RocksDB point write) and a slightly longer `prunable_tx_index_ids` (union of the in-progress set with the past-retention-eligible set). Worth it for schema uniformity.
- **Pruning marker is steady-state-only.** Phase 3 Pass 1 also deletes past-retention `index:N:txhash` keys (and any `.idx` / `.pack` / events / `.bin` files for chunks below the retention floor), but does NOT set the pruning marker — at startup `service_ready = false`, so there are no queries to gate. Pass 1 also clears any `pruning:index:N` it finds during its sweep, so the lifecycle loop's initial sweep doesn't re-attempt work Pass 1 already finished.

---

## Query Contract

Query serving is gated on Phase 4 (live ingestion) being reached. `getLedger`, `getTransaction`, `getEvents` all return **HTTP 4xx** during Phases 1–3.

### Readiness Signal

- In-memory boolean `service_ready`, flipped to `true` by `set_service_ready()` once on-disk state is consistent — that means after Phases 1–3 complete and `compute_resume_ledger` returns, but BEFORE Phase 4's captive-core spinup. Reasoning: queries against frozen artifacts (chunks `<=` `streaming:last_committed_ledger`) don't depend on captive core having started, so 4xx-during-spinup adds no correctness; it only adds an unnecessary 4-5 minute query outage on every restart. Not persisted; every startup begins `false`.
- HTTP server binds at service startup (before Phase 1 (catchup)), so `getHealth` is always servable. The QueryRouter routes `getHealth` unconditionally and gates `getLedger` / `getTransaction` / `getEvents` on `service_ready`: `false` → HTTP 4xx; `true` → route normally.
- Clients see `HTTP 4xx` from the three read endpoints on every startup until Phase 4 is reached, regardless of prior runs. Intentional — catchup and recovery phases must complete before the service serves, every time.

### Behavior During Phases 1–3

- `/getLedger`, `/getTransaction`, `/getEvents` → `HTTP 4xx` with no payload detail.
- `/getHealth` → always served. Response payload matches the existing stellar-rpc shape: `status` (`catching_up` during Phases 1–3, `healthy` during Phase 4 (live ingestion)), `latestLedger` (= `streaming:last_committed_ledger`, or `0` if absent), `oldestLedger` (first ingested ledger), `ledgerRetentionWindow`. No drift field, no network-tip field.
- No partial / incremental serving. The service does not serve "whatever is ingested so far" while Phases 1–3 are running.

### Behavior When an Index Is Being Pruned

- `prune_tx_index` sets `pruning:index:{tx_index_id:08d} = "1"` before touching any files, deletes `index:{tx_index_id:08d}:txhash` after all artifacts are gone, and clears `pruning:index:{tx_index_id:08d}` last. QueryRouter checks the `pruning:index:*` key first; if set, returns HTTP 4xx as if the index were past retention.
- Queries for a ledger in a pruning index return HTTP 4xx the instant `pruning:index:N` is set, not when files actually disappear. No window where queries route into a half-deleted index.

### Rationale

- Without an explicit gate, implementations drift toward "best-effort serve whatever is ingested" — inconsistent across operators, breaks client assumptions.
- Explicit `service_ready` + HTTP 4xx gives clients an unambiguous signal.
- `catching_up` health status gives operators visibility into progress.

---

## Resilience

Flag-after-fsync makes the meta store authoritative for every startup decision — never filesystem scanning. Streaming extends backfill's resilience model with per-ledger checkpoint discipline, per-kind single-flight freeze gates, and a two-phase prune marker.

### Crash Recovery

No separate recovery phase. Every startup runs Phases 1–4 — already-complete work is detected and skipped via meta-store flags.

#### Invariants

In addition to the backfill subroutine's invariants in [01-backfill-workflow.md — Crash Recovery](./01-backfill-workflow.md#crash-recovery), streaming adds the following:

1. **Monotonic progress marker.** `streaming:last_committed_ledger` advances only via `advance_progress_marker` (never regresses). Two writers: Phase 1 catchup (post-catchup, to `last_ledger_in_chunk(highest completed chunk)`) and live ingestion loop (per ledger, after all three active stores durably commit). Resume is `last_committed_ledger + 1` — except in the no-BSB long-downtime case where the marker is stale and below the new retention floor, in which case `compute_resume_ledger` deletes it and skips forward to the retention-aligned start.
2. **No separate recovery phase.** Startup is Phases 1–4. Nothing else.
3. **Max-1-transitioning per freeze.** A freeze transition must complete before the next one starts, per kind (LFS, events, RecSplit). Applies in steady state and crash recovery.
4. **Retention immutable.** `config:retention_ledgers` is stored on first run and compared thereafter. No mid-run retention change.
5. **Pruning intent marker.** `pruning:index:{N} = "1"` is set BEFORE any file delete and cleared AFTER all artifacts and the `index:{N}:txhash` key are gone. QueryRouter treats its presence as "tx index N is unservable" → 4xx. Crashes mid-prune leave the marker set; the lifecycle loop's `prunable_tx_index_ids` picks it up via `scan_prefix("pruning:index:")` and re-runs the prune idempotently. See [Pruning](#pruning).
6. **Hot-key tracking + retention-aware cleanup.** Every active store directory has a `hot:*` key, set BEFORE `mkdir` and cleared AFTER dir removal. Phase 3's two passes are both meta-store-driven (no filesystem scan anywhere): pass 1 discards past-retention orphans (active dirs and stale freeze flags below the retention floor — common after long downtime where the floor moves forward); pass 2 reconciles in-flight transitions for entries above the floor.
7. **No permanently-partial tx index.** Every persisted tx index reaches a terminal state — either *complete* (`index:N:txhash = "1"`, RecSplit built) or *fully discarded* (all chunks + `index:N:txhash` deleted). The intermediate "trailing partial" state (chunks with `:lfs+:events` but no `index:N:txhash`) only persists transiently — it is completed by either: (a) a future Phase 1 invocation extending the backfill range past `last_chunk_in_tx_index(N)`, (b) Phase 4 ingestion reaching `last_chunk_in_tx_index(N)` (which fires `on_tx_index_boundary` → `build_tx_index_recsplit_files`), or (c) Phase 3 Pass 1 discarding it as past-retention. No fourth path; no tx index ever stays partial forever.
8. **No permanent orphans.** Every meta-store flag has a corresponding artifact (or is mid-cleanup, recoverable via the file-before-flag-delete invariant). Every active-store directory has a `hot:*` key (set before mkdir). Every immutable file has a freeze flag (set after fsync). The only states without a recovery path are operator-introduced (manually deleted a meta-store key while files survive, or restored a filesystem snapshot inconsistently) — explicitly out of scope per the meta-store-driven-recovery principle. In all other cases, every artifact on disk traces back to a meta-store record that some recovery path (backfill self-heal, Phase 3 Pass 1, Phase 3 Pass 2, or pruning lifecycle) will eventually act on.

#### Compound Recovery Scenarios

Backfill's crash-recovery model in [01-backfill-workflow.md](./01-backfill-workflow.md#crash-recovery) handles every Phase 1 (catchup) crash. Streaming adds:

- **Crash during Phase 2 (`.bin` hydration).** All sub-cases are recoverable because every cleanup pair runs file-delete BEFORE flag-delete (see [Flag Semantics](#flag-semantics)).
  - **Sweep 1, mid-loop.** Already-cleaned chunks: flag absent → skipped on retry. Pending chunks: flag + file still present → cleaned on retry.
  - **Sweep 1, between file-delete and flag-delete.** Flag set, file already gone. Restart: flag triggers retry, `delete_if_exists` is a no-op on the missing file, flag deleted.
  - **Sweep 2, between `load_bin_into_rocksdb` and file-delete.** Flag set, file present, data already durable in the active txhash RocksDB. Restart: re-loads (RocksDB put is idempotent on the same key/value), then deletes file, then flag.
  - **Sweep 2, between file-delete and flag-delete.** Flag set, file gone, data durable. Restart: flag triggers retry, `os.path.exists(bin_path)` is False so load is skipped, file delete is a no-op, flag deleted.
  - **No filesystem scan needed in any case** — the meta-store flag is the only signal the next start consults.

- **Crash between per-ledger checkpoint and freeze completion (LFS / events).**
  - State: `streaming:last_committed_ledger = last_ledger_in_chunk(chunk_id)`; `chunk:{chunk_id}:lfs` absent; `hot:chunk:{chunk_id}:lfs` set; active ledger store dir present.
  - Phase 1 (catchup) on restart (assumes `[BSB]` configured): `:lfs` missing → re-runs `process_chunk(chunk_id)` with a fresh per-task BSB (idempotent per artifact).
  - Phase 3 (reconcile) iterates `hot:*` keys. Hits SCENARIO B (freeze flag now set + chunk_id < resume): `delete_dir_if_exists` + clear hot key. Cleanup is idempotent.
  - Cost: ~10_000 ledgers of redundant ingestion per affected chunk. Correctness preserved.

- **Crash mid-RecSplit.**
  - State: `index:{tx_index_id}:txhash` absent; `hot:index:{tx_index_id}:txhash` set; all `:lfs` chunks present; partial `.idx` files possibly on disk.
  - Phase 3 (reconcile) hits SCENARIO C → `finish_interrupted_freeze` re-runs the build (its preamble blanket-deletes partial `.idx` files).

- **Crash mid hot-store creation.**
  - State: `hot:chunk:{chunk_id}:lfs` (or events / txhash) set, but `mkdir` / RocksDB open didn't complete. Dir absent or partially set up; freeze flag absent.
  - Phase 3 (reconcile): if `chunk_id == resume`, SCENARIO A — keep; Phase 4 reopens via `open_active_*_store` (idempotent — mkdir no-ops on existing dir, RocksDB recovers from partial WAL). If `chunk_id < resume`, SCENARIO C — `finish_interrupted_freeze` reopens and re-runs the freeze.
