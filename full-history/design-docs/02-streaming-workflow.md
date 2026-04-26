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

- **Leapfrog** (colloquial) — how the service picks a starting ledger when retention is configured: the start always lands on a tx-index boundary, never mid-index, so the first tx index ingested is complete. Without this rounding, the chunks before the start would fall below the retention floor and never be ingested, leaving the tx index broken and the ingest-work on its later chunks wasted. Used in two places: Phase 1 (catchup) when there's a remote object store to read from, and at Phase 4 (live ingestion) entry on a no-object-store fresh start.

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
| `URLS` | []string | **required** | List of Stellar history archive URLs. Used to sample network tip for Phase 4 (live ingestion)'s leapfrog-from-tip computation (when `[BSB]` is absent on first-ever start).|

**[BSB]** (optional)

- Same schema as in the backfill doc. Presence in the config file determines Phase 1 (catchup) behavior:
  - Present: Phase 1 (catchup) invokes backfill over the BSB (fast, parallel per-chunk catchup).
  - Absent: Phase 1 (catchup) is a no-op; Phase 4 (live ingestion)'s captive core archive-catches-up from a leapfrog'd `resume_ledger` (slower, but no object-store dep).
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
| Pruning-history | `N × LEDGERS_PER_TX_INDEX`, N ≥ 1 | present | Backfill over retention window (leapfrog-aligned start) | Windowed history with bulk initial catchup. |
| Tip-tracker | `N × LEDGERS_PER_TX_INDEX`, N ≥ 1 | absent | **No-op.** Phase 4 (live ingestion)'s captive core archive-catches-up from a leapfrog'd `resume_ledger` | App developer; short retention; no object-store dep. |
| (invalid) | `0` | absent | — | Rejected by `validate_config`: full history requires BSB. |

---

## Meta Store Keys

*This section is a reference for the key schema and lifecycle. It reads more naturally after [Startup Sequence](#startup-sequence) below, which defines the phases that write and consume these keys.*

Single RocksDB instance, WAL (Write-Ahead Log) always enabled. Authoritative source for every startup decision.

### Keys Introduced by Streaming

| Key | Value | Written when |
|---|---|---|
| `streaming:last_committed_ledger` | uint32 (big-endian) | Written only by the live ingestion loop after all three active stores durably commit a ledger. **Never written at bootstrap.** When absent, [`compute_resume_ledger`](#compute-resume-ledger) derives resume from the contiguous `:lfs` prefix (first-ever post-Phase-1) or by leapfrogging down from the current network tip to an index boundary (tip-tracker fresh start). Phase 1 (catchup) progress is tracked by `chunk:{chunk_id}:lfs` flags alone. |
| `config:retention_ledgers` | decimal string | First run (stored); enforced on subsequent starts. |
| `hot:chunk:{chunk_id:08d}:lfs` | `"1"` | Written **before** the active ledger store directory is created; deleted **after** that directory is removed by the freeze task. Presence indicates the directory exists or its lifecycle is incomplete (creation in flight, or freeze cleanup not yet finished). |
| `hot:chunk:{chunk_id:08d}:events` | `"1"` | Same pattern as `hot:chunk:lfs`, scoped to the active events store directory. |
| `hot:index:{tx_index_id:08d}:txhash` | `"1"` | Same pattern, scoped to the active txhash store directory. Per-index cadence (one per tx index, not per chunk). |

### Keys Shared with Backfill

Defined in [01-backfill-workflow.md — Meta Store Keys](./01-backfill-workflow.md#meta-store-keys); streaming uses the same contract:

- `config:chunks_per_tx_index`
- `chunk:{chunk_id:08d}:lfs`
- `chunk:{chunk_id:08d}:events`
- `chunk:{chunk_id:08d}:txhash`
- `index:{tx_index_id:08d}:txhash`

Streaming-specific use of these keys (which paths write them when, and the `"deleting"` marker on `index:txhash`) is shown in [Key Lifecycle in Streaming](#key-lifecycle-in-streaming) below.

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

Pruning (background, when tx_index_id is past retention) — two-phase marker:
  index:{tx_index_id}:txhash = "deleting"   (queries return 4xx from here on)
  delete all files + per-chunk :lfs + :events keys for tx_index_id
  index:{tx_index_id}:txhash → deleted
```

### Flag Semantics

- **Flag-after-fsync (creation order).** A flag is set only AFTER the artifact it represents has been fsynced. Flag absent ⇒ artifact missing or incomplete; flag present ⇒ artifact is durable.
- **File-before-flag-delete (cleanup order).** The file is removed FIRST; the flag is cleared LAST. Flag present ⇒ cleanup may be incomplete; flag absent ⇒ cleanup done, no file exists. Reverse order would orphan a file with no meta-store record on a crash mid-pair, recoverable only by filesystem scan.
- **Flag-driven recovery.** Every startup decision — hydration, transition replay, RecSplit spawn, prune eligibility, active-store directory reconciliation — derives from meta-store key presence. No filesystem-scan-and-infer anywhere.

Together: the meta-store flag is the always-correct signal of artifact state on disk, both for immutable files and for active-store directories. A crash anywhere leaves a state the next start recovers from by flag presence alone.

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
- **Crash recovery.** Active-store directories surviving a crash are reconciled on the next start — see [Phase 3 — Reconcile Orphaned Transitions](#phase-3--reconcile-orphaned-transitions).

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
- **Phase 4 — live ingestion.** Opens active stores, starts captive core, spawns the lifecycle task, flips the `service_ready` flag, enters the ingestion loop. Runs until process exit.

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
    start_http_server(config)
    phase1_catchup(config, meta_store)
    phase2_hydrate_txhash(config, meta_store)
    phase3_reconcile_orphans(config, meta_store)
    resume_ledger = compute_resume_ledger(config, meta_store)
    phase4_live_ingest(config, meta_store, resume_ledger)
```

Query serving is gated on Phase 4 (live ingestion) being reached — see [Query Contract](#query-contract).

### Phase 1 — Catchup

- **No-op path:** if `config.bsb is None` (no `[BSB]` configured), Phase 1 (catchup) returns immediately. Phase 4 (live ingestion)'s captive core will catch up from a leapfrog'd resume ledger.
- **BSB path:** runs the backfill subroutine (`run_backfill` from [01-backfill-workflow.md](./01-backfill-workflow.md)) once per BSB-tip sample, until BSB has no new complete chunks beyond the last scheduled range.
- Unit of work = one whole chunk, never partial. DAG dispatches chunk IDs; `process_chunk(chunk_id, config)` ingests `first_ledger_in_chunk..last_ledger_in_chunk` inclusive. Every chunk Phase 1 (catchup) persists starts at `..._02`, ends at `..._01` — the chunk-alignment invariant the no-gaps guarantee rests on.
- Phase 1 reads from BSB, so the relevant horizon is BSB's latest chunk-aligned position — not the network tip. The gap between BSB's tip and the actual network tip (typically minutes of upload lag) is closed by Phase 4 (live ingestion)'s captive core.

```python
def phase1_catchup(config, meta_store):
    if config.bsb is None:
        return                                                 # [BSB] absent → no-op

    retention_ledgers = config.service.retention_ledgers
    last_scheduled_end_chunk = -1

    while True:
        end_chunk = bsb_latest_complete_chunk_id(config.bsb)
        if end_chunk <= last_scheduled_end_chunk:
            return                                             # BSB has no new complete chunks
        start_chunk = retention_aligned_start_chunk(last_ledger_in_chunk(end_chunk), retention_ledgers)
        if end_chunk < start_chunk:
            return                                             # leapfrog past tip
        log.info(f"phase1_catchup bsb_tip_chunk={end_chunk} range=[{start_chunk}, {end_chunk}]")
        run_backfill(config, start_chunk, end_chunk)
        last_scheduled_end_chunk = end_chunk


def retention_aligned_start_chunk(tip_ledger, retention_ledgers):
    # Aligns DOWN to a tx-index boundary (no-gaps); up to LEDGERS_PER_TX_INDEX - 1 ledgers below strict retention.
    if retention_ledgers == 0:
        return 0
    target_ledger = max(tip_ledger - retention_ledgers, GENESIS_LEDGER)
    return first_chunk_id_of_tx_index_containing(target_ledger)
```

**Worker concurrency:** `run_backfill` caps DAG concurrency at `MAX_CPU_THREADS` — see [01-backfill-workflow.md — process_chunk](./01-backfill-workflow.md#process_chunk). Catchup time ≈ `retention_window / (BSB throughput)`.

### Phase 2 — Hydrate TxHash Data from `.bin`

- Phase 1 (catchup) may leave `.bin` files for chunks in the last (incomplete) tx index.
- Phase 2 (`.bin` hydration) loads each into the active txhash store, then deletes the `.bin` + `chunk:{chunk_id:08d}:txhash` flag.
- After Phase 2 (`.bin` hydration): no `.bin` files and no `:txhash` chunk flags remain.

```python
def phase2_hydrate_txhash(config, meta_store):
    # Both sweeps: file-before-flag-delete (see Flag Semantics).

    # Sweep 1: clean leftover .bin from completed indexes (cleanup_txhash crashed mid-pair).
    for tx_index_id in tx_index_ids_with_txhash_flag(meta_store):
        for chunk_id in chunks_for_tx_index(tx_index_id):
            if meta_store.has(f"chunk:{chunk_id:08d}:txhash"):
                delete_if_exists(raw_txhash_path(chunk_id))
                meta_store.delete(f"chunk:{chunk_id:08d}:txhash")

    # Sweep 2: hydrate the trailing incomplete tx index into the active txhash store.
    incomplete_tx_index_id = current_incomplete_tx_index_id(meta_store)
    if incomplete_tx_index_id is None:
        return

    txhash_store = open_active_txhash_store(config, meta_store, incomplete_tx_index_id)
    try:
        for chunk_id in chunks_for_tx_index(incomplete_tx_index_id):
            if not meta_store.has(f"chunk:{chunk_id:08d}:txhash"):
                continue
            bin_path = raw_txhash_path(chunk_id)
            if os.path.exists(bin_path):
                load_bin_into_rocksdb(bin_path, txhash_store)
            delete_if_exists(bin_path)
            meta_store.delete(f"chunk:{chunk_id:08d}:txhash")
    finally:
        txhash_store.close()   # Phase 4 re-opens by path; flock would collide otherwise.
```

- **Why "load then delete".** Without it, every restart during the incomplete-index lifetime would re-load the same `.bin` files. Load-then-delete makes Phase 2 a no-op on every subsequent restart until Phase 1 (catchup) deposits new `.bin` files.
- **Pure-streaming restarts** (no recent Phase 1 output) never see `.bin` files; the live path writes txhash directly to the active store. Phase 2 is a no-op.

### Phase 3 — Reconcile Orphaned Transitions

Completes any in-flight transitions left by a prior crash. All decisions derive from meta store state + on-disk store directories.

```python
def phase3_reconcile_orphans(config, meta_store):
    last_committed_ledger = meta_store.get("streaming:last_committed_ledger")
    if last_committed_ledger is None:
        return                                                    # fresh start — nothing in flight

    resume_chunk_id    = chunk_id_of_ledger(last_committed_ledger + 1)
    resume_tx_index_id = tx_index_id_of_chunk(resume_chunk_id)

    # Iterate hot:* keys; each branch acts on the parsed (store_kind, id).
    for hot_key in meta_store.scan_prefix("hot:"):
        store_kind, chunk_or_tx_index_id = parse_hot_key(hot_key)
        resume_chunk_or_tx_index_id = (
            resume_chunk_id if store_kind.startswith("chunk:") else resume_tx_index_id
        )
        store_path      = active_store_path_for(store_kind, chunk_or_tx_index_id)
        freeze_flag_key = freeze_flag_key_for(store_kind, chunk_or_tx_index_id)

        if chunk_or_tx_index_id == resume_chunk_or_tx_index_id:
            continue                                              # A: resume target — Phase 4 reopens.

        elif meta_store.has(freeze_flag_key):
            # B: flag-is-truth. Frozen, but cleanup didn't finish.
            delete_dir_if_exists(store_path)
            meta_store.delete(hot_key)

        elif chunk_or_tx_index_id < resume_chunk_or_tx_index_id:
            # C: freeze interrupted; restart it to completion.
            finish_interrupted_freeze(store_kind, chunk_or_tx_index_id, meta_store)

        else:
            # D: future-orphan — shouldn't occur. Log + defensive cleanup.
            log.warn(f"phase3: future-orphan {store_kind}/{chunk_or_tx_index_id:08d} > resume {resume_chunk_or_tx_index_id:08d}")
            delete_dir_if_exists(store_path)
            meta_store.delete(hot_key)
```

`finish_interrupted_freeze` reopens the active store (idempotent on existing or partial dirs) and runs the corresponding live-path freeze ([LFS](#lfs-transition), [events](#events-transition), or [RecSplit](#recsplit-transition)) to produce the artifact.

### Compute Resume Ledger

- `compute_resume_ledger` is a shared helper called once per service start, AFTER Phase 3 (reconcile) and BEFORE Phase 4 (live ingestion). Scans meta-store state end-to-end, validates on-disk consistency, and returns `resume_ledger` — the ledger sequence captive core is told to start emitting at via `PrepareRange(UnboundedRange(resume_ledger))`.
- **Runs AFTER Phase 3 (reconcile).** Phase 3 writes the `:lfs` flag for chunks whose freeze was in flight at a prior crash; running `compute_resume_ledger` before Phase 3 would see those mid-freeze chunks as internal `:lfs` gaps and false-positive-fatal at startup.
- **Scans every startup, even when `streaming:last_committed_ledger` is already set.** The scan's primary output in the mid-life-restart case is validation, not derivation; catching broken on-disk state before opening active stores is strictly safer than silently resuming on top.
- **Validation failures are fatal.** Any inconsistency aborts startup with "migration to streaming failed" + an operator-readable error naming what's wrong. The service exits non-zero; no active stores are opened.

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
    cpi  = config.service.chunks_per_tx_index
    scan = scan_all_chunk_and_index_keys(meta_store)
    validate_scan(scan, cpi)

    last_committed_ledger = meta_store.get("streaming:last_committed_ledger")
    if last_committed_ledger is not None:
        validate_last_committed_consistency(scan, last_committed_ledger)
        return last_committed_ledger + 1
    if scan.lfs_chunks:
        return last_ledger_in_chunk(scan.lfs_chunks[-1]) + 1   # first-ever post-Phase-1
    return retention_aligned_resume_ledger(config)             # tip-tracker fresh start (no BSB)


def validate_scan(scan, cpi):
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
        fatal(":lfs / :events mismatch — process_chunk crashed mid-run, unrecovered")

    first_complete_tx_index_id = first_fully_covered_tx_index_id(start)
    last_complete_tx_index_id  = last_fully_covered_tx_index_id(end)
    complete  = set(range(first_complete_tx_index_id, last_complete_tx_index_id + 1))
    missing   = complete - set(scan.txhash_indexes)
    if missing:
        fatal(f"complete tx indexes {sorted(missing)} missing index:txhash flag")


def validate_last_committed_consistency(scan, last_committed_ledger):
    # L = last_committed_ledger ⇒ all chunks up to chunk_id_of_ledger(L)-1 must have :lfs (L's own chunk is still ingesting).
    active_chunk_id = chunk_id_of_ledger(last_committed_ledger)
    required_last   = active_chunk_id - 1
    if required_last < 0:
        return
    actual_last = scan.lfs_chunks[-1] if scan.lfs_chunks else -1
    if actual_last < required_last:
        fatal(f"streaming:last_committed_ledger={last_committed_ledger} requires :lfs "
              f"through chunk {required_last}; scan's highest is {actual_last}")


def retention_aligned_resume_ledger(config):
    # Tip-tracker fresh start (no BSB, no on-disk chunks). [BSB]-absent + retention=0 is rejected by validate_config.
    network_tip_ledger = get_latest_network_tip(config.history_archives.urls)
    retention_ledgers  = config.service.retention_ledgers
    target_ledger = max(network_tip_ledger - retention_ledgers, GENESIS_LEDGER)
    return first_ledger_of_tx_index_containing(target_ledger)
```

### Phase 4 — Live Ingestion

Opens active stores for the resume position, spawns the lifecycle task, starts captive core, and enters the ingestion loop. Query serving starts here (see [Query Contract](#query-contract)).

```python
def phase4_live_ingest(config, meta_store, resume_ledger):
    # streaming:last_committed_ledger is first written by the live loop, not at bootstrap.
    active_stores = open_active_stores_for_resume(config, meta_store, resume_ledger)
    run_in_background(run_prune_lifecycle_loop, config, meta_store)

    ledger_backend = make_ledger_backend(config.captive_core.config_path)
    ledger_backend.PrepareRange(UnboundedRange(resume_ledger))

    set_service_ready()   # in-memory; unblocks queries
    run_live_ingestion_loop(config, ledger_backend, active_stores, meta_store, resume_ledger)


def open_active_stores_for_resume(config, meta_store, resume_ledger):
    resume_chunk_id    = chunk_id_of_ledger(resume_ledger)
    resume_tx_index_id = tx_index_id_of_chunk(resume_chunk_id)

    # Each open_active_*_store sets its hot:* key before mkdir (see Flag Semantics).
    return ActiveStores(
        ledger = open_active_ledger_store(config, meta_store, resume_chunk_id),
        events = open_active_events_store(config, meta_store, resume_chunk_id),
        txhash = open_active_txhash_store(config, meta_store, resume_tx_index_id),
    )
```

Captive core takes 4–5 minutes to spin up and start emitting at `resume_ledger`. During that window `getHealth` remains in `catching_up` state (see [Query Contract](#query-contract)).

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
- **Meta-store is single-writer.** Writers are the ingestion loop (per-ledger checkpoint), freeze tasks (`:lfs` / `:events` / `:txhash` flags), and the lifecycle loop (`"deleting"` marker + prune key delete). The meta-store wrapper serializes them on top of RocksDB's single-writer semantics.
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
    # Initial sweep catches "deleting" state from a prior crashed prune; later sweeps fire on boundary.
    retention_ledgers = config.service.retention_ledgers
    _run_prune_sweep(meta_store, retention_ledgers, config)
    while True:
        wait_for_chunk_boundary_notification()
        _run_prune_sweep(meta_store, retention_ledgers, config)


def _run_prune_sweep(meta_store, retention_ledgers, config):
    for tx_index_id in prunable_tx_index_ids(meta_store, retention_ledgers):
        prune_tx_index(tx_index_id, meta_store, config)


def prunable_tx_index_ids(meta_store, retention_ledgers):
    # Eligible: tx_index fully past retention AND `:txhash` is `"1"` or `"deleting"`.
    if retention_ledgers == 0:
        return []
    last_committed_ledger = meta_store.get("streaming:last_committed_ledger")
    max_eligible_tx_index_id = max_prunable_tx_index_id(last_committed_ledger, retention_ledgers)
    if max_eligible_tx_index_id < 0:
        return []
    result = []
    for tx_index_id in range(0, max_eligible_tx_index_id + 1):
        val = meta_store.get(f"index:{tx_index_id:08d}:txhash")
        if val in ("1", "deleting"):
            result.append(tx_index_id)
    return result


def prune_tx_index(tx_index_id, meta_store, config):
    # Two-phase marker: "deleting" set first, key cleared last. Idempotent on retry.
    meta_store.put(f"index:{tx_index_id:08d}:txhash", "deleting")
    for chunk_id in chunks_for_tx_index(tx_index_id):
        # Files-first, flags-last; all deletes idempotent.
        delete_if_exists(ledger_pack_path(chunk_id))
        delete_events_segment(chunk_id)
        delete_if_exists(raw_txhash_path(chunk_id))   # defence-in-depth; normally already gone
        meta_store.delete(f"chunk:{chunk_id:08d}:lfs")
        meta_store.delete(f"chunk:{chunk_id:08d}:events")
        meta_store.delete(f"chunk:{chunk_id:08d}:txhash")
    delete_recsplit_idx_files(tx_index_id)
    meta_store.delete(f"index:{tx_index_id:08d}:txhash")
```

- **Why index-atomic.** Per-chunk pruning would open a window where `getTransaction` resolves to a ledger seq whose pack has been deleted; whole-index gating closes it.
- **Extra data on disk.** Up to `LEDGERS_PER_TX_INDEX - 1` ledgers past strict retention. `RETENTION_LEDGERS` is a multiple of `LEDGERS_PER_TX_INDEX`, so the next-eligible index is exactly `LEDGERS_PER_TX_INDEX` further.

---

## Query Contract

Query serving is gated on Phase 4 (live ingestion) being reached. `getLedger`, `getTransaction`, `getEvents` all return **HTTP 4xx** during Phases 1–3.

### Readiness Signal

- In-memory boolean `service_ready`, flipped to `true` by `set_service_ready()` at Phase 4 (live ingestion) entry, after Phases 1–3 complete and active stores are opened. Not persisted; every startup begins `false`.
- HTTP server binds at service startup (before Phase 1 (catchup)), so `getHealth` is always servable. The QueryRouter routes `getHealth` unconditionally and gates `getLedger` / `getTransaction` / `getEvents` on `service_ready`: `false` → HTTP 4xx; `true` → route normally.
- Clients see `HTTP 4xx` from the three read endpoints on every startup until Phase 4 is reached, regardless of prior runs. Intentional — catchup and recovery phases must complete before the service serves, every time.

### Behavior During Phases 1–3

- `/getLedger`, `/getTransaction`, `/getEvents` → `HTTP 4xx` with no payload detail.
- `/getHealth` → always served. Response payload matches the existing stellar-rpc shape: `status` (`catching_up` during Phases 1–3, `healthy` during Phase 4 (live ingestion)), `latestLedger` (= `streaming:last_committed_ledger`, or `0` if absent), `oldestLedger` (first ingested ledger), `ledgerRetentionWindow`. No drift field, no network-tip field.
- No partial / incremental serving. The service does not serve "whatever is ingested so far" while Phases 1–3 are running.

### Behavior When an Index Is Being Pruned

- `prune_tx_index` sets `index:{tx_index_id:08d}:txhash = "deleting"` before touching any files, and deletes the key after all files are gone. Query routing treats `"deleting"` identically to `"absent"` (key-not-present).
- Queries for a ledger in a pruning index return HTTP 4xx (past retention) starting the instant the `"deleting"` marker is set, not when the files actually disappear. No window where queries route into a half-deleted index.

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

1. **Per-ledger checkpoint.** `streaming:last_committed_ledger` is written only after all three active stores durably commit. Resume is `last_committed_ledger + 1`.
2. **No separate recovery phase.** Startup is Phases 1–4. Nothing else.
3. **Max-1-transitioning per freeze.** A freeze transition must complete before the next one starts, per kind (LFS, events, RecSplit). Applies in steady state and crash recovery.
4. **Retention immutable.** `config:retention_ledgers` is stored on first run and compared thereafter. No mid-run retention change. Past-retention orphans can only arise from leapfrog — and leapfrog is deterministic, so Phase 1 (catchup) itself avoids producing them.
5. **Two-phase prune marker.** `index:{tx_index_id}:txhash = "deleting"` is set before any file delete; the key is cleared last. Queries treat `"deleting"` as absent. Idempotent on crash (`prunable_tx_index_ids` picks `"deleting"` back up). See [Pruning](#pruning).
6. **Hot-key tracking.** Every active store directory has a `hot:*` key, set BEFORE `mkdir` and cleared AFTER dir removal. Phase 3 (reconcile) iterates `hot:*` keys to find directories needing recovery — no filesystem scan anywhere.

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
