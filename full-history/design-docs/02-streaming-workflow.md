# Streaming Workflow

## Overview

The stellar-rpc daemon is the full-history RPC service. One binary, one invocation, one long-running process.

- Operator runs `stellar-rpc --config path/to/config.toml`. No subcommand. No `--mode` flag. No behavior-switching flags.
- On every start the daemon runs four sequential startup phases, then enters a live ingestion loop it stays in until killed.
- Behavior across the three operator profiles — **archive** (full history), **pruning-history** (retention-windowed history with BSB catchup), **tip-tracker** (retention-windowed history, no object store; captive-core-only) — is determined entirely by TOML config; no profile flag. Full matrix: [Operator Profiles](#operator-profiles).
- Backfill (`01-backfill-workflow.md`) is used as an internal subroutine by Startup Phase 1 (catchup). Operators never invoke backfill directly.

**What the daemon does end-to-end:**
- Validates config against immutable meta-store state: `CHUNKS_PER_TXHASH_INDEX` (chunks-per-tx-index constant; defines on-disk layout) and `RETENTION_LEDGERS` (history window in ledgers, or `0` for full history). Both detailed in [Configuration](#configuration).
- Catches up to the current **network tip** (most recent ledger the Stellar network has produced, sampled from the history archive — defined in [Terminology](#terminology)) using **BSB** (Buffered Storage Backend — remote object-store reader for `LedgerCloseMeta`; see [Ledger Source](#ledger-source)) or captive core (embedded `stellar-core` subprocess; see [Ledger Source](#ledger-source)), whichever is configured.
- Hydrates any in-flight state left by a prior run.
- Ingests live ledgers from `CaptiveStellarCore` (the stellar Go SDK's captive-core client type — wraps the embedded `stellar-core` subprocess) at ~1 per 6 seconds.
- Writes each live ledger to three **active stores** — mutable per-chunk or per-index RocksDB instances for ledger, txhash, events — detailed in [Active Store Architecture](#active-store-architecture).
- Freezes active stores to immutable files at chunk and index boundaries in background.
- Prunes past-retention indexes atomically when retention is configured.
- Serves `getLedger`, `getTransaction`, `getEvents` only after startup phases complete. Returns HTTP 4xx during startup.

---

## Terminology

Terms used repeatedly throughout this doc. Skim on first read, refer back when a term surfaces later.

- **Daemon** — the stellar-rpc binary running as one long-lived process. The only operator-facing entry point.
- **Startup phases 1–4** — sequential bootstrap work the daemon runs once per process start, before serving queries. Not a lifecycle concept — once Phase 4 (live ingestion) is reached, it stays there until the process exits. [Details](#startup-sequence).
- **Phase 1 (catchup)** — the startup phase that closes the gap between the last-committed ledger and the current network tip. Invokes the backfill subroutine internally.
- **Backfill (subroutine)** — a self-contained mechanism that ingests a known `[range_start, range_end]` chunk range via a static DAG of per-chunk tasks (`process_chunk`, `build_txhash_index`, `cleanup_txhash`). Specified in `01-backfill-workflow.md`. In the unified design, backfill is an internal callable only — no CLI (command-line) entry point exists.
- **Leapfrog** (colloquial) — when retention is configured (`RETENTION_LEDGERS > 0`), Phase 1 (catchup) skips past ledgers older than `tip - RETENTION_LEDGERS` by starting ingestion at the first ledger of the txhash index that contains `tip - RETENTION_LEDGERS`. Always lands on an index boundary — upholds the invariant that every persisted chunk is the first chunk of its index or a forward-contiguous extension of one. Implemented by the `retention_aligned_start_chunk` helper (Phase 1 (catchup) callsite) and the `retention_aligned_resume_ledger` helper (`compute_resume_ledger`'s no-BSB fresh-start branch).
- **`compute_resume_ledger`** — shared helper called once per daemon start, AFTER Phase 3 (reconcile) and BEFORE Phase 4 (live ingestion). Scans meta-store state end-to-end, validates on-disk consistency, and returns `resume_ledger` for Phase 4 (live ingestion). Runs post-Phase-3 so any in-flight freezes Phase 3 finished (and their newly-set `:lfs` flags) are visible to the scan. See [Compute Resume Ledger](#compute-resume-ledger).
- **`streaming:last_committed_ledger` (per-ledger checkpoint)** — meta-store key written once per live ledger inside Phase 4 (live ingestion)'s ingestion loop. Tracks live-streaming progress. Never touched during Phases 1–3. Bound locally as `last_committed_ledger` in pseudocode.
- **`network_tip_ledger`** — the most recent ledger the Stellar network has produced. Always sampled from the history archive via HTTP GET on `/.well-known/stellar-history.json` against `HISTORY_ARCHIVES.URLS`, wrapped in the `get_latest_network_tip()` helper (handles retries + the archive-tip-lags-true-tip-by-up-to-63-ledgers quirk). Called only in startup-phase contexts: the Phase 1 (catchup) loop per iter, and `retention_aligned_resume_ledger` for the no-BSB fresh-start case. Phase 4 (live ingestion) steady state does NOT sample tip. Different from `last_committed_ledger` (the daemon's own progress).
- **Active store** — a mutable store holding in-flight ledger data for the chunk or index currently being ingested. Three kinds:
  - Ledger active store — a per-chunk RocksDB (one instance per chunk).
  - TxHash active store — a per-index RocksDB with 16 column families (one instance per index).
  - Events active store — per-chunk RocksDB (one instance per chunk; schema + column families per [getEvents full-history design](../../design-docs/getevents-full-history-design.md)).
- **Immutable store** — on-disk files produced by freezing an active store. Three kinds:
  - Ledger pack file (one per chunk).
  - **RecSplit** index `.idx` files (16 per index) — minimal-perfect-hash files for `txhash → ledger_seq` lookup.
  - Events cold segment (three files per chunk: `events.pack`, `index.pack`, `index.hash`).
- **Freeze transition** — a background goroutine that converts an active store's contents to immutable files and deletes the active store. Three flavors: **LFS** (shorthand for the ledger active store → `.pack` file freeze) and **events** (events active store → cold segment) run per chunk; **RecSplit** (txhash active store → 16 `.idx` files) runs per index.
- **Chunk** — a block of 10_000 consecutive ledgers. Atomic unit of ingestion and freeze. `first_ledger_in_chunk(chunk_id)` always ends in `..._02`; `last_ledger_in_chunk(chunk_id)` always ends in `..._01`. No partial chunks — every chunk on disk is a full 10_000-ledger chunk.
- **Txhash index** (a.k.a. "tx index", "index") — `CHUNKS_PER_TXHASH_INDEX` consecutive chunks. Atomic unit of retention pruning. Formulas in [Geometry](#geometry). Both docs use "tx index" as the dominant narrative form; "txhash index" appears where the output's role as a txhash lookup is the emphasis.
- **Chunk boundary** — the moment ingestion commits the last ledger of a chunk. Triggers background LFS + events freeze for that chunk.
- **Index boundary** — the moment ingestion commits the last ledger of an index. Triggers background RecSplit build for that index. Every index boundary is also a chunk boundary.
- **Catchup** — synonym for "close the gap between last-committed ledger and current tip". Performed inside Phase 1 (catchup).
- **`.bin` file** — a backfill-produced raw txhash flat file (transient). Exists only for chunks the backfill subroutine has flagged `:txhash` but whose containing index has not yet had its RecSplit built. Deleted by Phase 2 (`.bin` hydration) once loaded into the active txhash RocksDB. Streaming's live path never produces `.bin` files.

---

## Geometry

See [01-backfill-workflow.md — Geometry](./01-backfill-workflow.md#geometry). Streaming uses the same constants (`GENESIS_LEDGER`, `LEDGERS_PER_CHUNK`, `LEDGERS_PER_INDEX`, `CHUNKS_PER_TXHASH_INDEX`), mapping functions, and derived helpers.

---

## Configuration

Streaming reads the same TOML file as backfill, plus additional keys described below.

### Shared Config (from backfill)

`[SERVICE]` (daemon-wide settings — `DEFAULT_DATA_DIR`, `CHUNKS_PER_TXHASH_INDEX`), `[BSB]` (Buffered Storage Backend source settings), `[IMMUTABLE_STORAGE.*]` (on-disk paths for immutable artifacts — ledger packs, events, raw txhash, txhash index), `[META_STORE]` (meta-store RocksDB path), `[LOGGING]` (log level + format) are detailed in [01-backfill-workflow.md — Configuration](./01-backfill-workflow.md#configuration). Streaming adds extra keys to `[SERVICE]` and introduces `[CAPTIVE_CORE]` (embedded `stellar-core` subprocess settings), `[ACTIVE_STORAGE]` (active RocksDB paths), `[HISTORY_ARCHIVES]` (Stellar history-archive URLs for tip sampling) — all defined below.

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
| `NETWORK_PASSPHRASE` | string | **required** | Stellar network passphrase — for example, `"Public Global Stellar Network ; September 2015"` for pubnet; `"Test SDF Network ; September 2015"` for testnet. Must match the `NETWORK_PASSPHRASE` in the captive-core config file. Surfaced to all daemon code via the runtime config struct. |

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

| Key | Type | Default | Description |
|---|---|---|---|
| `URLS` | []string | **required** | List of Stellar history archive URLs. Used to sample tip via `/.well-known/stellar-history.json` for Phase 4 (live ingestion)'s leapfrog-from-tip computation (when `[BSB]` is absent on first-ever start). Same key the existing ingest service reads. |

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

**No other flags.** No `--mode`, no `--start-ledger`, no `--end-ledger`, no subcommand. Any per-run behavior is either driven by config or derived at runtime from meta store + tip.

### Validation Rules

- `CHUNKS_PER_TXHASH_INDEX` immutable across runs (see [Immutable Keys](#immutable-keys-stored-in-meta-store-fatal-if-changed)).
- `RETENTION_LEDGERS` immutable across runs.
- `RETENTION_LEDGERS` must be `0` OR a positive integer multiple of `LEDGERS_PER_INDEX`. Valid at `cpi=1_000`: `0`, `10_000_000`, `20_000_000`, `30_000_000`, etc. Invalid: `15_000_000` (not a multiple), `5_000_000` (below minimum). Rationale: pruning runs at whole-index granularity; retention windows that don't align to index boundaries would leave partial indexes perpetually on disk.
- `[BSB]` optional. When present → Phase 1 (catchup) invokes backfill over the BSB; when absent → Phase 1 (catchup) is a no-op and Phase 4 (live ingestion)'s captive core handles initial catchup. May be added or removed between runs.
- **`[BSB]` absent AND `RETENTION_LEDGERS = 0` is fatal.** Full history requires BSB — captive-core archive-catchup from genesis would take weeks-to-months. Not a supported operating mode.
- `[HISTORY_ARCHIVES].URLS` required in all profiles.
- `[CAPTIVE_CORE].CONFIG_PATH` required in all profiles.
- `[CAPTIVE_CORE].STELLAR_CORE_BINARY_PATH` required in all profiles.
- `[SERVICE].NETWORK_PASSPHRASE` required in all profiles.

### Validation Pseudocode

```python
def validate_config(config, meta_store):
    cpi = config.service.chunks_per_txhash_index
    retention_ledgers = config.service.retention_ledgers

    if retention_ledgers != 0 and (retention_ledgers <= 0 or (retention_ledgers % LEDGERS_PER_INDEX) != 0):
        fatal(f"RETENTION_LEDGERS={retention_ledgers} must be 0 or a positive multiple of "
              f"LEDGERS_PER_INDEX={LEDGERS_PER_INDEX}.")

    if config.bsb is None and retention_ledgers == 0:
        fatal("[BSB] is absent AND RETENTION_LEDGERS=0 (full history). Full history requires "
              "BSB — captive-core-from-genesis is not supported. Either add [BSB] or set "
              "RETENTION_LEDGERS > 0.")

    # Fatals with a clear "X is required" message for any key marked **required**
    # in the [Configuration] tables above that is absent or empty.
    ensure_required_config_fields_exist(config)

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

Single RocksDB instance, WAL (Write-Ahead Log) always enabled. Authoritative source for every startup decision.

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
| Events | `{ACTIVE_STORAGE.PATH}/events-store-chunk-{chunk_id:08d}/` | per [getEvents full-history design](../../design-docs/getevents-full-history-design.md) | per [getEvents full-history design](../../design-docs/getevents-full-history-design.md) | Every 10_000 ledgers (chunk) |

- Ledger and txhash stores are RocksDB. WAL required.
- TxHash store uses 16 column families (`cf-0`..`cf-f`) routed by `txhash[0] >> 4`.
- Events active store is a per-chunk RocksDB; schema + column families per [getEvents full-history design](../../design-docs/getevents-full-history-design.md). Per-ledger writes are idempotent.

### Store Lifecycle

- **Creation.** Active stores are opened on-demand, synchronously, at the boundary where they're first needed:
  - Phase 4 (live ingestion) entry opens exactly one store per data type: `resume_chunk`'s ledger + events stores, and `resume_tx_index`'s txhash store.
  - Each chunk boundary synchronously opens the next chunk's ledger + events stores after capturing the current ones as transitioning handles.
  - Each tx-index boundary synchronously opens the next tx-index's txhash store similarly.
- **Synchronous open cost.** mkdir + RocksDB open + column-family setup is ~100–200 ms. At live cadence (6 s/ledger) this fits entirely inside the inter-ledger idle time — zero throughput impact. During archive replay (~500 ledgers/s) the cost is absorbed once per chunk boundary, ~100 ms each; over Alice's 10M-retention fresh start (~1_000 chunks) that's ~100 s of cumulative stall distributed across a ~6 h replay, sub-1%.
- **Transition.** At each boundary, the ingestion loop (a) captures the current store handle as `transitioning`, (b) synchronously opens the next store, (c) spawns the background freeze goroutine with the `transitioning` handle. Ingestion proceeds against the new active store immediately.
- **Deletion.** The freeze goroutine closes the transitioning handle and deletes its RocksDB directory AFTER writing the immutable artifact and setting the meta-store flag (flag-after-fsync). A crash between flag-set and dir-delete leaves an orphan that Phase 3 (reconcile) classifies as flag-is-truth and deletes.
- **Crash recovery.** Phase 3 (reconcile) classifies each on-disk active-store directory by chunk/index ID + flag presence:
  - Dir is for `resume_chunk` / `resume_tx_index` → keep (the active store the live loop will resume against).
  - `:lfs` / `:events` / `:txhash` flag present + dir present → delete dir (flag-is-truth; freeze completed, delete lingered).
  - Flag absent + chunk/index ID < resume → `finish_interrupted_ledger_freeze` (or equivalent) — complete the freeze, set flag, delete dir.
  - Else → future orphan; delete dir.

### Max Concurrent Stores

| Store | Max active | Max transitioning | Max total |
|---|---|---|---|
| Ledger | 1 | 1 | 2 |
| Events | 1 | 1 | 2 |
| TxHash | 1 | 1 | 2 |

---

## Ledger Source

- **Backfill (Phase 1 (catchup)) uses `BSBSource` only.** Each `process_chunk` instantiates its own per-chunk BSB via the `make_bsb` partial, prepares range for its 10_000 ledgers, reads, tears down. Captive core cannot be a backfill source — see [Backfill vs Phase 1 (catchup)](#backfill-vs-phase-1-catchup).
- **Live streaming (Phase 4 (live ingestion)) uses captive core directly** — no `LedgerSource` wrapper. Phase 4 (live ingestion) calls the stellar Go SDK's `ledgerBackend.PrepareRange(UnboundedRange(resume_ledger)) + GetLedger(seq)` against the captive-core subprocess.

- **`BSBSource`** is the backfill-only ledger source — one instance per `process_chunk`, interface mirrors the stellar Go SDK's `LedgerBackend` (`PrepareRange` + `GetLedger`), torn down at end-of-task.
- **`make_bsb_partial(config)`** returns a partial that instantiates `BSBSource(config.bsb)` per call; returns `None` when `[BSB]` is absent so `phase1_catchup` can short-circuit.

---

## Startup Sequence

Four sequential phases, same code path for first start and every restart. The first three are bounded bootstrap work; Phase 4 (live ingestion) is the long-running state the daemon stays in until process exit.

- **Phase 1 — catchup.** Closes the gap between on-disk `:lfs` flags and current network tip **when `[BSB]` is configured**, by invoking the backfill subroutine in a loop. Without `[BSB]`, Phase 1 (catchup) is a no-op and Phase 4 (live ingestion)'s captive core handles initial catchup naturally via its own `PrepareRange(UnboundedRange(resume_ledger))`.
- **Phase 2 — hydrate txhash.** Loads any `.bin` files Phase 1 (catchup) left (for the trailing partial index) into the active txhash store, then deletes them.
- **Phase 3 — reconcile orphans.** Completes any in-flight freeze transitions left by a prior crash.
- **Phase 4 — live ingestion.** Opens active stores, starts captive core, spawns the lifecycle goroutine, flips the `daemon_ready` flag, enters the ingestion loop. Runs until process exit.

"Phase" here refers to the startup ordering only. Once Phase 4 (live ingestion) is entered, there's no Phase 5 — the daemon is in live-streaming steady state.

### Backfill vs Phase 1 (catchup)

- **Backfill** is the subroutine (`run_backfill` in [01-backfill-workflow.md](./01-backfill-workflow.md)). BSB-only; parallel per-chunk BSB instances. Captive core cannot be a backfill source — its subprocess is serial and expensive to spin up per instantiation.
- **Phase 1 (catchup)** is the startup phase that runs on every daemon start. Its job: close the gap between on-disk state and current network tip before Phase 4 (live ingestion) takes over. Invokes backfill as its mechanism when `[BSB]` is configured; otherwise no-op and Phase 4 (live ingestion)'s captive core handles catchup via `PrepareRange(UnboundedRange(resume_ledger))`.

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
    make_bsb = make_bsb_partial(config)
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
MAX_PHASE1_ITERATIONS = 5   # safety-net cap; hitting it means BSB is degraded.


def phase1_catchup(config, meta_store, make_bsb):
    if make_bsb is None:
        return                                                 # [BSB] absent → no-op

    retention_ledgers = config.service.retention_ledgers
    last_scheduled_end_chunk = -1

    for iter_count in range(1, MAX_PHASE1_ITERATIONS + 1):
        network_tip_ledger = get_latest_network_tip(config.history_archives.urls)
        end_chunk = last_completed_chunk_id(network_tip_ledger)
        if end_chunk <= last_scheduled_end_chunk:
            return                                             # converged
        start_chunk = retention_aligned_start_chunk(network_tip_ledger, retention_ledgers)
        if end_chunk < start_chunk:
            return                                             # leapfrog past tip
        log.info(f"phase1_catchup iter={iter_count}/{MAX_PHASE1_ITERATIONS} "
                 f"tip={network_tip_ledger} range=[{start_chunk}, {end_chunk}]")
        run_backfill(config, start_chunk, end_chunk, make_bsb)
        last_scheduled_end_chunk = end_chunk

    fatal(f"phase1_catchup exceeded {MAX_PHASE1_ITERATIONS} iters; "
          f"check [BSB].NUM_WORKERS / BUFFER_SIZE (backlog trail in logs).")


def retention_aligned_start_chunk(network_tip_ledger, retention_ledgers):
    # Called by: phase1_catchup (per loop iteration) to compute range_start_chunk_id.
    # Returns the first chunk Phase 1 (catchup) should backfill:
    #   - Archive profile (retention=0): chunk 0 (full history from genesis).
    #   - Pruning-history (retention>0): first chunk of the tx index containing
    #     (tip - retention_ledgers). Aligned DOWN to a tx-index boundary so the first
    #     persisted chunk starts a complete index (upholds the no-gaps invariant).
    # Worst case: up to LEDGERS_PER_INDEX - 1 extra ledgers below strict retention.
    if retention_ledgers == 0:
        return 0
    target_ledger = max(network_tip_ledger - retention_ledgers, GENESIS_LEDGER)
    return first_chunk_id_of_tx_index_containing(target_ledger)
```

**Worker concurrency:** `run_backfill` caps DAG concurrency at `GOMAXPROCS`. Each `process_chunk` owns its own BSB instance (`make_bsb()`), prepares range for its 10_000 ledgers, reads, and tears down — see [01-backfill-workflow.md — process_chunk](./01-backfill-workflow.md#process_chunkchunk_id-make_bsb).

**Retention effect:** retention determines Phase 1 (catchup)'s chunk range. Catchup time ≈ `retention_window / (BSB throughput)`.

### Phase 2 — Hydrate TxHash Data from `.bin`

- Phase 1 (catchup) may leave `.bin` files for chunks in the last (incomplete) tx index.
- Phase 2 (`.bin` hydration) loads each into the active txhash store, then deletes the `.bin` + `chunk:{chunk_id:08d}:txhash` flag.
- After Phase 2 (`.bin` hydration): no `.bin` files and no `:txhash` chunk flags remain.

```python
def phase2_hydrate_txhash(config, meta_store):
    # Sweep leftover .bin + flag for tx indexes already flagged complete (crash between
    # index:N:txhash set and cleanup_txhash finish).
    for tx_index_id in tx_index_ids_with_txhash_flag(meta_store):
        for chunk_id in chunks_for_tx_index(tx_index_id):
            if meta_store.has(f"chunk:{chunk_id:08d}:txhash"):
                meta_store.delete(f"chunk:{chunk_id:08d}:txhash")
                delete_if_exists(raw_txhash_path(chunk_id))

    # Load .bin for the trailing incomplete tx index into the active RocksDB.
    incomplete_tx_index_id = current_incomplete_tx_index_id(meta_store)
    if incomplete_tx_index_id is None:
        return

    txhash_store = open_active_txhash_store(config, incomplete_tx_index_id)
    try:
        for chunk_id in chunks_for_tx_index(incomplete_tx_index_id):
            if not meta_store.has(f"chunk:{chunk_id:08d}:txhash"):
                continue
            bin_path = raw_txhash_path(chunk_id)
            if os.path.exists(bin_path):
                load_bin_into_rocksdb(bin_path, txhash_store)
            meta_store.delete(f"chunk:{chunk_id:08d}:txhash")
            delete_if_exists(bin_path)

        # Sweep orphan .bin (flag already deleted, .bin lingered from a prior crash).
        for bin_file in scan_bin_files_for_tx_index(incomplete_tx_index_id):
            if not meta_store.has(f"chunk:{parse_chunk_id(bin_file):08d}:txhash"):
                os.remove(bin_file)
    finally:
        txhash_store.close()   # Phase 4 re-opens by directory path; flock would collide otherwise.
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
    last_committed_ledger = meta_store.get("streaming:last_committed_ledger")
    if last_committed_ledger is None:
        return                                                    # fresh start — nothing in flight

    resume_chunk_id = chunk_id_of_ledger(last_committed_ledger + 1)

    reconcile_ledger_store_dirs(config, meta_store, resume_chunk_id)
    reconcile_events_store_dirs(config, meta_store, resume_chunk_id)
    reconcile_txhash_store_dirs(config, meta_store, tx_index_id_of_chunk(resume_chunk_id))
```

Each `reconcile_*_store_dirs` helper scans its own active-store directory type and classifies each dir it finds:

- **`reconcile_ledger_store_dirs`** — per `chunk_id` found: `== resume_chunk_id` → keep (active); `chunk:{chunk_id}:lfs` flag present → delete dir (flag-is-truth, freeze completed); `< resume_chunk_id` and flag absent → call `finish_interrupted_ledger_freeze(store_dir, chunk_id, meta_store)`; else delete as future orphan.
- **`reconcile_events_store_dirs`** — identical classification with `:events` flag and `finish_interrupted_events_freeze`.
- **`reconcile_txhash_store_dirs`** — per `tx_index_id` found: `== resume_tx_index_id` → keep; `index:{tx_index_id:08d}:txhash` present → delete dir (RecSplit done); flag absent and all chunks of `tx_index_id` have `:lfs` → open the store synchronously and spawn `build_tx_index_recsplit_files` in background (the builder reads from the handle and closes it).

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
    cpi  = config.service.chunks_per_txhash_index
    scan = scan_all_chunk_and_index_keys(meta_store)
    validate_scan(scan, cpi)

    last_committed_ledger = meta_store.get("streaming:last_committed_ledger")
    if last_committed_ledger is not None:
        validate_last_committed_consistency(scan, last_committed_ledger)
        return last_committed_ledger + 1
    if scan.lfs_chunks:
        return last_ledger_in_chunk(scan.lfs_chunks[-1]) + 1   # first-ever post-Phase-1
    return retention_aligned_resume_ledger(config)             # Alice fresh start


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
    # `streaming:last_committed_ledger = L` implies every chunk up to chunk_id_of_ledger(L)-1
    # must have :lfs (the chunk containing L itself is the currently-ingesting one).
    active_chunk_id = chunk_id_of_ledger(last_committed_ledger)
    required_last   = active_chunk_id - 1
    if required_last < 0:
        return
    actual_last = scan.lfs_chunks[-1] if scan.lfs_chunks else -1
    if actual_last < required_last:
        fatal(f"streaming:last_committed_ledger={last_committed_ledger} requires :lfs "
              f"through chunk {required_last}; scan's highest is {actual_last}")


def retention_aligned_resume_ledger(config):
    # Called by: compute_resume_ledger (tip-tracker fresh-start branch; no BSB, no on-disk chunks).
    # First chunk captive core ingests will be the first chunk of the tx index containing
    # (tip - retention). validate_config already rejected the [BSB]-absent + retention=0
    # combination, so this helper is never called in archive-from-genesis shape.
    network_tip_ledger = get_latest_network_tip(config.history_archives.urls)
    retention_ledgers  = config.service.retention_ledgers

    target_ledger = max(network_tip_ledger - retention_ledgers, GENESIS_LEDGER)
    return first_ledger_of_tx_index_containing(target_ledger)
```

### Phase 4 — Live Ingestion

Opens active stores for the resume position, spawns the lifecycle goroutine, starts captive core, and enters the ingestion loop. Query serving starts here (see [Query Contract](#query-contract)).

```python
def phase4_live_ingest(config, meta_store, resume_ledger):
    # resume_ledger is already computed by the orchestrator (see Compute Resume Ledger).
    # Phase 4 (live ingestion) does NOT write streaming:last_committed_ledger at bootstrap — the first
    # write happens inside the live ingestion loop after the first durable commit.
    active_stores = open_active_stores_for_resume(config, meta_store, resume_ledger)
    run_in_background(run_prune_lifecycle_loop, config, meta_store)

    ledger_backend = make_ledger_backend(config.captive_core.config_path)
    ledger_backend.PrepareRange(UnboundedRange(resume_ledger))

    set_daemon_ready()   # in-memory; unblocks queries
    run_live_ingestion_loop(config, ledger_backend, active_stores, meta_store, resume_ledger)


def open_active_stores_for_resume(config, meta_store, resume_ledger):
    # Open exactly one store per data type for the resume position. Subsequent
    # chunks / indexes are opened on-demand at boundary time — see on_chunk_boundary
    # and on_tx_index_boundary.
    resume_chunk_id    = chunk_id_of_ledger(resume_ledger)
    resume_tx_index_id = tx_index_id_of_chunk(resume_chunk_id)

    return ActiveStores(
        ledger = open_or_create_ledger_store(config, resume_chunk_id),
        events = open_or_create_events_store(config, meta_store, resume_chunk_id),
        txhash = open_or_create_txhash_store(config, resume_tx_index_id),
    )
```

Captive core takes 4–5 minutes to spin up and start emitting at `resume_ledger`. During that window `getHealth` remains in `catching_up` state (see [Query Contract](#query-contract)).

---

## Ingestion Loop

Single goroutine. Pull-based: the daemon drives sequential `GetLedger(seq)` calls. Same code path drains captive core's internal buffer during catchup and switches cadence to live closes (~5 s per ledger) once caught up.

```python
def run_live_ingestion_loop(config, ledger_backend, active_stores, meta_store, resume_ledger):
    ledger_seq = resume_ledger
    while True:
        lcm = ledger_backend.GetLedger(ledger_seq)   # blocks until available

        # All three writes durably commit before advancing the checkpoint.
        wait_all(
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

Three independent background transitions per chunk/index boundary; each has its own goroutine, flag, and cleanup. Live ingestion never blocks on them.

- **LFS transition** — per chunk. Retired ledger RocksDB → `.pack` file.
- **Events transition** — per chunk. Retired events RocksDB store → cold segment (3 files).
- **RecSplit transition** — per index. Retired txhash RocksDB → 16 `.idx` files.
- Streaming's freeze transitions never produce `.bin` files; those are transient backfill output only (Phase 1).

### Concurrency Model

- **`active_stores` is the ingestion loop's owned state.** Fields (`ledger`, `events`, `txhash` — one handle per data type, no `*_next`) are mutated only by the ingestion loop thread — specifically inside `on_chunk_boundary` and `on_tx_index_boundary`. Freeze transitions receive a handle by value at spawn time and never read back through `active_stores`.
- **Meta-store is single-writer.** Meta-store flag writes come from: the ingestion loop (per-ledger checkpoint), freeze transitions (artifact `:lfs` / `:events` / `:txhash` flags after fsync), and the lifecycle loop (`"deleting"` marker + key delete during prune). Go's `sync.Mutex` inside the meta-store wrapper + RocksDB's own single-writer semantics keep these serialized.
- **`wait_for_lfs_complete()` / `wait_for_events_complete()` are per-kind single-flight gates.** One outstanding transition per kind (LFS / events / RecSplit). Implementation: an unbuffered `chan struct{}` per kind, or equivalently a `sync.Mutex`. `wait_for_lfs_complete()` acquires; `signal_lfs_complete()` at the end of `freeze_ledger_chunk_to_pack_file` releases. Second transition starts only after the first releases. Not a `sync.WaitGroup` — that would wait for ALL transitions globally, wrong semantics.
- **Query handlers read from storage-manager layer** — each per-data-type storage manager (ledger / events / txhash) owns its own state-transition synchronization; the query handler never touches `active_stores` directly. **Read-view invariant:** during a transition, a query sees either pre-transition data (routed to the transitioning store) or post-transition data (routed to the new active store + the newly-flagged immutable artifact) — never a half-state mix. **Flag-is-truth applies to reads too:** a query never routes to an immutable artifact whose `:lfs` / `:events` / `:txhash` flag isn't set. Concrete lock primitives + routing logic belong in a separate query-routing design doc.
- **Stores are opened on-demand at boundary.** `open_active_stores_for_resume` (Phase 4 entry) opens exactly one store per data type (`resume_chunk` ledger + events, `resume_tx_index` txhash). Each chunk/tx-index boundary synchronously opens the next store (~100-200 ms) AFTER capturing the current one as transitioning, then spawns the background freeze. At live cadence the sync open fits inside the 6 s inter-ledger idle — zero throughput impact.

### Chunk Boundary (every 10_000 ledgers)

Triggered when the ingestion loop commits `last_ledger_in_chunk(chunk_id)`. Handoffs to two freeze transitions (LFS + events) that run in background.

```python
def on_chunk_boundary(chunk_id, active_stores, meta_store):
    # LFS + events each: drain prior freeze, capture current handle, open chunk+1 sync (~100-200 ms),
    # spawn background freeze. LFS and events run independently (events doesn't wait on LFS).
    wait_for_lfs_complete()
    transitioning_ledger_store = active_stores.ledger
    active_stores.ledger = open_or_create_ledger_store(config, chunk_id + 1)
    run_in_background(freeze_ledger_chunk_to_pack_file, chunk_id, transitioning_ledger_store, meta_store)

    wait_for_events_complete()
    transitioning_events_store = active_stores.events
    active_stores.events = open_or_create_events_store(config, meta_store, chunk_id + 1)
    run_in_background(freeze_events_chunk_to_cold_segment, chunk_id, transitioning_events_store, meta_store)

    notify_lifecycle()   # wake prune loop
```

### LFS Transition

Converts the retired ledger RocksDB store to an immutable `.pack` file, then discards the store.

```python
def freeze_ledger_chunk_to_pack_file(chunk_id, transitioning_ledger_store, meta_store):
    # overwrite=True discards any prior partial; flag-after-fsync. Crash between flag
    # and store-delete leaves an orphan that Phase 3 (reconcile) picks up (flag-is-truth).
    pack_path = ledger_pack_path(chunk_id)
    writer = packfile.create(pack_path, overwrite=True)
    for ledger_seq in range(first_ledger_in_chunk(chunk_id), last_ledger_in_chunk(chunk_id) + 1):
        writer.append(transitioning_ledger_store.get(uint32_big_endian(ledger_seq)))
    writer.fsync_and_close()
    meta_store.put(f"chunk:{chunk_id:08d}:lfs", "1")
    transitioning_ledger_store.close()
    delete_dir(ledger_store_path(chunk_id))
    signal_lfs_complete()


```

`finish_interrupted_ledger_freeze(store_dir, chunk_id, meta_store)` is the Phase 3 (reconcile) synchronous form: opens the store at `store_dir`, runs the same write + fsync + flag + close + `delete_dir(store_dir)` sequence, no `signal_lfs_complete`.

### Events Transition

Converts the retired events RocksDB store to three immutable files (events cold segment).

```python
def freeze_events_chunk_to_cold_segment(chunk_id, transitioning_events_store, meta_store):
    events_path = events_segment_path(chunk_id)
    write_cold_segment(transitioning_events_store, events_path)   # 3 files: events.pack, index.pack, index.hash
    fsync_all(events_path)
    meta_store.put(f"chunk:{chunk_id:08d}:events", "1")
    transitioning_events_store.close()
    delete_dir(events_store_path(chunk_id))
    signal_events_complete()


```

`finish_interrupted_events_freeze(store_dir, chunk_id, meta_store)` is the Phase 3 (reconcile) synchronous form: opens the store at `store_dir`, runs the same write + fsync + flag + close + `delete_dir(store_dir)` sequence, no `signal_events_complete`.

### Tx-Index Boundary (every `LEDGERS_PER_INDEX` ledgers)

The last chunk of a tx index has just rolled over. Before RecSplit can start, every chunk in the tx index must have its `:lfs` and `:events` flags set.

```python
def on_tx_index_boundary(tx_index_id, active_stores, meta_store):
    # Drain all in-flight chunk-level freezes for this tx index before RecSplit.
    wait_for_lfs_complete()
    wait_for_events_complete()
    verify_all_chunk_flags(tx_index_id, meta_store)
    transitioning_txhash_store = active_stores.txhash
    active_stores.txhash       = open_or_create_txhash_store(config, tx_index_id + 1)
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
    # Initial sweep catches `"deleting"` state left by a prior crashed prune;
    # subsequent sweeps fire on chunk-boundary notifications.
    retention_ledgers = config.service.retention_ledgers
    _run_prune_sweep(meta_store, retention_ledgers, config)
    while True:
        wait_for_chunk_boundary_notification()
        _run_prune_sweep(meta_store, retention_ledgers, config)


def _run_prune_sweep(meta_store, retention_ledgers, config):
    for tx_index_id in prunable_tx_index_ids(meta_store, retention_ledgers):
        prune_tx_index(tx_index_id, meta_store, config)


def prunable_tx_index_ids(meta_store, retention_ledgers):
    # Returns tx_index_ids fully past retention and prune-eligible (`:txhash` is `"1"` or
    # `"deleting"`). Eligibility: last_committed_ledger > last_ledger_in_tx_index(N) + retention_ledgers.
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
    # Two-phase marker: set "deleting" first, clear the key last. Idempotent on retry.
    meta_store.put(f"index:{tx_index_id:08d}:txhash", "deleting")
    for chunk_id in chunks_for_tx_index(tx_index_id):
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
- The HTTP server binds its port at daemon startup (before Phase 1 (catchup)) so `getHealth` is always servable regardless of current phase. The QueryRouter routes `getHealth` unconditionally and gates `getLedger` / `getTransaction` / `getEvents` on `daemon_ready`.
- This means: clients see `HTTP 4xx` from `getLedger`/`getTransaction`/`getEvents` on every startup until Phase 4 (live ingestion) is reached, regardless of whether prior runs have served queries. Intentional: catchup and recovery phases must complete before the daemon serves, every time.
- Query handlers check the flag on each request. `false` → HTTP 4xx. `true` → route normally.

### Behavior During Phases 1–3

- `/getLedger`, `/getTransaction`, `/getEvents` → `HTTP 4xx` with no payload detail.
- `/getHealth` → always served. Response payload matches the existing stellar-rpc shape: `status` (`catching_up` during Phases 1–3, `healthy` during Phase 4 (live ingestion)), `latestLedger` (= `streaming:last_committed_ledger`, or `0` if absent), `oldestLedger` (first ingested ledger), `ledgerRetentionWindow`. No drift field, no network-tip field.
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
