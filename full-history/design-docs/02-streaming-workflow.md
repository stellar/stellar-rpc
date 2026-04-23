# Streaming Workflow

## Overview

The stellar-rpc daemon is the full-history RPC service. One binary, one invocation, one long-running process.

- Operator runs `stellar-rpc --config path/to/config.toml`. No subcommand. No `--mode` flag. No behavior-switching flags.
- On every start the daemon runs four sequential startup phases, then enters a live ingestion loop it stays in until killed.
- Behavior across the three operator profiles (archive, pruning-history, tip-tracker) is determined entirely by TOML config — no profile flag.
- Backfill (`01-backfill-workflow.md`) is used as an internal subroutine by Startup Phase 1. Operators never invoke backfill directly.

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
- **Startup phases 1–4** — sequential bootstrap work the daemon runs once per process start, before serving queries. Not a lifecycle concept — once Phase 4 is reached, it stays there until the process exits. [Details](#startup-sequence).
- **Phase 1 catchup** — the startup phase that closes the gap between the last-committed ledger and the current network tip. Invokes the backfill subroutine internally.
- **Backfill (subroutine)** — a self-contained mechanism that ingests a known `[range_start, range_end]` chunk range via a static DAG of per-chunk tasks (`process_chunk`, `build_txhash_index`, `cleanup_txhash`). Specified in `01-backfill-workflow.md`. In the unified design, backfill is an internal callable only — no CLI entry point exists.
- **Leapfrog** — when retention is configured (`RETENTION_LEDGERS > 0`), Phase 1 skips past ledgers older than `tip - RETENTION_LEDGERS` by starting ingestion at the first ledger of the txhash index that contains `tip - RETENTION_LEDGERS`. Always lands on an index boundary — upholds the invariant that every persisted chunk is the first chunk of its index or a forward-contiguous extension of one.
- **`phase1_coverage_end_ledger`** — the last ledger of the contiguous prefix of `chunk:{chunkId}:lfs` flags starting from the lowest chunk on disk. Phase 1 uses this to decide what's still left to ingest. Returned by the same-named function. **Not the same** as `streaming:last_committed_ledger`. (Prior drafts called this concept "Phase 1 low-water mark"; the term was retired because it's semantically a HIGH-water mark — the newest confirmed ledger in contiguous coverage.)
- **`streaming:last_committed_ledger` (per-ledger checkpoint)** — meta-store key written once per live ledger inside the Phase 4 ingestion loop. Tracks live-streaming progress. Never touched during Phases 1–3. Bound locally as `last_committed_ledger` in pseudocode.
- **`network_tip_ledger`** — the most recent ledger the Stellar network has produced. Sampled from `source.tip()`. For `BSBSource`: read from BSB's range-end metadata. For `CaptiveCoreSource`: fetched via HTTP GET on `/.well-known/stellar-history.json` against `HISTORY_ARCHIVE_URLS`. Different from `last_committed_ledger` (the daemon's own progress).
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
- **Catchup** — synonym for "close the gap between last-committed ledger and current tip". Performed inside Phase 1.
- **`.bin` file** — a backfill-produced raw txhash flat file (transient). Exists only for chunks the backfill subroutine has flagged `:txhash` but whose containing index has not yet had its RecSplit built. Deleted by Phase 2 once loaded into the active txhash RocksDB. Streaming's live path never produces `.bin` files.

---

## Geometry

See [01-backfill-workflow.md — Geometry](./01-backfill-workflow.md#geometry). Streaming uses the same constants (`GENESIS_LEDGER`, `LEDGERS_PER_CHUNK`, `LEDGERS_PER_INDEX`, `CHUNKS_PER_TXHASH_INDEX`) and the same mapping functions (`chunk_id_of_ledger`, `first_ledger_in_chunk`, `last_ledger_in_chunk`, `tx_index_id_of_chunk`, `first_ledger_in_tx_index`, `last_ledger_in_tx_index`).

---

## Configuration

Streaming reads the same TOML file as backfill, plus additional keys described below.

### Shared Config (from backfill)

All of `[SERVICE]`, `[BACKFILL]`, `[IMMUTABLE_STORAGE.*]`, `[META_STORE]`, `[LOGGING]` apply unchanged. See [01-backfill-workflow.md — Configuration](./01-backfill-workflow.md#configuration) for the full schema.

### Immutable Keys (stored in meta store, fatal if changed)

Two keys are stored on first start and enforced on every subsequent start. Changing either requires wiping the datadir.

| Key | Stored under | Set by | Rule |
|---|---|---|---|
| `CHUNKS_PER_TXHASH_INDEX` | `config:chunks_per_txhash_index` | first run | Fatal if changed. |
| `RETENTION_LEDGERS` | `config:retention_ledgers` | first run | Fatal if changed. |

Source selection (BSB vs captive core) is determined per-startup by `[BACKFILL.BSB]` presence. Operators may add or remove BSB between runs without wiping — the daemon extends coverage forward from `phase1_coverage_end_ledger` regardless of source. Retention immutability is what constrains the data envelope; source choice doesn't need its own immutability gate.

### Streaming-Specific TOML

**[STREAMING]**

| Key | Type | Default | Description |
|---|---|---|---|
| `RETENTION_LEDGERS` | uint32 | `0` | `0` = full history; otherwise must be a positive multiple of `LEDGERS_PER_INDEX`. See [Validation Rules](#validation-rules). |
| `CAPTIVE_CORE_CONFIG` | string | **required** | Path to CaptiveStellarCore config file. |
| `DRIFT_WARNING_LEDGERS` | uint32 | `10` | `getHealth` reports unhealthy when ingestion drift exceeds this. ~60 seconds at 10 ledgers. |

**[STREAMING.ACTIVE_STORAGE]**

| Key | Type | Default | Description |
|---|---|---|---|
| `PATH` | string | `{DEFAULT_DATA_DIR}/active` | Base path for active RocksDB stores (ledger, txhash, events). |

**[HISTORY_ARCHIVES]**

| Key | Type | Default | Description |
|---|---|---|---|
| `URLS` | []string | **required** | List of Stellar history archive URLs. Used to sample tip via `/.well-known/stellar-history.json` when Phase 1 uses captive core. Same key the existing ingest service reads. |

**[BACKFILL.BSB]** — optional when the daemon runs

Same schema as in the backfill doc. Presence in the config file determines which source Phase 1 uses:

- If `[BACKFILL.BSB]` is present: Phase 1 uses BSB (fast, parallel catchup from GCS).
- If `[BACKFILL.BSB]` is absent: Phase 1 uses captive core (slower, but no GCS dep).

See [Ledger Source](#ledger-source) for the full source-selection rule.

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
- `[BACKFILL.BSB]` optional — presence determines Phase 1 source. May be added or removed between runs.
- `[HISTORY_ARCHIVES].URLS` required in all profiles.
- `CAPTIVE_CORE_CONFIG` required in all profiles.

### Validation Pseudocode

```python
def validate_config(config, meta_store):
    """
    Runs once at startup before Phase 1. Enforces:
      - Immutable keys (CHUNKS_PER_TXHASH_INDEX, RETENTION_LEDGERS) match meta-store state.
      - RETENTION_LEDGERS is 0 or a positive multiple of LEDGERS_PER_INDEX.
      - Required config keys are present.

    Any failure is fatal — the daemon exits with a clear error. Operator fixes config
    (or wipes the datadir for an immutable-key change) and re-invokes.
    """
    cpi = config.backfill.chunks_per_txhash_index
    retention_ledgers = config.streaming.retention_ledgers
    ledgers_per_index = cpi * LEDGERS_PER_CHUNK

    # 1. Retention shape.
    if retention_ledgers != 0 and (retention_ledgers <= 0 or (retention_ledgers % ledgers_per_index) != 0):
        fatal(f"RETENTION_LEDGERS={retention_ledgers} must be 0 or a positive multiple of "
              f"LEDGERS_PER_INDEX={ledgers_per_index}. Valid values at this cpi: "
              f"0, {ledgers_per_index}, {2*ledgers_per_index}, ...")

    # 2. Required keys.
    if not config.streaming.captive_core_config:
        fatal("STREAMING.CAPTIVE_CORE_CONFIG is required.")
    if not config.history_archives.urls:
        fatal("HISTORY_ARCHIVES.URLS is required (list of at least one archive URL).")

    # 3. Immutable keys. Store on first run; fatal on mismatch thereafter.
    _enforce_immutable(meta_store, "config:chunks_per_txhash_index", str(cpi))
    _enforce_immutable(meta_store, "config:retention_ledgers",       str(retention_ledgers))


def _enforce_immutable(meta_store, key, current_value):
    stored = meta_store.get(key)
    if stored is None:
        meta_store.put(key, current_value)
    elif stored != current_value:
        fatal(f"{key} changed: stored={stored}, config={current_value}. "
              f"Wipe datadir to change.")
```

### Operator Profiles

Three profiles emerge from config combinations. No profile flag.

| Profile | `RETENTION_LEDGERS` | `[BACKFILL.BSB]` | Phase 1 source | Use case |
|---|---|---|---|---|
| Archive | `0` | present | BSB | Public archive node; full history. |
| Pruning-history | `N * LEDGERS_PER_INDEX`, N ≥ 1 | present | BSB | Windowed history with bulk initial catchup. |
| Tip-tracker | `N * LEDGERS_PER_INDEX`, N ≥ 1 | absent | captive core | App developer; small retention; no GCS dep. |

---

## Operator Scenarios

Worked examples showing what operators configure, what happens at runtime, and how crashes recover. Reference scenarios for PRD / test planning.

### Scenario A — Fresh full-history archive, seamless cutover to live

**Setup**: operator Bob wants a public archive node. Full history, retained forever, catchup from BSB, then live streaming.

**Config** (`/etc/stellar-rpc/config.toml`):

```toml
[SERVICE]
DEFAULT_DATA_DIR = "/data/stellar-rpc"

[BACKFILL]
CHUNKS_PER_TXHASH_INDEX = 1000                        # default; 10M ledgers per index

[BACKFILL.BSB]
BUCKET_PATH = "sdf-ledger-close-meta/v1/ledgers/pubnet"

[STREAMING]
RETENTION_LEDGERS   = 0                               # full history
CAPTIVE_CORE_CONFIG = "/etc/stellar-rpc/captive-core.cfg"

[HISTORY_ARCHIVES]
URLS = ["https://history.stellar.org/prd/core-live/core_live_001/"]

[LOGGING]
LEVEL  = "info"
FORMAT = "text"
```

**Invocation**:

```
stellar-rpc --config /etc/stellar-rpc/config.toml
```

**Happy path**:

- Daemon starts. `validate_config` stores `config:chunks_per_txhash_index = "1000"` and `config:retention_ledgers = "0"` on first start.
- Phase 1 picks `BSBSource` (BSB is configured). `run_backfill(0, last_complete_chunk_at_tip, source=BSBSource)`.
- Static DAG over ~5_600 chunks (at tip ~56M). Parallel BSB workers pull ledgers at GOMAXPROCS chunks at a time. Runs ~12h.
- Phase 1 exits when `T - L < 10_000`. All indexes 0..N complete with RecSplit built by the DAG; at most one partial trailing index remains.
- Phase 2 loads any `.bin` files left by the trailing partial index into the active txhash store, deletes `.bin` + `:txhash` flags.
- Phase 3 is a no-op (no orphan stores on a clean first run).
- Phase 4 opens active stores at `resume_ledger = last_phase1_ledger + 1`, starts captive core via `PrepareRange(UnboundedRange(resume_ledger))`, enters live ingestion.
- Queries begin serving at the moment Phase 4 flips `daemon_ready = true`.

**No operator action between Phase 1 and Phase 4.** The cutover is automatic.

**Crash recovery**:

- Crash during Phase 1's BSB download at chunk 3_457: on restart, `phase1_coverage_end_ledger` walks `:lfs` flags, returns the end of the contiguous prefix (say, chunk 3_200). `phase1_catchup` re-enters, `compute_backfill_chunk_range` produces a new range, backfill re-runs from chunk 3_201 forward. Chunks that already had `:lfs` are skipped via per-chunk idempotency.
- Crash after all chunks written but before index 3's RecSplit built: on restart, Phase 1 sees `index:3:txhash` absent → backfill's DAG re-runs the RecSplit build from the `.bin` files. Succeeds.
- Crash while `.bin` files from the trailing index are being loaded into the active txhash store (Phase 2): on restart, Phase 2 re-runs. Chunks that were already loaded had their `:txhash` flag deleted and `.bin` file removed — the loop skips them via the flag check. Chunks not yet loaded retain their `:txhash` flag and `.bin` file — the loop picks them up.
- Crash between per-ledger checkpoint commit and chunk freeze during live ingestion: `streaming:last_committed_ledger = last_ledger_in_chunk(chunk_id)` but `chunk:{chunk_id}:lfs` absent. Phase 3 triggers the missing transitions when the daemon restarts, before Phase 4 re-enters.

In every case: the daemon reaches a consistent state after one restart. No manual intervention. Dangling `.bin` files from incomplete indexes are cleaned by Phase 2 once the owning index progresses further.

### Scenario B — Alice's tip-tracker (no BSB, small retention)

**Setup**: Alice is building a wallet app. She wants live events only, starting from the current network tip. She doesn't want to stand up a GCS bucket for BSB. She picks `cpi=1` and `RETENTION_LEDGERS = 10_000` (one index worth, ~16 hours at 6s/ledger).

**Config**:

```toml
[SERVICE]
DEFAULT_DATA_DIR = "/data/stellar-rpc"

[BACKFILL]
CHUNKS_PER_TXHASH_INDEX = 1                           # minimum; one chunk per index

[STREAMING]
RETENTION_LEDGERS   = 10_000                          # one index = ~16 hours
CAPTIVE_CORE_CONFIG = "/etc/stellar-rpc/captive-core.cfg"

[HISTORY_ARCHIVES]
URLS = ["https://history.stellar.org/prd/core-live/core_live_001/"]

[LOGGING]
LEVEL  = "info"
FORMAT = "text"

# No [BACKFILL.BSB] section — Phase 1 uses captive core.
```

**Invocation**: same as Scenario A.

**Happy path on first-ever start** (say network tip is `56_342_637`):

- Daemon starts. Validates config; stores immutable keys.
- `[BACKFILL.BSB]` absent → Phase 1 source is `CaptiveCoreSource`.
- Source samples tip via HTTP GET against `HISTORY_ARCHIVE_URLS`: tip = `56_342_637`.
- `compute_backfill_chunk_range(last_committed_ledger=1, network_tip_ledger=56_342_637, retention_ledgers=10_000, cpi=1)` — leapfrog lands at `first_ledger_in_tx_index(tx_index_id_of_chunk(chunk_id_of_ledger(network_tip_ledger - retention_ledgers)))`:
  - `network_tip_ledger - retention_ledgers = 56_332_637`.
  - `chunk_id_of_ledger(56_332_637) = 5_633`. `tx_index_id_of_chunk(5_633) = 5_633` (cpi=1).
  - `first_ledger_in_tx_index(5_633) = 56_330_002`.
- Backfill range is chunks `5_633..5_633` (one chunk to close the gap to tip at chunk 5_633, which is `last_complete_chunk_at(56_342_637)`). Up to ~10_000 ledgers of archive-catchup via captive core. Takes ~3–8 minutes.
- Phase 2 loads the one `.bin` file into the active txhash store, deletes it.
- Phase 4 opens active stores, starts captive core for live streaming from `resume_ledger = 56_340_002`, enters ingestion loop.

**Why leapfrog lands ~10_000 ledgers back instead of exactly at tip**: Alice's first chunk must be a complete chunk (starts at `..._02`, ends at `..._01`). If the daemon started ingesting at `tip = 56_342_637` (mid-chunk), chunk 5_634 would be missing ledgers `56_340_002..56_342_636` — the no-gaps invariant would break and RecSplit for chunk 5_634's index could never be built. Leapfrog alignment is what keeps no-gaps intact.

**What if Alice picks `cpi=10` instead of `cpi=1`?**

- `LEDGERS_PER_INDEX = 100_000`. Minimum `RETENTION_LEDGERS = 100_000` (~7 days). Alice's `RETENTION_LEDGERS = 10_000` is invalid — `validate_config` fatals at startup with a clear error message.
- If Alice fixes retention to `100_000`, Phase 1 captive-core archive-catchup spans up to 100_000 ledgers (~30–60 min on first start). Once live, steady state is the same as cpi=1.

**What if Alice wants "just start from tip, don't catch up anything"?**

- Not possible under this design. The no-gaps invariant requires the first chunk to be complete. If tip falls mid-chunk, the daemon must ingest earlier ledgers to round down to an index boundary. Minimum leapfrog catchup at cpi=1 is ≤10_000 ledgers (~minutes via captive core). That's the floor.

**Subsequent restart** (say after 1h downtime):

- Daemon starts. `streaming:last_committed_ledger` is present from the prior run. Phase 1 samples tip; `network_tip_ledger - last_committed_ledger` is ~600 ledgers (10 min at 6s) — less than one chunk → Phase 1 exits immediately.
- Phase 2 finds no `.bin` files (deleted on first start). No-op.
- Phase 3 reconciles any orphan active stores from the crash — typical case is completing an interrupted chunk freeze.
- Phase 4 re-opens active stores, starts captive core, re-enters the ingestion loop. Captive core's own archive-catchup closes the 600-ledger gap in ~seconds, then cadence settles to live closes.

**Crash recovery within Phase 1 (first-ever start)**:

- Captive core subprocess crashes mid-archive-catchup: daemon retries spinning captive core up. No persisted state to roll back — the partial chunk's data was in the active store's WAL; captive core re-archive-catches-up from whatever ledger the WAL wasn't past.
- Daemon process itself crashes: on restart, `phase1_coverage_end_ledger` returns whatever contiguous prefix exists. Phase 1 re-enters. Eventually completes.

**Query behavior during Phase 1**: `HTTP 4xx` for all three query endpoints. `getHealth` reports `catching_up` + the drift.

## Meta Store Keys

Single RocksDB instance, WAL always enabled. Authoritative source for every startup decision.

### Keys Introduced by Streaming

| Key | Value | Written when |
|---|---|---|
| `streaming:last_committed_ledger` | uint32 (big-endian) | First written at top of Phase 4 to `phase1_coverage_end_ledger(meta_store)` (the end of the contiguous `:lfs` prefix — already a ledger sequence); subsequently after every committed live ledger. **Not updated during Phases 1–3.** Phase 1 progress is tracked by `chunk:{chunk_id}:lfs` flags alone. |
| `config:retention_ledgers` | decimal string | First run (stored); enforced on subsequent starts. |

### Keys Shared with Backfill

| Key | Semantics |
|---|---|
| `config:chunks_per_txhash_index` | Set on first run by whichever invocation runs first — here, first daemon start. |
| `chunk:{chunk_id:08d}:lfs` | Set after ledger pack file fsync. |
| `chunk:{chunk_id:08d}:events` | Set after events cold segment fsync. |
| `chunk:{chunk_id:08d}:txhash` | Set by backfill subroutine after `.bin` fsync; deleted during Phase 2 hydration after `.bin` is loaded into RocksDB. Streaming live path does not write this key — streaming writes txhash directly to the active RocksDB txhash store. |
| `index:{tx_index_id:08d}:txhash` | `"1"` after all 16 RecSplit CF `.idx` files built and fsynced. Transitions to `"deleting"` at the start of `prune_tx_index`, deleted entirely when prune completes. Query routing treats `"deleting"` the same as absent. |

### Key Lifecycle in Streaming

```
Phase 1 (backfill subroutine):
  chunk:{chunk_id}:lfs      = "1"   (after pack fsync)
  chunk:{chunk_id}:txhash   = "1"   (after .bin fsync)    # only present for chunks that still have .bin on disk
  chunk:{chunk_id}:events   = "1"   (after cold segment fsync)
  index:{tx_index_id}:txhash   = "1"   (after RecSplit, when all chunks of tx_index_id are done in Phase 1)

Phase 2 (.bin hydration — see Startup Sequence):
  For every chunk with :txhash flag and a .bin file:
    load .bin into RocksDB txhash store
    delete chunk:{chunk_id}:txhash flag
    delete .bin file
  After Phase 2, no chunk:{chunk_id}:txhash flags and no .bin files remain.

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
- On restart, a pre-created store is expected to exist — Phase 3 treats `resume_chunk + 1` (and `resume_index + 1`) as active, not an orphan.

### Max Concurrent Stores

| Store | Max active | Max transitioning | Max total |
|---|---|---|---|
| Ledger | 1 | 1 | 2 |
| Events | 1 (hot segment) | 1 (freezing cold segment) | 2 |
| TxHash | 1 | 1 | 2 |

---

## Ledger Source

Phase 1 reads ledgers from a source. Two implementations share one interface. Source is selected per-startup based on `[BACKFILL.BSB]` presence — no stored immutability gate. Operators may add or remove BSB between runs; retention immutability alone constrains the data envelope.

The interface mirrors the stellar Go SDK's `LedgerBackend` pattern (`PrepareRange` + `GetLedger`) — both implementations below (BSB and captive core) already expose that pattern in the SDK, so random-access reads are native and no sequential-iterator shim is needed.

```python
class LedgerSource:
    """
    Provides random-access LedgerCloseMeta reads for a prepared range. Used by the backfill
    subroutine inside Phase 1. Live streaming (Phase 4) does NOT go through this abstraction —
    it reads directly from CaptiveStellarCore via `ledgerBackend.PrepareRange(UnboundedRange(...))`
    + `ledgerBackend.GetLedger(seq)`.

    Usage pattern: run_backfill calls prepare_range ONCE for the full backfill run, then
    process_chunk tasks concurrently call get_ledger(seq) for any seq inside the prepared
    range. Implementations must be safe under concurrent get_ledger calls (the DAG dispatches
    up to max_parallelism() process_chunk workers).
    """

    def tip(self) -> int:
        """Current network tip ledger. Used to compute Phase 1 target range. Callable without
           a prior prepare_range."""

    def prepare_range(self, start_ledger, end_ledger) -> None:
        """Prime the source for random-access reads in [start_ledger, end_ledger] inclusive.
           Called once per run_backfill invocation (phase1_catchup may invoke run_backfill
           multiple times, each with its own range). Must tolerate re-invocation."""

    def get_ledger(self, ledger_seq) -> LedgerCloseMeta:
        """Return the LCM for ledger_seq. Requires prepare_range to have covered ledger_seq.
           Thread-safe under concurrent calls from process_chunk workers."""

    def max_parallelism(self) -> int:
        """Upper bound on concurrent get_ledger call chains the source can sustain. Backfill
           DAG honors this when dispatching process_chunk workers."""


class BSBSource(LedgerSource):
    """
    Reads from the BSB (Buffered Storage Backend) bucket configured in [BACKFILL.BSB].

    - Tip: queried from BSB's own range-end metadata. Same mechanism backfill uses today.
    - prepare_range: sets the BSB-backed LedgerBackend's range; BSB internal prefetch workers
      (BUFFER_SIZE, NUM_WORKERS) fill buffers ahead of get_ledger reads.
    - get_ledger: random-access via the SDK's GetLedger(seq); reads from the prefetch buffer.
    - max_parallelism: GOMAXPROCS (backfill's current default).
    """


class CaptiveCoreSource(LedgerSource):
    """
    Drives a CaptiveStellarCore subprocess to replay ledgers from the history archive + peers.

    - Tip: fetched via HTTP GET on /.well-known/stellar-history.json against HISTORY_ARCHIVE_URLS.
      Matches the existing ingest service pattern (Service.getNextLedgerSequence → archive.GetRootHAS()).
    - prepare_range: spins up (or re-primes) captive core with BoundedRange(start, end).
    - get_ledger: random-access via the SDK's GetLedger(seq); blocks until that ledger is
      available in the captive-core subprocess's emitted stream.
    - max_parallelism: 1. Captive core is a single heavy subprocess; parallelism would require
      multiple subprocesses, each consuming several GB RAM. Backfill DAG dispatches chunks
      sequentially when source is captive core.
    """
```

### Source Selection Rule

```python
def select_phase1_ledger_source(config):
    """Called once at the top of Phase 1. Re-evaluated per startup."""
    if config.backfill.bsb is not None:
        return BSBSource(config.backfill.bsb)
    return CaptiveCoreSource(config.streaming.captive_core_config,
                             config.history_archives.urls)
```

### Retention Semantics Under Captive Core

When Phase 1 uses captive core, `RETENTION_LEDGERS` directly determines how many ledgers captive core must archive-catchup on first start:

- `RETENTION_LEDGERS = 10_000_000` at `cpi=1_000`: captive core archive-catches-up ~10M ledgers (hours to days).
- `RETENTION_LEDGERS = 10_000` at `cpi=1`: captive core archive-catches-up ~10K ledgers (~3–8 min).

This is the main reason tip-tracker operators default to `cpi=1`: at cpi=1 a full index is 10K ledgers, so retention can be set small without violating the "multiple of LEDGERS_PER_INDEX" rule.

---

## Startup Sequence

Four sequential phases, same code path for first start and every restart. The first three are bounded bootstrap work; Phase 4 is the long-running state the daemon stays in until process exit.

- **Phase 1 — catchup.** Closes the gap between on-disk `:lfs` flags and current network tip by invoking the backfill subroutine in a loop.
- **Phase 2 — hydrate txhash.** Loads any `.bin` files Phase 1 left (for the trailing partial index) into the active txhash store, then deletes them.
- **Phase 3 — reconcile orphans.** Completes any in-flight freeze transitions left by a prior crash. Truncates events hot segment beyond the last committed ledger.
- **Phase 4 — live ingestion.** Opens active stores, starts captive core, spawns the lifecycle goroutine, flips the `daemon_ready` flag, enters the ingestion loop. Runs until process exit.

"Phase" here refers to the startup ordering only. Once Phase 4 is entered, there's no Phase 5 — the daemon is in live-streaming steady state.

```python
def run_streaming_daemon(config):
    meta_store = open_meta_store(config)
    validate_config(config, meta_store)                       # immutable key enforcement

    # ── Phase 1: catch up from last_committed_ledger (or genesis) to tip ──
    source = select_phase1_ledger_source(config)
    phase1_catchup(config, meta_store, source)

    # ── Phase 2: load any .bin files left by Phase 1 into RocksDB; delete them ──
    phase2_hydrate_txhash(config, meta_store)

    # ── Phase 3: reconcile orphaned transitions from prior crash ──
    phase3_reconcile_orphans(config, meta_store)

    # ── Phase 4: open active stores, spawn lifecycle goroutine, start captive core, ingest ──
    phase4_live_ingest(config, meta_store)
```

Query serving is gated on Phase 4 being reached — see [Query Contract](#query-contract).

### Phase 1 — Catchup

Runs the backfill subroutine (`run_backfill` from `01-backfill-workflow.md`) once per source-tip sample, until the gap closes to less than one chunk.

- Phase 1's unit of work is an entire chunk — never a partial chunk. Backfill's DAG dispatches integer chunk IDs; `process_chunk(chunk_id)` ingests ledgers `first_ledger_in_chunk(chunk_id)..last_ledger_in_chunk(chunk_id)` inclusive. Every chunk ever persisted by Phase 1 starts at `..._02` and ends at `..._01`. This is the chunk-alignment invariant the no-gaps guarantee rests on.
- Works the same whether the source is BSB (parallel) or captive core (sequential) — per-chunk work is atomic in both cases.

```python
def phase1_catchup(config, meta_store, source):
    """
    Close the gap between what's already on disk and the current network tip.

    Control flow (outer loop):
      1. derive last_committed_ledger from :lfs flags on disk (NOT from
         streaming:last_committed_ledger — that key isn't written during Phases 1–3).
      2. sample the current network_tip_ledger from the source.
      3. if (network_tip_ledger - last_committed_ledger) is less than one chunk, exit
         (captive core will close the residual few-thousand-ledger gap in Phase 4 via
         its own archive-catchup).
      4. compute the chunk range to backfill this iteration. Leapfrog-alignment inside
         compute_backfill_chunk_range guarantees range_start is the first chunk of an
         index when retention is configured.
      5. invoke backfill's static-DAG subroutine. Backfill's own per-chunk idempotency
         + crash recovery handle mid-iteration crashes.
      6. re-derive last_committed_ledger from :lfs flags. Loop.

    The while loop is needed because the network tip advances while we catch up —
    each run_backfill call covers the range known at the start of that iteration,
    and subsequent iterations close whatever new ledgers accumulated.
    """
    cpi = config.backfill.chunks_per_txhash_index
    retention_ledgers    = config.streaming.retention_ledgers
    last_committed_ledger = phase1_coverage_end_ledger(meta_store)

    while True:
        network_tip_ledger = source.tip()
        if (network_tip_ledger - last_committed_ledger) < LEDGERS_PER_CHUNK:
            break                                             # less than one chunk remaining

        range_start_chunk_id, range_end_chunk_id = compute_backfill_chunk_range(
            last_committed_ledger, network_tip_ledger, retention_ledgers, cpi)
        if range_end_chunk_id < range_start_chunk_id:
            # Leapfrog landed past the last complete chunk at tip — happens when the
            # network hasn't produced a full chunk past the retention line yet. Exit.
            break

        # Backfill's DAG ingests [range_start_chunk_id..range_end_chunk_id] inclusive.
        # Per-chunk idempotent: chunks with :lfs already set are skipped. Crash here
        # resumes on restart.
        run_backfill(config, range_start_chunk_id, range_end_chunk_id, source=source)

        # Re-derive last_committed_ledger — not just range_end_chunk_id — because a
        # mid-iteration crash could leave holes in [range_start..range_end] that the
        # contiguous-prefix scan catches.
        last_committed_ledger = phase1_coverage_end_ledger(meta_store)


def compute_backfill_chunk_range(last_committed_ledger, network_tip_ledger, retention_ledgers, cpi):
    """
    Returns (range_start_chunk_id, range_end_chunk_id). Leapfrog aligns DOWN to the
    first chunk of the tx index containing (network_tip_ledger - retention_ledgers).
    No-op when retention_ledgers = 0 (full history archive).

    - retention_ledgers is a multiple of LEDGERS_PER_INDEX (validated at startup), but
      network_tip_ledger itself is arbitrary — so (network_tip_ledger - retention_ledgers)
      is NOT on a tx-index boundary in general. Leapfrog must explicitly round that value
      down to the first ledger of its containing tx index. That rounded value is the new
      head of coverage; every earlier ledger is past retention and skipped.
    - Worst case: up to LEDGERS_PER_INDEX - 1 ledgers past the strict retention line are
      ingested and held on disk. At cpi=1_000 this is ~10M ledgers; at cpi=1 it is ~10k.
    """
    gap_start_ledger = last_committed_ledger + 1
    if retention_ledgers > 0:
        target_ledger   = max(network_tip_ledger - retention_ledgers, GENESIS_LEDGER)
        target_chunk_id = (target_ledger - GENESIS_LEDGER) // LEDGERS_PER_CHUNK
        target_tx_index_id = target_chunk_id // cpi
        # First ledger of target_tx_index_id = (target_tx_index_id * LEDGERS_PER_INDEX) + GENESIS_LEDGER.
        leapfrog_start_ledger = (target_tx_index_id * cpi * LEDGERS_PER_CHUNK) + GENESIS_LEDGER
    else:
        leapfrog_start_ledger = GENESIS_LEDGER

    range_start_ledger   = max(gap_start_ledger, leapfrog_start_ledger)
    range_start_chunk_id = (range_start_ledger - GENESIS_LEDGER) // LEDGERS_PER_CHUNK
    # range_end_chunk_id: largest chunkId such that last_ledger_in_chunk(chunkId)
    # is <= network_tip_ledger.
    # last_ledger_in_chunk(chunkId) = ((chunkId + 1) * LEDGERS_PER_CHUNK) + (GENESIS_LEDGER - 1)
    #   <= network_tip_ledger  iff  (chunkId + 1) <= (network_tip_ledger - (GENESIS_LEDGER - 1)) / LEDGERS_PER_CHUNK
    #   iff  chunkId <= ((network_tip_ledger - (GENESIS_LEDGER - 1)) // LEDGERS_PER_CHUNK) - 1
    range_end_chunk_id   = ((network_tip_ledger - (GENESIS_LEDGER - 1)) // LEDGERS_PER_CHUNK) - 1
    return range_start_chunk_id, range_end_chunk_id


def phase1_coverage_end_ledger(meta_store):
    """
    Returns the last ledger of the contiguous tail of :lfs flags starting at the lowest
    chunk currently on disk.

    - Finds min_chunk_id = lowest chunkId with chunk:{chunkId}:lfs set.
    - Walks forward from min_chunk_id counting contiguous :lfs flags. Stops at the first gap.
    - Returns GENESIS_LEDGER - 1 if no :lfs flags exist at all.

    Contiguous-tail semantics matter because:
    - BSB workers complete chunks in parallel; a mid-Phase-1 crash can leave holes in the
      middle of the ingested range. Resuming from the highest :lfs would skip those holes
      and break the no-gaps invariant.
    - Lifecycle pruning removes :lfs flags of past-retention tx indexes. The lowest
      remaining :lfs after prune is naturally the head of surviving coverage — no separate
      tip sample or leapfrog calculation needed here.

    Leapfrog decisions (where Phase 1 should start ingesting THIS run) are made separately
    inside compute_backfill_chunk_range, which has access to the current tip sample.
    """
    min_chunk_id = None
    for key in meta_store.iter_prefix("chunk:"):
        if not key.endswith(":lfs"):
            continue
        chunk_id = parse_chunk_id(key)
        if min_chunk_id is None or chunk_id < min_chunk_id:
            min_chunk_id = chunk_id
    if min_chunk_id is None:
        return GENESIS_LEDGER - 1

    chunk_id = min_chunk_id
    while meta_store.has(f"chunk:{chunk_id:08d}:lfs"):
        chunk_id += 1
    return last_ledger_in_chunk(chunk_id - 1)                    # last contiguous chunk
```

**Worker concurrency**: `run_backfill` honors `source.max_parallelism()` when dispatching `process_chunk` tasks. With BSB this is GOMAXPROCS (unchanged from backfill today). With captive core it is 1 — the DAG dispatches chunks sequentially to avoid spawning multiple captive core subprocesses.

**Retention semantics** depend on source:
- With BSB: retention determines the Phase 1 range; catchup time scales with `RETENTION_LEDGERS / (BSB throughput)`.
- With captive core: retention determines the Phase 1 range AND captive core's archive-catchup scope. Operators must size retention against the wall-clock cost of captive-core archive catchup.

### Phase 2 — Hydrate TxHash Data from `.bin`

Phase 1 may leave `.bin` files for chunks in the last (incomplete) index. Phase 2 loads each into the active txhash store and deletes the `.bin` file + its `chunk:{chunk_id}:txhash` flag. After Phase 2, no `.bin` files and no `chunk:{chunk_id}:txhash` flags remain.

```python
def phase2_hydrate_txhash(config, meta_store):
    """
    Loads every remaining .bin into the active txhash store, then deletes the .bin and flag.

    - Runs on every startup for robustness. On a restart where a previous Phase 2 completed,
      no :txhash flags remain and this is a no-op.
    - After each chunk is loaded: delete the flag FIRST, then delete the .bin. A crash
      between these two steps leaves an orphan .bin that the sweep in step 3 handles.
    - The txhash store must be opened (not re-created) — prior Phase 2 runs may have loaded
      earlier chunks, and their .bin files are already gone.
    """
    cpi = config.backfill.chunks_per_txhash_index

    # 1. Backfill may have completed a tx index (index:{tx_index_id}:txhash = "1") before
    #    a crash prevented cleanup_txhash from deleting leftover .bin. Sweep those first.
    for tx_index_id in tx_index_ids_with_txhash_flag(meta_store):
        for chunk_id in range(tx_index_id * cpi, (tx_index_id + 1) * cpi):
            if meta_store.has(f"chunk:{chunk_id:08d}:txhash"):
                meta_store.delete(f"chunk:{chunk_id:08d}:txhash")
                delete_if_exists(raw_txhash_path(chunk_id))

    # 2. Load .bin files for the current incomplete tx index (if any) into the active
    #    txhash RocksDB.
    incomplete_tx_index_id = current_incomplete_tx_index_id(meta_store)
    if incomplete_tx_index_id is None:
        return

    txhash_store = open_active_txhash_store(config, incomplete_tx_index_id)   # WAL recovery; do NOT recreate
    try:
        for chunk_id in range(incomplete_tx_index_id * cpi, (incomplete_tx_index_id + 1) * cpi):
            if not meta_store.has(f"chunk:{chunk_id:08d}:txhash"):
                continue                                                       # already loaded (flag cleared)
            bin_path = raw_txhash_path(chunk_id)
            if os.path.exists(bin_path):
                load_bin_into_rocksdb(bin_path, txhash_store)                  # idempotent writes
            meta_store.delete(f"chunk:{chunk_id:08d}:txhash")                  # delete flag first
            delete_if_exists(bin_path)                                         # delete .bin second

        # 3. Sweep orphan .bin files (flag already gone, .bin lingering from crash between
        #    flag-delete and file-delete in a prior run).
        for bin_file in scan_bin_files_for_tx_index(incomplete_tx_index_id):
            if not meta_store.has(f"chunk:{parse_chunk_id(bin_file):08d}:txhash"):
                os.remove(bin_file)
    finally:
        # Must close before returning — Phase 4's open_active_stores_for_resume re-opens the same
        # directory, and RocksDB's directory flock would collide if this handle is still
        # open. WAL remains on disk; reopening is safe.
        txhash_store.close()
```

**Why "load then delete" matters.** Without immediate deletion, every restart during the incomplete-index lifetime would re-load the same `.bin` files into RocksDB. At `cpi=1_000` with frequent restarts over a day, that is thousands of redundant loads. Deleting the `.bin` after the first successful load makes Phase 2 a no-op on every subsequent restart until the next Phase 1 deposits new `.bin` files.

**Pure-streaming restarts** (no recent Phase 1 output) never see `.bin` files — streaming's live path writes txhash directly to the active RocksDB txhash store. Phase 2 is a trivial no-op in that case.

### Phase 3 — Reconcile Orphaned Transitions

Completes any in-flight transitions left by a prior crash. All decisions derive from meta store state + on-disk store directories.

```python
def phase3_reconcile_orphans(config, meta_store):
    """
    Finishes any mid-flight LFS flush, events freeze, or RecSplit build from a crashed run.

    - Active store for resume_chunk_id: keep (Phase 4 will open it).
    - Pre-created store for resume_chunk_id + 1: keep.
    - Orphaned ledger store:
        flag present → cleanup lingered; delete the store.
        flag absent, chunk below resume_chunk_id → mid-flush crash; complete the flush.
        flag absent, chunk above resume_chunk_id + 1 → orphan future store; delete.
    - Orphaned txhash store:
        flag present → cleanup lingered; delete the store.
        flag absent, all chunks of tx index tx_index_id have :lfs set → spawn RecSplit build.

    On a fresh datadir (no :lfs flags anywhere, Phase 1 had nothing to do) this is a no-op:
    resume_ledger = GENESIS_LEDGER, resume_chunk_id = 0, no active stores on disk yet.
    """
    # Derive resume_ledger the SAME way Phase 4 will — otherwise Phase 3 and Phase 4 can
    # disagree on which chunk's active store to preserve, causing Phase 4 to open a fresh
    # store while Phase 3's kept-active-store is left as an orphan.
    #
    # Priority order (matches phase4_live_ingest):
    #   1. streaming:last_committed_ledger if set (live-path crash mid-chunk or at boundary).
    #   2. phase1_coverage_end_ledger otherwise (first-start after Phase 1, or fresh datadir).
    cpi = config.backfill.chunks_per_txhash_index
    last_committed_ledger = meta_store.get("streaming:last_committed_ledger")
    if last_committed_ledger is None:
        last_committed_ledger = phase1_coverage_end_ledger(meta_store)
    resume_ledger = last_committed_ledger + 1
    if resume_ledger < GENESIS_LEDGER:
        resume_ledger = GENESIS_LEDGER
    resume_chunk_id = (resume_ledger - GENESIS_LEDGER) // LEDGERS_PER_CHUNK

    # Ledger stores
    for store_dir in scan_ledger_store_dirs(config):
        chunk_id = parse_chunk_id_from_dir(store_dir)
        if chunk_id == resume_chunk_id or chunk_id == resume_chunk_id + 1:
            continue                                              # active or pre-created
        if meta_store.has(f"chunk:{chunk_id:08d}:lfs"):
            delete_dir(store_dir)                                 # orphaned post-flush cleanup
        elif chunk_id < resume_chunk_id:
            finish_interrupted_ledger_freeze(store_dir, chunk_id, meta_store)   # mid-flush crash; finish
        else:
            delete_dir(store_dir)                                 # orphan future store

    # Txhash stores
    resume_tx_index_id = resume_chunk_id // cpi
    for store_dir in scan_txhash_store_dirs(config):
        tx_index_id = parse_tx_index_id_from_dir(store_dir)
        if tx_index_id == resume_tx_index_id or tx_index_id == resume_tx_index_id + 1:
            continue                                              # active or pre-created
        if meta_store.has(f"index:{tx_index_id:08d}:txhash"):
            delete_dir(store_dir)                                 # RecSplit done, cleanup lingered
        elif all_chunks_in_tx_index_have_lfs_flag(meta_store, tx_index_id, cpi):
            # RecSplit build for tx_index_id was never started or was interrupted. Open
            # the store and spawn the build — pass the handle, not the directory path,
            # because build_tx_index_recsplit_files reads from the store and closes it
            # on completion.
            transitioning_txhash = open_active_txhash_store(config, tx_index_id)
            run_in_background(build_tx_index_recsplit_files, tx_index_id, transitioning_txhash, meta_store)

    # Events hot segment: truncate any persisted deltas beyond resume_ledger - 1.
    # Prevents duplicate event IDs when Phase 4 replays the first live ledger.
    truncate_events_hot_segment(config, resume_ledger - 1)
```

### Phase 4 — Live Ingestion

Opens active stores for the resume position, spawns the lifecycle goroutine, starts captive core, and enters the ingestion loop. Query serving starts here (see [Query Contract](#query-contract)).

```python
def phase4_live_ingest(config, meta_store):
    last_committed_ledger = meta_store.get("streaming:last_committed_ledger")
    if last_committed_ledger is None:
        # First start after Phase 1: set checkpoint to end of Phase 1's coverage.
        last_committed_ledger = phase1_coverage_end_ledger(meta_store)
        meta_store.put("streaming:last_committed_ledger", last_committed_ledger)
    resume_ledger = last_committed_ledger + 1

    active_stores = open_active_stores_for_resume(config, meta_store, resume_ledger)

    run_in_background(run_prune_lifecycle_loop, config, meta_store)

    # Prime captive core for unbounded stream from resume_ledger.
    ledger_backend = make_ledger_backend(config.streaming.captive_core_config)
    ledger_backend.PrepareRange(UnboundedRange(resume_ledger))

    set_daemon_ready()                                            # in-memory flag; unblocks queries

    run_live_ingestion_loop(config, ledger_backend, active_stores, meta_store, resume_ledger)


def open_active_stores_for_resume(config, meta_store, resume_ledger):
    """
    Open or create the three active stores for resume_ledger's chunk + tx index. Also
    pre-create the next chunk's / next tx index's stores up front so the first chunk
    rollover doesn't pay creation latency.

    - Ledger active: per-chunk RocksDB for chunk_id_of_ledger(resume_ledger). WAL-recovered
      if the directory exists (mid-chunk restart); fresh-created otherwise.
    - Events hot segment: in-memory for chunk_id_of_ledger(resume_ledger). If persisted deltas
      exist for this chunk (mid-chunk restart), replay them to rebuild bitmaps.
      Phase 3 already truncated anything past last_committed_ledger, so replay is safe.
    - TxHash active: per-index RocksDB for tx_index_id_of_chunk(chunk_id_of_ledger(resume_ledger)). May
      already contain data from Phase 2's .bin hydration (which closed the handle
      before returning — see Phase 2 pseudocode). WAL-recovered on reopen.
    - Pre-created: also open/create (chunk_id + 1) and (tx_index_id + 1) stores so the
      first boundary rollover is a pointer swap only.
    """
    resume_chunk_id    = (resume_ledger - GENESIS_LEDGER) // LEDGERS_PER_CHUNK
    resume_tx_index_id = resume_chunk_id // config.backfill.chunks_per_txhash_index

    return ActiveStores(
        ledger         = open_or_create_ledger_store(config, resume_chunk_id),
        ledger_next    = open_or_create_ledger_store(config, resume_chunk_id + 1),
        events         = open_or_create_events_hot_segment(config, meta_store, resume_chunk_id, resume_ledger),
        events_next    = open_or_create_events_hot_segment(config, meta_store, resume_chunk_id + 1, None),
        txhash         = open_or_create_txhash_store(config, resume_tx_index_id),
        txhash_next    = open_or_create_txhash_store(config, resume_tx_index_id + 1),
    )
```

Captive core takes 4–5 minutes to spin up and start emitting at `resume_ledger`. During that window `getHealth` remains in `catching_up` state (see [Query Contract](#query-contract)).

---

## Ingestion Loop

Single goroutine. Pull-based: the daemon drives sequential `GetLedger(seq)` calls. Same code path drains captive core's internal buffer during catchup and switches cadence to live closes (~5 s per ledger) once caught up.

```python
def run_live_ingestion_loop(config, ledger_backend, active_stores, meta_store, resume_ledger):
    """
    Sequential pull-based live ingestion. The daemon stays here until process exit.

    Per-ledger steps:
      1. Block on GetLedger(seq) until the ledger is available.
      2. Fan out writes to all three active stores in parallel. Each write is atomic
         + WAL-backed, so each store alone is crash-safe.
      3. wait_all — all three must succeed before the per-ledger checkpoint advances.
      4. Commit streaming:last_committed_ledger = seq. This is the atomic 'the daemon
         owns everything up to and including seq' signal.
      5. If seq completes a chunk, fire on_chunk_boundary (non-blocking — freeze
         transitions run in background).
      6. If seq completes an index, fire on_tx_index_boundary — RecSplit build kicks off.
      7. seq += 1. Loop.

    Immutable config values (cpi) are read once outside the loop — never per ledger.
    """
    cpi = config.backfill.chunks_per_txhash_index                 # immutable; read once at loop entry
    ledger_seq = resume_ledger
    while True:
        lcm = ledger_backend.GetLedger(ledger_seq)                # blocks until this ledger is available

        # Write to all three active stores in parallel. Order: fan out, wait for all.
        # Each store is idempotent on re-write of the same ledger (crash-safe).
        wait_all(
            run_in_background(write_ledger_store,       active_stores.ledger, ledger_seq, lcm),
            run_in_background(write_txhash_store,       active_stores.txhash, ledger_seq, lcm),
            run_in_background(write_events_hot_segment, active_stores.events, ledger_seq, lcm),
        )

        # Commit the per-ledger checkpoint (streaming:last_committed_ledger) only AFTER
        # all three active stores have durably committed the ledger. This is the key
        # atomic boundary for Phase 4 crash recovery — the checkpoint is the sole
        # 'the daemon owns everything up to and including this ledger' signal. It's NOT
        # the same as Phase 1's coverage-end-ledger (which derives from :lfs flags).
        meta_store.put("streaming:last_committed_ledger", ledger_seq)

        # Chunk rollover: hand off to background LFS + events freeze transitions.
        chunk_id = (ledger_seq - GENESIS_LEDGER) // LEDGERS_PER_CHUNK
        if ledger_seq == last_ledger_in_chunk(chunk_id):
            on_chunk_boundary(chunk_id, active_stores, meta_store)

        # Tx-index rollover — every tx-index boundary is also a chunk boundary, so this
        # runs AFTER on_chunk_boundary has already dispatched the last chunk's freeze
        # transitions.
        tx_index_id = chunk_id // cpi
        if ledger_seq == last_ledger_in_tx_index(tx_index_id):
            on_tx_index_boundary(tx_index_id, active_stores, meta_store)

        ledger_seq += 1
```

Each per-store write is atomic: RocksDB WriteBatch + WAL for ledger and txhash stores; atomic commit of events hot-segment + persisted deltas. Key/value schemas are in [Active Store Architecture](#active-store-architecture).

---

## Freeze Transitions

Three independent background transitions per chunk/index boundary. Each has its own goroutine, flag, and cleanup. Live ingestion never waits on them synchronously — they must not stall the ingestion loop.

- **LFS transition** — per chunk. Converts the retired ledger RocksDB to a `.pack` file.
- **Events transition** — per chunk. Converts the retired events hot segment to a cold segment (three files).
- **RecSplit transition** — per index. Builds 16 `.idx` files from the retired txhash RocksDB.

Streaming's freeze transitions never produce `.bin` files. `.bin` files exist only as transient output of the backfill subroutine (inside Phase 1).

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
    """
    Swap active stores and kick off LFS + events freeze transitions for this chunk_id.

    Ingestion for (chunk_id + 1) continues unimpeded — active_stores.ledger now points
    at the ledger_next store that was pre-created at Phase 4 entry (or by the prior
    chunk's boundary handler).

    Also pre-creates (chunk_id + 2)'s stores in background, so the NEXT chunk rollover
    finds its pre-created store already opened.
    """

    # LFS transition — drain the last in-flight LFS freeze (max-1-transitioning invariant),
    # then swap pointers so the next chunk writes to the pre-created store.
    wait_for_lfs_complete()
    transitioning_ledger_store = active_stores.ledger
    active_stores.ledger = active_stores.ledger_next                  # pointer swap, no I/O
    run_in_background(freeze_ledger_chunk_to_pack_file, chunk_id, transitioning_ledger_store, meta_store)

    # Events transition — same shape. Independent goroutine; does NOT wait for LFS.
    wait_for_events_complete()
    freezing_events_segment = active_stores.events
    active_stores.events = active_stores.events_next                  # pointer swap
    run_in_background(freeze_events_chunk_to_cold_segment, chunk_id, freezing_events_segment, meta_store)

    # Pre-create (chunk_id + 2)'s ledger + events so the NEXT boundary is also a pointer
    # swap. Low priority; not part of the hot path. Runs in background.
    run_in_background(precreate_next_boundary_stores, active_stores, meta_store, chunk_id + 2)

    # Wake the lifecycle goroutine — it will check prune eligibility. Freeze transitions
    # above are NOT dispatched via the lifecycle loop; they run as direct children of the
    # ingestion-loop thread. The notification is specifically for pruning.
    notify_lifecycle()


def precreate_next_boundary_stores(active_stores, meta_store, target_chunk_id):
    """
    Opens / creates the "next-next" ledger store + events hot segment in background so
    the NEXT chunk rollover doesn't pay creation latency on the hot path.

    Similarly handles tx-index-next pre-creation when target_chunk_id crosses a tx-index
    boundary. Idempotent — safe to run on a restart where the target stores already exist.
    """
    active_stores.ledger_next = open_or_create_ledger_store(config, target_chunk_id)
    active_stores.events_next = open_or_create_events_hot_segment(config, meta_store, target_chunk_id, None)
    cpi = config.backfill.chunks_per_txhash_index
    target_tx_index_id = target_chunk_id // cpi
    if target_tx_index_id != tx_index_id_of_chunk(target_chunk_id - 1):
        active_stores.txhash_next = open_or_create_txhash_store(config, target_tx_index_id)
```

### LFS Transition

Converts the retired ledger RocksDB store to an immutable `.pack` file, then discards the store.

```python
def freeze_ledger_chunk_to_pack_file(chunk_id, transitioning_ledger_store, meta_store):
    """
    Read all LEDGERS_PER_CHUNK ledgers for chunk_id from its active store, write the
    pack file, fsync, flag, then delete the store.

    Order matters:
      1. Open pack file with overwrite=True so a prior crashed attempt's bytes are discarded.
      2. Write all ledgers in order.
      3. fsync_and_close — the pack file is durable on disk after this.
      4. Set :lfs flag — the 'flag-after-fsync' invariant. Queries can now route here.
      5. Close and delete the active store. Crash between (4) and (5) leaves an orphan
         directory; Phase 3's scan_ledger_store_dirs + :lfs-present check deletes it.
    """
    pack_path = ledger_pack_path(chunk_id)
    writer = packfile.create(pack_path, overwrite=True)                   # 1
    for ledger_seq in range(first_ledger_in_chunk(chunk_id), last_ledger_in_chunk(chunk_id) + 1):
        writer.append(transitioning_ledger_store.get(uint32_big_endian(ledger_seq)))   # 2
    writer.fsync_and_close()                                              # 3
    meta_store.put(f"chunk:{chunk_id:08d}:lfs", "1")                      # 4

    transitioning_ledger_store.close()                                    # 5
    delete_dir(ledger_store_path(chunk_id))
    signal_lfs_complete()


def finish_interrupted_ledger_freeze(store_dir, chunk_id, meta_store):
    """
    Phase 3 helper. Re-runs freeze_ledger_chunk_to_pack_file for a chunk whose active
    ledger store exists on disk but whose :lfs flag is absent — i.e., a crash
    interrupted the freeze after the per-ledger checkpoint but before the flag was set.

    Identical to freeze_ledger_chunk_to_pack_file except:
      - No signal_lfs_complete call (not running under the max-1-transitioning gate;
        Phase 3 is synchronous with startup and runs to completion before Phase 4 starts).
      - Opens the existing store (WAL-recovered) rather than receiving a handle.
    """
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
    """
    Freeze the events hot segment for chunk_id. Same flag-after-fsync + cleanup order
    as freeze_ledger_chunk_to_pack_file.
    """
    events_path = events_segment_path(chunk_id)
    write_cold_segment(freezing_events_segment, events_path)              # 3 files: events.pack, index.pack, index.hash
    fsync_all(events_path)
    meta_store.put(f"chunk:{chunk_id:08d}:events", "1")                   # flag-after-fsync

    freezing_events_segment.discard()                                     # drops in-memory bitmaps + persisted deltas
    signal_events_complete()
```

### Tx-Index Boundary (every `LEDGERS_PER_INDEX` ledgers)

The last chunk of a tx index has just rolled over. Before RecSplit can start, every chunk in the tx index must have its `:lfs` and `:events` flags set.

```python
def on_tx_index_boundary(tx_index_id, active_stores, meta_store):
    """
    Dispatch RecSplit build for this tx_index_id. Prerequisites:
      - Every chunk in tx_index_id has finished its LFS + events freeze transitions.
      - No LFS or events transition is in flight for any chunk of tx_index_id (would
        race the RecSplit input).
    """

    # Drain ALL in-flight LFS + events transitions. on_chunk_boundary dispatches them;
    # here we wait for them to finish — the final chunk of tx_index_id may still be
    # in-flight.
    wait_for_lfs_complete()
    wait_for_events_complete()
    verify_all_chunk_flags(tx_index_id, meta_store)                       # defense-in-depth

    # Swap the txhash active store. RecSplit reads from the retired store.
    transitioning_txhash_store = active_stores.txhash
    active_stores.txhash       = active_stores.txhash_next
    run_in_background(build_tx_index_recsplit_files, tx_index_id, transitioning_txhash_store, meta_store)
```

### RecSplit Transition

Builds the 16 RecSplit `.idx` files for tx_index_id from the retired txhash active store.

```python
def build_tx_index_recsplit_files(tx_index_id, transitioning_txhash_store, meta_store):
    """
    Same flag-after-fsync pattern as LFS / events freeze:
      1. Delete any partial .idx files from a prior crashed attempt.
      2. Build the 16 RecSplit indexes (one per CF).
      3. fsync all .idx files.
      4. Verify spot-check against the txhash store.
      5. Flag.
      6. Close + delete the txhash active store.
    """
    idx_path = recsplit_index_path(tx_index_id)
    delete_partial_idx_files(idx_path)                                    # 1
    build_recsplit(transitioning_txhash_store, idx_path)                  # 2 (16 .idx files)
    fsync_all_idx_files(idx_path)                                         # 3
    verify_spot_check(tx_index_id, idx_path, meta_store)                  # 4
    meta_store.put(f"index:{tx_index_id:08d}:txhash", "1")                # 5

    transitioning_txhash_store.close()                                    # 6
    delete_dir(txhash_store_path(tx_index_id))
```

---

## Pruning

Retention is enforced by a single background goroutine, woken at chunk boundaries. Prune granularity is the whole txhash index — never per chunk.

```python
def run_prune_lifecycle_loop(config, meta_store):
    """
    Runs as a single background goroutine. Prune gate is uniform across all artifact
    kinds — LFS, events, RecSplit — for a given index.

    Wake-up sources:
      - Initial scan at entry — catches any index left in "deleting" state by a prior
        crashed prune before the first chunk-boundary notification of this run arrives.
        Without this, a crashed prune could sit unserviced for up to 10_000 ledgers
        (~16 hours at cpi=1).
      - Chunk-boundary notifications from the ingestion loop (see on_chunk_boundary).

    The freeze transitions (freeze_ledger_chunk_to_pack_file, freeze_events_chunk_to_cold_segment, build_tx_index_recsplit_files) are
    NOT spawned by this loop — the ingestion loop's on_chunk_boundary / on_tx_index_boundary
    dispatch them directly. run_prune_lifecycle_loop is scoped to pruning.
    """
    cpi = config.backfill.chunks_per_txhash_index
    retention_ledgers = config.streaming.retention_ledgers

    _run_prune_sweep(meta_store, retention_ledgers, cpi, config)      # initial scan
    while True:
        wait_for_chunk_boundary_notification()
        _run_prune_sweep(meta_store, retention_ledgers, cpi, config)


def _run_prune_sweep(meta_store, retention_ledgers, cpi, config):
    for tx_index_id in prunable_tx_index_ids(meta_store, retention_ledgers, cpi):
        prune_tx_index(tx_index_id, meta_store, config)


def prunable_tx_index_ids(meta_store, retention_ledgers, cpi):
    """
    Returns tx_index_ids whose entire footprint is past the retention window and are
    still prune-eligible (either :txhash == "1" meaning prune hasn't started, or
    "deleting" meaning a prior run crashed mid-prune).

    - retention_ledgers = 0 → no pruning; archive profile retains everything.
    - retention_ledgers > 0 → tx_index_id is eligible when
        last_committed_ledger > last_ledger_in_tx_index(tx_index_id) + retention_ledgers.
    - 'tip ledger' used in the check is streaming:last_committed_ledger (the daemon's
      own progress), not the source-reported network tip.

    Upper bound derivation:
      last_ledger_in_tx_index(tx_index_id) = ((tx_index_id + 1) * LEDGERS_PER_INDEX) + (GENESIS_LEDGER - 1)
      Eligible iff  last_committed_ledger > ((tx_index_id + 1) * LEDGERS_PER_INDEX) + (GENESIS_LEDGER - 1) + retention_ledgers
               iff  last_committed_ledger - (GENESIS_LEDGER - 1) - retention_ledgers > (tx_index_id + 1) * LEDGERS_PER_INDEX
               iff  (tx_index_id + 1) < (last_committed_ledger - (GENESIS_LEDGER - 1) - retention_ledgers) / LEDGERS_PER_INDEX
               iff  tx_index_id <= ((last_committed_ledger - (GENESIS_LEDGER - 1) - retention_ledgers - 1) // LEDGERS_PER_INDEX) - 1
      Simplify: last_committed_ledger - (GENESIS_LEDGER - 1) - 1 = last_committed_ledger - GENESIS_LEDGER.
               max_eligible_tx_index_id = ((last_committed_ledger - GENESIS_LEDGER - retention_ledgers) // LEDGERS_PER_INDEX) - 1

    Numeric check at last_committed_ledger=70_000_002, retention_ledgers=10_000_000,
    cpi=1_000 (LEDGERS_PER_INDEX=10_000_000):
      max_eligible_tx_index_id = (70_000_002 - 2 - 10_000_000) // 10_000_000 - 1 = 6 - 1 = 5.
      tx_index_id=5 has last_ledger_in_tx_index(5) + retention_ledgers = 60_000_001 + 10_000_000 = 70_000_001.
        70_000_002 > 70_000_001 → tx_index_id=5 eligible. ✓
      tx_index_id=6 has last_ledger_in_tx_index(6) + retention_ledgers = 70_000_001 + 10_000_000 = 80_000_001.
        70_000_002 > 80_000_001 is false → tx_index_id=6 NOT eligible. ✓
    """
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
    """
    Deletes every artifact for tx_index_id and clears its meta store keys. Two-phase
    marker for query-routing safety:

    - Set :txhash = "deleting" FIRST. Queries short-circuit (treat as absent).
    - Delete files + chunk keys.
    - Delete :txhash key LAST.

    Crash between set-deleting and delete-key leaves :txhash == "deleting"; next
    startup re-runs prune_tx_index, which is idempotent (rm -f + delete_if_exists
    semantics).
    """
    cpi = config.backfill.chunks_per_txhash_index

    # Stage 1: commit to pruning. Once this lands, queries for any ledger in
    # tx_index_id return HTTP 4xx (past retention).
    meta_store.put(f"index:{tx_index_id:08d}:txhash", "deleting")

    # Stage 2: delete files and per-chunk keys. Idempotent on re-run.
    for chunk_id in range(tx_index_id * cpi, (tx_index_id + 1) * cpi):
        delete_if_exists(ledger_pack_path(chunk_id))
        delete_events_segment(chunk_id)
        meta_store.delete(f"chunk:{chunk_id:08d}:lfs")
        meta_store.delete(f"chunk:{chunk_id:08d}:events")
    delete_recsplit_idx_files(tx_index_id)

    # Stage 3: clear the tx-index key. Tx index is now fully gone.
    meta_store.delete(f"index:{tx_index_id:08d}:txhash")
```

**Why index-atomic.** Per-chunk pruning would create a window where `getTransaction` resolves to a ledger sequence whose pack file has already been deleted. Gating every artifact kind on whole-index past-retention closes that window completely.

**How much extra data sits on disk.** At most `LEDGERS_PER_INDEX - 1` ledgers past the strict retention line. Because `RETENTION_LEDGERS` is a multiple of `LEDGERS_PER_INDEX`, the strict retention line itself does not bisect an index — the next-eligible index is exactly `LEDGERS_PER_INDEX` further.

---

## Query Contract

Query serving is gated on Phase 4 being reached. `getLedger`, `getTransaction`, `getEvents` all return **HTTP 4xx** during Phases 1–3.

### Readiness Signal

- An in-memory boolean `daemon_ready` is set by `set_daemon_ready()` at the top of Phase 4, after Phases 1–3 complete and active stores are opened.
- Not persisted. On every startup the flag starts `false`; on every Phase 4 entry it flips to `true`. Clean shutdown discards it implicitly (process exits).
- This means: clients see `HTTP 4xx` from `getLedger`/`getTransaction`/`getEvents` on every startup until Phase 4 is reached, regardless of whether prior runs have served queries. Intentional: catchup and recovery phases must complete before the daemon serves, every time.
- Query handlers check the flag on each request. `false` → HTTP 4xx. `true` → route normally.

### Behavior During Phases 1–3

- `/getLedger`, `/getTransaction`, `/getEvents` → `HTTP 4xx` with no payload detail.
- `/getHealth` → always served; returns `catching_up` + drift when daemon is pre-Phase-4, otherwise `streaming` + drift.
- No partial / incremental serving. The daemon does not serve "whatever is ingested so far" while Phases 1–3 are running.

### Behavior When an Index Is Being Pruned

- `prune_tx_index` sets `index:{tx_index_id:08d}:txhash = "deleting"` before touching any files, and deletes the key after all files are gone. Query routing treats `"deleting"` identically to `"absent"` (key-not-present).
- Queries for a ledger in a pruning index return HTTP 4xx (past retention) starting the instant the `"deleting"` marker is set, not when the files actually disappear. No window where queries route into a half-deleted index.

### Rationale

Without an explicit gate, implementations drift toward "best-effort serve whatever is ingested." That produces inconsistent results across operators and breaks client assumptions. An explicit `daemon_ready` flag + HTTP 4xx error gives clients an unambiguous signal, and the `catching_up` health status gives operators visibility into progress.

---

## Crash Recovery

No separate recovery phase. Every startup runs Phases 1–4 regardless — already-complete work is detected and skipped via meta store flags.

### Invariants

In addition to the backfill subroutine's invariants in [01-backfill-workflow.md — Crash Recovery](./01-backfill-workflow.md#crash-recovery), streaming adds the following:

1. **Per-ledger checkpoint.** `streaming:last_committed_ledger` is written only after all three active stores durably commit. Resume is `last_committed_ledger + 1`.
2. **No separate recovery phase.** Startup is Phases 1–4. Nothing else.
3. **Max-1-transitioning per freeze.** A freeze transition must complete before the next one starts, per kind (LFS, events, RecSplit). Applies in steady state and crash recovery.
4. **Retention immutable.** `config:retention_ledgers` is stored on first run and compared thereafter. No mid-run retention change. Past-retention orphans can only arise from leapfrog — and leapfrog is deterministic, so Phase 1 itself avoids producing them.
5. **Two-phase prune marker.** `prune_tx_index` writes `index:{tx_index_id}:txhash = "deleting"` before any file delete and clears the key after. Queries treat `"deleting"` as absent. Crash mid-prune resumes idempotently on restart because `"deleting"` is still picked up by `prunable_tx_index_ids`.

### Compound Recovery Scenarios

The backfill doc's crash recovery model (Section: Crash Recovery in `01-backfill-workflow.md`) handles every Phase 1 crash. Streaming extends it with per-ledger and per-transition recovery:

- **Crash during Phase 2 `.bin` hydration.** On restart, Phase 2 re-runs. Chunks whose `.bin` was loaded and deleted on the first pass have no `:txhash` flag and no `.bin` file — the loop skips them via the flag check. Chunks not yet loaded still have their `:txhash` flag and `.bin` file — picked up by the same loop.
- **Crash between live per-ledger checkpoint and LFS freeze completion.** `streaming:last_committed_ledger = last_ledger_in_chunk(chunk_id)` but `chunk:{chunk_id}:lfs` is absent (freeze transition was killed before setting the flag). On restart, Phase 1 sees `:lfs` missing for chunk_id and re-runs `process_chunk(chunk_id)` against its configured source — idempotent per-artifact. Phase 3 then finds the active ledger store for chunk_id still on disk, sees `:lfs` now set, and deletes the orphaned store. Known inefficiency: ~10_000 ledgers of redundant ingestion work per affected chunk. Correctness is preserved.
- **Crash mid-RecSplit.** `index:{tx_index_id}:txhash` absent. Phase 3 detects all chunks for tx_index_id have `:lfs` set, re-spawns the RecSplit build. Partial `.idx` files are deleted first.
- **Crash mid-prune.** Some files deleted, some chunk keys cleared, `index:{tx_index_id}:txhash = "deleting"` still present. On restart tx_index_id is still in `prunable_tx_index_ids` (the function picks up `"deleting"` as well as `"1"`), so `prune_tx_index(tx_index_id)` runs again — idempotent because file deletes are `rm -f` and key deletes are `delete_if_exists`.

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

| Error | Action |
|---|---|
| CaptiveStellarCore unavailable | RETRY with backoff; ABORT after `CAPTIVE_CORE_RETRY_MAX` retries (implementation-defined) |
| Ledger / txhash / events write failure | ABORT — disk full or storage corruption |
| Meta store write failure | ABORT — cannot maintain checkpoint |
| LFS flush failure | Do NOT set `chunk:{chunk_id}:lfs`; ABORT transition; restart retries |
| Events freeze failure | Do NOT set `chunk:{chunk_id}:events`; ABORT transition; restart retries |
| RecSplit build failure | Do NOT set `index:{tx_index_id}:txhash`; ABORT transition; restart deletes partials and rebuilds |
| RecSplit verification mismatch | ABORT; do NOT delete transitioning txhash store; operator investigates |
| Startup: immutable key changed | FATAL — wipe datadir to change |
| Startup: `RETENTION_LEDGERS` not a multiple of `LEDGERS_PER_INDEX` | FATAL — fix config |
| Startup: head not index-aligned | FATAL — datadir corruption; wipe |
| Startup: gap in chunk flags | FATAL — datadir corruption; wipe |

---

## Related Documents

- [01-backfill-workflow.md](./01-backfill-workflow.md) — backfill subroutine: DAG, `process_chunk`, partial index handling
- [getEvents full-history design](../../design-docs/getevents-full-history-design.md) — events hot/cold segments, bitmap indexes, freeze process
- Query routing — separate design document (TBD)
