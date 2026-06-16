# Streaming Workflow

## Overview

Full-history RPC runs as one daemon in one mode. On startup it figures out how far behind the network tip it is and catches up automatically; once caught up, it serves live ledgers as they're produced. There is no separate backfill command or explicit catch-up step for the operator to invoke.

The daemon does three things:

- **Catches up on startup** by running bulk catch-up as a subroutine. This brings on-disk coverage in line with the current retention window — pulling from a configured LedgerBackend (BSB — the Buffered Storage Backend, which reads ledgers from an object store — by default; captive core or any other conformant backend if BSB isn't available) any chunks inside that window that aren't already frozen — while skipping the tip chunk that captive core is actively ingesting; hot DB ingestion finishes that one. This covers first-ever start, downtime gaps, and retention-widening gaps.
- **Ingests** live ledgers from `CaptiveStellarCore` into one hot RocksDB per chunk — ledgers, transaction hashes, and events as column families, written in one atomic batch per ledger.
- **Freezes** completed chunks to immutable files, **rebuilds** the current tx-hash index from its frozen inputs on every chunk boundary, and **prunes** superseded and past-retention artifacts. All run in a background lifecycle goroutine.

---

## Geometry

The Stellar blockchain starts at ledger 2 (`GENESIS_LEDGER`). Two units organize all storage; everything in this doc is described in terms of them:

- **Chunk** — 10,000 ledgers (hardcoded). The atomic unit of ingestion, freezing, and crash recovery.
- **Window** (tx-hash index) — `chunks_per_txhash_index` chunks (default 1000 = 10M ledgers). The unit of the rolling tx-hash index. Configurable, but immutable once stored.

```
chunkID(seq)         = floor((seq - 2) / 10_000)
chunkFirstLedger(c) = c * 10_000 + 2
chunkLastLedger(c)  = (c + 1) * 10_000 + 1
indexID(c)          = c / chunks_per_txhash_index       # takes a CHUNK id
chunksInIndex(w)    = [w*cpi, (w+1)*cpi - 1]            # cpi = chunks_per_txhash_index
```

Chunk ids are **signed**, and `chunkID` uses floor division. The only sub-genesis sequence the daemon ever forms is the "nothing ingested" watermark sentinel `earliest_ledger - 1` (which is `1` when `earliest_ledger` is genesis); floor division maps it to **chunk −1**, and `chunkLastLedger(-1) = 1` reproduces the sentinel. Chunk −1 means "before the first chunk" and exists only as a transient in derivation arithmetic — the cold and positional terms of `deriveCompleteThrough`, and the watermark mid-chunk test at startup. It is never serialized: every chunk id written to a meta-store key or file path is a real chunk `≥ 0`, so `%08d` only ever sees non-negative ids. (`chunkID(seq)` for an in-range `seq ≥ GENESIS_LEDGER` is unaffected — floor and truncating division agree on non-negative numerators.)

All chunk and window ids use uniform `%08d` zero-padding. Example, default `chunks_per_txhash_index = 1000`:

| Window | First ledger | Last ledger | Chunks |
|---|---|---|---|
| 0 | 2 | 10,000,001 | 0–999 |
| 1 | 10,000,002 | 20,000,001 | 1000–1999 |
| N | N×10M + 2 | (N+1)×10M + 1 | N×1000 – (N+1)×1000−1 |

---

## What the daemon guarantees

The daemon is built around four guarantees over its data:

- **Retention is complete.** No gaps within the retention window — for every ledger in the window, all data derived from it (transactions, events) is present on disk and available to serve any data request that falls entirely within it.
- **Cold is canonical, hot is transient.** Frozen chunks and finalized indexes live in immutable cold artifacts. The chunk hot DB is discarded once every cold artifact derived from the chunk is durable *and* the rolling index covers the chunk — so a tx lookup always has exactly one home: the chunk's hot DB until coverage, the `.idx` after. The current index is logically mutable — re-derived on every chunk boundary from the frozen `.bin` files — until its window finalizes.
- **The meta-store catalogs what's on disk.** Disk content is exactly what the meta-store specifies — every file is named by a meta-store key and every key in a final state has its file. File and key writes/deletes are ordered to preserve this across crashes.
- **Storage tracks retention.** Disk usage scales with `retention_chunks`, not with uptime — files and meta-store keys for ledger ranges below the effective retention floor are pruned as the floor advances.

The retention window is bounded above by `last_committed_ledger` (the most recent ledger the daemon has durably committed) and below by `effectiveRetentionFloor` (computed from `retention_chunks` and `earliest_ledger`; defined in [Startup](#startup)).

The rest of this doc explains how the daemon maintains these guarantees through three operational phases. The [Correctness](#correctness) section at the end gives the formal statement plus substrate assumptions, coverage scenarios, and audit shapes.

---

## How the daemon runs

Three activities, in the order a fresh daemon encounters them; the last two run together as the steady state.

**Catch up.** On first start the daemon checks how far behind the network it is by sampling the network tip via the configured LedgerBackend; on subsequent starts it picks up from `last_committed_ledger`. It then runs the [catch-up primitives](#catch-up-primitives) (`processChunk`, `buildTxhashIndex`) over the missing range. The catch-up loop excludes the chunk captive core is currently ingesting — that one's finished by hot DB ingestion, not by the catch-up source.

**Hot DB ingestion.** Once caught up, the daemon streams live ledgers from CaptiveStellarCore into the live chunk's hot RocksDB — ledgers, tx hashes, and events land in their column families via **one atomic, synced WriteBatch per ledger**, so a ledger is either fully in the hot DB or absent. Ingestion's own progress marker, `last_committed_ledger`, advances per ledger only after that batch is durable — it is a local of the ingestion loop, shared with nothing.

**Freeze and prune.** A background goroutine wakes whenever ingestion's set of hot chunk DBs changes — each chunk boundary, plus once when ingestion starts — and runs one **tick** of three stages: **plan-and-execute** (the same resolver and executor catch-up uses, which freezes complete chunks to immutable files and folds them into the current tx-hash index), **discard** (retire hot DBs the cold artifacts now fully serve), then **prune** (sweep demoted artifacts and everything past the retention window). Each stage sees the previous stage's effects.

The current tx-hash index is **re-derived from scratch on every chunk boundary** to absorb the chunk that just froze, growing until its window is complete; only the window the network tip is in is ever rebuilt, and a completed window's index is finalized (inputs cleaned up) and never touched again. The build is cheap relative to the ~chunk cadence (a full-window rebuild with streamhash — the minimal-perfect-hash index library behind tx-hash lookups — is ≈1 minute against a chunk boundary every ~14 hours at mainnet rates), so rebuilding from scratch each boundary is affordable.

The boundary between "in-retention" and "past-retention" is the `effectiveRetentionFloor`. As the network tip advances, the floor advances with it; complete chunks below the floor are removed by the prune stage.

[Daemon flow](#daemon-flow) below has the pseudocode for each phase. [Data model](#data-model) describes what's on disk and in the meta store. [Correctness](#correctness) details the invariants the design maintains.

---

## Configuration

One TOML file (`--config`) configures the daemon.

**[service]**

| Key | Type | Default | Description |
|---|---|---|---|
| `default_data_dir` | string | **required** | Base directory for the meta store and default storage paths. |

**[catch_up]**

| Key | Type | Default | Description |
|---|---|---|---|
| `chunks_per_txhash_index` | uint32 | `1000` | Chunks per tx-hash window. Defines data layout — immutable once stored (startup aborts on mismatch; see `validateConfig`). |
| `workers` | int | `GOMAXPROCS` | Concurrent task slots for bulk catch-up. |
| `max_retries` | int | `3` | Retries per catch-up task before the daemon aborts. |

**[catch_up.bsb]** — Buffered Storage Backend (the default bulk LedgerBackend; required **unless** another conformant LedgerBackend is configured as the bulk source — `backendNetworkTip`/`validateBackendCovers`/`processChunk`'s default `source` all go through whichever backend is configured)

| Key | Type | Default | Description |
|---|---|---|---|
| `bucket_path` | string | **required** | Remote object store path for LedgerCloseMeta (no `gs://` prefix for GCS). |
| `buffer_size` | int | `1000` | Prefetch buffer depth per connection. |
| `num_workers` | int | `20` | Download workers per connection. |

**[immutable_storage.*]** — one optional `path` per artifact tree (defaults under `{default_data_dir}`):

| Section | Default path | Holds |
|---|---|---|
| `[immutable_storage.ledgers]` | `{default_data_dir}/ledgers` | `.pack` files |
| `[immutable_storage.events]` | `{default_data_dir}/events` | events cold segments |
| `[immutable_storage.txhash_raw]` | `{default_data_dir}/txhash/raw` | transient `.bin` files |
| `[immutable_storage.txhash_index]` | `{default_data_dir}/txhash/index` | per-window `.idx` |

**[meta_store]** — optional `path` (default `{default_data_dir}/meta/rocksdb`).

**[logging]** — optional `level` (`debug`/`info`/`warn`/`error`, default `info`) and `format` (`text`/`json`, default `text`).

**[streaming]**

| Key | Type | Default | Description |
|---|---|---|---|
| `retention_chunks` | uint32 | `0` | Retention window in chunks. `0` = full history. |
| `earliest_ledger` | uint32 \| `"genesis"` \| `"now"` | `"genesis"` | Earliest ledger this daemon will ever have data for. Acts as a fixed lower floor on history; combines with `retention_chunks` (the effective floor is the higher of the two). Must be chunk-aligned (i.e., `chunkFirstLedger` of some chunk); `"now"` resolves to `chunkFirstLedger(chunkID(backendNetworkTip()))` at first start. Stored on first start; immutable thereafter. Setting it higher than genesis skips upfront catch-up — useful for *frontfill* deployments (`earliest_ledger = "now"`) where bringing a fast bulk source online isn't possible. The current immutability is enforced only by `validateConfig`; the rest of the system reads the value through the meta store, so a future `set-earliest-ledger` admin command would be a small change. |
| `captive_core_config` | string | **required** | Path to CaptiveStellarCore config file. |

**[streaming.hot_storage]**

| Key | Type | Default | Description |
|---|---|---|---|
| `path` | string | `{default_data_dir}/hot` | Base path for hot RocksDB databases. |

**CLI**

| Flag | Type | Default | Description |
|---|---|---|---|
| `--config` | string | **required** | Path to TOML config file. |

---

## Data model

The daemon's durable state lives in two places: the meta-store RocksDB (state markers and config pins) and the filesystem (immutable files plus one per-chunk hot RocksDB that holds in-progress data during ingestion).

Throughout this section, `chunk` is a chunk id, `txhash_index` is a window id, and `chunks_per_index` is shorthand for `config.chunks_per_txhash_index`.

### Filesystem artifacts

The per-chunk artifacts are each written once at chunk freeze; the txhash index is rebuilt on each chunk boundary while its window is current and then finalized. All four are produced by the [catch-up primitives](#catch-up-primitives):

| Artifact | Granularity | Format | Produced by |
|---|---|---|---|
| Ledger pack file | per chunk | `.pack` | `processChunk` |
| Events cold segment | per chunk | three files per chunk (format defined in the events doc) | `processChunk` |
| Sorted txhash file | per chunk | `.bin` (sorted streamhash entries; see [rule 5](#catch-up-primitives)) | `processChunk` |
| Streamhash txhash index | per index | one `.idx` file per **coverage**, named `{lo:08d}-{hi:08d}.idx` inside the window's dir; at most one coverage frozen at any moment | `buildTxhashIndex` |

The `.bin` is transient — it is the input to `buildTxhashIndex` and exists only until its index window finalizes, at which point the terminal build's commit batch demotes it to `"pruning"` and the sweep removes it. While a window is current, every boundary re-reads its `.bin` files to rebuild the index, so they are retained for the whole window. The pack file and events segment persist until retention-driven pruning removes them. The txhash index is rebuilt at a **new coverage** on every boundary (mark the new coverage's key `"freezing"` → write `{lo}-{hi'}.idx` → one atomic batch promotes it to `"frozen"` and demotes the predecessor coverage to `"pruning"`), then persists until pruning once its window has finalized and slid past retention. Key name and filename are a bijection, so every index file on disk — including a crashed attempt's partial — is reachable from its key alone; nothing ever lists the directory.

### Directory layout

Chunk-level files group into buckets of 1,000 chunks (`bucket_id = chunk_id / 1000`, formatted `%05d`) — a filesystem concern only; bucket ids never appear in meta-store keys. Directories are created on demand.

```
{default_data_dir}/
├── meta/rocksdb/                                  ← meta store (WAL always on)
├── hot/{chunk:08d}/                               ← per-chunk hot RocksDB (transient)
├── ledgers/{bucket:05d}/{chunk:08d}.pack
├── events/{bucket:05d}/{chunk:08d}-events.pack    (+ -index.pack, -index.hash)
└── txhash/
    ├── raw/{bucket:05d}/{chunk:08d}.bin           ← transient until window finalization
    └── index/{window:08d}/{lo:08d}-{hi:08d}.idx   ← one frozen file per window, coverage-named
```

### The chunk hot DB

During ingestion the daemon maintains **one hot RocksDB per chunk** at `{hot_storage.path}/{chunk:08d}/`, holding everything for that chunk not yet materialized to cold artifacts. The data types are column families of the one instance:

| Column family | Holds | Serves |
|---|---|---|
| `ledgers` | compressed LCMs (LedgerCloseMeta), keyed by seq | `getLedger` for the live chunk; the source `processChunk` reads at freeze |
| `txhash` | tx hash → seq | `getTransaction` for the live chunk |
| events CFs | live events (schema per the events doc) | `getEvents` for the live chunk |

CFs share the instance's WAL, so each ledger commits as **one atomic WriteBatch across all CFs** — there is no cross-store ordering to reason about within a chunk. Per-CF options keep tuning independent (the events CFs carry their own settings). The DB is created when ingestion enters the chunk and discarded whole once every cold artifact derived from the chunk is durable **and** the rolling index covers the chunk; it keeps serving tx lookups across the brief freeze-to-coverage interval, and freeze, rebuild, and discard all chain within one lifecycle tick.

### Meta-store keys

The meta store holds three groups of keys: per-chunk artifact state keys, hot DB state keys, and config pins.

**Artifact state keys**:

| Key | Value | Meaning |
|---|---|---|
| `chunk:{chunk:08d}:lfs` | `"freezing"` \| `"frozen"` \| `"pruning"` | Per-chunk pack file state. |
| `chunk:{chunk:08d}:txhash` | `"freezing"` \| `"frozen"` \| `"pruning"` | Per-chunk `.bin` file state. Transient — removed at window finalization. |
| `chunk:{chunk:08d}:events` | `"freezing"` \| `"frozen"` \| `"pruning"` | Per-chunk events cold segment state. |
| `index:{txhash_index:08d}:{lo:08d}:{hi:08d}` | `"freezing"` \| `"frozen"` \| `"pruning"` | One key per index **coverage**. The key *name* carries the coverage `[lo, hi]` and maps 1:1 to the file `{lo:08d}-{hi:08d}.idx`; the *value* is pure lifecycle state — the same three values as every other artifact key. At most one coverage per window is `"frozen"` at any moment, and a key with `hi` = its window's last chunk is **terminal** by definition (see [Index keys](#index-keys) below). |

For the per-chunk keys, `"freezing"` means the immutable file is being written; `"frozen"` means it's fsynced and durable; `"pruning"` means the file is queued for removal; key absent means neither file nor in-progress write exists. Index keys use the **same three states with the same meanings** — a rebuild marks its coverage `"freezing"` before any I/O, and its commit batch flips it to `"frozen"` while demoting the superseded coverage to `"pruning"`. Every artifact key therefore obeys one set of crash rules: `"freezing"` = delete (or re-derive) the file, `"pruning"` = finish the delete, `"frozen"` = truth.

**Hot DB state key**:

| Key | Value | Tracks |
|---|---|---|
| `hot:chunk:{chunk:08d}` | `"transient"` \| `"ready"` | The chunk's hot DB. |

`"ready"` means the RocksDB dir exists and is usable. `"transient"` brackets a directory operation in flight — creation or deletion; no code path ever needs to know which, since the recovery is the same either way (the open path wipes and recreates; the discard scan re-runs). A crash mid-operation is detectable from the key value alone. One key per chunk; the column families inside the DB carry no individual meta-store state.

**Config pins** (there is no stored watermark — see below):

| Key | Value | Written when |
|---|---|---|
| `config:earliest_ledger` | `uint32` (decimal string, chunk-aligned) | On the first daemon start. Immutable thereafter — changing it currently requires wiping the data directory, until a `set-earliest-ledger` admin command exists (see [Configuration](#configuration); the floor machinery already converges for either direction). |
| `config:chunks_per_txhash_index` | `uint32` (decimal string) | On the first daemon start; immutable thereafter. Startup aborts if the config value doesn't match. |

**Progress is derived, never stored — and never shared.** The hot DB's synced per-ledger WriteBatch *is* the durable commit; recording it again in the meta store would only create a second copy of the same fact, plus the ordering rule needed to keep the copy honest. Two derivations read progress back out of the catalog, one per consumer, at the two granularities they need. Both lean on one **key-creation invariant**: a `hot:chunk` key is created only after every ledger below its chunk has durably committed — at a boundary, ingestion closes chunk C's write handle *before* creating C+1's key (the [ingestion loop](#hot-db-ingestion) enforces the ordering); at startup, the resume chunk's key is created only after derivation has already run. The highest hot key therefore *is* the live chunk, and everything below it is complete.

The lifecycle tick needs only chunk granularity — which chunks are complete, and where the sliding retention floor anchors:

```go
// completeThrough is the highest ledger the lifecycle may treat as durably
// ingested. Two complementary terms: a COLD term — the highest chunk whose
// artifacts are all durable — which leads at startup (catch-up just ran;
// ingestion hasn't started); and a POSITIONAL term — everything below the
// live chunk, by the key-creation invariant — which leads in steady state
// (a chunk completes long before its cold artifacts exist).
func deriveCompleteThrough(cat Catalog) uint32 {
	// Cold term: a chunk counts only when pendingArtifacts() is empty (lfs
	// AND events frozen; txhash frozen or index-covered). NOT merely "lfs
	// frozen": a crash mid-freeze can leave lfs frozen while events is still
	// "freezing", and counting that chunk would let reads open over a
	// partial artifact. An incompletely frozen tip chunk must DEGRADE the
	// bound so catch-up / re-ingestion repairs it. highestDurableChunk
	// returns -1 when NO chunk is durable (a fresh start), so the cold term
	// is then chunkLastLedger(-1) = 1 — the pre-genesis sentinel — never a
	// spurious chunk-0 bound that would resume a young network past its tip.
	through := chunkLastLedger(highestDurableChunk(cat))
	// Positional term. hotChunkKeys returns every hot:chunk:* key regardless
	// of value — counting a "transient" key is sound because it is only ever
	// put after the predecessor chunk's write handle closed. When the live
	// chunk is chunk 0 (a young genesis network), maxChunk-1 = -1 and
	// chunkLastLedger(-1) = 1: nothing below chunk 0 is complete.
	if hot := hotChunkKeys(cat); len(hot) > 0 {
		through = max(through, chunkLastLedger(maxChunk(hot)-1))
	}
	return max(through, cat.EarliestLedger()-1)
}
```

Ingestion's resume point at startup needs the exact ledger — the one consumer of sub-chunk precision:

```go
// deriveWatermark is deriveCompleteThrough refined by exactly ONE read:
// sub-chunk precision only ever matters inside the live chunk, and a lower
// ready chunk can never hold the maximum (key-creation invariant, above), so
// only the live chunk's DB is opened. Runs once, before ingestion starts —
// the only time opening a hot DB is safe (once ingestion runs, the live DB
// is held exclusively by its writer).
func deriveWatermark(cat Catalog) uint32 {
	for _, c := range readyHotChunks(cat) {
		if !dirExists(hotChunkPath(c)) {
			// Checked for EVERY ready key, not just the one opened below:
			// derivation runs before every other open site, so without this
			// a lost hot volume dies as an opaque RocksDB error (or worse,
			// a predecessor's loss is silently healed by discard) instead
			// of the curated recovery instruction. Never skip silently —
			// that would auto-heal the mount-misconfiguration case.
			fatalf("hot:chunk:%08d is \"ready\" but its dir is missing — "+
				"hot storage lost; run surgical recovery (case 4).", c)
		}
	}
	w := deriveCompleteThrough(cat)
	if live, ok := highestReadyHotChunk(cat); ok {
		w = max(w, maxCommittedSeq(openReadOnly(live)))
	}
	return w
}
```

During operation no shared watermark exists at all: ingestion keeps its progress as a plain local, and each lifecycle tick calls `deriveCompleteThrough` fresh. The meta-store catalog thereby stays a *pure* catalog — every key names a file/dir state or a config pin. Postcondition-driven catch-up is what makes a derived watermark safe: catch-up converges *ranges*, not resume pointers, so derivation can never hide a hole — and a lost hot volume self-degrades the watermark to the last frozen boundary instead of requiring a manual rewind ([surgical recovery case 4](#scenario-coverage)).

### Index keys

An index key `index:{txhash_index:08d}:{lo:08d}:{hi:08d}` carries the chunk range `[lo, hi]` its `.idx` covers in the key **name**; the value holds only lifecycle state. The filename is derived from the key by a fixed bijection — `txhash/index/{txhash_index:08d}/{lo:08d}-{hi:08d}.idx` — so resolving a key to its file never involves reading the value or listing a directory. [The transactions design](./gettransaction-full-history-design.md) (§6.3) is the canonical reference for coverage semantics, with the rationale and a worked example; the properties this doc's protocols depend on:

- **Coverage is the whole identity** — there is no per-attempt counter. A retry of a crashed build re-marks the same key and rewrites the same file from scratch, exactly as rule 1 re-materializes a per-chunk artifact at its canonical path.
- **`lo`** rises above the window's first chunk when `earliest_ledger` or the sliding floor cuts into the window at build time (`lo` rising is how a mid-window floor is encoded); **`hi`** advances by one chunk per boundary while the window is current, and equals the window's last chunk once it finalizes.
- **Terminal-ness is derived, not stored**: the key whose `hi` equals its window's last chunk (computable forever from the immutable `chunks_per_txhash_index` pin). A window whose frozen key is terminal is finalized — its `.bin` inputs were demoted in the same commit, and its index is never rebuilt again (only a retention-widening catch-up re-derives it, at its new, wider coverage).
- **The uniqueness invariant: at most one coverage per window is `"frozen"` at any moment.** The rebuild's commit is one atomic synced batch ([rule 3](#catch-up-primitives)'s commit step holds the exact composition), so the frozen coverage changes hands atomically and readers resolve "the window's index" as *the unique frozen key* — no tie-break, no value parsing. Everything else under the window's prefix is transient debris: `"freezing"` = a crashed attempt (re-marked and overwritten if its coverage is built again, otherwise swept); `"pruning"` = a superseded coverage (finish the unlink, drop the key).

So the `.idx` hashes exactly the transactions in chunks `[lo, hi]`: chunks below `lo` are out of scope (floor); chunks above `hi` are served from their chunks' hot DBs until the next rebuild advances `hi`. While the window is current, `lo` tracks the floor and `hi` the tip automatically — no separate floor-driven rebuild is ever needed. Once finalized, the `.idx` is static; a floor that later advances within the window leaves it stale (`lo` referencing chunks pruning has since removed), which the reader retention contract handles cleanly. A window-straddling floor exists at most once at any moment — the window containing the effective retention floor.

### Per-chunk artifact lifecycle

Pack files, events segments, and `.bin` files (the `processChunk` outputs) are write-once:

```
    absent  ──►  ingesting  ──►  freezing  ──►  frozen  ──►  pruning  ──►  absent
```

- **Absent** — no artifact key, no immutable file.
- **Ingesting** — hot DB holds the data being written; artifact key is absent; immutable file doesn't exist yet.
- **Freezing** — `processChunk` has put `"freezing"` and is materializing the file. The file may be partial on disk. A crash here is detectable from the key value alone — recovery either re-writes (within retention) or deletes the partial file (past retention).
- **Frozen** — immutable file is fsynced at its canonical path; artifact key is `"frozen"`. Once all three of a chunk's artifacts are frozen *and* the rolling index covers the chunk, the chunk's hot DB is discarded.
- **Pruning** — retention is deleting the immutable file; artifact key is `"pruning"`; the file may or may not still be on disk.

The `"freezing"` mark is set **before** any I/O for that artifact. This gives the invariant **"any file on disk has its meta-store key set"** — a retention scan iterates keys, and every file is reachable that way. Without the pre-write mark, a crash between writing the file and setting the key would leave an artifact that no scan could see.

### Index artifact lifecycle

The streamhash index is the one logically-mutable cold artifact: it is re-derived on every chunk boundary while its window is current. Physically a file is writable only while its key has never been `"frozen"` in this run — mutation happens by freezing the next coverage and demoting the old one; the frozen file readers resolve is immutable until unlinked.

Each coverage runs the same lifecycle as every per-chunk artifact:

```
    absent  ──►  freezing  ──►  frozen  ──►  pruning  ──►  absent
```

- **Freezing** — the key was put (with its coverage in the name) *before* any I/O; the file may be partial or absent. A crashed attempt parks here. If its coverage is built again, the build re-marks the key and rewrites the file wholesale; one the prune scan observes was not retried — **delete file and key, never salvage**. Salvage would require proving the file complete; deletion needs no proof, and a rebuild re-derives identical bytes anyway (the merge is a deterministic function of the coverage).
- **Frozen** — the file and its dirent are fsynced and the commit batch has landed. The window's unique frozen coverage *is* the live index.
- **Pruning** — a newer coverage superseded this one (demoted in its commit batch), or retention is removing the window. The standard sweep finishes: unlink → `fsyncDir` → delete the key.

The *window-level* progression — coverage advancing boundary by boundary, then finalization — emerges from the coverage chain: each boundary freezes the widened coverage and demotes its predecessor in one atomic batch, and the terminal key is the one whose `hi` equals the window's last chunk. The batch maintains **at most one frozen coverage per window at all times** — a crash at any instant leaves either the old coverage frozen (batch not landed; the new one is `"freezing"` debris) or the new one frozen (predecessor already `"pruning"`), never both, never neither.

Why rewriting coverage-named files in place is safe — readers hold the live `.idx` open while the next coverage is written into the same directory — is argued in full in [the transactions design](./gettransaction-full-history-design.md) (§7.5). Four facts carry it: the build's skip rule (no scheduled build ever targets the name readers resolve), the stage ordering plus the eager sweep's window-locality and pruning-only scope, the floor's monotonicity within a run plus reader handles dying with the process, and the merge's determinism. A change to any of the four must re-prove the argument.

### Hot DB lifecycle

```
    absent  ──►  transient  ──►  ready  ──►  transient  ──►  absent
                 (creating)                  (deleting)
```

- **Absent** — no hot DB key, no dir on disk.
- **Transient** — a directory operation is in flight: either creation (key put, RocksDB dir initializing) or deletion (discard rmdir'ing the dir). A crash in either leaves a possibly-partial dir; the recovery is identical regardless of which operation was interrupted — the open path wipes and recreates, the discard scan re-runs — which is why one value suffices.
- **Ready** — dir exists and is usable for reads and writes. The chunk's contents at any moment run up to `last_committed_ledger`'s position within the chunk; ledgers above it are pending streaming.

The hot DB is discarded whole once the chunk's cold artifacts are all frozen and the rolling index covers the chunk — freeze, rebuild, and discard chain within one lifecycle tick. All lifecycles in this section are observable purely from meta-store keys — no filesystem inspection needed.

### One write protocol

Every durable artifact — per-chunk files and index coverages alike — uses the same protocol, **mark-then-write**: put `"freezing"` *before* any I/O; write the file; fsync the file, its parent dirent, and (when the parent was just created) the grandparent dirent; flip the key to `"frozen"`. The pre-mark guarantees *every file on disk has a key*, so all cleanup is key-driven — nothing ever lists a directory to find work — and a crash mid-write is visible as a `"freezing"` key. The dirent barriers guarantee the key never outlives the file's creation: without them, a power crash can revert the file's — or a freshly created directory's — creation under a durable `"frozen"` key, which key-only idempotency would then never repair; that is why writes that create their parent dir barrier the grandparent too (the same two-level barrier `openHotDB` uses).

Per-chunk artifacts write at a canonical path and flip with a single-key put. The index extends the flip into a **commit batch** — the artifact is logically mutable, so everything a build changes commits *together* in one atomic synced write; [rule 3](#catch-up-primitives)'s commit step holds the exact composition. The batch extension changes what commits together, not how a file becomes durable.

Exits mirror the entries: every sweep demotes a still-`"frozen"` key first, then removes the *file before the key* with an `fsyncDir` barrier between (`sweepChunkArtifacts` and `sweepIndexKey` — the system's only two deletion bodies, one per key family), giving the complementary guarantee — *key absent ⟹ file gone*. The hot DB's `transient`/`ready` bracket is the same two ideas applied to a directory.

---

## Catch-up primitives

Two primitives materialize cold artifacts: `processChunk` and `buildTxhashIndex`. They have exactly two callers — the [Startup](#startup) catch-up loop and the [Lifecycle](#lifecycle) tick — and both call them the same way: through the [resolver](#postcondition-driven-scheduling) and executor, sharing one set of postconditions and one scheduler, so the two regimes can never disagree about what done looks like or how it gets there. Five protocol rules govern them; the [resolver](#postcondition-driven-scheduling) and the [execution model](#catch-up-execution-model) below turn them into a schedule.

**(1) Artifact key values.** `processChunk` applies the [one write protocol](#one-write-protocol) to each requested kind (`lfs`, `events`, `txhash`/`.bin`): `"freezing"` before that kind's I/O begins, `"frozen"` only after its file and dirent barriers (a new bucket dir, every 1000th chunk, barriers the grandparent too). The per-kind idempotency rule: skip iff the key's value is `"frozen"`; a `"freezing"`, `"pruning"`, or absent key triggers re-materialization, itself idempotent — the writer overwrites the file at its canonical path and flips to `"frozen"`. The streamhash index uses the **same pattern at its coverage-named path**; only its commit differs — the `"freezing"`→`"frozen"` flip rides in rule 3's atomic batch instead of a single-key put.

**(2) `processChunk(chunk, artifacts)`.** `artifacts` is the subset of outputs to produce; the [resolver](#postcondition-driven-scheduling) uses it to skip producing `.bin` when the window's `.idx` already covers the chunk. The LCM source is chosen internally by `catchupSource`, in preference order — and the same rule serves *both* callers, which is what lets the lifecycle's freeze be ordinary plan execution:

1. **A ready, complete hot DB** (`maxCommittedSeq ≥ chunkLastLedger(chunk)`): read locally via `HotLedgers` — this is how a just-closed chunk freezes without refetching, and how a complete-but-unfrozen chunk is produced even when the bulk source lags behind it.
2. **The frozen local `.pack`**, when `lfs` is not among the requested outputs: re-derivation without a download.
3. **The configured bulk backend** (BSB by default — see `[catch_up.bsb]`).

The hot branch distinguishes *loss* from *staleness*: a `"ready"` key whose directory is **missing or unopenable** is hot-volume loss — the same case-4 fatal `deriveWatermark` enforces, never silently healed; a hot DB that opens but is **incomplete** is legitimate staleness (a leftover awaiting the discard scan, or a surgically stripped chunk's stale neighbor) and simply falls through to the next source — re-derivation *is* its recovery. Per ledger, the needed extractors run over one LCM stream; tx-hash entries are collected and **sorted in memory** before the `.bin` is written (rule 5).

```go
// processChunk materializes the requested artifact kinds for one chunk from a
// single pass over its 10,000 LCMs, sourced by catchupSource (rule 2's
// preference order).
func processChunk(chunk ChunkID, artifacts ArtifactSet, cfg Config) error {
	cat := cfg.Catalog
	for _, kind := range artifacts.Kinds() { // rule 1 idempotency: frozen kinds self-skip
		if cat.State(chunk, kind) == Frozen {
			artifacts = artifacts.Remove(kind)
		}
	}
	if artifacts.Empty() {
		return nil
	}
	source := catchupSource(chunk, artifacts, cfg)

	batch := cat.NewBatch() // mark-then-write: "freezing" BEFORE any I/O
	for _, kind := range artifacts.Kinds() {
		batch.Put(chunkKey(chunk, kind), "freezing")
	}
	batch.Commit()

	// One streaming pass; only the requested extractors run. Files are
	// (re)created at their canonical paths — re-materialization overwrites.
	w := newArtifactWriters(chunk, artifacts) // .pack, events segment, in-memory txhash entries
	for seq := chunkFirstLedger(chunk); seq <= chunkLastLedger(chunk); seq++ {
		w.Add(source.GetLedger(seq))
	}
	w.Finish()   // sorts txhash entries in memory, writes the .bin (rule 5)
	w.FsyncAll() // files + parent dirents (+ grandparent for a new bucket dir)
	//              — all durable BEFORE the flips below (rule 1)

	batch = cat.NewBatch()
	for _, kind := range artifacts.Kinds() {
		batch.Put(chunkKey(chunk, kind), "frozen")
	}
	batch.Commit()
	return nil
}

// catchupSource implements rule 2's preference order. The hot branch fatals
// only on loss (ready key, missing/unopenable dir — deriveWatermark's rule,
// third call site); an incomplete-but-present DB is staleness and falls
// through, because re-derivation from the next source IS its recovery.
func catchupSource(chunk ChunkID, artifacts ArtifactSet, cfg Config) LedgerSource {
	cat := cfg.Catalog
	if state, _ := cat.Get(hotChunkKey(chunk)); state == "ready" {
		if !dirExists(hotChunkPath(chunk)) {
			fatalf("hot:chunk:%08d is \"ready\" but its dir is missing — "+
				"hot storage lost; run surgical recovery (case 4).", chunk)
		}
		if db := openRocksDBReadOnly(hotChunkPath(chunk)); maxCommittedSeq(db) >= chunkLastLedger(chunk) {
			return &HotLedgers{chunk: chunk, store: db}
		} // incomplete: stale leftover — fall through; the discard scan owns it
	}
	if cat.State(chunk, LFS) == Frozen && !artifacts.Has(LFS) {
		return packReader(chunk) // re-derive locally; no redundant download
	}
	return bulkBackend(cfg) // BSB by default — see [catch_up.bsb]
}
```

**(3) `buildTxhashIndex(w, lo, hi)` — rolling rebuild.** The lifecycle rebuilds the **current** window's index on every chunk boundary, so a frozen chunk's tx hashes move into the index promptly and its hot DB is discarded in the same tick. The covered chunk range is explicit:

- `lo` defaults to the window's first chunk, rising to `chunkID(effectiveRetentionFloor)` when the floor cuts into this index.
- `hi` is the highest frozen chunk in the window — the window's last chunk once it's complete, lower while it's still filling.

The build runs the one write protocol with the batch-commit extension:

1. **Skip check**: if the window's unique frozen key already covers exactly `[lo, hi]`, return — there is nothing to write, and any leftover transient keys are the sweeps' job (rule 4), not the builder's. (A frozen key covering the full window is terminal by definition — `hi` equals the window's last chunk — so the skip also covers re-scheduled builds of finalized windows, which must not demand `.bin` inputs the sweep has deleted.)
2. **Mark**: put `index:{txhash_index:08d}:{lo:08d}:{hi:08d}` = `"freezing"` — an idempotent overwrite when a crashed attempt (or a demoted coverage made desired again by a cross-restart regression) left the key behind. The build is **terminal** iff `hi` is the window's last chunk — a derived property, marked nowhere.
3. **Write**: k-way merge the sorted `.bin` files for chunks `[lo, hi]` into streamhash's `SortedBuilder`, writing `txhash/index/{txhash_index:08d}/{lo:08d}-{hi:08d}.idx` — created or truncated wholesale; a writer only ever holds a file whose key is non-frozen, never one a reader can resolve. Fsync the file and its dir (and the dir's own dirent in `txhash/index/` when this build created the window dir — first build of a window).
4. **Commit**: one atomic synced batch — this coverage `"freezing"`→`"frozen"`; the window's predecessor frozen coverage (if any) →`"pruning"`; and iff this is the terminal build, every `chunk:{chunk}:txhash` key in the window →`"pruning"`. This batch is the *entire* finalization protocol — there is no separate cleanup step; the demoted keys become ordinary sweep work (rule 4).

A crash before step 4 leaves the predecessor frozen and the new coverage as `"freezing"` debris (file partial or complete — irrelevant; it is deleted unread). A crash after step 4 leaves the new coverage frozen and the demoted keys as `"pruning"` work the sweeps finish. There is no crash point at which two coverages are frozen, the live index is unreachable, or a `"frozen"` `chunk:c:txhash` key's `.bin` has been deleted — the batch only ever *demotes* keys, and files are touched exclusively by the sweeps, under non-frozen keys.

Precondition: every chunk in `[lo, hi]` has `chunk:{chunk}:txhash == "frozen"` (its `.bin` exists). The function fails loudly if violated. Catch-up calls the same function for every window its range overlaps — a complete window's full desired range (a terminal build) or the trailing window's producible range (non-terminal); the terminal batch finalizes in the same write, so finalization is never a separate step for either caller.

The full build mechanics live in [the transactions design](./gettransaction-full-history-design.md) (§7): the `buildTxhashIndex` pseudocode, the rationale for rebuilding from scratch each boundary, the disk bounds and provisioning numbers (≈2× index size transient per rebuild; ~12.5 GB written in ~1 minute at a dense window's end), the crash matrix, and the safety argument for rewriting coverage-named files. This doc keeps the protocol surface above — the steps, the commit batch's composition, and the precondition — because the resolver, the sweeps, and the crash analysis depend on exactly those.

**(4) Key-driven sweeps.** All file deletion in the system happens through keys in a transient state — never by listing directories. Two sweep rules cover every case, sharing one mechanic (unlink the file → `fsyncDir` the parent → delete the key, batching the fsyncs and key-deletes when sweeping many at once):

- **Index `"freezing"` keys** — an abandoned build attempt, left by a crash. A retry of the same coverage re-marks and overwrites the key in place (rule 3 step 2), and builds run before this sweep in every regime — so any `"freezing"` key the sweep observes was *not* retried: its coverage is no longer desired. Disposition: **delete file and key, never salvage**. The file might even be complete, but proving that buys nothing — a rebuild re-derives identical bytes — and a single no-questions rule collapses the crash inventory. (Per-chunk `"freezing"` keys are *not* swept this way: their artifacts live at canonical paths, so rule 1's idempotent re-materialization repairs them in place within retention, and the retention prune removes them past it. One exception: a `"freezing"` `chunk:c:txhash` key inside a *finalized* window — re-materialization will never be scheduled for a covered window, so the prune scan's redundant-input branch demotes and sweeps it like its `"frozen"` siblings.)
- **`"pruning"` keys** — superseded index coverages, a finalized window's `.bin` inputs (demoted en masse by the terminal commit batch), and everything demoted by retention pruning. The sweep finishes the removal. Because the key outlives the unlink — the `fsyncDir` barrier makes the unlink durable *before* the key delete commits — a power loss anywhere leaves the key in place and the sweep re-runs. This is the exit-side counterpart of rule 1's invariant: **key absent ⟹ file gone.**

The unlink-before-key order is load-bearing: deleting the key first would, on a crash, leave a file with no key — invisible to every key-driven scan, the one orphan class this design cannot find.

Both sweeps have two call sites: **eagerly, inside every `IndexBuild`'s execution** (`buildThenSweep`, right after the commit batch — whichever regime ran it), and from the tick's prune stage, which is the backstop for crash leftovers and the owner of retention pruning. The eager site is what bounds disk: without it, a long backfill would accumulate every finalized window's demoted `.bin`s until the first tick (≈20 bytes per transaction across all of history); with it, transient `.bin` disk is bounded by the windows actually in flight — the floor is one dense window's worth (~60 GB), irreducible because a window's build merges all of its `.bin`s at once. Crash anywhere mid-sweep leaves `"pruning"` keys the next tick finishes — the same convergence story regardless of caller.

**(5) Streamhash formats.** The tx-hash artifacts use the streamhash pipeline, specified in [the transactions design](./gettransaction-full-history-design.md) (§6). What this doc's protocols rely on: the `.bin` is a **sorted** per-chunk run (`processChunk` sorts the chunk's entries in memory before writing — ~3M entries ≈ 60 MB, negligible), which is what makes the every-boundary rebuild a single streaming k-way merge instead of a two-pass build over unsorted input; the `.idx` is one self-contained streamhash MPHF file per coverage (its `MinLedger` derived from `lo`; no sidecar metadata); and hot tx hashes live as **a single column family inside the per-chunk hot DB** — there is no dedicated txhash store at any layer. The formats match the measured pipeline in the bench harness (`bench-fullhistory`), which is what makes the ~1-minute full-window build figure transfer to this design.

### Postcondition-driven scheduling

A naive scheduler would register every per-chunk and per-window task on every run and rely on each task to self-skip — but catch-up runs on every restart, and that shape re-derives every chunk's `.bin` only for finalization to immediately demote and delete it again: wasted work proportional to the retention window. Instead, catch-up has a contract — *given a range, ensure every artifact derived from every ledger in it is durable and servable* — and resolves what's missing before scheduling anything. The resolver is a registry of **kind rules**: each artifact kind contributes one rule that compares its postcondition against the catalog and emits the difference as tasks. The current kinds:

- **`lfs`** (per-chunk): needed for chunk `c` iff `chunk:{c}:lfs` isn't `"frozen"`.
- **`events`** (per-chunk): same rule against `chunk:{c}:events`.
- **`txhash`** (per-window, the one kind with a cross-chunk artifact): for **each window overlapping the range**, compare the stored coverage (`{lo, hi}` from the *name* of the window's unique **frozen** index key) with the desired coverage `[max(window_start, chunkID(floor)), min(window_last_chunk, range_end)]` — the upper cap is what makes the rule uniform: for a complete window it's the window's last chunk, for the trailing window it's the range end, and no special trailing case exists.
  - **Desired ⊆ stored** → schedule *nothing* for this window: no `.bin` production, no build. Three states land here: every steady-state restart; a floor that *rose* (the stale stored `lo` is the reader retention contract's problem, not a rebuild trigger); and a finalized window the range ends inside — a crash right after a terminal commit resumes exactly at that window's last ledger, where the terminal coverage already covers any desired range and the leftover `"pruning"` demotions stay the sweeps' job.
  - **Desired exceeds stored** (`desired_lo < stored_lo`, or `desired_hi > stored_hi`, or no frozen key exists) → request `.bin` production for **every** chunk in the desired range — chunks whose `.bin` is already frozen self-skip inside `processChunk`, chunks the old `.idx` covered re-derive from local `.pack` files (no BSB) — and emit one index build `buildTxhashIndex(w, desired_lo, desired_hi)`. The build is terminal (input demotion) iff `desired_hi` is the window's last chunk. The `stored_hi` clause matters: a window that was *current* at shutdown carries a frozen key with `hi < last_chunk`, and when downtime crosses the window boundary it becomes a complete window that still needs its tail chunks' `.bin` and the full build — classifying by `lo` alone would strand chunks `(hi, last_chunk]` permanently.

A new data type slots in as a new rule: a per-chunk kind adds a key check, an indexed kind adds another window loop contributing index builds. The skeleton that executes the plan (below) never changes.

The comparison can trust `"frozen"` blindly: **a `"frozen"` `chunk:c:txhash` key implies its `.bin` exists, unconditionally.** Input keys are demoted to `"pruning"` in the same synced write that freezes the terminal coverage, and files are only ever deleted by the sweeps, under non-frozen keys — so no crash at any point can leave a frozen key whose file is gone. Whatever transient keys a crash does leave behind (`"freezing"` attempts, half-swept `"pruning"` demotions) are invisible to the resolver — it classifies on frozen state only — and are swept by the first lifecycle tick, rung at ingestion start.

The per-window comparison is therefore crash-only-recoverable in *every* index state: a finalized window, a window mid-roll at shutdown, a terminal commit that landed but whose sweeps didn't run, and a crashed build attempt all converge through "desired vs stored → re-derive, rebuild," with inputs that are guaranteed producible (`.pack` files are within retention by definition of the desired range). One composition needs help from outside the resolver: a widening catch-up that re-froze a finalized window's `.bin` keys — or crashed mid-write, leaving one `"freezing"` — and then retention is narrowed back before its rebuild. The resolver then correctly schedules nothing (desired ⊆ stored), so re-materialization will never repair those keys; the prune stage's redundant-input branch demotes and sweeps them, `"frozen"` and `"freezing"` alike (see [Prune](#prune)).

In code, the kind rules produce one flat value:

```go
// Both strata are pure data — no behavior is baked into the plan; the
// executor interprets it. That is what makes "the plan is just a value"
// literally true: it can be logged, diffed, and tested without running it.
type ChunkBuild struct {
	Chunk     ChunkID
	Artifacts ArtifactSet // which kinds this chunk still needs — one processChunk pass produces all
}

type IndexBuild struct {
	Window WindowID
	Lo, Hi ChunkID // coverage to build; terminal iff Hi == windowLastChunk(Window)
	// No input list: the build's dependencies are derivable — every chunk in
	// [Lo, Hi] that has a ChunkBuild in the same plan. Carrying them as a
	// field would be a second copy that can drift.
}

type Plan struct {
	ChunkBuilds []ChunkBuild
	IndexBuilds []IndexBuild
}

// resolve computes the diff between desired state and the catalog. Pure read;
// the plan is just a value, recomputed from durable keys on every run — a
// restart re-plans from what is actually on disk, with nothing to reconcile.
func resolve(cfg Config, rangeStart, rangeEnd ChunkID) Plan {
	if rangeEnd < rangeStart {
		return Plan{} // young network: no complete chunk exists yet
	}
	cat := cfg.Catalog
	floor := chunkFirstLedger(rangeStart) // rangeStart already encodes the floor
	needs := map[ChunkID]ArtifactSet{}    // per-chunk work, union across kinds

	for c := rangeStart; c <= rangeEnd; c++ { // per-chunk kinds
		for _, kind := range []Kind{LFS, Events} {
			if cat.State(c, kind) != Frozen {
				needs[c] = needs[c].Add(kind)
			}
		}
	}

	var builds []IndexBuild
	for _, w := range windowsOverlapping(rangeStart, rangeEnd) { // the txhash kind
		desired := Range{
			Lo: max(windowFirstChunk(w), chunkID(floor)),
			Hi: min(windowLastChunk(w), rangeEnd), // capped by range end ⇒ uniform trailing-window handling
		}
		stored := frozenCoverage(cat, w) // the unique "frozen" key's coverage, or none
		if stored.Covers(desired) {
			continue // steady-state restart, risen floor, or finalized window: nothing
		}
		for c := desired.Lo; c <= desired.Hi; c++ {
			if cat.State(c, TxHashBin) != Frozen {
				needs[c] = needs[c].Add(TxHashBin)
			}
		}
		builds = append(builds, IndexBuild{Window: w, Lo: desired.Lo, Hi: desired.Hi})
	}
	return Plan{ChunkBuilds: chunkBuilds(needs), IndexBuilds: builds}
}

// buildThenSweep is how the executor runs an IndexBuild. The build's commit
// batch only demotes keys (rule 3); this eagerly runs the standard sweeps
// (rule 4) so the demoted files come back without waiting for a lifecycle
// tick. The sweep is WINDOW-LOCAL — it walks only this window's keys, so
// concurrent windows' sweeps touch disjoint keys — and as a bonus it
// finishes any "pruning" leftovers a previous crashed pass left in the same
// window.
func buildThenSweep(b IndexBuild, cfg Config) error {
	cat := cfg.Catalog
	if err := buildTxhashIndex(b.Window, b.Lo, b.Hi, cat); err != nil {
		return err
	}
	for _, key := range indexKeys(cat, b.Window) { // superseded coverage(s)
		if key.State == Pruning {
			sweepIndexKey(key, cat)
		}
	}
	var demoted []ArtifactRef // terminal build: the window's .bin inputs
	for c := windowFirstChunk(b.Window); c <= windowLastChunk(b.Window); c++ {
		if cat.State(c, TxHashBin) == Pruning {
			demoted = append(demoted, ArtifactRef{Chunk: c, Kind: TxHashBin})
		}
	}
	if len(demoted) > 0 {
		sweepChunkArtifacts(demoted, cfg, cat)
	}
	return nil
}
```

### Catch-up execution model

`executePlan` executes a plan; `runBackfill` is just backend validation plus `executePlan(resolve(…))`, and the [lifecycle tick](#lifecycle) calls the *same* `executePlan` for its production work — one scheduler, two callers. The shape is map/reduce without the shuffle or the job tracker: chunk builds are the maps, index builds are the per-group reduces (the `.bin`s are map-side-sorted runs, so each reduce is one streaming merge), and completion is recorded as the artifacts themselves. There is deliberately no task engine and no persisted task state, for two reasons. First, the dependency structure is two strata with one edge type — an index build waits on the chunk builds inside its coverage — which the runtime expresses directly: each chunk build closes a done-channel, each index build waits on the in-coverage channels, and the *ready-set* a DAG scheduler would maintain is simply the goroutines parked on the one worker semaphore. Second, a persisted task graph would be a second source of truth about progress, one that can drift from the artifact keys it describes; `resolve` re-plans from the keys on every run, so there is nothing to resume and nothing to reconcile.

```go
func runBackfill(ctx context.Context, cfg Config, rangeStart, rangeEnd ChunkID) error {
	// Every in-range chunk must be producible from SOME source: durable
	// artifacts (self-skips), a complete ready hot DB, the local .pack, or
	// the bulk backend — fail before any work otherwise.
	if err := validateBackendCovers(cfg, rangeStart, rangeEnd); err != nil {
		return err
	}
	return executePlan(ctx, resolve(cfg, rangeStart, rangeEnd), cfg)
}

func executePlan(ctx context.Context, plan Plan, cfg Config) error {
	slots := make(chan struct{}, cfg.Workers) // the ONLY concurrency knob: one pool, all work kinds
	done := make(map[ChunkID]chan struct{}, len(plan.ChunkBuilds))
	for _, cb := range plan.ChunkBuilds {
		done[cb.Chunk] = make(chan struct{})
	}

	g, gctx := errgroup.WithContext(ctx)
	for _, cb := range plan.ChunkBuilds {
		g.Go(func() error {
			defer close(done[cb.Chunk])                // completion broadcast
			slots <- struct{}{}                        // acquire a worker slot
			defer func() { <-slots }()                 // release
			return withRetries(gctx, cfg.MaxRetries, func() error {
				return processChunk(cb.Chunk, cb.Artifacts, cfg)
			})
		})
	}
	for _, b := range plan.IndexBuilds {
		g.Go(func() error {
			for c := b.Lo; c <= b.Hi; c++ { // wait on the in-coverage chunk builds —
				if ch, ok := done[c]; ok { //  derived, not stored; already-frozen
					select {               //  inputs have no channel and no wait
					case <-ch:
					case <-gctx.Done():
						return gctx.Err()
					}
				}
			}
			slots <- struct{}{}                        // index builds draw from the same pool
			defer func() { <-slots }()
			return withRetries(gctx, cfg.MaxRetries, func() error {
				return buildThenSweep(b, cfg)
			})
		})
	}
	return g.Wait()
}
```

- **`cfg.Workers` is the only resource knob** (default `GOMAXPROCS`). The goroutines are structure, not resources: thousands may exist, parked either on the semaphore (queued tasks) or on done-channels (builds awaiting inputs), costing a few KB each; at most `Workers` tasks execute at any instant, drawn from all windows' eligible work mixed together. An index build fires the moment its own in-coverage chunk builds finish, without waiting on other windows. (The derived wait slightly over-approximates — a build also waits on an in-coverage chunk producing only `lfs`/`events` — which is harmless: waiting longer is always safe, and the case arises only in widening scenarios.)
- The executor runs each `IndexBuild` via `buildThenSweep` (defined with `resolve` above), which lands the commit batch (terminal for complete windows) and then runs the eager `"pruning"` sweep (rule 4). The sweep is window-local — this window's demoted inputs and superseded coverages, not a store-wide scan — so concurrent windows' sweeps touch disjoint keys, and `fsyncDir` on a bucket dir shared with another window's in-flight `.bin` writes is safe (a dir fsync with concurrent creates just makes more entries durable).
- Done-channels broadcast *completion*, not success: a chunk build that exhausts its retries still closes its channel (the `defer`), so a dependent index build can win the race against context cancellation and start — whereupon it fails `buildTxhashIndex`'s loud `.bin` precondition check before writing any key, landing on the same abort-and-restart path as the original failure. The precondition check is load-bearing here.
- A task that exhausts its retries aborts the daemon, per the [error policy](#lifecycle); restart re-resolves from durable keys, and completed work never repeats.
- **Single-process enforcement:** the meta store holds a kernel `flock` on a `LOCK` file; a second daemon opening the **same meta-store path** fails immediately, and the lock releases on any process exit (including `kill -9`). Because `[meta_store]` and each `[immutable_storage.*]` path are independently configurable, the meta-store lock alone cannot stop two daemons with *different* meta stores from sharing one artifact tree — the daemon therefore also takes a `flock` in each configured storage root.

---

## Daemon flow

### Startup

Startup runs in two steps — catch up, then serve:

1. **Catch up via backfill.** Bring on-disk coverage in line with the retention window. Each pass backfills up through the last complete chunk at the network tip, with one exclusion: when the watermark is **mid-chunk** and within one chunk of the tip, the partial resume chunk is left to ingestion — core replays its tail faster than a bulk refetch would gate serving, and a mid-chunk watermark can only have come from the live hot DB, so the data is local by construction. Every *complete* chunk is in range — including one whose hot DB holds it but whose artifacts aren't frozen yet (a boundary crash): `catchupSource`'s hot branch produces it locally, no refetch. (On a first-deployment frontfill the loop simply terminates because the range is empty.) The loop re-passes if new chunks appear at the tip while a pass is in flight. Catch-up brings **every** overlapping window's index to its desired coverage via the per-window rule — the trailing partial window included — so when it returns, every in-retention range at or below the last backfilled chunk is servable from durable artifacts.

2. **Serve + ingest.** Open the resume chunk's hot DB, start captive core, start serving reads, run the lifecycle goroutine and the hot DB ingestion loop — whose first act, having opened its hot DB, is one lifecycle notification. **That first tick doubles as startup convergence**: it finishes whatever a crash left behind (every leftover is a key in a transient state — `"freezing"` attempts to delete, `"pruning"` demotions to finish) and removes downtime leftovers (hot DBs and artifacts now past the effective retention floor), concurrently with early serving. There is no startup-only cleanup action and no startup-only tick: the sweeps are key-driven, so the ordinary tick reaches everything without inspecting a single directory.

No other preparation exists: the resume chunk's hot DB is simply reopened (steady-state restart) or created fresh, and the backfilled windows' tx hashes — the trailing window's included — are already queryable through the `.idx` files catch-up built.

**Serve-readiness is established entirely by step 1 plus the resume chunk's hot DB.** Catch-up's postcondition covers every complete in-retention chunk from durable artifacts — boundary-crash leftovers included, produced locally through `catchupSource`'s hot branch — and the only chunk it ever skips is the *partial* resume chunk, whose data lives in the hot DB startup reopens before `serveReads()` (a mid-chunk watermark can only have come from that DB). Nothing gates serving on a cleanup pass, because crash debris and downtime leftovers are reader-invisible at *every* moment of operation — readers resolve `"frozen"` keys exclusively, and the retention check masks past-floor files — so the first tick clears them concurrently with serving rather than ahead of it. The store reaches quiescence within that first tick — typically seconds after reads open, longer when it prunes a long-downtime backlog; from then on the [invariant audits](#correctness) carry their usual meaning. (The one nicety surrendered: a store so damaged that tick ops fail aborts seconds after joining the pool rather than just before — the restart loop is identical either way.)

Operational note — **peak disk after long downtime**: pruning runs only in the first tick's prune stage, *after* catch-up has materialized every newly-in-retention chunk, so a downtime approaching or exceeding the retention window transiently holds up to ~2× the retention footprint (the stale window plus its replacement). Size volumes accordingly, or prune stale ranges manually before restarting after very long downtime; a disk-full during catch-up otherwise aborts before the relieving prune can run, on every retry.

`lastCommitted` is a *mutating* local that the catch-up loop advances as backfill makes progress; it determines `resumeLedger` and the hot DB ingestion start point. It is never written to the meta store, never shared — and not even carried into the ingestion loop, which needs no progress variable at all: each synced batch *is* the progress, re-derived from durable state at the next startup.

The retention floor itself is computed by:

```go
const (
	GenesisLedger   = 2
	LedgersPerChunk = 10_000
)

// effectiveRetentionFloor is the lower bound of the retention window,
// chunk-aligned: the first ledger of the lowest in-scope chunk. Combines the
// sliding retention floor (lastCompleteChunkAt(upperBound) - retentionChunks
// + 1, when retentionChunks > 0) with the fixed earliest-ledger floor.
//
// The upper-bound ledger is ingestion's progress at runtime; the catch-up
// loop passes max(sampled network tip, derived watermark). The max() guards
// a lagging bulk tip: anchored on the tip alone, the floor would regress
// below where pruning has already advanced, scheduling a spurious re-derive
// of a pruned range. When the tip leads (long downtime), the tip is simply
// the correct anchor — it places the floor where retention will sit once
// caught up, so backfill starts at the true floor instead of wastefully
// below it. On a true first start the watermark is absent and the tip alone
// anchors the floor.
func effectiveRetentionFloor(upperBound uint32, retentionChunks uint32, earliest uint32) uint32 {
	sliding := uint32(GenesisLedger)
	if retentionChunks > 0 {
		slidingChunk := lastCompleteChunkAt(upperBound) - int64(retentionChunks) + 1
		sliding = chunkFirstLedger(max(slidingChunk, 0))
	}
	return max(sliding, earliest)
}

// lastCompleteChunkAt is the inverse of chunkLastLedger: the largest chunk
// whose last ledger is <= ledger. E.g., lastCompleteChunkAt(10_001) == 0
// (chunk 0 spans ledgers 2..10_001).
func lastCompleteChunkAt(ledger uint32) int64 {
	return int64(ledger-1)/LedgersPerChunk - 1
}
```

```go
func startStreaming(ctx context.Context, cfg Config) error {
	cat := openMetaStore(cfg)
	cfg.Catalog = cat // catch-up's plumbing (resolve, runBackfill) reads it from cfg
	validateConfig(cfg, cat)

	retentionChunks := cfg.RetentionChunks
	earliest := cat.EarliestLedger()

	// Derived, not read: highest frozen chunk end vs ready hot DBs' max
	// committed seq, clamped by earliest - 1 (the frontfill floor).
	lastCommitted := deriveWatermark(cat)

	// Step 1: catch up via backfill. The loop re-passes while new chunks
	// appear at the tip; backfilledThrough guards against infinite re-passes
	// when the tip stops moving (a fixed rangeEnd matching the previous
	// iteration breaks the loop). Edge case: on a network younger than one
	// chunk, rangeEnd = lastCompleteChunkAt(anchor) = -1, and the watermark
	// sentinel reads as a chunk boundary (Geometry convention) so the
	// mid-chunk branch below leaves rangeEnd at -1 — the rangeEnd < rangeStart
	// guard then catches it cleanly.
	backfilledThrough := int64(-1)
	for {
		tip := backendNetworkTip(cfg)
		anchor := max(tip, lastCommitted) // guards a lagging bulk tip, in BOTH uses below
		rangeStart := chunkID(effectiveRetentionFloor(anchor, retentionChunks, earliest))
		// Anchoring rangeEnd on the watermark too matters when the bulk tip
		// lags: a complete watermark chunk must fall inside the range so the
		// per-window rule folds it into its index before serving. The span
		// beyond the bulk tip consists only of chunks that are already
		// durable (production self-skips) or complete in a ready hot DB
		// (produced locally via catchupSource's hot branch) — the bulk
		// backend is never asked for them.
		rangeEnd := lastCompleteChunkAt(anchor)
		// The watermark sentinel (lastCommitted = earliest_ledger-1, e.g. 1 on
		// a genesis fresh start) sits on a chunk boundary by construction —
		// earliest_ledger is chunk-aligned — so chunkID maps it to its chunk
		// (chunk -1 for the genesis sentinel, per the Geometry convention) and
		// this reads false, never spuriously mid-chunk.
		watermarkMidChunk := lastCommitted != chunkLastLedger(chunkID(lastCommitted))
		withinOneChunkOfTip := int64(tip)-int64(lastCommitted) < LedgersPerChunk
		//                     ^ signed: a lagging bulk tip can sit BELOW the resume point
		if withinOneChunkOfTip && watermarkMidChunk {
			// The partial resume chunk is ingestion's: near the tip, core
			// replays its tail faster than a bulk refetch would gate serving.
			// Mid-chunk watermarks only ever come from the live hot DB, so
			// the data is local by construction.
			rangeEnd = chunkID(lastCommitted) - 1
		}
		if rangeEnd < rangeStart || rangeEnd <= backfilledThrough {
			break
		}
		if err := runBackfill(ctx, cfg, rangeStart, rangeEnd); err != nil {
			return err
		}
		lastCommitted = max(lastCommitted, chunkLastLedger(rangeEnd))
		backfilledThrough = rangeEnd
	}
	resumeLedger := lastCommitted + 1

	// Step 2: serve + ingest. The first tick — rung by ingestion's at-start
	// notification — finishes anything a crash left half-done and prunes
	// downtime leftovers, concurrently with early serving.
	hotDB := openHotDBForChunk(cfg, cat, chunkID(resumeLedger))
	core := startCaptiveCore(cfg, resumeLedger)
	doorbell := make(chan struct{}, 1)
	go lifecycleLoop(ctx, cfg, cat, doorbell)
	serveReads()
	return runIngestionLoop(cfg, core, hotDB, cat, doorbell)
}
```

After `runBackfill` returns, every chunk in the backfilled range has `lfs` and `events` frozen, every overlapping window's index is at its desired coverage, and `txhash` keys are in one of three states: frozen (window still rolling), swept (finalized window whose terminal commit this pass landed — the batch demoted them, the eager sweep removed them), or `"pruning"` leftovers from a *pre-crash* terminal commit, which the resolver correctly skipped (desired ⊆ stored) and the first tick's prune phase sweeps; partially processed chunks were retried idempotently. The lowest chunk in the backfilled range is `chunkID(effectiveRetentionFloor(max(tip, lastCommitted), …))` — the same `max()` anchor the loop uses; if this falls mid-window, that window's finalized index is built with `lo` = that chunk (its terminal index key carrying that `lo` and `hi` = the window's last chunk). Streaming startup therefore doesn't re-validate contiguity or per-chunk flag-completeness — they follow from how backfill works.

```go
func validateConfig(cfg Config, cat Catalog) {
	// Stateless config validation (no pins touched yet).
	if cfg.ChunksPerTxhashIndex == 0 {
		fatalf("chunks_per_txhash_index must be > 0 (it defines the index layout).")
	}
	if cfg.Workers < 1 {
		fatalf("workers must be > 0 (got %d) — a zero pool deadlocks executePlan.", cfg.Workers)
	}
	if cfg.MaxRetries < 0 {
		fatalf("max_retries must be >= 0 (got %d).", cfg.MaxRetries) // 0 = run once, no retry
	}
	// The two layout pins (chunks_per_txhash_index, earliest_ledger) are
	// committed together in one atomic batch on first start (below), so they
	// exist all-or-nothing: BOTH present ⟹ a prior first start completed and the
	// layout is immutable; otherwise startup never got past config validation,
	// no artifacts exist, and re-validating + re-pinning is safe.
	cpiStored, cpiPinned := cat.Get("config:chunks_per_txhash_index")
	earliestStored, earliestPinned := cat.Get("config:earliest_ledger")

	if cpiPinned && earliestPinned {
		// Restart: the layout is committed — confirm nothing changed, write nothing.
		if cpiStored != itoa(cfg.ChunksPerTxhashIndex) {
			fatalf("chunks_per_txhash_index changed: stored=%s, config=%d",
				cpiStored, cfg.ChunksPerTxhashIndex)
		}
		// earliest_ledger immutability. The backend tip is NOT re-sampled (it
		// may lag below the pinned floor — the startup loop's
		// max(tip, lastCommitted) handles that). "now" is a no-op: it resolved
		// once at first start and is now pinned.
		if cfg.EarliestLedger != "now" {
			want := uint32(GenesisLedger)
			if cfg.EarliestLedger != "genesis" {
				want = atoi(cfg.EarliestLedger)
			}
			if want != atoi(earliestStored) {
				fatalf("earliest_ledger changed: stored=%s, config=%s. Wipe the data "+
					"directory to change earliest_ledger (or use the future "+
					"set-earliest-ledger admin command).", earliestStored, cfg.EarliestLedger)
			}
		}
		return
	}

	// First start (or an incomplete prior start — no artifacts yet). Resolve
	// earliest_ledger, sampling the tip for "now" and rejecting a floor past the
	// tip; then commit BOTH layout pins in one atomic synced batch.
	var earliest uint32
	switch cfg.EarliestLedger {
	case "genesis":
		earliest = GenesisLedger
	case "now":
		earliest = chunkFirstLedger(chunkID(backendNetworkTip(cfg)))
	default:
		earliest = atoi(cfg.EarliestLedger)
		if earliest != chunkFirstLedger(chunkID(earliest)) {
			fatalf("earliest_ledger (%d) must be chunk-aligned.", earliest)
		}
	}
	if earliest > backendNetworkTip(cfg) {
		fatalf("earliest_ledger (%d) is past the current tip; reject.", earliest)
	}
	batch := cat.NewBatch()
	batch.Put("config:chunks_per_txhash_index", itoa(cfg.ChunksPerTxhashIndex))
	batch.Put("config:earliest_ledger", itoa(earliest))
	batch.Commit()
}

func openHotDBForChunk(cfg Config, cat Catalog, chunk ChunkID) *HotDB {
	// createChunkHotDB creates the instance with its column families:
	// ledgers, txhash, and the events CFs (schema per the events doc).
	return openHotDB(cat, hotChunkKey(chunk), hotChunkPath(chunk), createChunkHotDB)
}
```

### Hot DB helpers

These functions implement the hot DB state machine. Both startup and the lifecycle loop use them.

`openHotDB` opens a ready hot DB, recovers from a prior crash, or creates a fresh one:

```go
// openHotDB returns an open handle to the hot DB. If the key is "ready",
// opens the existing DB. Otherwise — "transient" from a crashed create or
// discard, or absent on first use — wipes any leftover dir and creates
// fresh. The caller owns the returned handle.
func openHotDB(cat Catalog, hotKey, path string, create func(string) *HotDB) *HotDB {
	if state, _ := cat.Get(hotKey); state == "ready" {
		if !dirExists(path) {
			// The key promises a DB the filesystem doesn't have — hot storage
			// was lost out from under a surviving meta store (e.g. ephemeral
			// NVMe died). Recreating empty would silently lose the chunk's
			// ledgers, so refuse: the operator deletes the orphaned hot:chunk
			// keys (surgical recovery case 4) and restarts — the derived
			// watermark then lands at the last frozen boundary automatically,
			// and re-ingestion fills the gap. The fatal stays (rather than
			// auto-healing) because a missing dir can also mean a mount
			// misconfiguration, where auto-wiping state would be wrong.
			fatalf("%s is \"ready\" but %s is missing — hot storage lost; "+
				"run surgical recovery (case 4).", hotKey, path)
		}
		return openExistingRocksDB(path)
	}
	// "transient" or absent — wipe any leftover dir and recreate.
	deleteDirIfExists(path)
	cat.Put(hotKey, "transient")
	db := create(path)
	fsyncDir(path)       // dir + dirent durable BEFORE "ready" — else a power
	fsyncParentDir(path) // crash fabricates the ready-without-dir fatal above
	cat.Put(hotKey, "ready")
	return db
}
```

`discardHotDBForChunk` retires a chunk's hot DB once every cold artifact derived from the chunk is durable (or the chunk has fallen past retention):

```go
func discardHotDBForChunk(chunk ChunkID, cat Catalog) {
	if !cat.Has(hotChunkKey(chunk)) {
		return
	}
	cat.Put(hotChunkKey(chunk), "transient")
	deleteDirIfExists(hotChunkPath(chunk))
	fsyncParentDir(hotChunkPath(chunk))
	cat.Delete(hotChunkKey(chunk))
}
```

`HotLedgers` is the hot-DB reader `catchupSource` returns when its hot branch wins — a read-only view of the `ledgers` CF, opened and completeness-checked by `catchupSource` itself (the loss-vs-staleness rule in rule 2) before the wrapper is handed out:

```go
type HotLedgers struct {
	chunk ChunkID
	store *RocksDB // opened (and verified complete) by catchupSource
}

func (h *HotLedgers) GetLedger(seq uint32) LedgerCloseMeta {
	return decompressLCM(h.store.GetCF("ledgers", beUint32(seq)))
}
```

### Hot DB Ingestion

```go
func runIngestionLoop(cfg Config, core *CaptiveCore, hotDB *HotDB, cat Catalog,
	doorbell chan struct{}) error {

	notify := func() { // payload-free doorbell: non-blocking send, coalescing
		select {
		case doorbell <- struct{}{}:
		default:
		}
	}
	notify() // first act: the hot-chunk set just changed (the resume DB was opened)

	for lcm := range core.StreamLedgers() {
		// One atomic, synced WriteBatch across all CFs — a ledger is either
		// fully in the hot DB or absent. The batch IS the durability
		// boundary; the loop keeps no progress variable at all — progress is
		// re-derived from durable state at the next startup.
		batch := hotDB.NewBatch()
		putLedger(batch, lcm)   // ledgers CF
		putTxHashes(batch, lcm) // txhash CF
		putEvents(batch, lcm)   // events CFs
		batch.Commit( /*sync=*/ true)

		seq := lcm.LedgerSeq()
		if seq == chunkLastLedger(chunkID(seq)) { // chunk boundary
			// Close the write handle BEFORE creating the next chunk's hot
			// key — the moment that key exists, a tick's derivation
			// classifies this chunk as complete and may freeze and discard
			// this hot DB, and no writer may hold it then.
			hotDB.Close()
			hotDB = openHotDBForChunk(cfg, cat, chunkID(seq)+1)
			notify()
		}
	}
	return nil
}
```

A batch error causes the loop to retry the entire ledger (the batch is all-or-nothing, so a retry can't double-apply). On repeated failure the daemon aborts; the next startup's derived watermark equals exactly what the last synced batch committed — there is no second durable write that could disagree with it — and ingestion resumes from the next seq. The close-before-open order at the boundary is load-bearing: the next chunk's hot key is what makes this chunk *visibly complete* to the lifecycle's derivation, so the write handle must already be released when that key appears — otherwise a tick still in flight from the *previous* notification could rmdir a dir whose writer is live. Readers hold their own independent read-only handles.

The doorbell carries no payload, so its delivery semantics can be maximally sloppy: a non-blocking send on a size-1 buffered channel, coalescing freely. Nothing is lost because the notification carries no information to lose — eligibility derives entirely from durable state, and a tick triggered by one notification processes everything the catalog shows, however many boundaries contributed to it. The doorbell only answers "when should the lifecycle look", never "what should it see".

### Lifecycle

The lifecycle goroutine runs one **tick** per notification, in three stages: **plan-and-execute** (the same `resolve` + `executePlan` catch-up uses, from the retention floor up to `completeThrough` — this is where a just-closed chunk freezes, from its hot DB via `catchupSource`'s hot branch, and where the current window's index folds it in), then the **discard** scan (retire hot DBs the cold artifacts now fully serve), then the **prune** scan (sweep demoted and past-retention files). The retention floor plays two roles with *opposite safe directions*, and the design keeps them separate. As a **retention boundary** (the prune scan, the reader gate) it errs permissive: anchored on `completeThrough`, a floor that sits a little low keeps an extra chunk briefly, or admits a read that at worst lands on already-pruned data and returns not-found via the reader's missing-data-file rule — harmless either way. As a **production boundary** it would err dangerous: planning a build below existing storage means demanding chunks from the bulk source that nobody validated it can produce. So production below storage never consults the floor — the tick's plan range starts at the lowest chunk already materialized, and extending the *bottom* of storage (which is what retention widening means) is exclusively catch-up's job, the one path that runs `validateBackendCovers` before demanding anything. Ordering lives in two places, each natural to its half: freeze-before-build is a *plan dependency* (the window's `IndexBuild` waits on its in-coverage chunk builds' done-channels), and build-before-discard / demote-before-sweep is the *stage sequence* — the scans run after `executePlan` returns, so they see every commit the plan landed. Correctness never depends on any of it: every decision derives from durable keys, so work whose enabler hasn't landed is simply not scheduled, and the next tick picks it up.

The one input the tick needs beyond the keys themselves is *how far ingestion has durably gotten* — which chunks are complete, and where the sliding retention floor anchors. That is `deriveCompleteThrough`, defined with the [derived-progress machinery](#meta-store-keys) in the data model; the tick derives it once, at tick start:

```go
func runLifecycleTick(ctx context.Context, cfg Config, cat Catalog) {
	// One derivation per tick — all stages see the same snapshot, so a
	// boundary committing mid-tick can't make one stage's view contradict
	// another's; the new chunk is simply next tick's work.
	through := deriveCompleteThrough(cat)
	floor := effectiveRetentionFloor(through, cfg.RetentionChunks, cat.EarliestLedger())
	start := chunkID(floor)
	if low, ok := lowestMaterializedChunk(cat); ok && low > start {
		// floor is a retention boundary (pruning, read gating), where erring
		// low is harmless. As a PRODUCTION boundary it would err dangerous:
		// a below-storage build demands chunks from a bulk source nobody
		// validated. So the tick's plan range starts at existing storage;
		// extending the bottom is catch-up's job, behind validateBackendCovers.
		start = low
	}

	if err := executePlan(ctx, resolve(cfg, start, lastCompleteChunkAt(through)), cfg); err != nil {
		fatalf("lifecycle tick: %v", err) // error policy: retries exhausted ⇒ abort;
		//                                   startup is the recovery path
	}
	for _, op := range eligibleDiscardOps(cfg, cat, through) {
		op()
	}
	for _, op := range eligiblePruneOps(cfg, cat, through) {
		op()
	}
	// Assertable postcondition: re-running resolve and both scans against
	// this same `through` snapshot yields nothing — a tick finishes
	// everything its snapshot showed. (A fresh derivation may legitimately
	// see a boundary that landed mid-tick; that is the next tick's work,
	// not a violation.)
}

// lowestMaterializedChunk is one more derivation over the same keys: the
// lowest chunk holding any chunk:* artifact key or hot:chunk key; ok=false
// on an empty catalog (first frontfill tick — resolve's inverted-range
// guard makes that tick a no-op anyway).
func lowestMaterializedChunk(cat Catalog) (ChunkID, bool)

func lifecycleLoop(ctx context.Context, cfg Config, cat Catalog, doorbell <-chan struct{}) {
	for range doorbell {
		runLifecycleTick(ctx, cfg, cat)
	}
}
```

With this, the tick is a pure function of the catalog: the two goroutines share no state at all, and any process holding the meta store could run a tick and reach the same decisions. One narrow consequence of the positional term: a crash *between* closing chunk C's handle and creating chunk C+1's key leaves C as the highest hot chunk, so the derivation conservatively treats it as live until ingestion opens the resume chunk's DB. That is a latency wart, not a correctness one — C's hot DB keeps serving it — and it closes at the first tick, which ingestion rings the moment that DB is open (the reason ingestion notifies at start, not just at boundaries).

The goroutine is event-driven, not polled. Notifications arrive from exactly one source — ingestion's hot-chunk-set changes: each boundary, plus the one at ingestion start, whose tick doubles as startup convergence. Between notifications the goroutine is idle — and idle means *quiescent*: a re-scan would produce no ops, so the [invariant audits](#correctness) are meaningful at any moment between ticks.

**Error policy.** A failing op is retried with backoff a bounded number of times within the tick; on persistent failure the daemon aborts — the same policy as the ingestion loop. Aborting is safe because startup *is* the recovery path: catch-up plus the first tick re-derive or finish whatever the failure interrupted. No op failure is ever deferred to the next boundary (~14 h away) silently.

#### Production (plan-and-execute)

The tick's first stage is catch-up's machinery verbatim: `resolve` diffs `[floor, completeThrough]` against the catalog and `executePlan` runs the result. In steady state the plan is tiny — one `ChunkBuild` for the chunk that just closed (its artifacts produced from its hot DB, which `catchupSource`'s hot branch selects) and one `IndexBuild` folding it into the current window — and at quiescence the plan is empty. The hot DB is *not* touched by production: it keeps serving the chunk's tx lookups until the index covers it, and only the discard stage retires it. Nothing but a terminal `IndexBuild`'s commit ever finalizes a window.

#### Discard

The discard scan walks `hot:chunk:*` keys. Per chunk: past retention → discard; complete, nothing pending, and the index covers it (cold artifacts fully serve it) → discard; otherwise (live, or frozen and awaiting coverage) → leave alone. `discardHotDBForChunk`'s coverage-gated branch is what retires hot DBs in steady state, and re-deriving it from durable keys makes it self-healing across a crash between build and discard. A past-retention discard leaves the chunk's artifact files to the prune stage on the same tick — they carry their own keys.

#### Prune

The prune scan is the system's only file-deleter, driven entirely by keys — one stage, both key families:

- **`index:*` keys**: any key in a transient state is swept regardless of window — `"freezing"` (a crashed build attempt) means delete file and key, never salvage; `"pruning"` (a coverage demoted by a later build's commit batch, or by retention) means finish the removal. A `"frozen"` index key is swept only when its window has fallen wholly past the retention floor.
- **`chunk:*` keys**: chunks wholly past retention are swept whole (all artifacts, any state). Within retention, `chunk:c:txhash` keys reading `"pruning"` — demoted by their window's terminal commit batch — are swept batched. One more in-retention branch: a `"frozen"` *or* `"freezing"` `chunk:c:txhash` key inside a window whose frozen index key is terminal (re-derived — or left mid-write — by a widening catch-up that crashed before its rebuild, then abandoned when retention narrowed back) is provably redundant — the final `.idx` covers the chunk, and the resolver will never schedule re-materialization for a covered window — so it is swept here; this branch is what makes INV-2's no-leftover-txhash-keys clause self-healing rather than merely auditable.

Every sweep, both families, runs the same mechanic: **demote to `"pruning"` if the key is still `"frozen"`** (never unlink under a frozen key), then unlink → `fsyncDir` → key delete, batched per family. The two prune walks never interact with each other — index sweeps touch only index keys, chunk sweeps only chunk keys — and their only cross-family *read* (is the window's frozen index key terminal?) concerns a key no sweep modifies.

#### Eligibility

Each `eligible*` function scans the meta store and returns a list of zero-arg callables — each one a closure over the op to run and its arguments. The lifecycle loop just calls them in order.

```go
func eligibleDiscardOps(cfg Config, cat Catalog, through uint32) []func() {
	floor := effectiveRetentionFloor(through, cfg.RetentionChunks, cat.EarliestLedger())
	var ops []func()
	for _, chunk := range hotChunkKeys(cat) {
		switch {
		case chunkLastLedger(chunk) < floor: // past retention OR below earliest_ledger
			ops = append(ops, func() { discardHotDBForChunk(chunk, cat) })
		case chunkLastLedger(chunk) <= through &&
			pendingArtifacts(chunk, cfg, cat).Empty() &&
			indexCovers(chunk, cfg, cat): // cold artifacts fully serve it
			ops = append(ops, func() { discardHotDBForChunk(chunk, cat) })
			// else: live, or frozen and awaiting coverage — leave alone.
		}
	}
	return ops
}

// pendingArtifacts lists which processChunk outputs this chunk still needs.
// The per-chunk counterpart of catch-up's per-window rule: txhash/.bin is
// exempt when the window's index already covers the chunk — after
// finalization the chunk:c:txhash keys are legitimately demoted ("pruning")
// or swept away, and regenerating the .bin would orphan it.
func pendingArtifacts(chunk ChunkID, cfg Config, cat Catalog) ArtifactSet {
	var need ArtifactSet
	for _, kind := range []Kind{LFS, Events} {
		if cat.State(chunk, kind) != Frozen {
			need = need.Add(kind)
		}
	}
	if cat.State(chunk, TxHashBin) != Frozen && !indexCovers(chunk, cfg, cat) {
		need = need.Add(TxHashBin)
	}
	return need
}

// indexCovers reports whether the durable .idx for chunk's window already
// hashes that chunk.
func indexCovers(chunk ChunkID, cfg Config, cat Catalog) bool {
	fk := frozenCoverage(cat, indexID(chunk)) // the unique "frozen" index key, or none
	return fk != nil && fk.Lo <= chunk && chunk <= fk.Hi
}

func eligiblePruneOps(cfg Config, cat Catalog, through uint32) []func() {
	floor := effectiveRetentionFloor(through, cfg.RetentionChunks, cat.EarliestLedger())
	windowFloor := WindowID(-1)
	chunkFloor := ChunkID(-1)
	if floor != GenesisLedger {
		windowFloor = indexID(chunkID(floor)) - 1
		chunkFloor = lastCompleteChunkAt(floor - 1)
	}
	var ops []func()

	for _, key := range indexKeys(cat) { // index family
		switch {
		case key.State == Freezing || key.State == Pruning:
			// Transient debris from any window — an abandoned attempt
			// ("freezing": delete, never salvage — a retried coverage was
			// re-marked and frozen before this scan ran) or an unfinished
			// demotion ("pruning"). Safe to run only because no build is in
			// flight when this scan runs: the prune stage follows
			// executePlan's return within the tick, and catch-up finishes
			// before the lifecycle goroutine starts.
			ops = append(ops, func() { sweepIndexKey(key, cat) })
		case key.Window <= windowFloor:
			// A frozen index key wholly below the floor; the sweep demotes
			// it first — never unlink under a "frozen" key.
			ops = append(ops, func() { sweepIndexKey(key, cat) })
		}
	}

	var refs []ArtifactRef // chunk family, swept in one batch
	for _, ref := range chunkArtifactKeys(cat) { // (chunk, kind) per key
		switch {
		case ref.Chunk <= chunkFloor: // wholly past retention: any state goes
			refs = append(refs, ref)
		case cat.State(ref.Chunk, ref.Kind) == Pruning:
			// In-retention .bin demoted by its window's terminal commit batch.
			refs = append(refs, ref)
		case ref.Kind == TxHashBin: // "frozen" OR "freezing" inside a finalized window
			if fk := frozenCoverage(cat, indexID(ref.Chunk)); fk != nil && fk.Hi == windowLastChunk(indexID(ref.Chunk)) {
				// Redundant input: re-derived (or left mid-write) by a
				// widening catch-up that crashed before its terminal rebuild,
				// then abandoned. The terminal .idx provably covers the chunk
				// and the resolver never re-materializes a covered window.
				refs = append(refs, ref)
			}
		}
	}
	if len(refs) > 0 {
		ops = append(ops, func() { sweepChunkArtifacts(refs, cfg, cat) })
	}
	return ops
}
```

Hot DBs that outlived their retention window because of long downtime are removed by the discard stage; their files, if any, carry their flag keys and are picked up by the prune stage in the same tick.

#### Op bodies

```go
func sweepChunkArtifacts(refs []ArtifactRef, cfg Config, cat Catalog) {
	batch := cat.NewBatch() // demote first — never unlink under a "frozen" key
	for _, ref := range refs {
		if cat.State(ref.Chunk, ref.Kind) != Pruning {
			batch.Put(chunkKey(ref.Chunk, ref.Kind), "pruning")
		}
	}
	batch.Commit()

	var paths []string // unlink (idempotent on already-gone paths)
	for _, ref := range refs {
		deleteArtifactFiles(ref.Chunk, ref.Kind)
		paths = append(paths, artifactPaths(ref.Chunk, ref.Kind)...)
	}
	fsyncParentDirs(paths) // unlinks durable BEFORE the keys go

	batch = cat.NewBatch()
	for _, ref := range refs {
		batch.Delete(chunkKey(ref.Chunk, ref.Kind))
	}
	batch.Commit()
}

func sweepIndexKey(key IndexKey, cat Catalog) {
	if key.State == Frozen {
		cat.Put(key, "pruning") // never unlink under a "frozen" key — a crash
		//                         mid-sweep must not leave a frozen key fileless
	}
	// "freezing" (crashed attempt — never salvage) and "pruning" (superseded
	// or retention-demoted) take the same path from here; the key outlives
	// the durable unlink, so a crash anywhere re-runs the sweep.
	deleteFileIfExists(indexFilePath(key)) // filename derived from the key name
	fsyncDir(indexWindowDir(key))
	cat.Delete(key)
	rmdirIfEmpty(indexWindowDir(key)) // best-effort tidiness; an empty dir is
	//                                   not an artifact
}
```

The discard stage has no separate op body — `discardHotDBForChunk` is called directly from the eligibility closures above. These two sweeps are the *entire* deletion surface of the system: one body per key family, identical internal shape (demote if frozen → unlink → `fsyncDir` → key delete).

The prune walk's two families are independent of each other and of discard: a chunk swept while its containing window's `.idx` is still around could leave a `getTransaction` query resolving to a missing `.pack`, but the [reader retention contract](#reader-retention-contract) handles that — past-retention seqs return not-found regardless. Discard touches only hot DBs, which the prune walk's flag-key iteration can't see.

### Concurrency model

Two writers; readers only read. The ingestion loop is one goroutine; the lifecycle is one goroutine whose tick's plan stage fans work out to the executor's bounded worker pool — every worker operating strictly below the live chunk, so the pool inherits the lifecycle's side of the partition. Their domains partition at the live chunk:

- **The ingestion loop owns the live chunk** — the highest chunk with a `hot:chunk:*` key. It is the only writer of that chunk's hot DB and the creator of each chunk's `hot:chunk:{chunk}` key (via `openHotDBForChunk` at the boundary).
- **The lifecycle goroutine owns everything below the live chunk** — handed-off hot DBs (freeze + discard), all `chunk:*` and `index:*` artifact keys, and the deletion side of `hot:chunk:*` keys.

**The two goroutines share no state.** Their only connection is the payload-free doorbell, and the partition itself is encoded in the catalog: the lifecycle's derivation treats the highest hot key as the live chunk and touches only what lies below it. The handoff fence is the boundary's write order — the ingestion loop closes its write handle *before* creating the next chunk's hot key. Creating that key is the act that moves the partition: the instant it exists, the closed chunk lies below the live chunk and any lifecycle scan (including one already in flight from the previous notification) may freeze and discard it — by which point no writer holds it. The two goroutines never write the same meta-store key, and never touch the same per-chunk hot RocksDB instance; both do write the meta store concurrently — on disjoint keys, relying on RocksDB's thread safety for the instance itself. The derivation is monotonic within the run (hot keys and frozen keys only advance), so a tick racing a boundary only under-approximates eligibility — work deferred to the next tick, never incorrect work. Readers hold their own read-only handles and resolve files through meta-store keys, so writer-side activity never races them. (The serving side will also need a notion of current progress — the [reader retention contract](#reader-retention-contract) bounds every read by the retention window — but how readers obtain it is the query-routing design's concern, not this doc's.)

### One boundary, end to end

Ledger 53,510,001 closes chunk 5350 (window 5, floor at chunk 5100, frozen index covering chunks 5100–5349):

```
ingestion   batch for seq 53_510_001 commits (one fsync)
            hotDB.Close()                        ← chunk 5350 handed off
            open chunk 5351's hot DB (hot:chunk:00005351 = "ready")
                                                 ← chunk 5350 now visibly complete:
                                                   it sits below the highest hot key
            notify ──────────────────────────────────────────────────┐
lifecycle   deriveCompleteThrough → 53_510_001 (positional term)     ▼
            plan-and-execute:
              resolve → Plan{ChunkBuild 5350, IndexBuild w5 [5100,5350]}
              ChunkBuild 5350: catchupSource picks the hot DB (ready,
                complete) → .pack, events segment, .bin all "frozen"
                (the hot DB itself stays)
              IndexBuild w5 (waited on 5350's done-channel):
                put index:00000005:00005100:00005350 = "freezing"
                merge .bin[5100..5350] → write 00005100-00005350.idx
                → fsync → commit batch {[5100,5350] → "frozen",
                                        [5100,5349] → "pruning"}
                → eager sweep: unlink 00005100-00005349.idx → fsyncDir
                               → delete key
            discard stage:  index covers 5350 → discard chunk 5350's hot DB
            prune stage:    nothing left (the eager sweep already ran; floor
                            pinned at 5100 by earliest_ledger, so it doesn't
                            slide this tick)
reads       tx in 5350: hot DB's txhash CF until the discard, .idx after
            tx in 5351: hot DB of the new live chunk
```

Every arrow is the one write protocol or its exit sweep; at the end of the tick a re-plan and re-scan find nothing to do.

---

## Reader retention contract

A read for any seq below `effectiveRetentionFloor` returns *not found*, regardless of whether the underlying file still exists on disk. This is the contract that lets pruning remove chunks the moment they pass retention **without coordinating with the index lifecycle**: a stale `.idx` may resolve a tx-hash to a `.pack` that's been deleted, but a below-floor read is not-found regardless. From the storage layer's perspective, retention is the single source of truth for "is this data available?", and it is all the prune and sweep stages rely on.

How the reader actually dispatches between hot DBs and frozen `.idx` files, and how it stays correct while sweeps and pruning unlink files concurrently with in-flight reads (tier dispatch, coverage re-resolution, the file-vanishes-mid-read cases), is the **query-routing design's** concern — out of scope here and in the transactions design (§8.4).

---

## Correctness

This section states what the streaming workflow guarantees, the assumptions it relies on, and the operator actions and crash timings the design covers.

### Invariants

The **retention window** is `[effectiveRetentionFloor, last_committed_ledger]`. The floor serves *retention* consumers only — pruning and the reader gate, where erring low is safe; it is never a production boundary (the plan ranges that produce data start at existing storage in the tick, and at a validated floor in catch-up). A future floor consumer picks its side by the err-direction test: if that consumer erring low would be dangerous, it is a production consumer and belongs behind catch-up's validation. **Quiescence** means the tick's plan is empty and both scans produce empty op lists.

**INV-1 (read correctness).** Any data request whose ledger scope falls entirely within the retention window returns correct results — content matches what a conformant LedgerBackend would produce, no partial state is visible, no in-retention range is unreachable. One transient exception mirrors INV-2's: after hot-volume loss, the floor (anchored on `completeThrough`) regresses with the lost completeness, so for the minutes until re-ingestion re-advances it, the window's *bottom* admits a few chunks that were already pruned under the pre-loss floor — those reads fail soft via the reader's missing-data-file rule (not-found, never wrong data: artifacts are write-once and pruning only unlinks) and the gap closes as the floor re-advances.

**INV-2 (single canonical state).** The meta-store records one home for each data range:
- **at most one `"frozen"` index key per window — at all times**, quiescent or not (the commit batch promotes and demotes in one write; this is what makes "the window's index" well-defined for readers);
- at quiescence, no artifact key anywhere is `"freezing"` or `"pruning"` — index transients are swept by the tick that observes them; per-chunk `"freezing"` keys are repaired by re-materialization (the plan stage, for chunks within `[floor, completeThrough]`, from whichever source `catchupSource` selects) and `"pruning"` keys are finished by the sweeps. One reachable exception: after hot-volume loss combined with a lagging bulk-backend tip, a partially-frozen chunk *above* the derived watermark can hold `"freezing"` keys at served quiescence — it lies outside every plan range (above `completeThrough`), and its ledgers exist nowhere any source can reach — until re-ingestion replays the chunk minutes later; it sits outside the retention window throughout, so no read can observe it;
- hot DB keys add one tolerated in-flight transient: `"transient"` brackets a directory operation in progress (the boundary's `openHotDBForChunk`, startup's resume-chunk open, a discard mid-op) and can be observed while the lifecycle sits idle between ticks; a crash-left bracket is finished by the next `openHotDB` or discard scan;
- at quiescence, no `hot:chunk:c` key for a chunk `c` whose artifacts are all durable *and* whose window's index covers `c` (the chunk is fully served by cold artifacts, so the hot DB must be gone);
- at quiescence, no `chunk:c:txhash` key for a chunk `c` in a window whose frozen index key is terminal (the terminal commit demoted them; the sweep removed them; the prune scan's redundant-input branch demotes any that a crashed widening re-froze or left mid-freeze).

**INV-3 (disk matches meta-store).** At quiescence, the set of artifact files and hot DB directories on disk equals exactly the set the meta-store specifies. Every key in a final state names exactly one expected path; the disk holds those paths and no others — no orphan files, no dangling keys, no duplicate artifacts. By INV-2 every artifact key at served quiescence *is* in a final state — the hot-key `"transient"` bracket around an in-flight directory operation is the one tolerated exception — so the correspondence is exact, with no tolerance carve-outs for artifacts: a non-key-named file in an index window dir is a real bug, not mid-tick debris.

**INV-4 (retention bound).** At quiescence, no file or meta-store key maps to a ledger range strictly below the effective retention floor.

Each invariant has a distinct audit. INV-1 you check by issuing reads or by re-deriving artifacts and byte-comparing. INV-2 you check by walking meta-store keys and cross-checking forbidden co-existence. INV-3 you check by walking the filesystem against the meta-store. INV-4 you check by walking meta-store keys against the floor. None of the invariants reference the phase scans that maintain them — so a bug in any scan shows up as a real invariant violation, not as something the buggy code silently considers acceptable. Quiescence between ticks makes these walks meaningful on a live daemon, so an `audit` admin command can implement them directly (with an optional deep mode that re-derives sampled artifacts via a conformant LedgerBackend and byte-compares, for INV-1).

### Convergence

From any storage state — partial-completion crashes, the state left after an operator action, the state left after surgical recovery — **startup** (the catch-up pass, then the first lifecycle tick, rung at ingestion start) drives the system to a quiescent state satisfying INV-1 ∧ INV-2 ∧ INV-3 ∧ INV-4 within the first tick (typically seconds after serving opens; bounded by that tick's freeze, rebuild, and prune workload). From any state reachable *during* a run, the lifecycle tick alone does, within a bounded number of ticks — and since runtime op failure aborts the daemon, every state a run can leave behind is one startup is built to converge.

The split matters because some repairs are inherently catch-up's, not the tick's: a per-chunk `"freezing"` key with no hot DB behind it (a crashed catch-up write) is repaired by re-materialization, and a surgically removed range is re-derived from the LedgerBackend — no tick phase produces data. The tick's province is everything else: index transients, demotions, freezes from live hot DBs, prunes.

Convergence rests on three properties shared by the resolver and the scans — eligibility is computed from durable meta-store state alone; ops are idempotent; everything is re-derived on every notification — plus catch-up's postcondition contract. Together, whatever a crash leaves half-done, the next tick or the next startup finishes.

### Substrate assumptions

Properties we rely on the underlying storage to provide:

- **Sync WAL.** All meta-store puts and deletes that the invariants depend on use RocksDB's `WriteOptions.sync = true`, which fsyncs the WAL before the write returns. Multi-key commits — the index commit batch, the sweeps' key-delete batches — are single atomic synced WriteBatches: all-or-nothing across keys.
- **Per-ledger durability.** The chunk hot DB's synced WriteBatch (atomic across all CFs) is the sole per-ledger durability boundary; the watermark is derived from it, so no cross-store ordering exists to maintain. Per-artifact: the per-chunk file **and its directory entry** are fsynced before its key flips to `"frozen"`, and an index coverage's `.idx` (and its dir entry) is fsynced before the commit batch freezes its key.
- **Deterministic, idempotent writes.** Re-applying any write produces byte-identical state. Backed by deterministic LCM bytes from any conformant LedgerBackend and a byte-identical streamhash index from byte-identical sorted inputs.
- **Monotonic progress.** Within a process run, ingestion only moves forward (each synced batch extends the last), and the lifecycle's derived `completeThrough` only advances with it (hot keys and frozen keys move forward, never back). Across a crash, the startup derivation equals exactly the durable state — the pre-crash value or marginally above it (a batch that committed in the instant before the crash); it sits *below* the pre-crash value only when hot state was removed or lost, or when surgery demoted keys feeding the cold term (case 3's no-hot-key corner). There is no stored watermark to rewind; surgical recovery shrinks the derivation's inputs by demoting or removing state, not by editing a counter.

### Design invariants

These are streaming-specific properties the implementation guarantees on top of the substrate, and that INV-1 through INV-4 depend on:

- **Every key precedes its file.** The pre-write `"freezing"` mark and post-fsync `"frozen"` flip mean any file on disk — per-chunk artifact or index file, partial or complete — has its meta-store key set. Every scan and sweep iterates keys, so every file is reachable that way; nothing ever lists a directory to find work.
- **Index promotion is atomic and gap-free.** The commit batch freezes the new coverage and demotes its predecessor in one synced write, so the window's unique frozen key changes hands atomically — never two frozen keys, never none once the window has one. A reader following the frozen key always lands on a complete, fsynced index; a crash mid-build leaves the prior coverage frozen and the attempt as `"freezing"` debris that is either overwritten by the next build of that coverage or deleted unread by the sweeps.
- **Key absent ⟹ file gone.** Every sweep's shared ordering (unlink → `fsyncDir` → atomic key delete) gives the exit-side counterpart.
- **Hot DB keys bracket the directory.** The `hot:chunk:{chunk}` key is put (`"transient"`) before the directory is created, and deleted only after rmdir completes — with `"transient"` re-marked first.
- **Tx hashes always have a queryable home.** The hot DB is discarded only after the durable `.idx` covers the chunk — hot CF, then `.idx`, with no gap. (The `.bin` is never a serving tier; it is rebuild input, demoted to `"pruning"` by the terminal commit batch — the same write that freezes the final `.idx` — or by retention pruning once its chunk falls past the floor, and deleted only by the sweep after that.)
- **`"frozen"` ⟹ the file is durable and complete.** Flips to `"frozen"` happen only after fsync, and files are deleted only under non-frozen keys (sweeps demote first) — so frozen keys can be trusted blindly by readers, the resolver, and `buildTxhashIndex`'s precondition check.
- **`"pruning"` is committed.** Once a key is in `"pruning"` — demoted by a commit batch or by retention — the sweep runs to completion on subsequent scans. Catch-up treats any non-`"frozen"` state as empty and overwrites cleanly if the range is re-ingested.

### Scenario coverage

INV-1 holds at every point the daemon is serving reads — transient states are never externally visible, because readers resolve `"frozen"` keys exclusively and the retention check masks everything else. INV-2, INV-3, and INV-4 hold at every quiescence reached after the events below; startup's first quiescence arrives when the first tick completes, shortly after reads open.

1. **Steady-state operation.** Hot DB ingestion advances `last_committed_ledger`; the lifecycle goroutine freezes complete chunks within retention and prunes anything past it. All four invariants hold by induction on `last_committed_ledger`.
2. **Operator state changes** — retention widening or shortening (`retention_chunks`), `earliest_ledger` raised. Both reduce to "`effectiveRetentionFloor` recomputes; the next startup converges to the new state." Catch-up's per-window resolver rule re-derives and rebuilds any window whose desired coverage now exceeds its stored coverage; the prune stage removes anything below a raised floor. The "next startup" is load-bearing for widening, enforced by the floor's two-role split: a lowered floor takes effect immediately in its *retention* role (pruning simply stops sooner), but the tick's *production* range still starts at existing storage — only the next catch-up, behind `validateBackendCovers`, materializes the new bottom.
3. **Surgical recovery, frozen-range case** (tainted range strictly below `chunkID(last_committed_ledger)`). The operator never touches the filesystem. Recovery is **one atomic meta-store batch**: every `chunk:{c}:*` key in the tainted range and every `index:*` key of every window overlapping it → `"freezing"` — the state that already means *this file is not to be trusted: re-derive or delete* — and any leftover `hot:chunk` key in the range (a crash between a tick's build and its discard stage leaves a frozen chunk's hot DB behind) → `"transient"`, which makes it instantly ineligible as a source (`catchupSource` reads only `"ready"`). The batch commits atomically or not at all, so there is no interruption analysis and re-running it is a no-op; the meta store's lock means it can only be written against a stopped daemon. Every demoted key then converges through machinery that already exists: on restart, catch-up re-derives the freezing chunk artifacts from a conformant LedgerBackend — overwriting the tainted files in place, rule 1's ordinary re-materialization — and rebuilds each window's index, re-marking the freezing index key whose coverage is still desired (or leaving it to the prune scan's sweep-on-sight when retention has moved past it); the discard scan retires the transient hot key once the rebuilt chunk regains coverage — or past retention, after long downtime — unlinking the tainted hot DB unread. `last_committed_ledger` is exactly unchanged whenever ingestion has ever started: the batch keeps every hot key, the positional term counts them value-blind, and the live chunk's `"ready"` DB restores the watermark precisely. Only in the no-hot-key corner — the daemon stopped during initial catch-up, before ingestion opened its first hot DB — does the cold term lead the derivation, and there it can regress through every untainted chunk of a finalized window the taint overlaps (their `.bin` keys were swept at finalization, and the demoted index key breaks coverage), bounded by one window; catch-up's `max(tip, lastCommitted)` anchor re-derives forward regardless.
4. **Surgical recovery, partial-tail-chunk case** (tainted range includes the live chunk), and equally **hot-volume loss**. The same artifact-key batch as case 3, but hot keys are **removed**, not demoted — case 3's leftover hot DB holds data that exists elsewhere and sits below the watermark, so a demoted key is harmlessly reclaimed; here the hot DB *was* the data, and a kept key either re-fires the fatal or silently inflates the watermark. Remove **every** `hot:chunk` key whose dir is missing or whose contents are partial. A half-recovery that keeps a `"ready"` key merely relocates the fatal to the next restart. One that keeps the boundary-crash `"transient"` key — the next chunk's key, sitting above the last frozen boundary — is recoverable but worse than compliance: the positional term counts all hot keys while the fatal checks only `"ready"` ones, so the kept key silently props the derived watermark up to the lost chunk's end, pulling the lost chunk into catch-up's anchored range, which re-derives it from the bulk source — stalling startup until that source has the chunk, where full removal would have resumed from the last frozen boundary immediately. Remove any surviving dirs along with the keys — dirs first, keys last: an interrupted pass then leaves keys whose dirs are missing, which the next startup catches (the fatal for `"ready"`, the watermark-prop analysis above for `"transient"`), never a keyless orphan dir no key-driven scan can find. The hot DB is the only copy of its ledgers — discarding it loses them, and the **derived watermark admits as much automatically**: with the hot DB gone, derivation lands at the last frozen boundary, and the next startup re-ingests from there. There is no watermark to edit; recovery is key removal, never file surgery beyond the lost dirs themselves.
5. **First deployment / downtime between restarts.** `last_committed_ledger` derives to `max(frozen/hot maxima, earliest_ledger - 1)`, ensuring `resumeLedger ≥ earliest_ledger`. Backfill fills `[earliest_ledger, lastCompleteChunkAt(network_tip)]` if needed (a no-op for `earliest_ledger = "now"` first deployment).
6. **LedgerBackend choice or mid-flight swap.** The LedgerBackend contract guarantees canonical LCM bytes for any range, so any conformant backend produces byte-identical artifacts. Different backends differ in performance, not behavior. An operator using BSB for backfill and CaptiveCore for hot DB ingestion, or swapping mid-deployment, satisfies all four invariants.
7. **Crash at any point during any of the above.** Sync WAL plus per-ledger durability ordering mean the meta store on next start is internally coherent and the derived watermark equals exactly what the last synced batch committed. Idempotency means re-running any half-finished op is safe. Convergence finishes whatever the crash interrupted.

### What a bug looks like

The invariants describe what storage should look like, not how the phase scans maintain it. So common bugs show up as concrete violations:

- **A meta-store key claims something the file doesn't actually deliver** — e.g., a per-chunk writer flips a key to `"frozen"` before fsync (leaving a partial file the meta store advertises as complete), or an index key freezes before its `.idx` is fully fsynced, or the key name's `{lo, hi}` doesn't match the file's actual coverage, or a frozen file is mutated post-freeze ⟹ reads through the meta key see wrong or missing data. **INV-1** violated. Detectable by re-deriving an artifact via a conformant LedgerBackend and byte-comparing against the on-disk file.
- **Pruning too aggressive** ⟹ a request whose ledger scope is in retention returns wrong or missing results. Issue a read to find it. **INV-1** violated.
- **Two frozen index keys in one window** — a commit batch failed to demote the predecessor, or promotion and demotion landed as separate writes ⟹ readers have no well-defined index. Walk `index:*` keys, count `"frozen"` per window. **INV-2** violated.
- **A `"freezing"` or `"pruning"` key survives served quiescence** ⟹ its recovery mechanism was skipped — an index transient the sweeps should have deleted, a `"pruning"` demotion the sweeps should have finished, or a per-chunk `"freezing"` key that the freeze phase or startup catch-up should have re-materialized. Walk keys for transient values at quiescence. **INV-2** violated.
- **Chunk scan misses an orphan** ⟹ a hot DB persists for a chunk that cold artifacts fully serve. Walk `hot:chunk:c` keys whose chunk has its artifacts durable and its window's index covering `c`. **INV-2** violated.
- **Finalization demotions don't complete** ⟹ per-chunk frozen tx hash files outlive the index that consumed them. Walk `chunk:c:txhash` keys whose window's frozen key has `hi` = the window's last chunk. **INV-2** violated.
- **A writer leaves a file on disk without its meta-store key** (file fsynced before key was durable, or a sweep deleted the key before its unlink was durable) ⟹ orphan file — invisible to every key-driven scan. Walk the filesystem against the meta-store. **INV-3** violated.
- **A meta-store key persists without its file** (file deleted before key) ⟹ dangling key. Walk the meta-store against the filesystem. **INV-3** violated.
- **Duplicate cold artifacts for the same logical data** (e.g., two events files for the same chunk, from a migration or buggy retry) ⟹ the meta-store names one expected path; the extras are orphans. Walk the filesystem against meta-store-specified paths. **INV-3** violated.
- **Pruning fails past the floor** ⟹ files or keys remain for ranges below `effectiveRetentionFloor`. Walk meta-store keys, compare ledger ranges to the floor. **INV-4** violated.

A storage walk against the invariants is enough to find these without inspecting the phase implementations.

---

## Related documents

- The transactions design ([gettransaction-full-history-design.md](./gettransaction-full-history-design.md)) — the tx-by-hash subsystem end to end: the hot `txhash` CF, the `.bin`/`.idx` formats, the rolling window index build protocol with its pseudocode and safety arguments, the `getTransaction` read path, and the capacity numbers. Canonical for everything rules 3 and 5 and the index-key section summarize.
- The events design ([getevents-full-history-design.md](./getevents-full-history-design.md), PR #635) — the cold-segment file formats and the hot events CF schema referenced by the data model.
- The reader / query-routing design — how reads dispatch between hot DBs and frozen files for in-retention queries.
- The original backfill workflow design ([../full-history/design-docs/03-backfill-workflow.md](../full-history/design-docs/03-backfill-workflow.md)) — the standalone backfill mode, **subsumed by this document**: its `process_chunk`, tx-hash index build and cleanup, geometry, configuration, directory layout, meta-store keys, crash recovery, and `getStatus` are all redefined here (and in the transactions design) in their current form. It is retained for history; where the two disagree, this document is current. In particular, that doc predates the 2026-06 streamhash redesign, so its 16-CF RecSplit `.idx` files, unsorted 36-byte `.bin` entries, `"1"`-valued meta keys, task-DAG scheduler, and standalone `full-history-backfill` CLI are all superseded.
