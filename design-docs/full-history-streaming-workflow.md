# Streaming Workflow

## Overview

Full-history RPC runs as one daemon in one mode: it both backfills old history and follows the live network.

It keeps two tiers of data. **Hot** data is the most recent ledgers near the network tip, written append-only into RocksDB. **Cold** data is older ledgers, held as immutable files on disk. On startup RPC backfills to the current tip, then ingests new ledgers continuously into the hot DB; when the hot DB fills, it writes the immutable cold files for that ledger range and discards the hot DB. This migration from hot to cold is called **freezing**.

The daemon does three things:

- **Backfills on startup.** Before it serves anything, it runs backfill as a subroutine to bring what's on disk in line with the current retention window. It pulls every chunk inside that window that isn't already frozen from a configured bulk source: an object-store lake read through BSB (the Buffered Storage Backend) when one is configured, otherwise captive core replaying from the history archives. It skips the partial chunk still forming at the tip; hot-DB ingestion fills that one once it starts. This single mechanism covers a first-ever start, gaps left by downtime, and gaps opened by widening retention.
- **Ingests** live ledgers from `CaptiveStellarCore` into one hot RocksDB per chunk — ledgers, transaction hashes, and events as column families, written in one atomic batch per ledger.
- **Freezes** completed chunks to immutable files, **rebuilds** the current tx-hash index from its frozen inputs on every chunk boundary, and **prunes** superseded and past-retention artifacts. All run in a background lifecycle goroutine.

---

## Geometry

The Stellar blockchain starts at ledger 2 (`GENESIS_LEDGER`). Two units organize all storage; everything in this doc is described in terms of them:

- **Chunk** — a run of 10,000 ledgers (hardcoded); the atomic unit of ingestion, freezing, and crash recovery. A hot DB holds at most one chunk, and each cold file — ledgers, events, transactions — spans exactly one chunk.
- **Window** — 1,000 chunks (10M ledgers); the unit of the rolling tx-hash index. The index is the one exception to the per-chunk rule: it maps transaction hashes to ledger sequences across a whole window.

```
chunkID(seq)         = floor((seq - 2) / 10_000)
chunkFirstLedger(c) = c * 10_000 + 2
chunkLastLedger(c)  = (c + 1) * 10_000 + 1
indexID(c)          = c / 1000                           # takes a CHUNK id
```

Every chunk id written to disk is `≥ 0`. Startup arithmetic also uses one signed id, **chunk −1**, meaning "before the first chunk": the value of `lastCompleteChunkAt` when no chunk is complete yet (`chunkLastLedger(-1) = 1` maps back). A daemon whose floor is pinned above genesis never produces it: its "nothing ingested yet" base `earliest_ledger - 1` maps to the ordinary chunk just below the floor.

All chunk and window ids use uniform `%08d` zero-padding. Example (window = 1,000 chunks):

| Window | First ledger | Last ledger | Chunks |
|---|---|---|---|
| 0 | 2 | 10,000,001 | 0–999 |
| 1 | 10,000,002 | 20,000,001 | 1000–1999 |
| N | N×10M + 2 | (N+1)×10M + 1 | N×1000 – (N+1)×1000−1 |

---

## Configuration

One TOML file configures the daemon, passed as `--config` to the v2 binary (`stellar-rpc-v2 --config <path>`). Decoding is strict: an unknown key or section is a startup error, so a stale or mistyped config fails loudly instead of being half-read. A fully-commented sample lives at `cmd/stellar-rpc/rpcv2/rpc-v2-sample-config.toml`.

Every TOML leaf is also settable from the command line:

- One flag per leaf, named by its dotted TOML path: `--storage.default_data_dir`, `--service.methods.getLedgers.queue_limit`.
- The flag set is derived from the config structs by reflection, so it can never drift from the file schema.
- Precedence: specificity beats source; within a tier, a set flag beats the file; compiled defaults are the last tier.
- `--config` stays required — the file is the source of truth, flags are one-off overrides.
- No environment variables.

**[service]** — the JSON-RPC read-serving policy (#882; entirely dormant today — the read server arrives with #772, and #881 wires `[service.fee_stats]` into live ingestion). Key naming: camelCase only for the JSON-RPC method table names, snake_case elsewhere. Durations are strings (`"10s"`); any duration under 1ms is rejected (a bare TOML integer parses as nanoseconds).

| Key | Type | Default | Description |
|---|---|---|---|
| `endpoint` | string | `localhost:8000` | JSON-RPC listen address. |
| `admin_endpoint` | string | `""` (disabled) | pprof/metrics endpoint, plaintext HTTP. |
| `max_concurrent_requests` | uint | `5000` | HTTP-layer gate: bounds total in-flight requests across ALL methods, in addition to per-method limits. |
| `max_request_execution_duration` | duration | `"25s"` | HTTP-layer global timeout. |
| `request_execution_warning_threshold` | duration | `"5s"` | Slower requests log a warning. |

**[service.fee_stats]** — sizes (in ledgers, 1..1000) of the in-memory fee windows behind getFeeStats; they will be fed by live ingestion (#881), and size ingestion-time memory rather than request handling, hence not method config:

| Key | Default |
|---|---|
| `classic_fee_window_ledgers` | `10` |
| `soroban_inclusion_fee_window_ledgers` | `50` |

**[service.methods.\<methodName\>]** — one table per served method (getHealth, getNetwork, getVersionInfo, getLatestLedger, getTransaction, getTransactions, getLedgers, getEvents, getFeeStats):

- Every method: `queue_limit` and `max_execution_duration`. v1's defaults: 1000 / 5s, except getFeeStats's queue is 100, and getLedgers/getEvents get 10s.
- The three paginated methods (getTransactions, getLedgers, getEvents) add `max_items_per_response` / `default_items_per_response`.
- getHealth adds `max_healthy_ledger_latency` (default `"30s"`).
- Bare `queue_limit` / `max_execution_duration` keys directly on `[service.methods]` form an optional methods-wide default tier: per-method value → wide default → compiled default.
- Not here: sendTransaction, simulateTransaction, getLedgerEntries and the preflight knobs — they arrive with the captive-core-endpoints work.

**[retention]** — the two inputs to the retention floor; the effective floor is the higher of the two:

| Key | Type | Default | Description |
|---|---|---|---|
| `retention_chunks` | uint32 | `0` | Retention window in chunks. `0` = full history. |
| `earliest_ledger` | uint32 \| `"genesis"` \| `"now"` | `"genesis"` | Earliest ledger this daemon will ever have data for — a fixed lower floor on history. Must be chunk-aligned; `"now"` resolves to the backfill backend's frontier chunk at first start. Resolved and pinned on the first start (a reachable backend is required, to resolve `"now"` and to reject a numeric floor past the tip; see `validateConfig`), immutable thereafter. Setting it above genesis typically skips upfront backfill — useful when no fast backfill source is available and the daemon only follows the live network (`earliest_ledger = "now"`). |

**[storage]** — the data root plus one optional path per on-disk tree; an unset per-store key defaults under `{default_data_dir}`:

| Key | Default path | Holds |
|---|---|---|
| `default_data_dir` | **required** | base directory for the catalog and default storage paths (moved here from `[service]` in #882) |
| `catalog` | `{default_data_dir}/catalog/rocksdb` | the catalog RocksDB |
| `ledgers` | `{default_data_dir}/ledgers` | `.pack` files |
| `events` | `{default_data_dir}/events` | events cold segments |
| `txhash_raw` | `{default_data_dir}/txhash/raw` | transient `.bin` files |
| `txhash_index` | `{default_data_dir}/txhash/index` | per-window `.idx` |
| `hot` | `{default_data_dir}/hot` | per-chunk hot RocksDB databases |

**[backfill]**

| Key | Type | Default | Description |
|---|---|---|---|
| `workers` | int | `GOMAXPROCS` | Concurrent task slots for backfill. |
| `max_retries` | int | `3` | Retries per backfill task, after the first attempt, before the task fails the run. |

**[backfill.datastore]** — the bulk ledger source (`type`, `params`, `schema.ledgers_per_file`, `schema.files_per_partition`; key names match the SDK's `datastore.DataStoreConfig`):

- Any SDK datastore (GCS, S3, Filesystem) works. Optional: an empty `type` means no lake, and backfill replays through captive core from the history archives instead.
- The network passphrase is deliberately not a key here: the daemon copies it from the captive-core file at startup, and when the lake carries a manifest the SDK verifies the lake was exported from the same network — a wrong-network lake fails startup. A manifest-less lake skips the check.

**[backfill.bsb]** — tuning for the buffered-storage stream that downloads ledger objects from the datastore. Optional:

| Key | Type | Default | Description |
|---|---|---|---|
| `buffer_size` | uint32 | `100` | Downloaded ledgers buffered in memory; >= 1. |
| `num_workers` | uint32 | `10` | Concurrent object downloads; >= 1. |
| `max_retries` | uint32 | `3` | Retries of ONE object download after a transient error; `0` = fail on the first error. Distinct from `[backfill].max_retries`, which re-runs a whole chunk task. |
| `retry_wait` | duration | `"5s"` | Pause between retries of one object download; >= 1ms. |

**[ingestion]** — the live-network (captive core) settings:

| Key | Type | Default | Description |
|---|---|---|---|
| `captive_core_config` | string | **required** | Path to the CaptiveStellarCore config file. Must define `NETWORK_PASSPHRASE`. |
| `history_archive_urls` | []string | **required** | History-archive URLs for the SDK's archive client. Not derivable from the captive-core file, whose `[HISTORY.*]` entries are shell commands. |
| `stellar_core_binary_path` | string | `stellar-core` on `PATH` | Path to the stellar-core binary. |
| `captive_core_storage_path` | string | `{default_data_dir}/captive-core` | Captive core's working directory. |

**[logging]** — optional `level` (`debug`/`info`/`warn`/`error`, default `info`) and `format` (`text`/`json`, default `text`).

**CLI**

| Flag | Type | Default | Description |
|---|---|---|---|
| `--config` | string | **required** | Path to TOML config file. |
| `--<section>.<key>` | per key | — | One auto-derived override flag per TOML leaf (see above). |

---

## Data model

The daemon's durable state lives in two places. The **catalog** — a small RocksDB — records what's on disk and the state each file is in, plus a few config values fixed on the first start. The **filesystem** holds the data itself: the immutable cold files, and one per-chunk hot RocksDB for data still being ingested.

Throughout this section, `chunk` is a chunk id and `txhash_index` is a window id.

### Filesystem artifacts

The per-chunk artifacts are each written once at chunk freeze; the txhash index is rebuilt on each chunk boundary while its window is current and then finalized. All four are produced by [the primitives](#the-primitives):

| Artifact | Granularity | Format | Produced by |
|---|---|---|---|
| Ledger pack file | per chunk | `.pack` | `processChunk` |
| Events cold segment | per chunk | three files per chunk (format defined in the events doc) | `processChunk` |
| Sorted txhash file | per chunk | `.bin` (sorted **streamhash** entries — the sorted on-disk tx-hash index format, specified in [the transactions design](./gettransaction-full-history-design.md) §6) | `processChunk` |
| Streamhash txhash index | per index | one `.idx` file per **coverage** (the chunk range `[lo, hi]` an index spans), named `{lo:08d}-{hi:08d}.idx` inside the window's dir; at most one coverage frozen at any moment | `buildTxhashIndex` |

The `.bin` files are transient — they are the input `buildTxhashIndex` merges, and the terminal build deletes them once its window is complete (or retention pruning removes them first, once its chunks drop below the floor). The pack files, events segments, and `.idx` files persist until retention pruning removes them. State for each lives in [Catalog keys](#catalog-keys); the write ordering is [One write protocol](#one-write-protocol).

### Directory layout

Chunk-level files group into buckets of 1,000 chunks (`bucket_id = chunk_id / 1000`, formatted `%05d`) — a filesystem concern only; bucket ids never appear in catalog keys. Directories are created on demand.

```
{default_data_dir}/
├── catalog/rocksdb/                                  ← catalog (WAL always on)
├── hot/{chunk:08d}/                               ← per-chunk hot RocksDB (transient)
├── ledgers/{bucket:05d}/{chunk:08d}.pack
├── events/{bucket:05d}/{chunk:08d}-events.pack    (+ -index.pack, -index.hash)
└── txhash/
    ├── raw/{bucket:05d}/{chunk:08d}.bin           ← transient until window finalization (or retention pruning)
    └── index/{window:08d}/{lo:08d}-{hi:08d}.idx   ← one frozen file per window, coverage-named
```

### The chunk hot DB

During ingestion the daemon maintains **one hot RocksDB per chunk** at `{storage.hot}/{chunk:08d}/`, holding everything for that chunk not yet materialized to cold artifacts. The data types are column families of the one instance:

| Column family | Holds | Serves |
|---|---|---|
| `ledgers` | compressed LCMs (LedgerCloseMeta), keyed by seq | `getLedger` for the live chunk; the source `processChunk` reads at freeze |
| `txhash` | tx hash → seq | `getTransaction` for the live chunk |
| events CFs | live events (schema per the events doc) | `getEvents` for the live chunk |

CFs share the instance's WAL, so each ledger commits as **one atomic WriteBatch across all CFs**. Per-CF options keep tuning independent (the events CFs carry their own settings). The DB is created when ingestion enters the chunk. It is discarded whole once every cold artifact derived from the chunk is durable **and** the rolling index covers the chunk. It keeps serving tx lookups across the brief freeze-to-coverage interval; freeze, rebuild, and discard all chain within one lifecycle run.

### Catalog keys

The catalog holds three groups of keys: per-chunk artifact state keys, hot DB state keys, and the config pin.

**Artifact state keys**:

| Key | Value | Meaning |
|---|---|---|
| `chunk:{chunk:08d}:ledgers` | `"freezing"` \| `"frozen"` \| `"pruning"` | Per-chunk pack file state. |
| `chunk:{chunk:08d}:txhash` | `"freezing"` \| `"frozen"` \| `"pruning"` | Per-chunk `.bin` file state. Transient — removed at window finalization, or by retention pruning if its chunk ages out first. |
| `chunk:{chunk:08d}:events` | `"freezing"` \| `"frozen"` \| `"pruning"` | Per-chunk events cold segment state. |
| `index:{txhash_index:08d}:{lo:08d}:{hi:08d}` | `"freezing"` \| `"frozen"` \| `"pruning"` | One key per index **coverage**. The key *name* carries the coverage `[lo, hi]` and maps 1:1 to the file `{lo:08d}-{hi:08d}.idx`; the *value* is pure lifecycle state — the same three values as every other artifact key. At most one coverage per window is `"frozen"` at any moment, and a key with `hi` = its window's last chunk is **terminal** by definition (see [Index keys](#index-keys) below). |

For the per-chunk keys, `"freezing"` means the immutable file is being written; `"frozen"` means it's fsynced and durable; `"pruning"` means the file is queued for removal; key absent means neither file nor in-progress write exists. Index keys use the **same three states with the same meanings** — a rebuild marks its coverage `"freezing"` before writing the file, and its commit batch flips it to `"frozen"` while demoting the superseded coverage to `"pruning"`. Every artifact key therefore obeys one set of crash rules: `"freezing"` = delete (or re-derive) the file, `"pruning"` = finish the delete, `"frozen"` = truth.

**Hot DB state key**:

| Key | Value | Tracks |
|---|---|---|
| `hot:chunk:{chunk:08d}` | `"transient"` \| `"ready"` | The chunk's hot DB. |

`"ready"` means the RocksDB dir exists and is usable. `"transient"` brackets a directory operation in flight — creation or deletion; no code path ever needs to know which, since the recovery is the same either way (the open path wipes and recreates; the discard scan re-runs). A crash mid-operation is detectable from the key value alone. One key per chunk; the column families inside the DB carry no individual catalog state.

**Config pin:**

| Key | Value | Written when |
|---|---|---|
| `config:earliest_ledger` | `uint32` (decimal string, chunk-aligned) | On the first daemon start. Immutable thereafter — changing it currently requires wiping the data directory, until a `set-earliest-ledger` admin command exists (see [Configuration](#configuration); the floor machinery already converges for either direction). |

**Resume point.** Recomputed at startup from the durable keys plus a read of the live hot DB (see [Startup](#startup)).

### Index keys

An index key `index:{txhash_index:08d}:{lo:08d}:{hi:08d}` names the chunk range `[lo, hi]` that its `.idx` covers, mapping 1:1 to the file `txhash/index/{txhash_index:08d}/{lo:08d}-{hi:08d}.idx`.

`hi` grows as the window fills: each rebuild folds in the chunks frozen since the last one, advancing `hi` — by one in steady state, by many when catching up. When `hi` reaches the window's last chunk, the window is **complete** and its index is **terminal** — rebuilt again only if retention widening later drops the floor into the window, when backfill re-derives the spent `.bin` inputs from the local packs and rebuilds the index wider.

`lo` is the higher of the window's first chunk and the retention floor, fixed when the index is built. So:

- a window still being rebuilt each boundary has its `lo` recomputed every time, so it rises as the floor does, dropping chunks that have aged out of retention;
- a terminal window's `.idx` keeps the `lo` it was built with; if the floor later climbs past that `lo`, the index still covers chunks that have dropped out of retention — but a read for any ledger below the floor returns not-found regardless of what the index says, so that stale coverage is never served.

So `lo` equals the window's first chunk unless the start of the window has dropped below the floor.

[The transactions design](./gettransaction-full-history-design.md) (§6.3) is canonical for coverage semantics, with a worked example.

### One write protocol

Every durable artifact — per-chunk files and index coverages alike — is written the same way, **mark-then-write**:

1. put `"freezing"` *before* the file is written;
2. write the file;
3. fsync the file and the directory entries naming it;
4. flip the key to `"frozen"`.

The key is always written before the file. So every file can be found from its key — cleanup walks keys, never directories — and a file left half-written by a crash carries a `"freezing"` key, which marks it for re-derivation or removal. Step 3 fsyncs the directory entries, not just the file, so the file's existence on disk survives a crash before its key flips to `"frozen"`.

Deletion is the same protocol in reverse: demote the key to `"pruning"`, unlink the file, then delete the key, with an `fsyncDir` between the unlink and the key delete. So a key is gone only once its file is — **key absent ⟹ file gone** — and no file is ever unlinked under a `"frozen"` key. One sweep per key family (per-chunk artifacts, index coverages) deletes every committed artifact; a writer that fails mid-write removes its own partial output under the `"freezing"` key it still holds.

---

## Backfill

Backfill makes every artifact derived from a range of ledgers durable and servable. It has three parts, in the order below: a **resolver** (`resolve`) that diffs what's wanted against the catalog and returns a plan of the missing work; the **primitives** (`processChunk`, `buildTxhashIndex`) that produce each artifact; and an **executor** (`executePlan`) that runs the plan concurrently. The [Startup](#startup) backfill loop and the [Lifecycle](#lifecycle) run are its two callers.

### Postcondition-driven planning

Backfill works from a postcondition: *given a range, every artifact derived from every ledger in it must be durable and servable.* `resolve` reads the catalog and returns a `Plan` of only the missing work — per-chunk artifacts whose key isn't `"frozen"`, and window indexes whose frozen coverage doesn't yet span the range. It reads nothing but durable keys, so every run re-plans from what's on disk; a restart neither redoes finished work nor skips unfinished work. The plan is a flat list of chunk builds and index builds:

```go
type ChunkBuild struct {
	Chunk     ChunkID
	Artifacts ArtifactSet // which kinds this chunk still needs — one processChunk pass produces all
}

type IndexBuild struct {
	Window WindowID
	Lo, Hi ChunkID // coverage to build; terminal iff Hi == windowLastChunk(Window)
	// dependencies are derivable (the ChunkBuilds in [Lo, Hi]), so no input list
}

type Plan struct {
	ChunkBuilds []ChunkBuild
	IndexBuilds []IndexBuild
}

// resolve returns the work missing for [rangeStart, rangeEnd].
func resolve(cfg Config, rangeStart, rangeEnd ChunkID) Plan {
	if rangeEnd < rangeStart {
		return Plan{} // young network: no complete chunk yet
	}
	cat := cfg.Catalog
	needs := map[ChunkID]ArtifactSet{}

	for c := rangeStart; c <= rangeEnd; c++ {
		for _, kind := range []Kind{Ledgers, Events} {
			if cat.State(c, kind) != Frozen {
				needs[c] = needs[c].Add(kind)
			}
		}
	}

	var builds []IndexBuild
	for _, w := range windowsOverlapping(rangeStart, rangeEnd) {
		desired := Range{
			Lo: max(windowFirstChunk(w), rangeStart),
			Hi: min(windowLastChunk(w), rangeEnd),
		}
		if frozenCoverage(cat, w).Covers(desired) {
			continue
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
```

### The primitives

`processChunk` writes a chunk's requested artifacts through the [one write protocol](#one-write-protocol), reading ledgers from `backfillSource`. Its hot-DB branch is what lets the lifecycle freeze a just-closed chunk from its own hot DB, on the same path as a cold backfill.

```go
func processChunk(cfg Config, chunk ChunkID, artifacts ArtifactSet) error {
	cat := cfg.Catalog
	source, err := backfillSource(cfg, chunk, artifacts)
	if err != nil {
		return err
	}

	batch := cat.NewBatch() // mark "freezing" before the writes
	for _, kind := range artifacts.Kinds() {
		batch.Put(chunkKey(chunk, kind), "freezing")
	}
	batch.Commit()

	w := newArtifactWriters(chunk, artifacts)
	for seq := chunkFirstLedger(chunk); seq <= chunkLastLedger(chunk); seq++ {
		w.Add(source.GetLedger(seq))
	}
	w.Finish()
	w.FsyncAll() // durable before the keys flip to "frozen"

	batch = cat.NewBatch()
	for _, kind := range artifacts.Kinds() {
		batch.Put(chunkKey(chunk, kind), "frozen")
	}
	batch.Commit()
	return nil
}

// backfillSource picks a chunk's ledger source in a fixed preference order. The
// hot branch errors only when a "ready" hot DB won't open — its data is lost.
// An incomplete-but-present DB is just stale: it falls through to the next
// source, which re-derives the chunk and recovers it.
func backfillSource(cfg Config, chunk ChunkID, artifacts ArtifactSet) (LedgerSource, error) {
	cat := cfg.Catalog
	if state, _ := cat.Get(hotChunkKey(chunk)); state == "ready" {
		db, err := openRocksDBReadOnly(hotChunkPath(chunk))
		if err != nil {
			return nil, fmt.Errorf("hot DB for chunk %d is ready but won't open: %w", chunk, err)
		}
		if maxCommittedSeq(db) >= chunkLastLedger(chunk) {
			return &HotLedgers{chunk: chunk, store: db}, nil
		}
		db.Close() // incomplete: stale leftover — close and fall through; the discard scan owns it
	}
	if cat.State(chunk, Ledgers) == Frozen && !artifacts.Has(Ledgers) {
		return packReader(chunk), nil // re-derive locally
	}
	// Backfill backend: the only source for a chunk with no local copy. If its
	// tip lags below this chunk, wait for coverage.
	waitForBackendCoverage(cfg, chunk) // bounded; a timeout fails the task
	return backfillBackend(cfg), nil    // BSB by default
}
```

**`buildTxhashIndex(w, lo, hi, cat)`** rebuilds window `w`'s index to cover chunks `[lo, hi]` — `lo` the lowest in-floor chunk, `hi` the highest frozen chunk (the window's last once the window is complete). The lifecycle calls it on every chunk boundary while the window is current.

```go
func buildTxhashIndex(w WindowID, lo, hi ChunkID, cat Catalog) error {
	prev := frozenCoverage(cat, w)
	if prev != nil && prev.Lo == lo && prev.Hi == hi {
		return nil // already built (e.g. a buildThenSweep retry re-entering after the commit)
	}

	key := indexKey(w, lo, hi)
	cat.Put(key, "freezing") // mark before the writes

	sb := streamhash.NewSortedBuilder(indexFilePath(key))
	for entry := range kWayMerge(binFiles(lo, hi)) { // sorted .bin files → one stream
		sb.Add(entry)
	}
	sb.Finish()
	fsyncFile(indexFilePath(key))
	fsyncDirs(indexFilePath(key)) // the dirent chain — incl. a just-created window dir — durable before the key freezes

	batch := cat.NewBatch() // one atomic synced write — the whole finalization
	batch.Put(key, "frozen")
	if prev != nil {
		batch.Put(indexKey(w, prev.Lo, prev.Hi), "pruning") // demote predecessor
	}
	if hi == windowLastChunk(w) { // terminal: the merged .bin inputs are spent
		for c := lo; c <= hi; c++ {
			batch.Put(chunkKey(c, TxHashBin), "pruning")
		}
	}
	batch.Commit()
	return nil
}
```

`kWayMerge` and `SortedBuilder` are streamhash internals, covered in [the transactions design](./gettransaction-full-history-design.md) (§6–§7). A coverage containing no transactions builds a valid **empty** index — a zero-key `.idx` is an ordinary artifact, and lookups against it simply miss.

### Execution model

`executePlan` runs a plan from either caller — startup backfill or the [lifecycle run](#lifecycle). Chunk builds run concurrently under one worker semaphore; each index build waits on the done-channels of the chunk builds inside its coverage, then runs.

```go
func executePlan(ctx context.Context, cfg Config, plan Plan) error {
	slots := make(chan struct{}, cfg.Workers) // the only concurrency knob
	done := make(map[ChunkID]chan struct{}, len(plan.ChunkBuilds))
	for _, cb := range plan.ChunkBuilds {
		done[cb.Chunk] = make(chan struct{})
	}

	g, gctx := errgroup.WithContext(ctx)
	for _, cb := range plan.ChunkBuilds {
		g.Go(func() error {
			slots <- struct{}{}
			defer func() { <-slots }()
			if err := withRetries(gctx, cfg.MaxRetries, func() error {
				return processChunk(cfg, cb.Chunk, cb.Artifacts)
			}); err != nil {
				return err // leave done[cb.Chunk] open; the error cancels gctx, freeing waiters
			}
			close(done[cb.Chunk]) // success: dependents may now read this chunk's .bin
			return nil
		})
	}
	for _, b := range plan.IndexBuilds {
		g.Go(func() error {
			for c := b.Lo; c <= b.Hi; c++ { // wait on the in-coverage chunk builds
				if ch, ok := done[c]; ok {
					select {
					case <-ch: // this chunk's .bin is frozen
					case <-gctx.Done(): // a build failed (or cancel) — bail
						return gctx.Err()
					}
				}
			}
			slots <- struct{}{}
			defer func() { <-slots }()
			return withRetries(gctx, cfg.MaxRetries, func() error {
				return buildThenSweep(cfg, b)
			})
		})
	}
	return g.Wait()
}

// buildThenSweep runs an IndexBuild, then eagerly sweeps the keys its commit
// demoted (this window only), so freed disk returns without waiting for a run.
func buildThenSweep(cfg Config, b IndexBuild) error {
	cat := cfg.Catalog
	if err := buildTxhashIndex(b.Window, b.Lo, b.Hi, cat); err != nil {
		return err
	}
	for _, key := range indexKeys(cat, b.Window) { // superseded coverage(s)
		if key.State == Pruning {
			sweepIndexKey(cat, key)
		}
	}
	var demoted []ArtifactRef // terminal build: the window's .bin inputs
	for c := windowFirstChunk(b.Window); c <= windowLastChunk(b.Window); c++ {
		if cat.State(c, TxHashBin) == Pruning {
			demoted = append(demoted, ArtifactRef{Chunk: c, Kind: TxHashBin})
		}
	}
	if len(demoted) > 0 {
		sweepChunkArtifacts(cat, demoted)
	}
	return nil
}
```

- **`cfg.Workers`** (default `GOMAXPROCS`) is the only resource knob: at most that many tasks run at once, drawn from all windows' eligible work. Goroutines are cheap structure — thousands may be parked on the semaphore or on done-channels.
- Done-channels signal *success*: a chunk build closes its channel only once its `.bin` is frozen, so an index build proceeds only when every input it needs exists. A chunk build that exhausts its retries leaves its channel open and returns an error, which cancels `gctx`; any dependent waiting on it unblocks through the `<-gctx.Done()` case and bails. A task that exhausts its retries fails the run ([error policy](#lifecycle)); the next startup re-resolves from durable keys, so completed work never repeats.

---

## Daemon flow

After startup, the daemon runs two goroutines. **Hot-DB ingestion** pulls new ledgers from captive core into the per-chunk hot DBs as the network closes them, and hands each completed chunk to the lifecycle. (This is the live-network loop — distinct from startup backfill, which reads *old* ledgers into cold files.) The **lifecycle** is a background goroutine responsible for everything else, and it does two kinds of work: **freezing** complete chunks from hot storage into immutable cold files (rolling the tx-hash index forward as it goes), and **cleanup** — discarding hot DBs the cold files now serve, and pruning artifacts that are superseded or have fallen past the retention floor. The sections below cover startup, then each goroutine in turn.

### Startup

Startup runs in two steps, both in `startStreaming` below:

1. **Backfill** brings on-disk coverage in line with the retention window, up through the last *complete* chunk at the tip. The partial chunk still forming at the tip is left to hot-DB ingestion: on a restart its ledgers so far are already in the live hot DB, on a first start ingestion fetches them from the resume ledger forward, and either way ingestion completes the chunk as new ledgers arrive. Backfill re-runs if the tip advances mid-pass, and when it returns, the whole in-retention history up to that point is on disk as frozen files — ready to serve.
2. **Serve + ingest** opens the resume chunk's hot DB, starts captive core, serving, the lifecycle goroutine, and the hot-DB ingestion loop. The lifecycle is seeded with the last complete chunk — when one exists; a young network's first run waits for the first boundary — so its first run fires at once and finishes any crash/downtime leftovers concurrently with serving. Reads never wait for it, because a reader only ever resolves a `"ready"` hot DB or a `"frozen"` cold file — never a transient key.

Operational note — **peak disk after long downtime**: pruning runs only in the first run's prune stage, *after* backfill has materialized every newly-in-retention chunk, so a downtime approaching or exceeding the retention window transiently holds up to ~2× the retention footprint (the stale window plus its replacement). A deep backfill also holds the transient `.bin` inputs for every window it is building at once, not just one (the transactions design, §7.4). Size volumes accordingly; a disk-full during backfill fails the run before the relieving prune can fire, on every pass.

The retention floor and resume point are computed by:

```go
const (
	GenesisLedger        = 2
	LedgersPerChunk      = 10_000
	ChunksPerTxhashIndex = 1_000 // window = 10M ledgers
)

// retentionFloorChunk: the lowest chunk kept — retentionChunks back from
// lastChunk, never below earliest's chunk.
func retentionFloorChunk(lastChunk ChunkID, retentionChunks uint32, earliest uint32) ChunkID {
	floor := chunkID(earliest)
	if retentionChunks > 0 {
		floor = max(floor, lastChunk-ChunkID(retentionChunks)+1)
	}
	return floor
}

// lastCompleteChunkAt: the largest chunk whose last ledger is <= ledger.
func lastCompleteChunkAt(ledger uint32) int64 {
	return (int64(ledger)-1)/LedgersPerChunk - 1
}

// maxCommittedSeq: the highest ledger committed to a hot DB; an empty chunk-C DB
// counts as chunkFirstLedger(C) - 1 (the watermark just below the chunk), so the
// boundary-crash derivation is exact.
//
// lastCommittedLedger: the highest ledger in durable storage — the live hot DB's
// last, the highest durable chunk's if it leads, or earliest-1 if neither exists.
// A chunk is durable when its ledgers and events are "frozen" and its txhash is
// "frozen" or covered by the window's frozen index (pendingArtifacts' test).
func lastCommittedLedger(cat Catalog) uint32 {
	base := cat.EarliestLedger() - 1
	cold := highestDurableChunk(cat)
	hot := highestReadyHotChunk(cat)
	switch {
	case hot > cold:
		db := openReadOnly(hot)
		defer db.Close()
		return max(base, maxCommittedSeq(db))
	case cold >= 0:
		return max(base, chunkLastLedger(cold))
	default:
		return base
	}
}

func networkTip(cfg Config) (uint32, error) {
	tip, err := withBackoff(func() (uint32, error) { return backendNetworkTip(cfg) })
	if err != nil {
		return 0, err
	}
	if tip < GenesisLedger {
		return 0, fmt.Errorf("backend tip %d is below genesis — backend not ready", tip)
	}
	return tip, nil
}
```

```go
func startStreaming(ctx context.Context, cfg Config) error {
	cat := openCatalog(cfg)
	cfg.Catalog = cat
	validateConfig(cfg)

	earliest := cat.EarliestLedger()
	lastCommitted := lastCommittedLedger(cat)

	// Step 1: backfill from the floor up to each pass's target chunk, leaving the
	// partial tip chunk to ingestion. Re-pass while the tip moves.
	backfilledThrough := int64(-1)
	for {
		tip, err := networkTip(cfg)
		if err != nil {
			return err // no tip, no pass: never serve behind an unknown frontier
		}
		target := backfillTarget(tip, lastCommitted)
		if target <= backfilledThrough {
			break // the tip stopped advancing the target: backfill is done
		}
		rangeStart := retentionFloorChunk(target, cfg.RetentionChunks, earliest)
		if rangeStart > target {
			break // floor pinned above the target (e.g. a first start with "now")
		}
		if err := executePlan(ctx, cfg, resolve(cfg, rangeStart, target)); err != nil {
			return err
		}
		lastCommitted = max(lastCommitted, chunkLastLedger(target))
		backfilledThrough = target
	}
	resumeLedger := lastCommitted + 1

	// Step 2: serve + ingest. Seed the lifecycle with the last complete chunk so
	// its first run clears crash/downtime leftovers while serving is already live.
	hotDB, err := openHotDBForChunk(cat, chunkID(resumeLedger))
	if err != nil {
		return err
	}
	core := openCaptiveCore(cfg) // constructed here; starts on the loop's first pull
	if seed := lastCompleteChunkAt(resumeLedger - 1); seed >= 0 {
		notifyBoundary(seed) // no seed on a young network with no complete chunk
	}
	serveReads()
	go lifecycleLoop(ctx, cfg)
	return runIngestionLoop(ctx, cat, core, hotDB, resumeLedger)
}

// backfillTarget: the highest complete chunk a pass builds to — the last complete
// chunk at max(tip, lastCommitted), except that a mid-chunk lastCommitted within
// one chunk of the tip lowers it to the chunk below the resume point, leaving the
// partial resume chunk to ingestion. The retention floor derives from this target,
// so the floor never sits above what the pass actually builds.
func backfillTarget(tip, lastCommitted uint32) int64 {
	target := lastCompleteChunkAt(max(tip, lastCommitted))
	midChunk := lastCommitted != chunkLastLedger(chunkID(lastCommitted))
	nearTip := int64(tip)-int64(lastCommitted) < LedgersPerChunk
	if nearTip && midChunk {
		target = lastCompleteChunkAt(lastCommitted)
	}
	return target
}
```

A pass with no reachable tip fails the run rather than proceeding: the daemon never serves behind an unknown frontier, and the next run re-samples, so a transient backend outage self-heals.

`validateConfig` checks the config and, on the first start, resolves and pins `earliest_ledger`. `fail` here rejects startup outright — including a first start whose backend is unreachable at pin time; unlike the per-pass tip sampling, nothing here retries in-process, so the daemon refuses to start rather than run against a wrong or unresolvable pin:

```go
func validateConfig(cfg Config) {
	cat := cfg.Catalog
	if cfg.Workers < 1 {
		fail("workers must be > 0 (got %d)", cfg.Workers)
	}
	if cfg.MaxRetries < 0 {
		fail("max_retries must be >= 0 (got %d)", cfg.MaxRetries)
	}
	if cfg.EarliestLedger != "genesis" && cfg.EarliestLedger != "now" {
		n, err := parseUint32(cfg.EarliestLedger)
		if err != nil || n < GenesisLedger || n != chunkFirstLedger(chunkID(n)) {
			fail("earliest_ledger must be \"genesis\", \"now\", or a chunk-aligned "+
				"ledger >= %d; got %q.", GenesisLedger, cfg.EarliestLedger)
		}
	}

	earliestStored, earliestPinned := cat.Get("config:earliest_ledger")

	if earliestPinned { // restart: confirm nothing changed, write nothing
		if cfg.EarliestLedger != "now" { // "now" on restart keeps the pinned floor
			want := uint32(GenesisLedger)
			if cfg.EarliestLedger != "genesis" {
				want = atoi(cfg.EarliestLedger)
			}
			if want != atoi(earliestStored) {
				fail("earliest_ledger changed: stored=%s, config=%s; wipe the data dir to change it.",
					earliestStored, cfg.EarliestLedger)
			}
		}
		return
	}

	// First start: resolve earliest_ledger, then pin it. "now" and a numeric
	// floor each need a reachable backend — "now" to resolve, a numeric floor to
	// reject one past the tip (it is pinned immutably, so it can't be checked later).
	var earliest uint32
	switch cfg.EarliestLedger {
	case "genesis":
		earliest = GenesisLedger
	case "now":
		tip, err := networkTip(cfg)
		if err != nil {
			fail("earliest_ledger=now needs a reachable backend: %v", err)
		}
		earliest = chunkFirstLedger(chunkID(tip))
	default:
		earliest = atoi(cfg.EarliestLedger)
		tip, err := networkTip(cfg)
		if err != nil {
			fail("a numeric earliest_ledger needs a reachable backend to validate against the tip: %v", err)
		}
		if earliest > tip {
			fail("earliest_ledger (%d) is past the network tip (%d)", earliest, tip)
		}
	}
	cat.Put("config:earliest_ledger", itoa(earliest))
}
```

### Hot DB helpers

`openHotDBForChunk` opens a chunk's hot DB — the existing one, or a fresh one after a crash or on first use:

```go
func openHotDBForChunk(cat Catalog, chunk ChunkID) (*HotDB, error) {
	hotKey, path := hotChunkKey(chunk), hotChunkPath(chunk)
	if state, _ := cat.Get(hotKey); state == "ready" {
		db, err := openExistingRocksDB(path)
		if err != nil {
			return nil, fmt.Errorf("hot DB for chunk %d is ready but won't open: %w", chunk, err)
		}
		return db, nil
	}
	// transient or absent: wipe any leftover dir and create fresh.
	deleteDirIfExists(path)
	cat.Put(hotKey, "transient")
	db := createChunkHotDB(path)
	fsyncDir(path) // durable before the key flips to "ready"
	fsyncParentDir(path)
	cat.Put(hotKey, "ready")
	return db, nil
}
```

### Hot DB Ingestion

```go
func runIngestionLoop(ctx context.Context, cat Catalog, core LedgerBackend, hotDB *HotDB,
	resumeLedger uint32) error {

	for seq := resumeLedger; ; seq++ {
		lcm, err := core.GetLedger(ctx, seq) // blocks until ledger seq is available
		if err != nil {
			return err
		}

		// One atomic synced batch across all CFs, so a ledger is fully present or
		// absent; it is the only per-ledger durability boundary.
		batch := hotDB.NewBatch()
		putLedger(batch, lcm)
		putTxHashes(batch, lcm)
		putEvents(batch, lcm)
		batch.Commit( /*sync=*/ true)

		if seq == chunkLastLedger(chunkID(seq)) {
			// Close this chunk and open the next before notifying, so the lifecycle
			// never races a live writer for the chunk it is about to freeze.
			hotDB.Close()
			if hotDB, err = openHotDBForChunk(cat, chunkID(seq)+1); err != nil {
				return err
			}
			notifyBoundary(chunkID(seq))
		}
	}
}
```

A `GetLedger` failure returns from the loop and fails the run; the next startup resumes from where the last synced batch left off, since the batch is all-or-nothing. A clean shutdown cancels `ctx` and returns the same way, distinguished from a failure at the daemon's top level. The completed chunk id is all ingestion tells the lifecycle — *how far to go*; what to build, discard, and prune the lifecycle reads from the catalog. `notifyBoundary` never blocks and notifications coalesce: the lifecycle acts on the latest completed chunk, so ingestion can never be held back by a busy lifecycle, and the lifecycle can never fall behind by more than one run.

### Lifecycle

The lifecycle is a background goroutine. Each notification — one per ingestion boundary, plus a startup seed — wakes it for one **run** over the latest completed chunk; if several boundaries arrive while a run is in progress, the next run's single pass covers them all. A run does three stages in order:

1. **Plan-and-execute** — `resolve` + `executePlan` over `[floor, last complete chunk]`, the same machinery backfill uses. In steady state this freezes the just-closed chunk from its hot DB and folds it into the current window's index; rebuilding the whole window each boundary costs ≈1 minute against a boundary that arrives only every ~14 h at mainnet rates.
2. **Discard** — retire hot DBs the cold artifacts now fully serve.
3. **Prune** — sweep demoted and past-retention files.

At runtime the floor only rises (retention config is fixed for the life of the process; widening applies at the next startup), so a run only extends the top of storage and never reaches below it — the just-closed chunk in steady state, everything up to the latest boundary after a busy stretch. When the floor sits above the last complete chunk — retention outran production, as on a young `"now"` deployment — the range is empty and the run builds nothing. Extending the *bottom* of storage — a fresh start, or filling to a widened floor — is startup backfill's job.

Everything the run does derives from the catalog plus the one chunk id ingestion hands it:

```go
func runLifecycle(ctx context.Context, cfg Config, lastChunk ChunkID) error {
	floor := retentionFloorChunk(lastChunk, cfg.RetentionChunks, cfg.Catalog.EarliestLedger())

	if err := executePlan(ctx, cfg, resolve(cfg, floor, lastChunk)); err != nil {
		return err // fails the run; startup is the recovery path
	}
	for _, op := range eligibleDiscardOps(cfg, lastChunk, floor) {
		if err := op(); err != nil {
			return err
		}
	}
	for _, op := range eligiblePruneOps(cfg, floor) {
		if err := op(); err != nil {
			return err
		}
	}
	return nil
}
```

The loop around it is trivial: block until a boundary notification, read the latest completed chunk, run once.

Between runs the goroutine is idle, and idle means **settled**: a re-scan would produce no ops and every storage invariant holds, so an [audit](#correctness) run at any such moment would pass. A failing op retries with backoff, then fails the run — startup is the recovery path, the same policy as ingestion.

The discard and prune stages are the two `eligible*` scans below. **Discard** retires a chunk's hot DB once its cold artifacts fully serve it (the window's index covers the chunk), or once it falls past retention. **Prune** is the system's only file-deleter: it sweeps transient index keys, the `.bin` inputs a terminal commit demoted, and everything below the retention floor, through `sweepIndexKey`/`sweepChunkArtifacts`. Each scan returns zero-arg ops the run calls in order.

```go
func eligibleDiscardOps(cfg Config, lastChunk, floor ChunkID) []func() error {
	cat := cfg.Catalog
	var ops []func() error
	for _, chunk := range hotChunkKeys(cat) {
		switch {
		case chunk < floor:
			ops = append(ops, func() error { return discardHotDBForChunk(cat, chunk) })
		case chunk <= lastChunk &&
			pendingArtifacts(cfg, chunk).Empty() &&
			indexCovers(cfg, chunk): // cold artifacts fully serve it
			ops = append(ops, func() error { return discardHotDBForChunk(cat, chunk) })
		}
	}
	return ops
}

// pendingArtifacts lists which processChunk outputs the chunk still needs. The
// .bin is exempt once the window's index covers the chunk (the finalized window
// already demoted its key).
func pendingArtifacts(cfg Config, chunk ChunkID) ArtifactSet {
	cat := cfg.Catalog
	var need ArtifactSet
	for _, kind := range []Kind{Ledgers, Events} {
		if cat.State(chunk, kind) != Frozen {
			need = need.Add(kind)
		}
	}
	if cat.State(chunk, TxHashBin) != Frozen && !indexCovers(cfg, chunk) {
		need = need.Add(TxHashBin)
	}
	return need
}

// indexCovers reports whether the window's durable .idx already hashes the chunk.
func indexCovers(cfg Config, chunk ChunkID) bool {
	fk := frozenCoverage(cfg.Catalog, indexID(chunk))
	return fk != nil && fk.Lo <= chunk && chunk <= fk.Hi
}

func eligiblePruneOps(cfg Config, floor ChunkID) []func() error {
	cat := cfg.Catalog
	var ops []func() error

	for _, key := range indexKeys(cat) {
		switch {
		case key.State == Freezing || key.State == Pruning: // transient debris
			ops = append(ops, func() error { return sweepIndexKey(cat, key) })
		case windowLastChunk(key.Window) < floor: // frozen, wholly below the floor
			ops = append(ops, func() error { return sweepIndexKey(cat, key) })
		}
	}

	var refs []ArtifactRef
	for _, ref := range chunkArtifactKeys(cat) {
		switch {
		case ref.Chunk < floor: // wholly past retention
			refs = append(refs, ref)
		case cat.State(ref.Chunk, ref.Kind) == Pruning:
			refs = append(refs, ref)
		case ref.Kind == TxHashBin: // redundant .bin in a finalized window
			if fk := frozenCoverage(cat, indexID(ref.Chunk)); fk != nil && fk.Hi == windowLastChunk(indexID(ref.Chunk)) {
				refs = append(refs, ref)
			}
		}
	}
	if len(refs) > 0 {
		ops = append(ops, func() error { return sweepChunkArtifacts(cat, refs) })
	}
	return ops
}
```

The op bodies — one discard, two sweeps — delete every committed artifact and hot DB:

```go
func discardHotDBForChunk(cat Catalog, chunk ChunkID) error {
	if !cat.Has(hotChunkKey(chunk)) {
		return nil
	}
	cat.Put(hotChunkKey(chunk), "transient")
	deleteDirIfExists(hotChunkPath(chunk))
	fsyncParentDir(hotChunkPath(chunk))
	return cat.Delete(hotChunkKey(chunk))
}

func sweepChunkArtifacts(cat Catalog, refs []ArtifactRef) error {
	batch := cat.NewBatch() // never unlink under a "frozen" key
	for _, ref := range refs {
		if ref.State == Frozen {
			batch.Put(chunkKey(ref.Chunk, ref.Kind), "pruning")
		}
	}
	batch.Commit()

	var paths []string
	for _, ref := range refs {
		deleteArtifactFiles(ref.Chunk, ref.Kind)
		paths = append(paths, artifactPaths(ref.Chunk, ref.Kind)...)
	}
	fsyncParentDirs(paths) // unlinks durable before the keys go

	batch = cat.NewBatch()
	for _, ref := range refs {
		batch.Delete(chunkKey(ref.Chunk, ref.Kind))
	}
	return batch.Commit()
}

func sweepIndexKey(cat Catalog, key IndexKey) error {
	if key.State == Frozen {
		cat.Put(key, "pruning") // never unlink under a "frozen" key
	}
	deleteFileIfExists(indexFilePath(key))
	fsyncDir(indexWindowDir(key))
	if err := cat.Delete(key); err != nil { // key outlives the unlink, so a crash re-runs the sweep
		return err
	}
	rmdirIfEmpty(indexWindowDir(key)) // best-effort; an empty dir is not an artifact
	return nil
}
```

`discardHotDBForChunk` removes a hot DB directory under its `hot:chunk` key; one sweep body per key family deletes every committed artifact. The prune walk's two families are independent of each other and of discard — a chunk swept while its window's `.idx` still resolves to it could leave a `getTransaction` pointing at a deleted `.pack`, but a below-floor read is not-found regardless ([reader contract](#reader-contract)).

### Concurrency model

Two writer goroutines and read-only readers. The catalog partitions their domains at the **live chunk** — the highest chunk with a `hot:chunk` key:

- **Ingestion** owns the live chunk: the sole writer of its hot DB, and the creator of each `hot:chunk` key (via `openHotDBForChunk` at the boundary).
- **The lifecycle** owns everything below it: handed-off hot DBs (freeze + discard), all `chunk:*` and `index:*` keys, and the deletion side of `hot:chunk` keys.

Their only link is the boundary notification. The handoff is by write ordering — ingestion closes the chunk and opens the next (moving the partition) *before* notifying — so the lifecycle never freezes a chunk a writer still holds. Both write the catalog at the same time but never the same key (RocksDB handles concurrent writes safely). And because the chunk ids ingestion hands over only increase, a chunk completing while a lifecycle run is already in progress just bumps the starting point of the *next* run — it can't disturb the one underway. Readers hold their own read-only handles and resolve files through keys, so writer activity never races them.

**Single-process enforcement.** All of the above assumes a *single* daemon owns the data. The catalog's RocksDB holds an exclusive lock, so a second daemon pointed at the same catalog fails to start; the lock releases on any exit, including `kill -9`, so it never goes stale. The storage trees carry no locks of their own: cold trees are write-once (a redundant writer produces identical bytes), and each hot chunk DB holds its own RocksDB lock. Pointing two daemons with *different* catalogs at the same storage tree is an unsupported misconfiguration the daemon does not defend against.

---

## Reader contract

A read resolves data through three rules, and the rest of the design relies on all of them:

1. **Only `"ready"` and `"frozen"` are visible.** A read resolves a chunk only from a `"ready"` hot DB or a `"frozen"` cold file — never from a key in a transient state (`"freezing"`, `"pruning"`, `"transient"`). So a reader never sees a half-written file, crash debris, or an in-progress sweep; transient keys are invisible to it.
2. **Below the floor is *not found*.** A read for any seq below the retention floor returns not-found, whether or not the file still exists on disk. This is what lets pruning delete a chunk the instant it passes retention: a stale `.idx` might resolve a tx-hash to a `.pack` that's been unlinked, but the below-floor read is not-found anyway.
3. **Cold wins where it covers.** When a chunk's frozen cold artifacts fully cover it, a read resolves the chunk from them; a `"ready"` hot DB for such a chunk is a leftover awaiting discard and may hold only part of the chunk. The hot tier is authoritative only for chunks the cold files don't fully cover. (The transactions design's probe rule, §8.1, is this rule specialized to tx lookups.)

Together they make retention the single source of truth for "is this data available?": the freeze, sweep, and prune stages constantly create transient states and delete below-floor data, and these rules guarantee a read never *resolves* either. (Whether a read already in flight survives a concurrent unlink is a separate question — see below.)

How a read is actually served — choosing the hot DB or the cold files for a given query, reading across the cold artifact types (`.pack` ledgers, events segments, `.idx` index), and staying correct when a sweep or prune unlinks a file while a read is mid-flight — is the **query-routing design's** concern, out of scope here and in the transactions design (§8).

---

## Correctness

This section states what the streaming workflow guarantees, the assumptions it relies on, and the operator actions and crash timings the design covers.

### Invariants

Two terms recur below. The **retention window** runs from the retention floor up to the last committed ledger; the reader gate and the prune scan both use the floor (rounding it a little low is harmless). The floor is also the bottom of the production range for both backfill and the lifecycle run, and at runtime it only rises — so a run never reaches below what's already on disk. The daemon is **settled** when a run's plan is empty and its discard and prune scans produce no ops: the state between runs, where the invariants below are meant to hold.

**INV-1 (read correctness).** Any data request whose ledger scope falls entirely within the retention window returns correct results: the content matches what a conformant LedgerBackend would produce, no partial state is visible, and no in-retention range is unreachable.

**INV-2 (single canonical state).** The catalog records exactly one home for each data range. What it guarantees:

- **One frozen index per window, at all times** (settled or not). The commit batch promotes the new coverage and demotes the old one in a single write, so "the window's index" is always well-defined for readers — never two frozen keys, never none once the window has one.
- **No transient artifact key survives a settled state.** Between runs, no `chunk:*` or `index:*` key is `"freezing"` or `"pruning"`. Each kind of transient has cleared: index transients by the run that observed them; per-chunk `"freezing"` keys by re-materialization (the plan stage rebuilds them, for chunks in `[floor, last complete chunk]`, from whatever source `backfillSource` picks) or by the below-floor prune once their chunk ages out; and `"pruning"` keys by the sweeps.
- **No leftover hot DB for a fully-cold chunk** (when settled). No `hot:chunk:c` exists for a chunk `c` whose artifacts are all durable *and* whose window's index covers `c` — that chunk is served entirely from cold files, so its hot DB must be gone.
- **No leftover `.bin` key in a finalized window** (when settled). No `chunk:c:txhash` exists for a chunk in a window whose frozen index is terminal: the terminal commit demotes the merged inputs `[lo, hi]` and the sweep removes them, chunks below the floor are cleared by retention pruning, and the prune scan's redundant-input branch catches any that a crashed widening re-froze.

Two transient states are tolerated even at a settled moment:

- **A hot DB's `"transient"` bracket** around an in-flight directory operation (the boundary's `openHotDBForChunk`, startup's resume-chunk open, a discard mid-op). A crash-left bracket is finished by the next `openHotDBForChunk` or discard scan.
- **After a recovery, when the backend tip lags, a partially-frozen chunk above the last committed ledger** may hold `"freezing"` keys while serving and settled. It sits above the last complete chunk — outside every plan range and the retention window, so no read can observe it — until the backend covers it or re-ingestion replays past it.

**INV-3 (disk matches catalog).** When settled, the files and hot-DB directories on disk are exactly the set the catalog names — no more, no less. Every key maps to its expected path or paths (the events key names three files), and because a key is written before its file (mark-before-write), even a partial file is reachable from its key. The hot `"transient"` bracket's tolerated shapes are the bracket's own: a key over a partly-created directory, or a key whose directory is already gone. So the match holds whether a key is in a final state or in one of the transients INV-2 tolerates. No orphan files, no dangling keys, no duplicates: a file that no catalog key names is a real bug, not mid-run debris.

**INV-4 (retention bound).** When settled, no file or catalog key maps to a ledger range strictly below the effective retention floor — with one exception: a frozen index key whose window straddles the floor keeps the `lo` it was built with, so its coverage `[lo, hi]` reaches below the floor. That below-floor portion is never served ([reader contract](#reader-contract) rule 2 returns not-found), and the key and its `.idx` are swept once the whole window falls below the floor.

Each invariant has a distinct audit. INV-1 you check by issuing reads or by re-deriving artifacts and byte-comparing. INV-2 you check by walking catalog keys and cross-checking forbidden co-existence. INV-3 you check by walking the filesystem against the catalog. INV-4 you check by walking catalog keys against the floor. None of the invariants reference the phase scans that maintain them — so a bug in any scan shows up as a real invariant violation, not as something the buggy code silently considers acceptable. A settled state between runs makes these walks meaningful on a live daemon, so an `audit` admin command can implement them directly (with an optional deep mode that re-derives sampled artifacts and byte-compares, for INV-1 — against the same backend and core version, since replayed meta is not byte-stable across versions).

### Convergence

**Startup converges from any on-disk state.** Whatever a partial-completion crash, an operator action, or surgical recovery leaves behind, startup drives the system to a settled state satisfying INV-1 ∧ INV-2 ∧ INV-3 ∧ INV-4. Startup here is the backfill pass followed by the first lifecycle run — fired by the startup seed when a complete chunk exists; a young store with none is settled by construction — and it reaches a settled state within that first run, typically seconds after serving opens, bounded by the run's freeze, rebuild, and prune workload. From any state reachable *during* a run, the lifecycle run alone converges, within a bounded number of runs. And since a runtime op failure fails the run and hands recovery back to startup, every state a run can leave behind is one startup is built to converge.

Two accepted limits. Convergence needs disk headroom: the prune that frees space runs after the backfill that needs it (the peak-disk note under [Startup](#startup)), so a disk sized below the transient peak fails the run on every pass until space is added. And a same-window 16-byte hash collision — the ~10⁻²⁰-per-window event the transactions design accepts (§8.2) — fails that window's index build permanently: the accepted risk presents as a run that never completes, never as wrong data.

The split matters because some repairs are inherently backfill's, not the run's: a per-chunk `"freezing"` key with no hot DB behind it (a crashed backfill write) is repaired by re-materialization, and a surgically removed range is re-derived from the LedgerBackend — no run phase produces data. The run's province is everything else: index transients, demotions, freezes from live hot DBs, prunes.

Convergence rests on three properties shared by the resolver and the scans — eligibility is computed from durable catalog state alone; ops are idempotent; everything is re-derived on every notification — plus backfill's postcondition contract. Together, whatever a crash leaves half-done, the next run or the next startup finishes.

### Substrate assumptions

Properties we rely on the underlying storage to provide:

- **Sync WAL.** All catalog puts and deletes that the invariants depend on use RocksDB's `WriteOptions.sync = true`, which fsyncs the WAL before the write returns. Multi-key commits — the index commit batch, the sweeps' key-delete batches — are single atomic synced WriteBatches: all-or-nothing across keys.
- **Per-ledger durability.** The chunk hot DB's synced WriteBatch (atomic across all CFs) is the sole per-ledger durability boundary; the last committed ledger is derived from it. Per-artifact: the per-chunk files **and the directory entries naming them** are fsynced before the key flips to `"frozen"`, and an index coverage's `.idx` (and the directory entries naming it, including a just-created window dir's own entry) is fsynced before the commit batch freezes its key.
- **Read-side WAL recovery.** A read-only open of a hot DB recovers the synced write-ahead log, so everything the last synced batch committed is visible to a reader — the freeze's completeness check and the startup derivation both depend on this, not just on the write side of the sync-WAL guarantee.
- **Deterministic, idempotent writes.** Re-applying any write from the same source produces byte-identical state: tx-hash entries are keyed on consensus-fixed transaction hashes (deterministic from any backend), and the streamhash index is byte-identical from byte-identical sorted inputs. Ledger bytes are deterministic per source: a lake serves the LCM bytes it stores, but captive-core replay is not guaranteed byte-stable across core versions — byte-level comparisons must use the same source and version.
- **Monotonic progress.** Within a process run, ingestion only moves forward: each synced batch extends the last, and the last-complete-chunk it hands the lifecycle climbs with it (strictly increasing chunk ids). Across a crash, the startup derivation equals exactly the durable state — the pre-crash value, or a hair above it (a batch that committed in the instant before the crash). It lands *below* the pre-crash value only when state was demoted or lost: surgical recovery shrinks the derivation's inputs by demoting them. For example, demoting hot state to `"transient"` rewinds the derivation to the durable cold boundary; demoting a finished window's index on a daemon interrupted during its first backfill (no hot DBs to anchor the derivation) can drop it below that window until backfill rebuilds the index — re-deriving the untainted chunks from their on-disk `.pack`s and re-fetching only the tainted ones.

### Design invariants

These are streaming-specific properties the implementation guarantees on top of the substrate, and that INV-1 through INV-4 depend on:

- **Every key precedes its file.** The pre-write `"freezing"` mark and post-fsync `"frozen"` flip mean any file on disk — per-chunk artifact or index file, partial or complete — has its catalog key set. Every scan and sweep iterates keys, so every file is reachable that way; nothing ever lists a directory to find work.
- **Index promotion is atomic and gap-free.** The commit batch freezes the new coverage and demotes its predecessor in one synced write, so the window's unique frozen key changes hands atomically — never two frozen keys, never none once the window has one. A reader following the frozen key always lands on a complete, fsynced index; a crash mid-build leaves the prior coverage frozen and the attempt as `"freezing"` debris that is either overwritten by the next build of that coverage or deleted unread by the sweeps.
- **Key absent ⟹ file gone.** Every sweep's shared ordering (unlink → `fsyncDir` → atomic key delete) gives the exit-side counterpart.
- **Hot DB keys bracket the directory.** The `hot:chunk:{chunk}` key is put (`"transient"`) before the directory is created, and deleted only after rmdir completes — with `"transient"` re-marked first.
- **Tx hashes always have a queryable home.** The hot DB is discarded only after the durable `.idx` covers the chunk (or the chunk has fallen below the retention floor, where reads are not-found regardless) — hot CF, then `.idx`, with no gap inside retention. (The `.bin` is never a serving tier; it is rebuild input, demoted to `"pruning"` by the terminal commit batch — the same write that freezes the final `.idx` — or by retention pruning once its chunk falls past the floor, and deleted only by the sweep after that.)
- **`"frozen"` ⟹ the file is durable and complete.** Flips to `"frozen"` happen only after fsync, and files are deleted only under non-frozen keys (sweeps demote first) — so frozen keys can be trusted blindly by readers and the resolver.
- **`"pruning"` is committed.** Once a key is in `"pruning"` — demoted by a commit batch or by retention — the sweep runs to completion on subsequent scans. Backfill treats any non-`"frozen"` state as empty and overwrites cleanly if the range is re-ingested.

### Scenario coverage

INV-1 holds at every point the daemon is serving reads — transient states are never externally visible, because a read resolves only a `"ready"` hot DB or a `"frozen"` cold artifact (never a `"freezing"`/`"pruning"`/`"transient"` key), resolves from cold wherever cold fully covers the chunk, and the retention check masks everything else. INV-2, INV-3, and INV-4 hold at every settled state reached after the events below; startup's first settled state arrives when the first run completes, shortly after reads open.

1. **Steady-state operation.** Hot DB ingestion advances the last committed ledger; the lifecycle goroutine freezes complete chunks within retention and prunes anything past it. All four invariants hold by induction on it.
2. **Operator state changes — widening or shortening retention (`retention_chunks`).** Changing `retention_chunks` recomputes the retention floor, and the next startup converges to the new state. Backfill's per-window rule rebuilds any window whose desired coverage now exceeds what's stored, and the prune stage removes anything below a raised floor.

   Widening takes effect on the *next startup*, not immediately: a running daemon holds the retention config it started with, so its floor never drops mid-run — the lower floor, and the backfill that fills down to it, apply only at the next startup. `earliest_ledger` is not a live change at all: it is pinned on the first start and immutable, so editing the config never moves the floor (the only way to change it is to wipe the data directory and start fresh).
3. **Surgical recovery (tainted data).** The operator never touches the filesystem. Recovery is **one atomic catalog batch** that *demotes* the affected keys — it never removes them — split by tier. Tainted cold artifacts (`chunk:{c}:*` and every overlapping `index:*` key) go to `"freezing"`, the state that already means *this file is not to be trusted: re-derive or delete*. When hot data is tainted, also demote **every `hot:chunk` at or above the lowest tainted hot chunk — the live chunk included** — to `"transient"`, not just the directly-tainted ones (the reason is the third paragraph); a purely cold taint needs no hot demotion. `"transient"` makes a hot DB instantly ineligible as a source (`backfillSource` reads only `"ready"`) and invisible to the last-committed-ledger derivation (which counts only `"ready"` keys). The batch commits atomically or not at all, and re-running it is a no-op; the catalog's lock means it can only be written against a stopped daemon.

   Everything then converges through machinery that already exists. Startup backfill re-derives the `"freezing"` cold artifacts from a conformant LedgerBackend — overwriting in place, the write protocol's ordinary re-materialization — and rebuilds each affected window's index; tainting any chunk (or the index) of a finalized window re-derives that window's spent `.bin` inputs, from the local packs where those survive, before its index rebuilds. (If the backend tip lags below a re-derived chunk, `backfillSource` waits for coverage; see [the primitives](#the-primitives).) The `"transient"` hot DBs need no file surgery: `openHotDBForChunk` wipes and recreates one when re-ingestion re-opens that chunk, and the discard scan retires any sitting below the live chunk.

   **Why every hot DB at or above the taint, not just the tainted one.** A demoted hot DB is never a source, so demoted *complete* chunks are re-derived as cold files by backfill; only the live partial chunk is repaired by re-ingestion, which replays **forward** from the last committed ledger. That watermark derives from the durable cold artifacts plus the highest `"ready"` hot DB — so demoting only the tainted chunk would leave a higher `"ready"` chunk, ultimately the live one, pinning the watermark above the taint, and the replay would never reach it. Once every hot DB at or above the lowest tainted chunk is demoted, the watermark falls to the highest durable boundary below the taint, backfill re-derives the demoted complete chunks from the backend, and re-ingestion replays the live tail forward. Every recovery demotes; nothing is removed by hand — the daemon's own sweeps and `openHotDBForChunk` handle the dirs in their existing crash-safe order.
4. **First deployment / downtime between restarts.** The last committed ledger derives to `max(frozen/hot maxima, earliest_ledger - 1)`, ensuring `resumeLedger ≥ earliest_ledger`. Backfill fills `[retentionFloorChunk(target), target]` per pass, if anything is missing (usually a no-op for an `earliest_ledger = "now"` first deployment — unless the tip has already completed the pinned chunk).
5. **LedgerBackend choice or mid-flight swap.** The LedgerBackend contract guarantees canonical LCM bytes for any range, so any conformant backend produces byte-identical artifacts. Different backends differ in performance, not behavior. An operator using BSB for backfill and CaptiveCore for hot DB ingestion, or swapping mid-deployment, satisfies all four invariants.
6. **Crash at any point during any of the above.** Sync WAL plus per-ledger durability ordering mean the catalog on next start is internally coherent and the derived last committed ledger equals exactly what the last synced batch committed. Idempotency means re-running any half-finished op is safe. Convergence finishes whatever the crash interrupted.

### What a bug looks like

The invariants describe what storage should look like, not how the phase scans maintain it. So common bugs show up as concrete violations:

- **A catalog key claims something the file doesn't actually deliver** — e.g., a per-chunk writer flips a key to `"frozen"` before fsync (leaving a partial file the catalog advertises as complete), or an index key freezes before its `.idx` is fully fsynced, or the key name's `{lo, hi}` doesn't match the file's actual coverage, or a frozen file is mutated post-freeze ⟹ reads through the catalog key see wrong or missing data. **INV-1** violated. Detectable by re-deriving an artifact via a conformant LedgerBackend and byte-comparing against the on-disk file.
- **Pruning too aggressive** ⟹ a request whose ledger scope is in retention returns wrong or missing results. Issue a read to find it. **INV-1** violated.
- **Two frozen index keys in one window** — a commit batch failed to demote the predecessor, or promotion and demotion landed as separate writes ⟹ readers have no well-defined index. Walk `index:*` keys, count `"frozen"` per window. **INV-2** violated.
- **A `"freezing"` or `"pruning"` key within `[floor, last complete chunk]` survives while serving and settled** ⟹ its recovery mechanism was skipped — an index transient the sweeps should have deleted, a `"pruning"` demotion the sweeps should have finished, or a per-chunk `"freezing"` key that the freeze phase or startup backfill should have re-materialized. Walk keys for transient values when settled, excluding the one corner INV-2 tolerates — a `"freezing"` artifact key *above* the last complete chunk after a recovery with a lagging backend tip, which no source can yet repair. **INV-2** violated.
- **Chunk scan misses an orphan** ⟹ a hot DB persists for a chunk that cold artifacts fully serve. Walk `hot:chunk:c` keys whose chunk has its artifacts durable and its window's index covering `c`. **INV-2** violated.
- **Finalization demotions don't complete** ⟹ per-chunk frozen tx hash files outlive the index that consumed them. Walk `chunk:c:txhash` keys whose window's frozen key has `hi` = the window's last chunk. **INV-2** violated.
- **A writer leaves a file on disk without its catalog key** (file fsynced before key was durable, or a sweep deleted the key before its unlink was durable) ⟹ orphan file — invisible to every key-driven scan. Walk the filesystem against the catalog. **INV-3** violated.
- **A catalog key persists without its file** (file deleted before key) ⟹ dangling key. Walk the catalog against the filesystem. **INV-3** violated.
- **Duplicate cold artifacts for the same logical data** (e.g., two events files for the same chunk, from a migration or buggy retry) ⟹ the catalog names one expected path; the extras are orphans. Walk the filesystem against catalog-specified paths. **INV-3** violated.
- **Pruning fails past the floor** ⟹ files or keys remain for ranges below the retention floor. Walk catalog keys, compare ledger ranges to the floor. **INV-4** violated.

A storage walk against the invariants is enough to find these without inspecting the phase implementations.

---

## Related documents

- The transactions design ([gettransaction-full-history-design.md](./gettransaction-full-history-design.md)) — the tx-by-hash subsystem end to end: the hot `txhash` CF, the `.bin`/`.idx` formats, the rolling window index rebuild — its streamhash merge internals and safety argument — the `getTransaction` read path, and the capacity numbers. Canonical for the streamhash `.bin`/`.idx` formats, the index merge internals, and the index-key coverage semantics this doc summarizes.
- The events design ([getevents-full-history-design.md](./getevents-full-history-design.md), PR #635) — the cold-segment file formats and the hot events CF schema referenced by the data model.
- The reader / query-routing design — how reads dispatch between hot DBs and frozen files for in-retention queries.
