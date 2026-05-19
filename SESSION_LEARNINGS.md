# Full-History RPC — Session Learnings

> Snapshot of state, decisions, surprises, and gotchas from the full
> backfill + migration + GCS upload + benchmarking session
> (May 18–19, 2026). Companion to `BENCHMARK_GOAL.md` (which has the
> actual numbers); this doc captures everything *around* the numbers.

## Repo state at end of session

| Item | Branch | Status |
|---|---|---|
| **`origin/rpc-hack`** (chowbao fork) | rpc-hack | up to date with `upstream/rpc-hack` (`891badf`) |
| `chowbao/full-history-backfill-script` | branched off `upstream/rpc-hack` | pushed to `origin`, opened as **PR #743** upstream |
| **`cherrypick-740`** (current working branch) | local only | uncommitted: PR #740's `chunk/` + `eventstore/` + new events/ files; plus our bench harness + cold-format-writing backfill |
| Throwaway local-only scripts | on `cherrypick-740` | `cmd/stellar-rpc/scripts/{cold-read,migrate-cold,spot-check,verify-pack,bench-fullhistory}/` |

PR #743 is on `upstream/rpc-hack` and represents the only production-bound code from this session. Everything else is exploratory local work.

## Data inventory

| Where | What | Size |
|---|---|---|
| `/mnt/nvme/disk2/ledgers/cold/` | Cold-format ledger pack files for chunks 4999–5999 (pubnet 50M–60M) | **1.4 TiB** |
| `/mnt/nvme/disk2/ledgers/hot-5000/` | RocksDB hot ledger store, chunk 5000 seeded for bench | 1.5 GB |
| `/mnt/nvme/disk2/ledgers/txhash-hot/` | RocksDB txhash hot store, chunk 5000 (3.01M entries) | 132 MB |
| `/mnt/nvme/disk2/ledgers/txhash-cold/00005000.bin` | Sorted-.bin cold txhash index (bench shim, *not* RecSplit) | 108 MB |
| `/mnt/nvme/disk2/ledgers/events-hot/hot/00005000/` | RocksDB hot eventstore, chunk 5000 (7.91M events) | 2.6 GB |
| `/mnt/nvme/disk2/ledgers/events-cold/00005000-*.{pack,hash}` | Cold eventstore (events.pack + index.pack + index.hash) | 431 MB |
| `/mnt/nvme/disk2/ledgers/events-corpus.json` | Sampled TermKeys for events bench (5000 contracts, 6 topic0, 5000 topic1) | 400 KB |
| `gs://rpc-full-history/cold/` | Mirror of cold ledger pack files (1.4 TB uploaded) | 1.4 TiB |
| `bench-out/` | 26 raw per-iteration latency CSVs + `summary.csv` | 168 KB |
| `BENCHMARK_GOAL.md` | Plan + methodology + results + interpretation | 289 lines |

## Machine layout (im4gn.8xlarge)

```
/                 96 GB    EBS  boot (small — never use for big data)
/mnt/xvdf         8.0 TB   EBS  attached, empty; durable; owned by root (needs sudo to set up)
/mnt/nvme/disk1   6.8 TiB  EPHEMERAL instance NVMe; owned by urvisavla (not writable as simon)
/mnt/nvme/disk2   6.8 TiB  EPHEMERAL instance NVMe; chowned subdir 'ledgers/' is the only place we own
```

**Key fact**: `disk1`/`disk2` are *ephemeral* — anything on them disappears on AWS instance stop/start (reboot is fine). The 1.4 TB cold dataset is also mirrored at `gs://rpc-full-history/cold/`, which is durable.

Only path on the instance store usable as `simon` is `/mnt/nvme/disk2/ledgers/` (was chowned earlier in the session via `sudo install -d -o simon ...`).

## Tooling locations

| Path | What |
|---|---|
| `cmd/stellar-rpc/scripts/full-history-backfill/main.go` | Production-target driver; reads BSB → writes ColdStoreWriter. **Subject of PR #743**. |
| `cmd/stellar-rpc/scripts/migrate-cold/main.go` | One-shot migration: old-format pack → cold-format pack. Obsolete now backfill writes cold-format directly. |
| `cmd/stellar-rpc/scripts/spot-check/main.go` | 100-sample readback verifier for old-format packs. Obsolete. |
| `cmd/stellar-rpc/scripts/verify-pack/main.go` | Single-pack trailer + ledger sanity check. Obsolete. |
| `cmd/stellar-rpc/scripts/cold-read/main.go` | Lookups via `ColdStoreReader` — used as a quick `getLedger` shim and for ad-hoc inspection. |
| `cmd/stellar-rpc/scripts/upload-cold/main.go` | GCS uploader using ADC (cloud.google.com/go/storage). |
| `cmd/stellar-rpc/scripts/bench-fullhistory/` | The benchmark harness (8 source files, single binary). Sub-commands: `seed-hot`, `seed-txhash-{hot,cold}`, `seed-events`, `build-cold-events-index`, `ledger-point`, `ledger-range`, `tx-page`, `tx-hash`, `events`. |
| `/tmp/bench-fullhistory` | Compiled bench binary |
| `/tmp/go126/go/bin/go` | Go 1.26 install (required by PR #740 — `streamhash` dep needs ≥ 1.26) |

## Build & run quick reference

```bash
# Go 1.26 needed for the cherry-pick branch
export PATH="/tmp/go126/go/bin:$PATH"

# Rebuild bench harness
go build -o /tmp/bench-fullhistory ./cmd/stellar-rpc/scripts/bench-fullhistory/...

# Run any bench
/tmp/bench-fullhistory ledger-point --tier=cold --chunk=5000 --iters=2000 \
    --hot-dir=/mnt/nvme/disk2/ledgers/hot-5000 \
    --out=bench-out

# Rebuild on `rpc-hack` (without PR #740) — go 1.25 works
unset PATH; export PATH="/home/simon/.local/go/bin:$PATH"   # default toolchain
```

## Decisions taken (with rationale)

| Decision | Rationale |
|---|---|
| **GCS auth via ADC, not `gcloud auth login`** | `gcloud storage` doesn't honor `GOOGLE_APPLICATION_CREDENTIALS`, but `cloud.google.com/go/storage` does. Writing a small Go uploader was simpler than fighting gcloud's separate credential store. |
| **`--workers 32` for migration** | Hardware has 32 vCPU; 32 was ~2.1× faster than 16 because workers pipeline I/O with zstd. Hit a 25 chunks/min steady-state rate. |
| **`--bsb-buffer-size 1000` for ingest** (rejected 10000) | Larger buffer was ~20% *slower* — network was already the bottleneck, more in-flight items only added queue contention. |
| **16-worker GCS upload** (rejected 32) | Network ceiling at ~232 MB/s aggregate; adding workers didn't move the needle. |
| **Drop EBS tier from benchmarks** | Scope-of-work decision; only Hot vs Cold-NVMe matters for the SLA story right now. |
| **Sorted-.bin for cold txhash bench** (rejected production RecSplit) | RecSplit is a 5–7-day implementation; the bench's conclusion ("ledger decode dominates") holds for any sub-millisecond cold index. Documented in `BENCHMARK_GOAL.md` caveats. |
| **No multi-chunk iterators** | Each bench stays within one chunk; cross-chunk routing is its own substantive design decision. |
| **Phase 4 ordering: events last** | Lowest certainty about ROI / highest implementation lift. By the time we got there, PR #740 had landed in a state worth cherry-picking. |
| **Cherry-pick PR #740 surgically** | PR #740 was branched before #739/`891badf` and would otherwise revert the ledger cold store. Pulled only the new files (`chunk/`, `eventstore/`, replacement events/ files) and the additive rocksdb/events changes; kept our ledger code intact. |

## Surprises / counter-intuitive findings

1. **Hot ≈ cold for ledger queries**, within 10%, cold often slightly faster. The bottleneck is zstd decompression (~1.3 ms / ~1 MB ledger on Graviton2), which both tiers pay equally. Storage I/O is in the noise.
2. **Cold beats hot 2.3× for sequential dense event reads** (`no-filter`). One `ReadRange` over the packfile + batched zstd beats RocksDB per-event Gets.
3. **Hot beats cold 1.8× for low-cardinality topic events**. With only 6 distinct topic-0 values in pubnet (events emit shared top-level names like "transfer"), each Lookup returns ~1000 events scattered across the chunk — cold's random-read pattern can't compete with hot's RocksDB block cache.
4. **Pubnet topic-0 cardinality is comically small** — 6 distinct values across 7.91M events in one chunk. Has design implications for any cold index that assumes high-cardinality keys (RecSplit, MPHF, etc. — those handle it fine, but filter selectivity expectations need calibration).
5. **`supportlog.New()` defaults to Warn level** — silently dropped my Infof calls on the first backfill run; I thought the process was hanging. Always `logger.SetLevel(logrus.InfoLevel)` explicitly.
6. **Larger BSB buffer was *worse*** — network was the bottleneck, more buffer just added GC pressure.
7. **GCS uploads consistently ran at ~232 MB/s** regardless of worker count past 8 — AWS egress (or GCS ingest, hard to tell which) is the ceiling.

## Code gotchas

- **`flag.Uint32Var` doesn't exist in stdlib** — used `flag.Uint64Var` + bounds check + cast.
- **`internal` package rules**: scripts under `cmd/stellar-rpc/scripts/` can import `cmd/stellar-rpc/internal/*`; same-named under `scripts/` at the repo root cannot. Don't put Go drivers at the repo root.
- **`xdr.LedgerCloseMeta.CountTransactions()` / `.TransactionHash(i)`** hide V0/V1/V2 — use these, don't switch on `lcm.V` directly.
- **`packfile.Writer.Close()` removes the partial file if `Finish` wasn't called.** Defer pattern is safe: `defer w.Close()` after `w, _ := Create(...)`; on the happy path `Finish` runs and `Close` becomes a no-op.
- **`ColdStoreReader` opens lazily** — `NewColdStoreReader` doesn't actually touch the file. Call `FirstSeq()` or any method to force the open and surface format errors.
- **`ColdStoreReader.IterateLedgers` silently clamps to a single chunk** — IterateLedgers(start, end) where end > chunk's lastSeq returns only what's in the chunk, no error. Must be paired with a chunk-routing wrapper to span boundaries.
- **`eventstore.ColdReader` needs 3 files** to open: `events.pack`, `index.pack`, `index.hash`. The `events.pack` writer is `ColdWriter`, the other two come from `WriteColdIndex(ctx, chunkID, hotStore.Index(), bucketDir)` — that takes the *hot* store's in-memory `events.BitmapIndex` as the source. So produce the cold index *after* hot ingestion completes.

## Open follow-ups (not done; tracked for later)

- **Production RecSplit cold txhash index** — design-doc-compliant index, 5–7 days. Sorted-.bin shim is in place for benchmarking; production wants RecSplit.
- ~~**Cold-cache benchmarks**~~ — done (✅) via `posix_fadvise(FADV_DONTNEED)` on a sidecar fd; no sudo needed. See "Cold-cache + concurrency addendum" below.
- ~~**Multi-threaded throughput**~~ — done (✅) for `ledger-point` via the new `ledger-point-concurrent-cold` sub-command. Other workloads (range, tx-page, events) still single-threaded.
- **Multi-chunk iterator** — for `IterateLedgers` / page queries spanning chunk boundaries. Would need a coordinator that holds multiple `ColdStoreReader`s.
- **Hot+cold composed reader** — "try hot, fall back to cold" for the eventual streaming-window staleness model. Not in scope for benching but is in scope for production routing.
- **Drop the 1.2 TB old-format ledger packs**: done (✅). Both old `/mnt/nvme/disk2/ledgers/00004/` and `/mnt/nvme/disk2/ledgers/00005/` deleted.
- **Drop local copies if the instance gets recycled**: `/mnt/nvme/disk2/ledgers/cold/` is on ephemeral storage. The same bytes are at `gs://rpc-full-history/cold/`; re-downloading from GCS is the recovery path. Will incur GCS egress (free in same region; otherwise $0.12/GB).
- **PR #740 rebase** — when the upstream PR is rebased onto current `rpc-hack`, our `cherrypick-740` branch's contents become redundant. Switch to using the upstream commit directly.
- **Wire any of this into `methods.getLedger` / `getEvents`** — none of the eventstore/cold readers are used by the RPC daemon yet. That's the missing plumbing for the actual RPC service.

## What an incoming engineer should read first

1. `BENCHMARK_GOAL.md` — the numbers and what they mean.
2. `full-history/design-docs/03-backfill-workflow.md` — the canonical design for the production backfill subcommand (which our exploratory script is a thin prototype of).
3. `design-docs/getevents-full-history-design.md` — events cold-segment design, now implemented in PR #740.
4. This file — the snapshot of what's actually on disk and what state things are in.

## Reproducing a single bench from scratch

```bash
export PATH="/tmp/go126/go/bin:$PATH"

# Build harness
go build -o /tmp/bench-fullhistory ./cmd/stellar-rpc/scripts/bench-fullhistory/...

# (Re)seed hot store if /mnt/nvme/disk2/ledgers/hot-5000 doesn't exist
/tmp/bench-fullhistory seed-hot --chunk 5000 \
    --cold-dir /mnt/nvme/disk2/ledgers/cold \
    --hot-dir /mnt/nvme/disk2/ledgers/hot-5000

# Run a single bench (example: hot-tier ledger point lookup)
/tmp/bench-fullhistory ledger-point --tier=hot --chunk=5000 --iters=2000 \
    --hot-dir=/mnt/nvme/disk2/ledgers/hot-5000 \
    --out=bench-out
```

Replace `ledger-point` with `ledger-range --n=100` / `tx-page --page-size=20` / `tx-hash` / `events --scenario=contract` etc. The seed sub-commands only need to run once per chunk per store.

---

## Cold-cache + concurrency addendum (May 19, 2026 follow-up session)

Extends the original (warm-cache, single-thread) bench harness with cold-OS-pagecache and multi-worker scenarios. Same machine, same dataset, same branch (`chowbao/full-history-benchmarks-poc`).

### Why cold cache matters for full-history

- Cold dataset is 1.4 TiB; instance has ~123 GiB RAM → at most ~9% of the data can ever be hot. Real "tail-history" queries miss the page cache by definition.
- Prior session methodology was **warm-cache** ("...same seed across tiers, warm-cache, single-threaded, 1000+ iters..."). Those numbers represent the *best case*, not the SLA against random historical queries.
- `drop_caches` requires sudo and nukes the whole system; **`posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED)`** is the right tool — no sudo, targeted per-file, more realistic (simulates LRU aging-out rather than nuclear wipe).
- "Cache filled with garbage" is *not* an effective substitute for cold: LRU eviction is cheap, so the cost of the unrelated pages is amortized to zero. What matters is whether the *target* packfile's pages are resident.

### New bench tooling

| File | Purpose |
|---|---|
| `cmd/stellar-rpc/scripts/bench-fullhistory/cache.go` | `evictFile(path)` (FADV_DONTNEED via sidecar fd) + `residency(path)` (mmap + raw `mincore` syscall — `unix.Mincore` is **not** in x/sys v0.40, must call SYS_MINCORE directly). Linux-only build tag. |
| `cmd/stellar-rpc/scripts/bench-fullhistory/bench_ledger_cold.go` | Two new sub-commands: `ledger-point-cold-open` (close+reopen+evict per iter, spread across chunks) and `ledger-point-concurrent-cold` (N workers, each cold-open spread across chunks, aggregate throughput). |

Wired into `main.go` dispatcher; `usage()` updated.

### Measured results (chunks 5000–5050, ~50 chunks span)

| Scenario | Concurrency | n | p50 | p90 | p99 | ops/sec |
|---|---|---|---|---|---|---|
| `ledger-point-cold-open` | 1 worker | 20 | 2.58 ms | 3.37 ms | 4.63 ms | 373 |
| `ledger-point-concurrent-cold` | 8 workers | 240 | 2.91 ms | 6.24 ms | 19.78 ms | **1240** |

vs. prior warm-cache numbers (b526fa8): `ledger-point cold` p50 = 1.29 ms.

**Implications:**
- Cold-open overhead ≈ 2.58 − 1.29 ≈ **~1.3 ms** added per fresh open (open syscall + 256KB tail read + index parse). One-time per request, not per ledger.
- 8-way concurrency yields ~3.3× throughput, **not 8×** — bottleneck is shared zstd CPU and NVMe pread queueing, not file-level contention.
- p99 inflates ~4× under 8 workers (4.6 ms → 19.8 ms) — classic disk-queue tail. Expect this pattern to compound at higher worker counts.
- Residency verification: 23/24 sampled iters had 0 resident pages after evict, confirming `FADV_DONTNEED` is reliable on this kernel (Linux 6.14 aarch64).

### Projected `getLedgers()` performance

Model: `T(N) = cold-open + N × per-ledger-decode`. With cold-open ≈ 1.3 ms and per-ledger-decode ≈ 1.3 ms (zstd-dominated, hot/cold-cache near-identical for record-level reads):

| Page size | Storage-layer p50 (cold) | + est. XDR/HTTP serialization |
|---|---|---|
| 1 | ~2.6 ms | ~4–5 ms |
| 10 | ~15 ms | ~17–18 ms |
| **50 (default)** | ~71 ms | ~74–76 ms |
| 100 | ~141 ms | ~145–148 ms |
| 200 (max) | ~282 ms | ~285–290 ms |

**Ledger throughput plateaus at ~700 ledgers/sec single-core** regardless of page size — this is the single-thread zstd ceiling.

**Theoretical aggregate (32 vCPU saturated, CPU-bound on zstd, ~10–15× scaling factor):**

| Page size | Sustained req/s (est.) |
|---|---|
| 1 | ~3,000–5,000 |
| 10 | ~700–1,000 |
| 50 | ~140–200 |
| 100 | ~70–100 |
| 200 | ~35–50 |

Aggregate ledger throughput ceiling: **~7,000–10,000 cold ledgers/sec** delivered.

**Caveats:**
- Per-ledger cold cost (1.3 ms) is *inferred* from warm-cache range numbers + a small NVMe-pread adjustment. To measure directly, add a `ledger-range-cold-open` scenario (open fresh, read N consecutive, close, repeat with different chunk). **Not yet implemented.**
- Concurrency scaling factor (10–15×) is extrapolated from 8-worker data; real saturation curve untested past 8 workers.
- Serialization (XDR encode → JSON → HTTP) overhead is a guess; benching the actual handler is the only way to nail it down.
- The Stellar RPC handler also reads from the local DB first (`BatchGetLedgers`) before falling back to the datastore reader — these projections cover only the cold-storage code path.

### Code gotchas (additions)

- **`golang.org/x/sys v0.40` does not export `unix.Mincore`.** It exports `Madvise` and `Fadvise` but mincore must be called via `unix.Syscall(unix.SYS_MINCORE, addr, len, vec)`. The constant `SYS_MINCORE` *is* exported per-arch (arm64 = 232).
- **Sidecar-fd pattern for fadvise**: `posix_fadvise` operates on the inode's pagecache, not the fd. You can open a fresh fd just for eviction and leave the long-lived reader's fd untouched. This means no need to expose the packfile.Reader's internal fd.
- **`FADV_DONTNEED` is a hint, but reliable on Linux 6.14 for clean file pages.** Always verify with mincore the first time you wire it up.
- **Kernel readahead survives DONTNEED**: after eviction, the first pread triggers ~128KB of speculative readahead. Fine for cold-open latency (it's part of the cost). For pure random-access measurements where you want zero prefetch, add `FADV_RANDOM` once on the fd.
- **ARM64 page size is 4096 on AWS Graviton** (per `getconf PAGE_SIZE`), so mincore granularity is 4KB. 64K pages on other ARM systems would need different math.

### What's still missing for a real SLA story

1. **Cold-range bench** (`ledger-range-cold-open`) — measure cold getLedgers(N) directly instead of extrapolating.
2. **End-to-end RPC handler bench** — hit the actual `getLedgers` JSON-RPC endpoint, not just the storage layer. Measures serialization, framework overhead, and the local-DB→datastore fallback path.
3. **Concurrency curve past 8 workers** — find the saturation point on 32 vCPU. Probably knee around 16–24.
4. **GOMAXPROCS sensitivity** — the bench inherits default GOMAXPROCS=32. Production may run with a different setting.
5. **Mixed warm/cold workloads** — production traffic is bimodal (recent ledgers warm, deep history cold). Pure cold-cache numbers are the worst case.

