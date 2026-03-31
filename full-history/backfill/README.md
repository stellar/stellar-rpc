# Backfill Workflow

Offline pipeline for ingesting historical Stellar ledger data. Writes LFS chunk files and raw txhash `.bin` files, then builds RecSplit indexes.

## Prerequisites

```bash
# RocksDB (required for meta store and RecSplit tests)
brew install rocksdb          # macOS
# OR: apt-get install librocksdb-dev  # Linux

# CGO environment (required for RocksDB and RecSplit)
export CGO_ENABLED=1
export CGO_CFLAGS="-I/opt/homebrew/include"
export CGO_LDFLAGS="-L/opt/homebrew/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
```

## Build

```bash
# From full-history/all-code/
make build-backfill-workflow

# Or directly:
CGO_ENABLED=1 go build -o bin/backfill-workflow ./backfill-workflow/cmd/
```

## Run

```bash
# Copy and edit the sample config (all settings documented with defaults):
cp backfill-workflow/backfill-config.toml my-config.toml
# Edit my-config.toml: set data_dir, ledger range, BSB bucket, etc.

./bin/backfill-workflow --config my-config.toml
```

### Log Verbosity

The `max_scope_depth` setting in `[logging]` controls how much per-component detail appears in logs. The scope nesting maps directly to the pipeline hierarchy:

| Depth | Scope | What you see |
|-------|-------|-------------|
| 1 | `BACKFILL` | Orchestrator: startup, 1-min progress, final summary |
| 2 | `BACKFILL:RANGE:0000` | DAG tasks: per-range transitions and completion |
| 3 | `BACKFILL:RANGE:0000:CHUNK:042` | Per-chunk task detail, RecSplit phases |
| 4 | `BACKFILL:RANGE:0000:CHUNK:042:DETAIL` | Per-chunk writes, per-CF RecSplit |

`0` = show all (default). For production runs with many ranges, `2` or `3` avoids per-chunk log flooding.

## Test

```bash
# From full-history/all-code/

# All tests (includes RocksDB and RecSplit tests)
CGO_ENABLED=1 go test ./backfill-workflow/... -v -count=1

# Specific test groups
CGO_ENABLED=1 go test ./backfill-workflow/... -run TestMeta -v        # Meta store tests
CGO_ENABLED=1 go test ./backfill-workflow/... -run TestRecSplit -v    # RecSplit tests
CGO_ENABLED=1 go test ./backfill-workflow/... -run TestChunkWriter -v # Chunk writer tests

# Race detector
CGO_ENABLED=1 go test ./backfill-workflow/... -race -v
```

## Signal Handling

- **SIGINT (Ctrl+C)**: Graceful shutdown — finishes current chunk, fsyncs, sets flags, then exits. Safe to restart.
- **SIGTERM**: Same as SIGINT.
- **SIGKILL**: Immediate kill — on restart, any in-progress chunks without flags will be fully rewritten.

## Example Logs

### Startup

On launch, the pipeline dumps its full configuration and backend details:

```
[:BACKFILL] ───────────────────────────────────────────────────
[:BACKFILL] Stellar Full-History Backfill Pipeline
[:BACKFILL] ───────────────────────────────────────────────────
[:BACKFILL] Config: /home/karthik/backfill-config.toml
[:BACKFILL] Data dir: /mnt/nvme/disk1/stellar-backfill
[:BACKFILL] Ledgers: 2 - 30000001
[:BACKFILL] Parallel ranges: 2
[:BACKFILL] Backend: GCS (bucket: sdf-ledger-close-meta/v1/ledgers/pubnet)
[:BACKFILL] BSB: buffer=1000, workers=20
[:BACKFILL]
[:BACKFILL] Opening meta store at /mnt/nvme/disk1/stellar-backfill/meta/rocksdb
```

### Resume (after restart)

When resuming from a prior run, the pipeline reports per-range state, lists all chunk gap regions, and summarizes which ranges are in which stage:

```
[:BACKFILL]   Range 0000: COMPLETE
[:BACKFILL]   Range 0001: RECSPLIT_BUILDING — 0/16 CFs done
[:BACKFILL]              pending CFs: [0 1 2 3 4 5 6 7 8 9 a b c d e f]
[:BACKFILL]   Range 0002: INGESTING — 0/1000 chunks done (0.0%), 1000 remaining
[:BACKFILL]              gap: chunks 2000-2999 (1000)
[:BACKFILL]
[:BACKFILL]   Status: RESUMING
[:BACKFILL]     Ranges complete:              0000 (1)
[:BACKFILL]     Ranges ingesting chunks:      0002 (1)
[:BACKFILL]     Ranges building RecSplit:     0001 (1)
```

When ranges are mid-ingestion, you'll see gap patterns from concurrent `process_chunk` tasks — since tasks make independent progress, a mid-run cancellation leaves non-contiguous gaps:

```
[:BACKFILL]   Range 0000: INGESTING — 598/1000 chunks done (59.8%), 402 remaining
[:BACKFILL]              gap: chunks 29-49 (21)
[:BACKFILL]              gap: chunks 80-99 (20)
[:BACKFILL]              ...
[:BACKFILL]              gap: chunks 980-999 (20)
```

### Crash Recovery

The pipeline handles three recovery scenarios depending on when it was killed:

**Killed during ingestion** — the DAG rebuilds with `process_chunk` tasks that use skip-sets. Already-completed chunks are skipped, remaining chunks are re-ingested:

```
[:BACKFILL] Task graph: 483 tasks   (482 process_chunk + 1 build, skip-set has 518 chunks)
[:BACKFILL:RANGE:0001:CHUNK:515] Complete: 10,000 ledgers in 42.3s
```

**Killed during RecSplit** — the DAG contains only 1 task (build_txhash_index with no dependencies). RecSplit reruns from scratch with all-or-nothing cleanup:

```
[:BACKFILL] Task graph: 1 tasks     (build only, no process deps)
[:BACKFILL:RANGE:0001:RECSPLIT] Phase 3/4: Building 16 indexes...
[:BACKFILL:RANGE:0001:RECSPLIT:CF:f] Build complete: 2,701,025 keys, 13.76 MB, 40.55s
...
[:BACKFILL:RANGE:0001:RECSPLIT] All phases complete — updating range state to COMPLETE
[:BACKFILL:RANGE:0001:RECSPLIT] Deleted raw/ — freed 1.45 GB
```

**All ranges already complete** — the DAG is empty, pipeline exits immediately:

```
[:BACKFILL] Task graph: 0 tasks
[:BACKFILL] BACKFILL COMPLETE
```

### 1-Minute Progress Ticker

Every 60 seconds, a progress block is logged for each range showing its current phase:

```
[:BACKFILL] ── Progress (12m elapsed) ────────────────────────────
[:BACKFILL]   Range 0000 [RECSPLIT:BUILDING]: 9/16 CFs done
[:BACKFILL]   Range 0001 [INGESTING]: 718/1,000 chunks (71.8%) — ETA 5m 12.3s
[:BACKFILL]     4,955 ledgers/s | 17,713 tx/s | 29.7 chunks/min
[:BACKFILL]     LFS p50=2.279ms p90=21.911ms — BSB p50=47.779µs p90=12.745ms
[:BACKFILL]   Range 0002 [INGESTING]: 312/1,000 chunks (31.2%) — ETA 18m 41.0s
[:BACKFILL]     4,916 ledgers/s | 226 tx/s | 29.5 chunks/min
[:BACKFILL]     LFS p50=1.937ms p90=2.089ms — BSB p50=16.661µs p90=13.232ms
[:BACKFILL]   Range 0003 [QUEUED]
[:BACKFILL]   Memory: 6.18 MB RSS (peak 6.18 MB) | Go heap: 4.2 MB alloc, 8.1 MB sys | 142 goroutines
[:BACKFILL] ──────────────────────────────────────────────────────
```

This reflects the DAG's interleaving: Range 0 has moved to RecSplit while Ranges 1–2 are still ingesting with freed worker slots.

**Reading the metrics:**

- **Throughput** (ledgers/s, tx/s, chunks/min) — averaged over the range's lifetime, not windowed.
- **LFS p50/p90** — per-chunk write latency (writing 10K serialized ledger close metas to disk). Early ranges with few transactions are fast (~2ms); later ranges with heavier transaction volume show higher p90.
- **BSB p50/p90** — per-ledger `GetLedger()` call latency from the GCS prefetch buffer. p50 in the microsecond range means the buffer is keeping up; p90 in the millisecond range means occasional waits for GCS downloads to land.
- **ETA** — simple linear extrapolation from chunks completed so far. On resume, this accounts for prior-run progress.

## Architecture

### DAG-Based Task Scheduler

The pipeline uses a dependency-driven DAG (directed acyclic graph) to coordinate all work. The orchestrator builds the DAG at startup by triaging each range's resume state, then executes it with bounded concurrency.

**Task types:**

| Task | Cadence | Scope | Dependencies |
|------|---------|-------|-------------|
| `process_chunk(range,chunk)` | Chunk | 1 chunk (10K ledgers) | None |
| `build_txhash_index(range)` | Range | 10M ledgers (1000 chunks) | All process_chunk tasks for range |

**Per-range dependency graph:**

```
process_chunk(R, 0)     ─┐
process_chunk(R, 1)     ─┤
...                      ├──► build_txhash_index(R)
process_chunk(R, 999)   ─┘
```

**Concurrency:** `workers` (default 40) bounds how many tasks execute simultaneously. All `process_chunk` tasks across all ranges compete for worker slots in the same flat pool — there is no range-level isolation. As tasks from one range finish, freed slots are claimed by tasks from other ranges.

**Phase transitions:** The DAG's dependency edges enforce ordering. When the last `process_chunk` for a range completes, the DAG decrements `build_txhash_index`'s in-degree to zero and dispatches it. The build task's first action is `SetRangeState(RECSPLIT_BUILDING)`. RecSplitFlow manages its own internal concurrency (100 goroutines for count/add/verify, 16 for build) — these are invisible to the DAG's worker pool.

**Future extensibility (when events are added):**

```
process_chunk(R, 0..999) ──► build_txhash_index(R) ──┐
                         └─► build_events_index(R) ──┴──► complete_range(R)
```

### Data Flow

```
Range (10M ledgers) → 1000 Chunks (10K ledgers each)

Ingestion (process_chunk tasks):
  Up to 40 concurrent tasks (shared across all ranges), each with its own GCS connection
  Each task: GCS fetch → LFS write + txhash .bin write → fsync → flag

RecSplit (build_txhash_index task):
  4-phase pipeline: Count → Add → Build → Verify
  Count/Add/Verify: 100 goroutines reading .bin files
  Build: 16 parallel goroutines (one per CF / hash nibble)
  On completion: set COMPLETE, delete raw/ to free disk
```

### Crash Recovery

The DAG is rebuilt from scratch on every startup. Resume speed comes from building the minimal task set:

| Prior State | Tasks Created | What Happens |
|-------------|--------------|-------------|
| COMPLETE | 0 | Skipped entirely |
| RECSPLIT_BUILDING | 1 (build only, no deps) | RecSplit reruns from scratch (all-or-nothing) |
| INGESTING | up to 1001 (chunks + build) | Tasks use skip-sets to avoid redoing completed chunks |
| NEW | 1001 (chunks + build) | Full processing |

Additionally, the startup reconciler runs before DAG construction:
- COMPLETE ranges: deletes leftover raw/ directories
- RECSPLIT_BUILDING ranges: verifies raw/ exists, deletes partial .idx files
- Detects orphan ranges not in current config
