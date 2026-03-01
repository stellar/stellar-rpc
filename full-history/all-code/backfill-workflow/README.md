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
| 2 | `BACKFILL:RANGE:0000` | Range workers: per-range progress and completion |
| 3 | `BACKFILL:RANGE:0000:BSB:00` | BSB instances, RecSplit builders |
| 4 | `BACKFILL:RANGE:0000:BSB:00:CHUNK:042` | Per-chunk writes, per-CF RecSplit |

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
[:BACKFILL] BSB: buffer=1000, workers=20, instances/range=20
[:BACKFILL]
[:BACKFILL] Opening meta store at /mnt/nvme/disk1/stellar-backfill/meta/rocksdb
```

### Resume (after restart)

When resuming from a prior run, the pipeline reports per-range state and lists all chunk gap regions (contiguous runs of incomplete chunks):

```
[:BACKFILL]   Range 0000: INGESTING — 279/1000 chunks done (27.9%), 721 remaining
[:BACKFILL]              gap: chunks 13-49 (37)
[:BACKFILL]              gap: chunks 63-99 (37)
[:BACKFILL]              ...
[:BACKFILL]              gap: chunks 964-999 (36)
[:BACKFILL]   Range 0001: INGESTING — 279/1000 chunks done (27.9%), 721 remaining
[:BACKFILL]              gap: chunks 1014-1049 (36)
[:BACKFILL]              ...
[:BACKFILL]              gap: chunks 1964-1999 (36)
[:BACKFILL]   Range 0002: NEW (no prior state)
```

The striped gap pattern comes from the 20 BSB instances per range — each instance handles 50 non-contiguous chunks, so a mid-run cancellation leaves one gap per instance.

### 1-Minute Progress Ticker

Every 60 seconds, a progress block is logged for each active range:

```
[:BACKFILL] ── Progress (4m elapsed) ────────────────────────────
[:BACKFILL]   Range 0000 [INGESTING]: 118/1,000 chunks (11.8%) — ETA 29m 53.9s
[:BACKFILL]     4,916 ledgers/s | 226 tx/s | 29.5 chunks/min
[:BACKFILL]     LFS p50=1.937ms p90=2.089ms — BSB p50=16.661µs p90=13.232ms
[:BACKFILL]   Range 0001 [INGESTING]: 119/1,000 chunks (11.9%) — ETA 29m 37.76s
[:BACKFILL]     4,955 ledgers/s | 17,713 tx/s | 29.7 chunks/min
[:BACKFILL]     LFS p50=2.279ms p90=21.911ms — BSB p50=47.779µs p90=12.745ms
[:BACKFILL]   Memory: 6.18 MB current, 6.18 MB peak
[:BACKFILL] ──────────────────────────────────────────────────────
```

**Reading the metrics:**

- **Throughput** (ledgers/s, tx/s, chunks/min) — averaged over the range's lifetime, not windowed.
- **LFS p50/p90** — per-chunk write latency (writing 10K serialized ledger close metas to disk). Early ranges with few transactions are fast (~2ms); later ranges with heavier transaction volume show higher p90.
- **BSB p50/p90** — per-ledger `GetLedger()` call latency from the GCS prefetch buffer. p50 in the microsecond range means the buffer is keeping up; p90 in the millisecond range means occasional waits for GCS downloads to land.
- **ETA** — simple linear extrapolation from chunks completed so far. On resume, this accounts for prior-run progress.

## Architecture

```
Range (10M ledgers) → 1000 Chunks (10K ledgers each)

Phase 1: Ingestion
  N BSB instances per range (default 20, each processing 50 chunks)
  Each instance: GCS fetch → LFS write + txhash .bin write → fsync → flag

Phase 2: RecSplit
  16 parallel goroutines (one per CF / hash nibble)
  Each CF: read all 1000 .bin files → filter → build RecSplit index → fsync → flag

Crash Recovery:
  If either flag absent → full chunk rewrite (no partial reuse)
  Per-CF done flags → only rebuild incomplete CFs
```
