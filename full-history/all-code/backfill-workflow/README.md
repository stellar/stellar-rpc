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
# Create config.toml:
cat <<'EOF' > config.toml
[service]
data_dir = "/data/stellar-rpc"

[backfill]
start_ledger = 2
end_ledger = 10000001

[backfill.bsb]
bucket_path = "sdf-ledger-close-meta/v1/ledgers/pubnet"
EOF

./bin/backfill-workflow --config config.toml
```

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
