# Build & Test

## Build — CGO required

Always use the Makefile from `full-history/all-code/`:

```bash
make check-rocksdb-env        # verify RocksDB first
make build                    # build all
make build-backfill-workflow  # just backfill-workflow
make clean                    # remove bin/
```

Never leave binaries in the repo.

## CGO environment (if running go commands directly)

### macOS
```bash
CGO_ENABLED=1 \
CGO_CFLAGS="-I$(brew --prefix rocksdb)/include" \
CGO_LDFLAGS="-L$(brew --prefix rocksdb)/lib -L$(brew --prefix snappy)/lib -L$(brew --prefix lz4)/lib -L$(brew --prefix zstd)/lib -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd" \
DYLD_LIBRARY_PATH=$(brew --prefix rocksdb)/lib
```

### Linux
```bash
CGO_ENABLED=1 \
CGO_CFLAGS="-I${ROCKSDB_HOME:-/usr/local}/include" \
CGO_LDFLAGS="-L${ROCKSDB_HOME:-/usr/local}/lib -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd" \
LD_LIBRARY_PATH=${ROCKSDB_HOME:-/usr/local}/lib
```

## Testing

| Type | CGO? | How |
|------|------|-----|
| Pure logic | No | `go test ./pkg/...` |
| Store tests (RocksDB) | Yes | Makefile CGO env |
| Integration | Yes | Makefile CGO env |

Test files: `foo_test.go` next to `foo.go`. All deps behind interfaces = trivial mocks.

## Never rules

- Never import `github.com/stellar/go` (archived) — use `github.com/stellar/go-stellar-sdk`
- Never leave binaries in repo — `make clean`
- Never run CGO tests without CGO flags
