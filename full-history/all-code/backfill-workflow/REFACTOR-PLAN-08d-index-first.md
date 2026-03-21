# Refactor Plan: Uniform %08d + Index-First Directory Layout

## Context

The design doc (`design-docs-temp/03-backfill-workflow.md`) has been updated to specify:
1. **Index-first directory layout**: all immutable data under `index-{indexID:08d}/`
2. **Uniform `%08d`** for all IDs in paths, meta store keys, task IDs, and log output
3. **Single `immutable_base`** config key replacing `ledgers_base` + `txhash_base` + `events_base`

## What's Already Done

- `meta_store.go` — key functions changed from `%010d` to `%08d`
- `paths.go` — fully rewritten: index-first layout, `%08d`, all functions take `immutableBase` + `indexID`
- `pkg/lfs/chunk.go` — path functions rewritten: take `immutableBase` + `indexID` + `chunkID`, use `%08d`

## What Remains — Code Changes

### 1. Fix `helpers/lfs/chunk.go` (duplicate of `pkg/lfs/chunk.go`)

Same changes as `pkg/lfs/chunk.go`. This file appears to be a duplicate — consider deleting it and using `pkg/lfs` everywhere.

### 2. Fix `pkg/lfs/discovery.go` — 4 call sites

All calls to `ChunkExists` need `indexID` parameter added.
Need to figure out how discovery derives `indexID` from `chunkID` — it may need a `Geometry` parameter or the caller must pass `indexID`.

### 3. Fix `pkg/lfs/iterator.go` — 4 call sites

Calls to `GetIndexPath` and `GetDataPath` need `indexID` parameter.
The iterator opens files by chunkID — it needs to know `indexID` to find the right directory.
Options: pass `indexID` at iterator creation, or pass `immutableBase` that already includes the index dir.

### 4. Fix `backfill-workflow/lfs_writer.go`

- Add `IndexID uint32` to `LFSWriterConfig`
- Update all `lfs.GetDataPath(cfg.DataDir, cfg.ChunkID)` → `lfs.GetDataPath(cfg.DataDir, cfg.IndexID, cfg.ChunkID)`
- Same for `GetIndexPath`, `GetChunkDir`
- Update `Abort()` method's file deletion calls
- The `DataDir` field should be renamed to `ImmutableBase` for clarity

### 5. Fix `backfill-workflow/lfs_source.go`

- `NewLFSPackfileSource` calls `lfs.ChunkExists` — needs `indexID` param
- Need to thread `indexID` through from `processChunkTask`

### 6. Fix `backfill-workflow/chunk_writer.go`

- `deletePartialFiles()` calls `lfs.GetDataPath`/`lfs.GetIndexPath` — needs `indexID`
- Already has `IndexID` in `ChunkWriterConfig` — just need to pass it to LFS functions
- Replace `cw.cfg.LedgersBase` with `cw.cfg.ImmutableBase` (or whatever the new field name is)

### 7. Fix `backfill-workflow/config.go`

- Replace `LedgersBase string` and `TxHashBase string` in `ImmutableConfig` with single `ImmutableBase string`
- Update `Validate()` default: `{data_dir}/immutable`
- Update all callers that reference `cfg.ImmutableStores.LedgersBase` or `cfg.ImmutableStores.TxHashBase`

### 8. Fix `backfill-workflow/tasks.go`

- `ProcessChunkTaskID`: `%06d` → `%08d`
- `BuildTxHashIndexTaskID`: `%04d` → `%08d`
- `processChunkTask`: replace `ledgersBase`/`txHashBase` fields with `immutableBase`
- `buildTxHashIndexTask`: replace `txHashBase` with `immutableBase`
- Log message in `processChunkTask.Execute`: `%06d` → `%08d`

### 9. Fix `backfill-workflow/pipeline.go`

- All `%04d` format strings for indexID → `%08d`
- Update `addChunkTasks` and `addBuildTask` to pass `immutableBase` instead of separate base paths
- Update reconciler creation to pass `immutableBase`

### 10. Fix `backfill-workflow/stats.go`

- All `%04d` format strings for indexID → `%08d`

### 11. Fix `backfill-workflow/reconciler.go`

- `TxHashBase` field → `ImmutableBase`
- Update `RawTxHashDir` calls to use new path functions

### 12. Fix `backfill-workflow/recsplit_flow.go`

- Replace all `txhashBase` references with `immutableBase`
- Update all path function calls (RawTxHashPath, RecSplitIndexPath, etc.)

### 13. Fix `backfill-workflow/main.go`

- Update pipeline config creation to pass `ImmutableBase` instead of `LedgersBase`/`TxHashBase`

### 14. Fix all test files

- `meta_store_test.go` — update key format expectations
- `paths_test.go` — update path expectations
- `chunk_writer_test.go` — update path calls, add `indexID`
- `lfs_writer_test.go` — update path calls, add `indexID`
- `pipeline_test.go` — update config struct, path expectations
- `reconciler_test.go` — update config struct
- `dag_test.go` — task ID format may change
- `config_test.go` — update for `ImmutableBase`
- `recsplit_flow_test.go` — update path calls

### 15. Fix `backfill-config.toml`

- Replace `ledgers_base` and `txhash_base` with `immutable_base`

## Testing

After all changes:
```bash
cd full-history/all-code
make build        # verify compilation
make test         # if test target exists, or:
CGO_ENABLED=1 CGO_CFLAGS="..." CGO_LDFLAGS="..." go test ./backfill-workflow/... -count=1
CGO_ENABLED=1 CGO_CFLAGS="..." CGO_LDFLAGS="..." go test ./pkg/lfs/... -count=1
```

## Order of Execution

1. Config change (`config.go`) — foundation, everything else depends on field names
2. LFS package (`pkg/lfs/chunk.go`, `helpers/lfs/chunk.go`) — already done for `pkg/lfs`
3. LFS iterator/discovery (`pkg/lfs/iterator.go`, `pkg/lfs/discovery.go`)
4. Backfill writers (`lfs_writer.go`, `chunk_writer.go`)
5. Tasks + pipeline (`tasks.go`, `pipeline.go`, `main.go`)
6. RecSplit flow (`recsplit_flow.go`)
7. Reconciler (`reconciler.go`)
8. Stats (`stats.go`)
9. All tests
10. Config file (`backfill-config.toml`)
11. Build + test
