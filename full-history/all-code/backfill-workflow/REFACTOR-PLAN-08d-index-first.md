# Refactor: Uniform %08d + Index-First Directory Layout

> **For agentic workers:** REQUIRED: Use superpowers:executing-plans to implement this plan. After each major task, run `go test ./backfill-workflow/... ./pkg/lfs/...` to verify. Take at least 3 verification passes after all code changes: (1) code compiles and tests pass, (2) every format string in every .go file uses %08d — grep for %04d/%06d/%010d and fix any survivors, (3) every path function produces paths matching the design doc's directory structure.

**Goal:** Complete the uniform `%08d` + index-first directory layout refactor across all Go code. The design doc (`design-docs-temp/03-backfill-workflow.md`) is the source of truth — the code must match it exactly.

**Architecture:** All immutable data lives under `{immutable_base}/index-{indexID:08d}/`. Three subtrees per index: `ledgers/`, `txhash/`, `events/`. All IDs (chunkID, indexID) use `%08d` everywhere — meta store keys, file paths, task IDs, log output. Config collapses `ledgers_base` + `txhash_base` + `events_base` into a single `immutable_base`.

**Tech Stack:** Go, RocksDB (meta store), CGO for builds/tests.

**Current state:** The design doc is done. Three code files are already updated (`meta_store.go`, `paths.go`, `pkg/lfs/chunk.go`). The code does NOT compile — all callers of the LFS path functions are broken because the function signatures changed (added `indexID` parameter). ~15 files need updating.

---

## Before you start

1. Read the design doc: `full-history/design-docs-temp/03-backfill-workflow.md` — especially the Directory Structure, Path Conventions, and Meta Store Keys sections. This is your reference for what the code should produce.
2. Read the three already-updated files to understand the new signatures:
   - `backfill-workflow/meta_store.go` — key format is now `%08d`
   - `backfill-workflow/paths.go` — all path functions take `immutableBase` + `indexID`, produce `index-{indexID:08d}/...` paths
   - `pkg/lfs/chunk.go` — `GetDataPath`, `GetIndexPath`, `GetChunkDir`, `ChunkExists` now take `immutableBase` + `indexID` + `chunkID`
3. Read `.skills/build-and-test.md` for CGO environment setup before any build/test commands.
4. **Do NOT touch `txhash-ingestion-workflow/`** — legacy, off-limits.

---

## Task 1: Config collapse

Collapse the three separate base paths into one.

**Files:** `backfill-workflow/config.go`, `backfill-workflow/config_test.go`, `backfill-workflow/backfill-config.toml`

**What to change:**
- `ImmutableConfig` struct: replace `LedgersBase string` + `TxHashBase string` with `ImmutableBase string` (`toml:"immutable_base"`)
- `Validate()`: default `ImmutableBase` to `{data_dir}/immutable` (not `{data_dir}/immutable/ledgers`)
- Update config tests
- Update `backfill-config.toml` example

**Design doc reference:** Configuration → Optional Sections table shows `immutable_base` defaulting to `{data_dir}/immutable`.

---

## Task 2: Fix all LFS callers

The `pkg/lfs/chunk.go` functions now require `indexID`. Thread it through everywhere.

**Files:** `pkg/lfs/discovery.go`, `pkg/lfs/iterator.go`, `helpers/lfs/chunk.go` (duplicate — consider deleting and using `pkg/lfs` everywhere)

**Key question:** The LFS iterator and discovery modules currently take a `dataDir` and derive chunk paths. They now need `indexID`. Either:
- Add `indexID` to their constructors/parameters
- Or restructure so the caller passes the already-resolved directory path

Read the callers to understand how `indexID` flows. The backfill workflow always knows the `indexID` when creating LFS writers/readers.

---

## Task 3: Fix backfill-workflow callers

**Files:** `lfs_writer.go`, `lfs_source.go`, `chunk_writer.go`, `tasks.go`, `pipeline.go`, `main.go`, `reconciler.go`, `recsplit_flow.go`, `stats.go`

**What to change:**
- Replace all `cfg.ImmutableStores.LedgersBase` / `cfg.ImmutableStores.TxHashBase` with `cfg.ImmutableStores.ImmutableBase`
- Replace all `%04d` / `%06d` format strings for indexID/chunkID with `%08d`
- Thread `indexID` through to any LFS function call that now requires it
- `LFSWriterConfig` may need an `IndexID` field (or the caller passes the resolved path)
- `processChunkTask` and `buildTxHashIndexTask` already have `indexID` — use it

**Design doc reference:**
- Path conventions table shows exactly what each function should produce
- Meta store key examples show the `%08d` format
- process_chunk pseudocode shows `index_id` used in all path construction

---

## Task 4: Fix all tests

**Files:** All `*_test.go` files in `backfill-workflow/` and `pkg/lfs/`

- Update meta store key format expectations from 10-digit to 8-digit
- Update path expectations to use `index-{indexID:08d}/` layout
- Add `indexID` parameter to all LFS function calls in tests
- Update config test expectations for `ImmutableBase`
- Update task ID format expectations (if any tests check task ID strings)

---

## Task 5: Build + test

```bash
cd full-history/all-code
make build
# Run tests with CGO (see .skills/build-and-test.md for exact flags):
CGO_ENABLED=1 CGO_CFLAGS="..." CGO_LDFLAGS="..." go test ./backfill-workflow/... ./pkg/lfs/... -count=1 -v
```

---

## Task 6: Five verification passes

### Pass 1 — No surviving old format strings

```bash
grep -rn '%04d\|%06d\|%010d' backfill-workflow/*.go pkg/lfs/*.go
```

This should return ZERO results. Fix any survivors.

### Pass 2 — Paths match design doc

For each path function in `paths.go` and `pkg/lfs/chunk.go`, verify the output matches the design doc's Path Conventions table. Write a quick test or manual check:

| Function | Expected output for indexID=0, chunkID=42 |
|----------|------------------------------------------|
| `IndexDir(base, 0)` | `{base}/index-00000000` |
| `LedgerPackPath(base, 0, 42)` | `{base}/index-00000000/ledgers/00000042.pack` |
| `GetDataPath(base, 0, 42)` | `{base}/index-00000000/ledgers/00000042.data` |
| `RawTxHashPath(base, 0, 42)` | `{base}/index-00000000/txhash/raw/00000042.bin` |
| `RecSplitIndexPath(base, 0, "a")` | `{base}/index-00000000/txhash/index/cf-a.idx` |
| `EventsDir(base, 0, 42)` | `{base}/index-00000000/events/00000042` |

### Pass 3 — Meta store keys match design doc

Verify key functions produce:
- `ChunkLFSKey(0)` → `chunk:00000000:lfs`
- `ChunkTxHashKey(42)` → `chunk:00000042:txhash`
- `IndexTxHashKey(0)` → `index:00000000:txhash`
- `AllIndexIDs` scan pattern uses `%08d`

### Pass 4 — End-to-end pipeline test

Run `TestPipelineSingleIndex` and `TestPipelineMixedStateResume` — these exercise the full DAG including path construction, meta store keys, and file I/O. Both must pass.

### Pass 5 — Config round-trip

Verify that `LoadConfig` + `Validate` with a TOML containing `immutable_base = "/data/immutable"` produces the correct default, and that omitting it defaults to `{data_dir}/immutable`.

---

## After all passes

Commit:
```bash
git add -A full-history/all-code/
git commit -m "refactor: complete uniform %08d + index-first directory layout across all code"
```

Then sync the design doc to the design branch (follow the standard sync workflow — see memory note on restoring design-docs-temp after branch switch).
