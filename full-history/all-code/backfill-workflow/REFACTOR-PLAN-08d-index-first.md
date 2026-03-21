# Session 1: Directory Layout + Packfile + Config Refactor

> **For agentic workers:** REQUIRED: Use superpowers:executing-plans to implement this plan. Use TDD where applicable. After each task, run tests. After all tasks, run 5 verification passes. Code must be heavily commented and DRY.

**Goal:** Complete the index-first directory layout, uniform `%08d`, packfile integration for LFS, and config collapse. The design doc (`design-docs-temp/03-backfill-workflow.md`) is the source of truth.

**Architecture:** All immutable data under `{immutable_base}/index-{indexID:08d}/`. LFS uses single `.pack` file per chunk (packfile library from `github.com/tamirms/event-analysis/packfile`). Config collapses to single `immutable_base`. All IDs `%08d` everywhere.

**Tech Stack:** Go, RocksDB (CGO), packfile library.

**Current state:** Three files already updated (`meta_store.go` → `%08d`, `paths.go` → index-first, `pkg/lfs/chunk.go` → `indexID` param). Code does NOT compile — ~15 files have broken call sites.

**Additional working directories to read from (read-only, do not modify):**
- `/Users/karthik/ws/new-world/event-analysis` — packfile library reference implementation (see `packfile/writer.go`, `packfile/reader.go`, `eventstore/writer.go` for usage patterns)

---

## Before you start

1. Read the design doc: `full-history/design-docs-temp/03-backfill-workflow.md` — Directory Structure, Path Conventions, Meta Store Keys sections are your reference
2. Read already-updated files: `meta_store.go`, `paths.go`, `pkg/lfs/chunk.go`
3. Read `.skills/build-and-test.md` for CGO setup
4. Read `.skills/code-patterns.md` for Go conventions
5. Read the packfile library: `/Users/karthik/ws/new-world/event-analysis/packfile/writer.go` and `reader.go` for the API
6. Read `/Users/karthik/ws/new-world/event-analysis/eventstore/writer.go` for an example of how to wrap packfile for a specific use case
7. **Do NOT touch `txhash-ingestion-workflow/`** — legacy, off-limits

---

## Task 1: Config collapse

**Files:** `config.go`, `config_test.go`, `backfill-config.toml`

Replace `ImmutableConfig.LedgersBase` + `ImmutableConfig.TxHashBase` with single `ImmutableConfig.ImmutableBase` (`toml:"immutable_base"`). Default: `{data_dir}/immutable`. Update `Validate()`, tests, and TOML example.

**Design doc reference:** Configuration → Optional Sections table.

---

## Task 2: Packfile integration for LFS

**Files:** `pkg/lfs/chunk.go`, `backfill-workflow/lfs_writer.go`, `backfill-workflow/lfs_writer_test.go`

The current LFS writer produces `.data` + `.index` file pairs using a custom format. Replace with the packfile library:

**2a. Update `pkg/lfs/chunk.go` path functions:**
- Replace `GetDataPath` + `GetIndexPath` with single `GetPackPath(immutableBase, indexID, chunkID)` → `index-{:08d}/ledgers/{:08d}.pack`
- Update `ChunkExists` to check for single `.pack` file
- Remove `GetChunkDir` (no longer needed — path goes through `IndexDir`)

**2b. Rewrite `lfs_writer.go` to use packfile:**
- Import `github.com/tamirms/event-analysis/packfile` (add to `go.mod`)
- The LFS writer wraps packfile with `RecordSize: 1` (each item = one compressed LCM) and `Format: packfile.Raw` (application-level zstd compression, not packfile-level)
- Writer API: `NewLFSWriter(cfg) → AppendLedger(lcm) → FsyncAndClose()`. Internally: zstd-compress the LCM, then `pw.Append(compressed)`, then `pw.Finish(nil)` at close
- The writer must track an in-memory map of `ledgerSeq → compressedSize` for the packfile's offset index (packfile handles this internally via its index — verify by reading the packfile reader API)
- Actually, read the packfile writer carefully — it tracks offsets automatically. The writer just calls `Append(data)` for each item. No manual offset tracking needed.

**2c. Update LFS reader/iterator (`pkg/lfs/iterator.go`, `pkg/lfs/discovery.go`):**
- These read LFS chunk data. Update to use `packfile.Open` + `ReadItem` / `ReadRange` instead of the custom `.data` + `.index` reader
- Thread `indexID` through where needed

**2d. Update `lfs_source.go`:**
- `NewLFSPackfileSource` uses `ChunkExists` and the LFS iterator — update for new signatures

**2e. Tests:**
- Write failing tests first (TDD)
- Test: write 10 ledgers via new LFS writer → read back via packfile reader → verify content matches
- Test: `ChunkExists` returns true after write, false before

---

## Task 3: Fix all remaining callers

**Files:** `chunk_writer.go`, `tasks.go`, `pipeline.go`, `main.go`, `reconciler.go`, `recsplit_flow.go`, `stats.go`, `helpers/lfs/chunk.go`

- Replace `cfg.ImmutableStores.LedgersBase` / `cfg.ImmutableStores.TxHashBase` everywhere with `cfg.ImmutableStores.ImmutableBase`
- Replace all `%04d` / `%06d` format strings with `%08d`
- Thread `indexID` to all LFS/path function calls
- `LFSWriterConfig`: replace `DataDir` with `ImmutableBase`, add `IndexID`
- `helpers/lfs/chunk.go`: either delete (if duplicate of `pkg/lfs`) or update to match
- `chunk_writer.go`: update `deletePartialFiles()` for `.pack` instead of `.data` + `.index`

---

## Task 4: Fix all tests

**Files:** All `*_test.go` in `backfill-workflow/` and `pkg/lfs/`

- Meta store key expectations: 10-digit → 8-digit
- Path expectations: index-first layout, `.pack` not `.data`/`.index`
- Config expectations: `ImmutableBase` not `LedgersBase`/`TxHashBase`
- Task ID format: `%08d`
- Add `indexID` to all LFS function calls

---

## Task 5: Build + test

```bash
cd full-history/all-code
make build
CGO_ENABLED=1 ... go test ./backfill-workflow/... ./pkg/lfs/... -count=1 -v
```

---

## Task 6: Five verification passes

1. **Build + all tests pass** — zero failures
2. **No old format strings** — `grep -rn '%04d\|%06d\|%010d' backfill-workflow/*.go pkg/lfs/*.go` → zero results
3. **Paths match design doc** — verify every path function against the Path Conventions table
4. **Meta store keys match design doc** — verify `ChunkLFSKey(0)` → `chunk:00000000:lfs`, etc.
5. **End-to-end pipeline tests pass** — `TestPipelineSingleIndex`, `TestPipelineMixedStateResume`

After all passes, commit. Then sync design doc to design branch.

---
---

# Session 2: StreamHash Integration

> **For agentic workers:** REQUIRED: Use superpowers:executing-plans. Use TDD. After all tasks, run 5 verification passes. Code must be heavily commented and DRY.

**Goal:** Add StreamHash as an alternative txhash index builder, selectable via `--use-streamhash` CLI flag. When enabled, build a single `txhash.idx` file using streamhash instead of 16 `cf-{nibble}.idx` files using RecSplit. Run benchmarks to compare build times.

**Architecture:** The `build_txhash_index` task checks the CLI flag and dispatches to either `RecSplitFlow` (existing, 16 CF files) or `StreamHashFlow` (new, single file). Both implement the same interface. Verification runs regardless of which builder is used.

**Tech Stack:** Go, `github.com/tamirms/streamhash` library, RocksDB (CGO).

**Prerequisites:** Session 1 must be complete (code compiles, tests pass, index-first layout).

**Additional working directories to read from (read-only, do not modify):**
- `/Users/karthik/ws/new-world/streamhash` — StreamHash library source code. Read `doc.go` for API overview, `builder.go` + `builder_options.go` for builder API, `index.go` for query API, `CLAUDE.md` for quality rules
- `/Users/karthik/ws/new-world/event-analysis` — reference for how packfile/streamhash are used together

---

## Before you start

1. Read the StreamHash API:
   - `/Users/karthik/ws/new-world/streamhash/doc.go` — package overview + usage examples
   - `/Users/karthik/ws/new-world/streamhash/builder.go` — `NewBuilder`, `AddKey`, `Finish`
   - `/Users/karthik/ws/new-world/streamhash/builder_options.go` — `WithPayload`, `WithFingerprint`, `WithWorkers`, `WithUnsortedInput`
   - `/Users/karthik/ws/new-world/streamhash/index.go` — `Open`, `QueryPayload`
   - `/Users/karthik/ws/new-world/streamhash/prehash.go` — `PreHash` (required for non-random input)
2. Read the spec: fetch `gh api repos/tamirms/streamhash/contents/streamhash-spec.md` — especially §1.7 (performance), §1.8 (algorithm choice), fingerprint section
3. Read the existing `recsplit_flow.go` to understand the current 4-phase pipeline
4. Read the design doc `design-docs-temp/03-backfill-workflow.md` — Task Details → build_txhash_index section

**Key API facts:**
- Builder: `streamhash.NewBuilder(ctx, outputPath, totalKeys, opts...)` → `AddKey(key, payload)` → `Finish()`
- Keys must be pre-hashed via `streamhash.PreHash(txhash[:])` (returns 16 bytes)
- Payload: 4 bytes for `ledgerSeq` (uint32 big-endian)
- Fingerprint: 2 bytes → false positive rate of 1 in 65,536 (`2^(-16)`)
- Query: `idx.QueryPayload(prehashedKey)` returns `(uint64, error)` where the uint64 is the payload (ledgerSeq). Returns `ErrFingerprintMismatch` for non-member keys.
- Workers: 8 is a good default (build throughput scales ~4x from 1→4 workers, diminishing after that)
- Use `WithUnsortedInput()` since the raw `.bin` files are not sorted by pre-hashed key order

---

## Task 1: Add CLI flag

**Files:** `main.go`, `config.go`

- Add `--use-streamhash` boolean flag to `Main()`
- Pass it through to `PipelineConfig` (add a `UseStreamHash bool` field)
- No TOML support — CLI only

---

## Task 2: Implement StreamHashFlow

**Files:** Create `streamhash_flow.go`, `streamhash_flow_test.go`

Implement `StreamHashFlow` as an alternative to `RecSplitFlow`. Same lifecycle:

**Phases:**

1. **COUNT** — scan all `.bin` files in the index's `txhash/raw/` directory, count total entries. Single pass, can parallelize file reading.

2. **BUILD** — create a single streamhash index:
   ```go
   builder, _ := streamhash.NewBuilder(ctx, outputPath, totalKeys,
       streamhash.WithPayload(4),          // 4 bytes for ledgerSeq
       streamhash.WithFingerprint(2),      // 2 bytes → 1/65536 FP rate
       streamhash.WithWorkers(8),          // parallel block solving
       streamhash.WithUnsortedInput(       // .bin files not sorted by pre-hash
           streamhash.TempDir(tmpDir),
       ),
   )

   // Read all .bin files, add each entry
   for each .bin file in index:
       for each 36-byte entry:
           txhash = entry[0:32]
           ledgerSeq = bigEndian(entry[32:36])
           preHashed = streamhash.PreHash(txhash)
           builder.AddKey(preHashed, ledgerSeqBytes)

   builder.Finish()
   ```

3. **VERIFY** (optional, controlled by `verify_recsplit` config) — re-read all `.bin` files, query each key:
   ```go
   idx, _ := streamhash.Open(outputPath)
   for each entry:
       preHashed := streamhash.PreHash(txhash)
       payload, err := idx.QueryPayload(preHashed)
       // err == nil means key found (fingerprint matched)
       // payload should equal ledgerSeq
   ```

**Output:** Single file `{immutable_base}/index-{indexID:08d}/txhash/index/txhash.idx`

---

## Task 3: Wire into build_txhash_index task

**Files:** `tasks.go`

In `buildTxHashIndexTask.Execute()`:
- If `UseStreamHash` → call `StreamHashFlow.Run(ctx)`
- Else → call `RecSplitFlow.Run(ctx)` (existing behavior)
- After either completes: set `index:{N}:txhash = "1"`, clean up raw files + txhash meta keys (same as now)

---

## Task 4: Tests

**Files:** `streamhash_flow_test.go`

TDD:
- Write test: create 100 entries in `.bin` files → build streamhash index → verify all lookups succeed
- Write test: query a key NOT in the index → verify `ErrFingerprintMismatch` (or returned payload doesn't match)
- Write test: build with `verify_recsplit = true` → verify phase runs and passes
- Write test: build with `verify_recsplit = false` → verify phase skipped

---

## Task 5: Benchmark comparison

Write a benchmark or standalone test that:
1. Creates a realistic set of `.bin` files (e.g., 100K–1M entries)
2. Builds with RecSplit (16 CF files) — measure wall time
3. Builds with StreamHash (1 file) — measure wall time
4. Compares: build time, index size on disk, verification time
5. Log results clearly so the user can compare

---

## Task 6: Five verification passes

1. **Build + all tests pass** — including streamhash tests
2. **RecSplit path unchanged** — `TestPipelineSingleIndex` still passes without `--use-streamhash`
3. **StreamHash path works** — run pipeline test with `UseStreamHash: true`
4. **Verification works for both paths** — both RecSplit and StreamHash verification phases produce correct results
5. **Benchmark runs** — produces comparison numbers

After all passes, commit. Update design doc if needed (add a note about streamhash as an alternative). Sync to design branch.
