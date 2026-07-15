# Session Handoff — Full-History Query POC (2026-07-15)

Resume document for continuing execution of `2026-07-15-fullhistory-query-poc.md` (same directory) in a fresh Claude Code session on another machine.

## How to resume

1. `git fetch && git checkout poc/fullhistory-query` (branched off `feature/full-history`).
2. Start a Claude Code session in the repo root and say:
   > Resume executing docs/superpowers/plans/2026-07-15-fullhistory-query-poc.md with superpowers:subagent-driven-development, per the HANDOFF file next to it. Workers + per-task reviews on opus, final review on fable. Resume at the first task not marked complete below.
3. Re-create the scratch ledger from the "Progress ledger" section below at `.superpowers/sdd/progress.md` (git-ignored, machine-local). Task briefs are regenerated with the skill's `scripts/task-brief PLAN_FILE N`.

## Progress ledger (authoritative copy at handoff)

```
Task 1: complete (commits 37a75dd1..432e6393, review clean)
  Minor (deferred to final review): hotchunk.go:248 panic string says "read-only chunk", imprecise now; hotchunk.go:147 note that mustExist is inert under ReadOnly.
Task 2: complete (commits 432e6393..6067a677, review clean after TxIdx-coverage fix)
  Minor (deferred to final review): multi-window Lo-sort ordering unasserted (single coverage in test); registry.go 418 lines vs 400 target (comments); golangci-lint not run locally.
Task 3: complete (commits 6067a677..82098f67, review clean)
  Minor (deferred to final review): GetLedgerRange fetches computed first as if stored (plan-mandated); BatchGetLedgers start>end returns (nil,nil) vs v1 error (handler-unreachable); mid-range ErrUnavailable fails whole batch, untested.
  Env note: serve pkg now links Rust libs; build once with RUSTUP_TOOLCHAIN=1.89 make build-libs.
Task 4: complete (commits 82098f67..a5f32e8d, review clean after floor-gate-at-index-seam fix)
  Minor (deferred to final review): fee-bump inner-hash lookup depends on whether index build stores inner keys (cross-task check); nil-hotDB-guard cut not comment-marked in tx_reader.go.
Task 5: IN FLIGHT at handoff (getEvents v2 handler) — worker died with the old machine; NO commits/files landed. Restart Task 5 from its plan section.
Tasks 6-9: not started.
```

## Execution protocol being used

- One opus implementer subagent per task (fresh context; task brief via `scripts/task-brief`), TDD, one commit per task, report file per task.
- One opus task-review subagent per task (brief + implementer report + `scripts/review-package BASE..HEAD` diff file). Critical/Important findings go back to the implementer, then focused re-review.
- Task 9 = whole-branch final review on fable (max effort) over `git merge-base origin/feature/full-history HEAD`..HEAD, plus full-tree `-short` run and repo golangci-lint.
- Commit messages: `fullhistory: <what> (query POC)`. Never any AI/Claude attribution or Co-authored-by lines (user rule).

## Mid-task design decisions already made (binding, not in the plan text)

1. **Task 4 amendment:** the below-floor gate for cold tx-hash candidates lives at the HashIndex seam (`floorGatedIndex` wrapping each `View.TxIdx` reader, returning `stores.ErrNotFound` for candidate seq < max(floor.FirstLedger(), earliest)) — NOT at the LedgerSource — so `txhash.TxReader` sees a clean miss and a below-floor-only hash ends as `db.ErrNoTransaction` (design R2). Landed in a5f32e8d.
2. Task 2 keeps superseded `.idx` readers open deliberately (mmap close would fault racing reads; no reaper in POC) but DOES close discarded hot handles (rocksdb close is lifecycle-guarded). Both `// POC:`-marked in registry.go.

## Environment prerequisites on the new machine

- **RocksDB (macOS):** grocksdb v1.10.7 needs RocksDB 10.9.1; brew's 11.x lacks a required symbol. Build 10.9.1 from source to `~/.rocksdb-1091`, then test with:
  `CGO_CFLAGS="-I$HOME/.rocksdb-1091/include" CGO_LDFLAGS="-L$HOME/.rocksdb-1091/lib -L/opt/homebrew/lib -Wl,-rpath,$HOME/.rocksdb-1091/lib -Wl,-rpath,/opt/homebrew/lib" go test ./cmd/stellar-rpc/internal/fullhistory/... -short -count=1`
  (Linux: distro/scripts/install-rocksdb.sh path works as-is.)
- **Rust libs:** the serve package imports `methods`, which links xdr2json/preflight. Build once: `RUSTUP_TOOLCHAIN=1.89 make build-libs` (default cargo 1.84 fails on an edition2024 dep).
- Gate test success on the command's exit code directly; do not pipe.

## Review-package / task-brief scripts

From the superpowers plugin: `~/.claude/plugins/cache/claude-plugins-official/superpowers/6.1.1/skills/subagent-driven-development/scripts/{task-brief,review-package}` (version dir may differ on the new machine — glob it).
