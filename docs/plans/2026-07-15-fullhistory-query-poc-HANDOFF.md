# Session Handoff — Full-History Query POC (2026-07-15)

> **STATUS: COMPLETE (2026-07-15, second session).** All 9 tasks done; final review verdict: ready for benchmarking. Final HEAD `6e35a7b9`. See "Completion summary" at the bottom. The notes below are retained for the record.

Companion record for `2026-07-15-fullhistory-query-poc.md` (same directory), the task-by-task implementation plan.

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
Task 5: IN FLIGHT at handoff (getEvents v2 handler) — no commits/files landed. Restarted from its plan section.
Tasks 6-9: not started at handoff.
```

## Execution protocol used

- One implementer per task (fresh context), TDD, one commit per task, report per task.
- One task-review per task over the task's `BASE..HEAD` diff. Critical/Important findings go back to the implementer, then a focused re-review.
- Task 9 = whole-branch final review (max effort) over `git merge-base origin/feature/full-history HEAD`..HEAD, plus a full-tree `-short` run and repo golangci-lint.
- Commit messages: `fullhistory: <what> (query POC)`.

## Mid-task design decisions already made (binding, not in the plan text)

1. **Task 4 amendment:** the below-floor gate for cold tx-hash candidates lives at the HashIndex seam (`floorGatedIndex` wrapping each `View.TxIdx` reader, returning `stores.ErrNotFound` for candidate seq < max(floor.FirstLedger(), earliest)) — NOT at the LedgerSource — so `txhash.TxReader` sees a clean miss and a below-floor-only hash ends as `db.ErrNoTransaction` (design R2). Landed in a5f32e8d.
2. Task 2 keeps superseded `.idx` readers open deliberately (mmap close would fault racing reads; no reaper in POC) but DOES close discarded hot handles (rocksdb close is lifecycle-guarded). Both `// POC:`-marked in registry.go.

## Environment prerequisites

- **RocksDB (macOS):** grocksdb v1.10.7 needs RocksDB 10.9.1; brew's 11.x lacks a required symbol. Build 10.9.1 from source to `~/.rocksdb-1091`, then test with:
  `CGO_CFLAGS="-I$HOME/.rocksdb-1091/include" CGO_LDFLAGS="-L$HOME/.rocksdb-1091/lib -L/opt/homebrew/lib -Wl,-rpath,$HOME/.rocksdb-1091/lib -Wl,-rpath,/opt/homebrew/lib" go test ./cmd/stellar-rpc/internal/fullhistory/... -short -count=1`
  (Linux: distro/scripts/install-rocksdb.sh path works as-is.)
- **Rust libs:** the serve package imports `methods`, which links xdr2json/preflight. Build once: `RUSTUP_TOOLCHAIN=1.89 make build-libs` (default cargo 1.84 fails on an edition2024 dep).
- Gate test success on the command's exit code directly; do not pipe.

## Completion summary (second session, Linux box, 2026-07-15)

Tasks 5-9 executed per the protocol above (per-task implementation + review, max-effort final review).

- Task 5: getEvents handler over eventstore.Query — 68cb0533 + I1 fix 91d05191 (chunk exhaustion gated on window coverage; collision-simulating regression test). Review clean.
- Task 6: [serve] JSON-RPC server + per-endpoint latency histogram + /metrics /health /ready — 4e502601. Review clean.
- Task 7: daemon wiring + TestServeE2E_QueryHotAndCold — fc63b4ee. Review clean; plan's BuildInitial/HotOpened prose order was backwards, implementation uses the correct order (review-confirmed).
- Task 8: tools/fhbench harness + README — 4350e2e6 + structured discovery probe 7ae10abf. Review clean.
- Lint gate: 846e389c (golangci-lint 0 issues on --new-from-rev origin/feature/full-history).
- Task 9 final review (max effort): no Critical; 3 CONFIRMED Important fixed in 6e35a7b9 (restart-safe metrics registration; TickCompleted live-handle race closed under r.mu; fee-bump inner-hash v1-parity gap POC-marked). 33 deferred minors adjudicated (21 accepted-POC, 7 rejected).

Env notes (Linux): RocksDB 10.9.1 at ~/.rocksdb, zstd 1.5.7 at ~/.zstd (system 1.5.5 too old); CGO_CFLAGS="-I$HOME/.rocksdb/include -I$HOME/.zstd/include" CGO_LDFLAGS="-L$HOME/.rocksdb/lib -L$HOME/.zstd/lib -Wl,-rpath,$HOME/.rocksdb/lib -Wl,-rpath,$HOME/.zstd/lib".

Known accepted gaps for benchmark interpretation: fee-bump inner-hash lookups NOT_FOUND (v1 finds them); cold readers open per-ledger (dominant cold-tier cost); getEvents re-query latency cliff for index-unselective/type-selective filters; error blips at chunk boundaries (fence ErrStoreClosed window); BuildInitial on supervised restart leaks prior View read handles until first tick.
