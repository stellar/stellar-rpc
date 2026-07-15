# Stage 3 — Wire the registry into the write side

## Mission

- Thread the stage-2 registry through ingestion, lifecycle, and startup so every serving-map transition publishes a View update at the spec's ordering points, hot DB ownership moves to the registry, and physical destruction defers to the reaper.
- After this stage the daemon maintains a correct live registry end-to-end; nothing consumes it yet (stages 4–6).

## Read first

- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/00-DECISIONS.md`, `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/CHECKPOINT.md` (stage 2's as-built registry API).
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/design-docs/query-routing-design.md` §View update points (the ordering table — this stage implements that table) and §Changes to the streaming workflow.
- Files you will edit:
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/hotloop.go`
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/startup.go`
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/daemon.go`
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/backfill/process.go` + `txindex.go`
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/lifecycle/lifecycle.go` + `eligibility.go`
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/catalog/catalog_sweep.go` (split demote vs destroy if needed)

## The hook table (implement exactly this ordering)

| Transition (call site, verified) | Hook | Ordering rule |
|---|---|---|
| `openHotDBForChunk` returns (hotloop.go) — key already `"ready"` | `registry.PublishHot(c, db)` | After the key flip, before the chunk's first ledger commits. |
| Per-ledger ingest: after `hotService.Ingest(ctx, seq, view)` returns nil (hotloop.go) | `registry.AdvanceLatest(seq)` | LAST step — after commit AND in-memory event applies (Ingest covers both). Keep `metrics.LastCommitted` + `Health.observe` as-is. |
| Boundary handoff (hotloop.go, `seq == closed.LastLedger()`) | do NOT `hotDB.Close()` — ownership already with registry via PublishHot; just stop writing, open next, publish boundary | The HANDOFF FENCE comment changes meaning: the fence is now "writer stopped + next key created", not "handle closed". Update the comment honestly. |
| `processChunk` freeze commit (`cat.FlipChunkFrozen`, backfill/process.go) | `registry.PublishFrozen(c, kinds...)` | After the commit. During startup backfill the registry doesn't exist yet — hook must be optional (nil-safe callback threaded via `ProcessConfig`/`ExecConfig`); the startup scan (BuildFromCatalog) covers those chunks. |
| `buildThenSweep` index commit (`cat.CommitTxHashIndex`, backfill/txindex.go) | `registry.SwapTxIndex(cov)` | After the atomic commit. The predecessor's FILE deletion (currently `buildThenSweep`'s eager sweep) moves behind the reaper: demote stays immediate (the commit batch already did it), `SweepTxHashIndexKey` (unlink + key delete) runs after grace T. |
| Discard scan op (`catalog.DiscardHotChunk`, lifecycle/eligibility.go) | `registry.UnpublishHot(c, destroy)` where destroy = the transient-mark + rmdir + key-delete body | Unpublish BEFORE catalog demotion and every destructive step. Reaper closes the handle, THEN runs destroy, after T. |
| Prune stage (lifecycle) | `registry.AdvanceFloor(floor)` FIRST, then demotions immediate, unlink + key-delete via reaper | Gate, unpublish, demote, then destroy (spec table row 6). |
| Startup `run()` after backfill, before serving (startup.go) | build registry: `BuildFromCatalog(cat, retention, lastCommitted, opts)` with the live chunk's pre-opened handle passed in; then hand it to ServeReads | Complete before the lifecycle goroutine starts, so no freeze lands between scan and live hooks. |

## Key changes in detail

### Hot DB ownership (the delicate one)

- Today: `runIngestionLoop` owns the live handle (defer-closes it; boundary closes the filled chunk's DB before opening the next). Lifecycle freeze opens its OWN read-only handle via `backfillSource`'s hot branch (read-only opens take no RocksDB LOCK — see the LIVE-CHUNK EXCLUSION comment in hotloop.go).
- After this stage:
  - `openHotDBForChunk` (both call sites: startup resume open, boundary next-open) registers the handle with the registry immediately; the registry is the closer.
  - Boundary: writer flushes/stops writing chunk C, opens C+1, publishes boundary. C's handle STAYS OPEN in the registry serving reads until discard retires it.
  - Loop exit (error/shutdown): the loop closes NOTHING; `run()`'s teardown calls `registry.Close()` which closes every hot handle + open readers immediately (no grace — process is exiting; the supervised restart reopens cleanly). This preserves the current lock-release-on-restart property — verify by running the existing restart tests.
  - `backfillSource`'s freeze-time read-only open can stay AS-IS (read-only opens don't conflict with the registry's write handle). Optional improvement (only if trivially clean): let freeze read through the registry handle — the spec's "HotProbe seam"; note in CHECKPOINT if you take it, but do not force it.
- `hotchunk.DB.Close()` idempotency: verify before relying on double-close anywhere; prefer single-owner (registry) so the question never arises.

### Deferred destruction (lifecycle ops)

- Split each destructive op into (demote now) + (destroy later):
  - Discard: `PutHotTransient(c)` stays in the lifecycle run (catalog demotion is immediate per spec); dir removal + key delete become the reaper-deferred destroy. Crash between = `"transient"` debris, already handled by existing scans.
  - Prune sweeps: `SweepChunkArtifacts`/`SweepTxHashIndexKey` currently demote+unlink+delete in one body. Restructure so demote commits in the run, unlink+key-delete run after T. Crash between = `"pruning"` keys, already re-swept by the next run — the catalog crash rules make the split safe; keep the fsync ordering inside the destroy step (unlink → fsyncDir → key delete).
- Startup: no new recovery code needed — demoted stores never enter `BuildFromCatalog`'s View, and the first lifecycle run's scans finish pending deletions immediately (spec §The reaper, "no persistent state").

### Plumbing

- `StartConfig` (startup.go) gains the registry (or a `ServeReads func(ctx, *registry.Registry) error` signature change — pick one, update the no-op default in daemon.go and the e2e recorder in `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/e2e_test.go`).
- `lifecycle.Config` + `backfill.ExecConfig`/`ProcessConfig`/`BuildConfig` gain nil-safe hook fields (small interfaces or func fields) so lifecycle-run calls publish and startup-backfill calls no-op.
- `runDaemonWith` constructs nothing new here beyond passing hooks; the registry is built inside `run()` (it must die and rebuild with each supervised restart).

## Sharp edges

- Admission-order counterpart on the write side: `PublishHot` for chunk C+1 must complete before C+1's first `AdvanceLatest` — the loop's sequencing gives this for free; do not reorder.
- `AdvanceLatest` only on the success path of `Ingest`.
- Never publish from startup-backfill's `processChunk` (registry doesn't exist yet); the nil-hook covers it.
- `registry.Close()` must be idempotent and safe with a concurrently-running reaper.
- Update the hotloop ownership comments (HANDOFF FENCE, defer-close block) — they are load-bearing documentation and will otherwise lie.

## Non-goals

- No readers/adapters, no HTTP, no gate. `ServeReads` stays a no-op default (signature may change).

## Acceptance

- `go build ./...`, `go vet ./...`; ALL existing fullhistory tests green — especially `e2e_test.go`, `hotloop_test.go`, `startup_test.go`, `daemon_test.go`, lifecycle tests. These pin the crash/restart behavior you are touching; if one fails, the code is wrong until proven otherwise.
- Extend the e2e test minimally: after the run, assert the registry View matches the catalog (hot handles for ready chunks, cold flags for frozen, index coverage present, floor correct, latest == last committed).
- CHECKPOINT.md updated: final hook API, ServeReads signature decision, whether freeze reads through the registry handle, any deviation from the hook table.
