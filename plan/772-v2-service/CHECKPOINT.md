# CHECKPOINT — #772 v2 Service Implementation

- Living handoff document. Read FIRST in every session; append your entry LAST (template at bottom).
- Protocol: `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/01-STAGES.md`. Decisions: `00-DECISIONS.md` (binding).

---

## Stage 0 — Stock-taking, decisions, plan (2026-07-15) — COMPLETE

- Session: planning session with Karthik (grill-me interview). No code written.
- What exists (verified against code, not just design docs):
  - v2 read primitives are ALL built and tested, unwired: `ledger.ColdReader`/`HotStore`, `eventstore.Reader` + `Query` engine (empty filters = match-all), `txhash.TxReader` (probe hot exact → cold fingerprinted → fetch-and-verify), `hotchunk.DB` facades, catalog scans, `HealthSignal` in `cmd/stellar-rpc/internal/fullhistory/health.go` (doc comment literally names it the #772 read-server seam).
  - `ServeReads` is a no-op `func(ctx) error` field on `StartConfig` (`cmd/stellar-rpc/internal/fullhistory/startup.go:131` call site).
  - v1's handler seam: `internal.HandlerParams` (`cmd/stellar-rpc/internal/jsonrpc.go:62`) — the four target methods consume only `db.LedgerReader`(+Tx), `db.TransactionReader`, `db.EventReader`.
  - Gotchas already known: `hotchunk.OpenReadOnly` is a LEDGERS-ONLY view (`Events()` panics) → registry must own warmed write handles; NETWORK_PASSPHRASE parsed only inside `newCaptiveCoreOpener` (daemon.go) → must be surfaced to serving; per-ledger ingest metrics exist only as PHASE histograms (no read-from-source, no e2e series, no max-ever); v1 `request_duration_seconds` summary lacks p75/max/avg surface.
- Verified-in-code signatures are quoted in each stage doc; explorer-agent reports behind them are session-local (not persisted) — the stage docs are the surviving source.
- Warnings for stage 1: none. Start with `stage-1-ingestion-metrics.md`.

---

<!-- APPEND NEW ENTRIES BELOW. Template:

## Stage N — <title> (<date>) — COMPLETE | PARTIAL(resume: <exact next action>)

- Files added/changed: absolute paths.
- As-built exported API: exact Go signatures for everything later stages consume.
- Deviations from stage doc + why.
- Verification: build/vet/test commands run and their results; manual smokes.
- Warnings / notes for the next stage.
-->
