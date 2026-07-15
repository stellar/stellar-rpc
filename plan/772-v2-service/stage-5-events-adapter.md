# Stage 5 — db.EventReader over the registry

## Mission

- Implement `db.EventReader` (the getEvents backend) as a veneer over the registry + the per-chunk `eventstore.Query` engine, stitching multi-chunk scans and preserving v1 cursor semantics exactly.
- Package: `cmd/stellar-rpc/internal/fullhistory/serve` (extends stage 4's package).

## Read first

- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/00-DECISIONS.md`, `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/CHECKPOINT.md` (stages 2–4 as-built).
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/design-docs/query-routing-design.md` §getEvents + §Cursors.
- The interface + its consumer:
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/db/event.go` — `EventReader`, `TopicFilters` (OR of AND `TopicCondition{Column, Value}`), `ScanFunction`, and CRITICALLY the concrete SQLite `GetEvents` (cursor arithmetic, ordering, how `eventTypes` filters, how the scan terminates). v1 behavior here is the contract.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/methods/get_events.go` — how the handler builds `protocol.CursorRange`, consumes the ScanFunction (`event xdr.DiagnosticEvent`, uses `event.Event.*` + `InSuccessfulContractCall`), and paginates (`LedgerScanLimit = 10000`).
- v2 engine (all verified present):
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/storage/stores/eventstore/query.go` — `Filter{ContractID, Topics [protocol.MaxTopicCount][]byte}` (AND within; empty field = wildcard), `Query(ctx, r, filters, opts)` (union of filters; empty []Filter = match-all), `QueryOptions{MaxEvents, Descending, Range}`, `EventIDRangeForLedgers(ofs, startLedger, endLedger)`.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/storage/stores/eventstore/reader.go` — `Reader` interface (`Offsets()`, `FetchRange`, …) implemented by HotStore + ColdReader.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/events/payload.go` — `Payload{TxHash, LedgerSequence, TxIdx, OpIdx, LedgerClosedAt, EventIdx, ContractEventBytes}`; EventIdx doc comment states IDs match the v1 SQLite path.

## Design

### Filter mapping (v1 shape → v2 engine)

```go
// v1 hands the adapter: contractIDs [][]byte (OR), topics db.TopicFilters
// (OR of AND-conditions, Column indexed as in the SQLite schema — READ
// db/event.go to confirm whether Column is 0- or 1-based before mapping!),
// eventTypes []int.
//
// v2 wants []eventstore.Filter — a union of conjunctions. Build the cross
// product: for each contractID (or nil if none) × each TopicFilter (or nil):
//   f := eventstore.Filter{ContractID: cid}
//   for _, cond := range topicFilter { f.Topics[cond.Column-base] = cond.Value }
// Empty contractIDs AND empty topics -> pass nil filters (engine match-all).
```

- `eventTypes`: the v2 store does not index event type — post-filter after decoding `ContractEventBytes` → `xdr.ContractEvent` → `.Type`, mirroring exactly how the SQLite query's `event_type IN (...)` behaves (same accepted set).

### Scan + cursor stitching

- Admission once per `GetEvents` call; ledger window = `[cursorRange.Start's ledger, cursorRange.End's ledger]` clamped to `[floor, latest]` with v1's exact edge behavior (derive from db/event.go + get_events.go, not from intuition).
- Chunk traversal ascending (v1 orders `id ASC`; D-record: ascending only). Per chunk:

```go
r := registry.EventReaderFor(view, c)          // hot -> warmed HotStore, cold -> cached ColdReader
ofs, _ := r.Offsets()
// Sub-batch by ledger windows (e.g. 512 ledgers) to bound memory on
// match-all queries: Query materializes []Payload, and a dense 10k-ledger
// chunk can hold ~10M events. For each sub-window:
idr, _ := eventstore.EventIDRangeForLedgers(ofs, subStart, subEnd)
payloads, _ := eventstore.Query(ctx, r, filters, eventstore.QueryOptions{Range: idr})
// feed payloads to the ScanFunction in order; stop the whole scan the
// moment f returns false (v1 contract) or the window is exhausted.
```

- Optimization permitted but optional: filters==match-all → `FetchRange` streaming instead of Query.
- Cursor per event: build `protocol.Cursor` from `Payload{LedgerSequence, TxIdx, OpIdx, EventIdx}` EXACTLY as v1 does — find the one shared construction (`internal/events` package cursor/ID helpers + db/event.go's row→cursor path, including any stage sentinels like `events.StageSentinels`) and reuse it; do not re-implement TOID math.
- Resume semantics: first ledger resumes strictly after `cursorRange.Start` (exclusive), matching the SQLite `id > cursor` predicate; verify against db/event.go and replicate for the (ledger, tx, op, event) tuple ordering within the first chunk's first sub-window.
- ScanFunction event arg: wrap `xdr.ContractEvent` (unmarshaled from `ContractEventBytes`) into `xdr.DiagnosticEvent`. The handler reads `event.Event.*` and `event.InSuccessfulContractCall` — determine the correct `InSuccessfulContractCall` value by checking what v1 stores for the same event (db/event.go InsertEvents / the shared `internal/events` extraction) so responses stay byte-identical. If the v2 payload cannot reproduce it faithfully, STOP and record the discrepancy in CHECKPOINT with a proposal (likely: v2 extraction already filters to the same event set v1 stores, making the flag constant-true — but VERIFY, don't assume).
- `txHash *xdr.Hash` arg: from `Payload.TxHash`.

## Non-goals

- No handler edits (get_events.go stays as-is unless a verified semantic mismatch forces a minimal patch — CHECKPOINT it).
- No descending support, no getEvents-v2-API work.

## Acceptance

- `go build ./...`, `go vet ./...`, fullhistory + eventstore tests green.
- Serve-package test over fixture data with real ContractEvent payloads (fixture LCMs must contain events — extend fhtest fixtures if `ZeroTxLCMBytes` has none; the eventstore package tests show how event-bearing fixtures are built): assert (a) contract-ID filter + topic filter return the same events the per-chunk engine returns, (b) a scan spanning cold→hot chunks concatenates in cursor order, (c) cursor resume mid-ledger is exclusive and duplicate-free, (d) eventTypes post-filter drops non-matching types, (e) f returning false stops the scan early.
- CHECKPOINT.md updated: filter-mapping decisions (Column base!), InSuccessfulContractCall verification result, sub-window size chosen, any handler patch.
