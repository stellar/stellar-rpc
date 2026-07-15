# Stage 4 — db.LedgerReader + db.TransactionReader over the registry

## Mission

- Implement the v1 reader interfaces that power getLedgers, getTransactions, getTransaction, getLatestLedger, getNetwork, getVersionInfo, getHealth — as thin veneers over the registry (decision D2).
- Package: `cmd/stellar-rpc/internal/fullhistory/serve`.

## Read first

- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/00-DECISIONS.md`, `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/CHECKPOINT.md` (stages 2–3 as-built APIs).
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/design-docs/query-routing-design.md` §Query routing (bounds, cursors, chunk traversal, getLedgers/getTransactions/getTransaction).
- Interfaces to satisfy: `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/db/ledger.go` and `transaction.go`.
- Consumers (understand what they actually call before implementing):
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/methods/get_ledgers.go`
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/methods/get_transactions.go`
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/methods/get_transaction.go`
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/methods/get_health.go`, `get_latest_ledger.go`, `get_network.go`, `get_version_info.go`, `util.go` (getProtocolVersion)
- v2 primitives:
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/storage/stores/ledger/` — `ColdReader.GetLedgerRaw/IterateLedgers/LastSeq`, `HotStore` equivalents. Check what bytes GetLedgerRaw returns (compressed vs raw XDR) — `pack_stream.go` and the hot store's Ingest path show the encode/decode convention; reuse the same decode helper the stores use.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/storage/stores/txhash/read_assembly.go` — `TxReader`, `HashIndex`, `LedgerSource`.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/db/transaction.go` `ParseTransaction(lcm, ingestTx)` — the LCM→db.Transaction decoder to reuse.

## Verified interface contracts (what you must satisfy)

```go
// db.LedgerReader — v1 handlers call: GetLedger, GetLedgerRange, NewTx,
// GetLatestLedgerSequence. StreamAllLedgers / StreamLedgerRange /
// GetLedgerCountInRange are v1-ingest/migration helpers: implement as honest
// errors (e.g. "not supported by the full-history backend") after grepping
// that no served endpoint reaches them.
type LedgerReader interface {
    GetLedger(ctx, sequence uint32) (xdr.LedgerCloseMeta, bool, error)
    StreamAllLedgers(ctx, f StreamLedgerFn) error
    GetLedgerRange(ctx) (ledgerbucketwindow.LedgerRange, error)   // {First,Last}Ledger LedgerInfo{Sequence, CloseTime}
    GetLedgerCountInRange(ctx, start, end uint32) (uint32, uint32, uint32, error)
    StreamLedgerRange(ctx, startLedger, endLedger uint32, f StreamLedgerFn) error
    NewTx(ctx) (LedgerReaderTx, error)
    GetLatestLedgerSequence(ctx) (uint32, error)
}
type LedgerReaderTx interface {
    GetLedger(ctx, sequence uint32) (xdr.LedgerCloseMeta, bool, error)
    GetLedgerRange(ctx) (ledgerbucketwindow.LedgerRange, error)
    BatchGetLedgers(ctx, start, end uint32) ([]LedgerMetadataChunk, error)  // {Header xdr.LedgerHeaderHistoryEntry, Lcm []byte(raw XDR)}
    Done() error
}
type TransactionReader interface {
    GetTransaction(ctx, hash xdr.Hash) (Transaction, error)   // db.ErrNoTransaction when absent
}
```

## Design (the admission mapping)

- `NewTx(ctx)` == the spec's admission: one `registry.Admit()` snapshot; every `LedgerReaderTx` method runs against that admitted `{latest, View}`; `Done()` is a no-op. This gives each getLedgers/getTransactions request exactly one immutable View — the spec's per-request consistency, for free, through the existing v1 call pattern.
- Non-Tx methods (`GetLedger`, `GetLedgerRange`, `GetLatestLedgerSequence`) admit per call.
- Bounds (spec §Bounds): ranges clamp to `[floor.FirstLedger(), latest]`; leading edge below floor → the same out-of-range error SHAPE the v1 handlers produce today (read get_ledgers.go / get_transactions.go error paths and match them — clients depend on it); trailing edge truncates at `latest`.
- `GetLedgerRange` close times: fetch the floor ledger's and latest ledger's LCMs through the router (two point reads; the cold one rides the LRU). If profiling shows this hot on getHealth, memoize per-View — note it, don't pre-optimize.

### Ledger fetch path

```go
// getLedgerRaw is the one routing primitive everything here uses:
//   chunk := chunk.IDFromLedger(seq)
//   below floor or above latest -> not found (R2 / never-ahead-of-latest)
//   handle := registry.LedgerReaderFor(view, chunk)   // cold wins, else hot
//   bytes := handle.GetLedgerRaw(seq)                 // decode to raw XDR if stored encoded
// GetLedger unmarshals to xdr.LedgerCloseMeta; BatchGetLedgers walks
// [start,end] chunk by chunk (ascending concatenation, spec §Chunk traversal),
// filling LedgerMetadataChunk{Header, Lcm} — Header comes from the decoded LCM
// (see how v1's BatchGetLedgers derives it; keep byte-for-byte parity).
```

- IMPORTANT: verify the stored-bytes convention once (hot CF and cold pack store zstd-compressed LCM per the streaming doc) and reuse the stores' own decode helper. `BatchGetLedgers` must return RAW XDR bytes in `.Lcm` — the handler base64s them straight into the response.

### db.TransactionReader

```go
// Per request: admit once, then assemble the spec's probe sets from the View:
//   hot  = []txhash.HashIndex{ every View.hot chunk's Txhash() } (newest first)
//   cold = []txhash.HashIndex{ every View.indexes[i].Idx }       (newest first)
//   ledgers = routingLedgerSource{view}   // LedgerSource over getLedgerRaw above
// txr, _ := txhash.NewTxReader(hot, cold, ledgers, passphrase)
// view, found, err := txr.GetTransaction(hash)
```

- Floor gate for by-hash (spec §getTransaction): a cold index may name a below-floor or pruned ledger. The routing LedgerSource returns not-found for below-floor seqs; VERIFY `TxReader.scan`'s soft-error path treats a ledger-fetch miss as "candidate rejected, keep probing" (read `read_assembly.go` + its test). If it hard-fails, wrap the LedgerSource to return the sentinel the scan skips on — do not patch TxReader semantics without noting it in CHECKPOINT.
- Map the result to `db.Transaction`: reuse `db.ParseTransaction(lcm, ingestTx)` if the TxReader's `ingest.LedgerTransactionView` converts cleanly (check what the view exposes — it wraps `ingest.LedgerTransaction`); fill `Ledger ledgerbucketwindow.LedgerInfo{Sequence, CloseTime}` from the fetched LCM. `db.ErrNoTransaction` when not found.
- Passphrase arrives via the serve package's constructor (stage 6 wires the real value; take it as a plain string param now).

## Non-goals

- No events (stage 5). No jsonrpc/gate/config (stage 6). No edits to methods/* unless an error-shape parity fix is unavoidable — record any such edit in CHECKPOINT.

## Acceptance

- `go build ./...`, `go vet ./...`, fullhistory tests green.
- A serve-package test over a fixture catalog+stores (reuse `fhtest` + the e2e harness patterns from `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/e2e_test.go`): ingest a couple of chunks, freeze one, then assert (a) BatchGetLedgers spans a cold→hot chunk boundary correctly, (b) GetTransaction finds a tx in the cold index AND one in the live hot chunk, (c) below-floor seq → not found / out-of-range shape.
- CHECKPOINT.md updated: as-built adapter APIs + the TxReader soft-miss verification result + the stored-bytes (compression) finding.
