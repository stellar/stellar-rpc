// Package views holds the thin RPC-side adapters over the zero-copy XDR-view
// extractors that live in the go-stellar-sdk ingest package
// (ingest.ExtractTxHashes, ingest.ExtractLedgerEvents,
// ingest.LedgerTransactionViewByHash / LedgerTransactionViewRange); this
// package adds only the product-specific shapes and policy the SDK should not
// carry:
//
//   - ExtractEvents — ingestion: one events.Payload per emitted contract event
//     (V1/V2 LCM only), composing ingest.ExtractLedgerEvents (one TxProcessing
//     walk yields hash + events together) with the RPC events-index Payload
//     shape and the Stage→(TxIdx, OpIdx) cursor sentinels. The per-event index
//     is positional (reconstructed at read time) and intentionally NOT stored.
//   - ExtractTxHashes — ingestion: every tx hash in apply order (V0/V1/V2 LCM),
//     wrapped into the RPC txhash.Entry shape.
//   - ExtractTxDetailsByHash / ExtractTransactions — read path
//     (getTransaction / paginated getTransactions): thin wrappers over the SDK
//     ingest view read-path, returning ingest.LedgerTransactionView (aliased
//     here as views.Transaction).
//
// Buffer-lifetime contract: ExtractEvents's per-operation ContractEventBytes
// and the raw Envelope/Result/Meta/event fields of Transaction alias the view
// buffer; callers copy what they retain. ExtractTxHashes copies its hashes.
//
// Trusted-input invariant (TransactionMeta V3): stellar-core only attaches
// SorobanMeta to Soroban transactions, so "SorobanMeta present ⟺ soroban tx"
// holds for any LCM core emits. ExtractEvents relies on this invariant directly
// (it emits V3 SorobanMeta.Events whenever present, via the SDK extractor). The
// read path additionally gates V3 contract events on the paired envelope's
// soroban-ness inside the SDK, matching the parsed reader's IsSorobanTx check.
package views
